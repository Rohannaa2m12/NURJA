#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NURJA — an offline "AI trading bot" workbench (single-file).

What it is
- A local, no-keys-required trading-bot lab: backtesting, paper trading, signal generation,
  risk management, portfolio accounting, and event-driven simulation.
- Includes deterministic pseudo-random market generators and a strategy DSL-ish layer.
- Stores runs in SQLite (local file), exports CSV/JSON, and produces plain-text reports.

What it isn't
- Financial advice or a live exchange client.
- A production HFT system.

Run
  python NURJA.py --help
  python NURJA.py quickstart
  python NURJA.py backtest --symbol NURJ/USD --days 90 --strategy loom_momentum
  python NURJA.py paper --symbol NURJ/USD --minutes 30 --strategy loom_meanrevert

Data
  Creates ./nurja_data.sqlite3
  Creates ./nurja_exports/ (on export)
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import dataclasses
import datetime as dt
import decimal
import functools
import hashlib
import hmac
import io
import json
import math
import os
import random
import secrets
import sqlite3
import statistics
import sys
import textwrap
import time
import typing as t
import uuid


# ---------------------------
# Global constants / flavor
# ---------------------------

APP_NAME = "NURJA"
APP_VERSION = "1.0.0"
DB_FILENAME = "nurja_data.sqlite3"
EXPORT_DIR = "nurja_exports"

DEC = decimal.Decimal
decimal.getcontext().prec = 40

NAN = float("nan")


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _ts() -> int:
    return int(time.time())


def _clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _safe_div(a: float, b: float, default: float = 0.0) -> float:
    if b == 0:
        return default
    return a / b


def _fmt_money(x: float, ccy: str = "USD") -> str:
    if math.isnan(x):
        return "NaN"
    sign = "-" if x < 0 else ""
    x = abs(x)
    if x >= 1_000_000:
        return f"{sign}{ccy} {x/1_000_000:.3f}m"
    if x >= 1_000:
        return f"{sign}{ccy} {x/1_000:.3f}k"
    return f"{sign}{ccy} {x:.2f}"


def _fmt_pct(x: float) -> str:
    if math.isnan(x):
        return "NaN"
    return f"{x*100:.2f}%"


def _sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _blake2b_hex(b: bytes, n: int = 32) -> str:
    h = hashlib.blake2b(b, digest_size=n)
    return h.hexdigest()


def _rand_id(prefix: str) -> str:
    # deterministic length, unpredictable content
    raw = secrets.token_bytes(24) + prefix.encode("utf-8") + secrets.token_bytes(7)
    return f"{prefix}_{_blake2b_hex(raw, 16)}"


def _human_time(seconds: float) -> str:
    seconds = float(seconds)
    if seconds < 1e-3:
        return f"{seconds*1e6:.1f}µs"
    if seconds < 1:
        return f"{seconds*1e3:.1f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    if seconds < 3600:
        return f"{seconds/60:.2f}m"
    return f"{seconds/3600:.2f}h"


class NurjaError(Exception):
    pass


class ConfigError(NurjaError):
    pass


class DataError(NurjaError):
    pass


class StrategyError(NurjaError):
    pass


class RiskError(NurjaError):
    pass


class ExchangeError(NurjaError):
    pass


# ---------------------------
# Logging
# ---------------------------


@dataclasses.dataclass
class LogEvent:
    at: float
    level: str
    scope: str
    msg: str
    data: dict[str, t.Any] = dataclasses.field(default_factory=dict)


class Logger:
    def __init__(self, verbose: bool = False) -> None:
        self.verbose = verbose
        self._events: list[LogEvent] = []

    def emit(self, level: str, scope: str, msg: str, **data: t.Any) -> None:
        ev = LogEvent(at=time.time(), level=level.upper(), scope=scope, msg=msg, data=dict(data))
        self._events.append(ev)
        if self.verbose or level.upper() in {"WARN", "ERROR"}:
            stamp = dt.datetime.fromtimestamp(ev.at).strftime("%H:%M:%S")
            payload = ""
            if data:
                payload = " " + json.dumps(data, sort_keys=True, default=str)
            print(f"[{stamp}] {ev.level:<5} {ev.scope}: {ev.msg}{payload}")

    def info(self, scope: str, msg: str, **data: t.Any) -> None:
        self.emit("INFO", scope, msg, **data)

    def warn(self, scope: str, msg: str, **data: t.Any) -> None:
        self.emit("WARN", scope, msg, **data)

    def error(self, scope: str, msg: str, **data: t.Any) -> None:
        self.emit("ERROR", scope, msg, **data)

    def dump_json(self) -> str:
        return json.dumps([dataclasses.asdict(e) for e in self._events], indent=2, sort_keys=True, default=str)


# ---------------------------
# SQLite persistence
# ---------------------------


class NurjaDB:
    def __init__(self, path: str, log: Logger) -> None:
        self.path = path
        self.log = log
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._migrate()
        self.log.info("db", "connected", path=self.path)

    def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None
        self.log.info("db", "closed")

    @contextlib.contextmanager
    def tx(self) -> t.Iterator[sqlite3.Connection]:
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        try:
            yield self._conn
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    def _migrate(self) -> None:
        assert self._conn is not None
        with self.tx() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    created_at INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    seed INTEGER NOT NULL,
                    config_json TEXT NOT NULL,
                    summary_json TEXT NOT NULL
                );
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS candles (
                    run_id TEXT NOT NULL,
                    i INTEGER NOT NULL,
                    t INTEGER NOT NULL,
                    o REAL NOT NULL,
                    h REAL NOT NULL,
                    l REAL NOT NULL,
                    c REAL NOT NULL,
                    v REAL NOT NULL,
                    PRIMARY KEY (run_id, i),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    run_id TEXT NOT NULL,
                    i INTEGER NOT NULL,
                    t INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    fee REAL NOT NULL,
                    note TEXT NOT NULL,
                    PRIMARY KEY (run_id, i),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    run_id TEXT NOT NULL,
                    k TEXT NOT NULL,
                    v REAL NOT NULL,
                    PRIMARY KEY (run_id, k),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );
                """
            )

    def save_run(
        self,
        run_id: str,
        kind: str,
        symbol: str,
        strategy: str,
        seed: int,
        config: dict[str, t.Any],
        summary: dict[str, t.Any],
        candles: list["Candle"],
        trades: list["Trade"],
        metrics: dict[str, float],
    ) -> None:
        with self.tx() as c:
            c.execute(
                """
                INSERT INTO runs(id, created_at, kind, symbol, strategy, seed, config_json, summary_json)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    run_id,
                    int(time.time()),
                    kind,
                    symbol,
                    strategy,
                    int(seed),
                    json.dumps(config, sort_keys=True, default=str),
                    json.dumps(summary, sort_keys=True, default=str),
                ),
            )
            for i, cd in enumerate(candles):
                c.execute(
                    "INSERT INTO candles(run_id, i, t, o, h, l, c, v) VALUES(?, ?, ?, ?, ?, ?, ?, ?);",
                    (run_id, i, cd.t, cd.o, cd.h, cd.l, cd.c, cd.v),
                )
            for i, tr in enumerate(trades):
                c.execute(
                    "INSERT INTO trades(run_id, i, t, side, qty, price, fee, note) VALUES(?, ?, ?, ?, ?, ?, ?, ?);",
                    (run_id, i, tr.t, tr.side, tr.qty, tr.price, tr.fee, tr.note),
                )
            for k, v in metrics.items():
                c.execute("INSERT INTO metrics(run_id, k, v) VALUES(?, ?, ?);", (run_id, k, float(v)))

    def list_runs(self, limit: int = 20) -> list[sqlite3.Row]:
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        cur = self._conn.execute(
            "SELECT id, created_at, kind, symbol, strategy, seed FROM runs ORDER BY created_at DESC LIMIT ?;",
            (int(limit),),
        )
        return list(cur.fetchall())

    def load_run_candles(self, run_id: str) -> list[sqlite3.Row]:
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        cur = self._conn.execute(
            "SELECT t, o, h, l, c, v FROM candles WHERE run_id=? ORDER BY i ASC;",
            (run_id,),
        )
        return list(cur.fetchall())

    def load_run_trades(self, run_id: str) -> list[sqlite3.Row]:
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        cur = self._conn.execute(
            "SELECT t, side, qty, price, fee, note FROM trades WHERE run_id=? ORDER BY i ASC;",
            (run_id,),
        )
        return list(cur.fetchall())

    def load_run_metrics(self, run_id: str) -> dict[str, float]:
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        cur = self._conn.execute("SELECT k, v FROM metrics WHERE run_id=?;", (run_id,))
        out: dict[str, float] = {}
        for row in cur.fetchall():
            out[str(row["k"])] = float(row["v"])
        return out


# ---------------------------
# Market data model
# ---------------------------


@dataclasses.dataclass(frozen=True)
class Candle:
    t: int
