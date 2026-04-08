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
    o: float
    h: float
    l: float
    c: float
    v: float


@dataclasses.dataclass(frozen=True)
class Trade:
    t: int
    side: str  # "BUY" | "SELL"
    qty: float
    price: float
    fee: float
    note: str


def _assert_candles(candles: list[Candle]) -> None:
    if not candles:
        raise DataError("no candles")
    last_t = candles[0].t
    for cd in candles[1:]:
        if cd.t <= last_t:
            raise DataError("candles not strictly increasing")
        if cd.h < cd.l:
            raise DataError("bad candle hi/lo")
        if not (cd.l <= cd.o <= cd.h and cd.l <= cd.c <= cd.h):
            raise DataError("bad candle o/c outside range")
        last_t = cd.t


# ---------------------------
# Synthetic market generators
# ---------------------------


@dataclasses.dataclass
class MarketGenConfig:
    symbol: str = "NURJ/USD"
    timeframe_sec: int = 60
    n: int = 720
    seed: int = 0
    start_price: float = 100.0
    drift: float = 0.00002
    vol: float = 0.0035
    vol_of_vol: float = 0.15
    mean_revert: float = 0.05
    jump_prob: float = 0.006
    jump_sigma: float = 0.03
    micro_noise: float = 0.0012
    volume_base: float = 1200.0
    volume_noise: float = 0.40


class SyntheticMarket:
    """
    Produces candles with:
    - stochastic volatility
    - weak drift
    - mean reversion component
    - occasional jumps
    - heteroskedastic volumes
    """

    def __init__(self, cfg: MarketGenConfig, log: Logger) -> None:
        self.cfg = cfg
        self.log = log
        self.rng = random.Random(cfg.seed)

    def generate(self, start_ts: int | None = None) -> list[Candle]:
        cfg = self.cfg
        if start_ts is None:
            start_ts = _ts() - cfg.n * cfg.timeframe_sec

        price = float(cfg.start_price)
        vol = float(cfg.vol)
        anchor = price

        out: list[Candle] = []
        tcur = int(start_ts)

        for i in range(cfg.n):
            # stochastic vol
            vol = max(1e-6, vol * math.exp(self.rng.gauss(0.0, cfg.vol_of_vol) * 0.05))

            # jump
            jump = 0.0
            if self.rng.random() < cfg.jump_prob:
                jump = self.rng.gauss(0.0, cfg.jump_sigma)

            # mean reversion towards anchor
            reversion = cfg.mean_revert * (anchor - price) / max(1e-9, anchor)
            drift = cfg.drift + reversion * 0.001

            # return
            ret = drift + self.rng.gauss(0.0, vol) + jump + self.rng.gauss(0.0, cfg.micro_noise)
            ret = _clamp(ret, -0.25, 0.25)
            nxt = max(0.01, price * math.exp(ret))

            o = price
            c = nxt

            # intra-candle range
            span = abs(ret) + abs(self.rng.gauss(0.0, vol)) * 0.6 + 0.0005
            span = _clamp(span, 0.0006, 0.12)
            hi = max(o, c) * (1 + span * self.rng.random())
            lo = min(o, c) * (1 - span * self.rng.random())

            # volume
            v = cfg.volume_base * (1 + cfg.volume_noise * self.rng.gauss(0.0, 1.0))
            v = max(1.0, v * (1 + abs(ret) * 25))

            out.append(Candle(t=tcur, o=o, h=hi, l=lo, c=c, v=v))

            # slowly update anchor
            anchor = 0.9995 * anchor + 0.0005 * c
            price = c
            tcur += cfg.timeframe_sec

        _assert_candles(out)
        self.log.info("market", "generated synthetic candles", symbol=cfg.symbol, n=len(out), seed=cfg.seed)
        return out


# ---------------------------
# Indicators
# ---------------------------


def sma(xs: list[float], n: int) -> list[float]:
    if n <= 0:
        raise ValueError("sma n<=0")
    out: list[float] = [NAN] * len(xs)
    s = 0.0
    for i, x in enumerate(xs):
        s += x
        if i >= n:
            s -= xs[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


def ema(xs: list[float], n: int) -> list[float]:
    if n <= 0:
        raise ValueError("ema n<=0")
    out: list[float] = [NAN] * len(xs)
    k = 2 / (n + 1)
    m = 0.0
    started = False
    for i, x in enumerate(xs):
        if not started:
            m = x
            started = True
        else:
            m = x * k + m * (1 - k)
        out[i] = m
    return out


def rsi(xs: list[float], n: int = 14) -> list[float]:
    if n <= 1:
        raise ValueError("rsi n<=1")
    out: list[float] = [NAN] * len(xs)
    gains = 0.0
    losses = 0.0
    for i in range(1, len(xs)):
        d = xs[i] - xs[i - 1]
        g = max(0.0, d)
        l = max(0.0, -d)
        if i <= n:
            gains += g
            losses += l
            if i == n:
                rs = _safe_div(gains, losses, default=0.0)
                out[i] = 100 - (100 / (1 + rs))
        else:
            gains = (gains * (n - 1) + g) / n
            losses = (losses * (n - 1) + l) / n
            rs = _safe_div(gains, losses, default=0.0)
            out[i] = 100 - (100 / (1 + rs))
    return out


def atr(candles: list[Candle], n: int = 14) -> list[float]:
    if n <= 1:
        raise ValueError("atr n<=1")
    out: list[float] = [NAN] * len(candles)
    trs: list[float] = []
    for i, cd in enumerate(candles):
        if i == 0:
            tr = cd.h - cd.l
        else:
            prev = candles[i - 1].c
            tr = max(cd.h - cd.l, abs(cd.h - prev), abs(cd.l - prev))
        trs.append(tr)
    # Wilder smoothing
    a = 0.0
    for i, tr in enumerate(trs):
        if i == n:
            a = sum(trs[1 : n + 1]) / n
            out[i] = a
        elif i > n:
            a = (a * (n - 1) + tr) / n
            out[i] = a
    return out


def zscore(xs: list[float], n: int) -> list[float]:
    if n <= 1:
        raise ValueError("zscore n<=1")
    out: list[float] = [NAN] * len(xs)
    win: list[float] = []
    for i, x in enumerate(xs):
        win.append(x)
        if len(win) > n:
            win.pop(0)
        if len(win) == n:
            mu = statistics.fmean(win)
            sd = statistics.pstdev(win)
            if sd <= 1e-12:
                out[i] = 0.0
            else:
                out[i] = (x - mu) / sd
    return out


# ---------------------------
# Portfolio & execution model
# ---------------------------


@dataclasses.dataclass
class Fees:
    maker_bps: float = 2.0
    taker_bps: float = 6.0
    slippage_bps: float = 3.0


@dataclasses.dataclass
class RiskConfig:
    max_pos_pct: float = 0.35
    max_leverage: float = 1.0
    max_daily_loss_pct: float = 0.04
    max_trade_loss_pct: float = 0.02
    kill_switch_drawdown_pct: float = 0.18
    min_order_usd: float = 15.0
    max_orders_per_hour: int = 12
    cooldown_sec: int = 20


@dataclasses.dataclass
class Portfolio:
    base_ccy: str = "USD"
    cash: float = 10_000.0
    asset_qty: float = 0.0
    avg_entry: float = 0.0
    realized_pnl: float = 0.0
    fees_paid: float = 0.0
    peak_equity: float = 10_000.0
    dd_killed: bool = False

    def equity(self, px: float) -> float:
        return self.cash + self.asset_qty * px

    def update_peak(self, px: float) -> None:
        eq = self.equity(px)
        if eq > self.peak_equity:
            self.peak_equity = eq

    def drawdown(self, px: float) -> float:
        eq = self.equity(px)
        if self.peak_equity <= 0:
            return 0.0
        return max(0.0, 1.0 - (eq / self.peak_equity))


@dataclasses.dataclass
class ExecutionContext:
    fees: Fees
    risk: RiskConfig
    log: Logger
    rng: random.Random


def _fee_for(side: str, notional: float, fees: Fees, passive: bool) -> float:
    bps = fees.maker_bps if passive else fees.taker_bps
    return notional * (bps / 10_000.0)


def _apply_slippage(side: str, px: float, fees: Fees, rng: random.Random) -> float:
    slip = fees.slippage_bps / 10_000.0
    # add randomness to slippage to stress-test strategies
    jitter = abs(rng.gauss(0.0, slip * 0.35))
    s = slip + jitter
    if side.upper() == "BUY":
        return px * (1 + s)
    return px * (1 - s)


class PaperExchange:
    """
    A basic paper exchange:
    - no order book, marketable trades executed with slippage+fees
    - only spot long/flat in this simplified model
    """

    def __init__(self, ctx: ExecutionContext) -> None:
        self.ctx = ctx
        self._last_order_ts: float = 0.0
        self._orders_in_window: list[float] = []

    def _rate_limit(self, now: float) -> None:
        # rolling 1h window
        self._orders_in_window = [t0 for t0 in self._orders_in_window if now - t0 <= 3600]
        if len(self._orders_in_window) >= self.ctx.risk.max_orders_per_hour:
            raise RiskError("rate limit: too many orders per hour")
        # cooldown
        if now - self._last_order_ts < self.ctx.risk.cooldown_sec:
            raise RiskError("cooldown: too soon after last order")
        self._orders_in_window.append(now)
        self._last_order_ts = now

    def buy(self, pf: Portfolio, qty: float, px: float, t: int, note: str) -> Trade:
        now = time.time()
        self._rate_limit(now)
        if qty <= 0:
            raise ExchangeError("buy qty<=0")
        fill = _apply_slippage("BUY", px, self.ctx.fees, self.ctx.rng)
        notional = qty * fill
        if notional < self.ctx.risk.min_order_usd:
            raise RiskError("order too small")
        fee = _fee_for("BUY", notional, self.ctx.fees, passive=False)
        total = notional + fee
        if total > pf.cash + 1e-9:
            raise RiskError("insufficient cash")

        # update average entry
        new_qty = pf.asset_qty + qty
        if new_qty <= 0:
            pf.avg_entry = 0.0
        else:
            pf.avg_entry = (pf.avg_entry * pf.asset_qty + fill * qty) / new_qty
        pf.asset_qty = new_qty
        pf.cash -= total
        pf.fees_paid += fee
        return Trade(t=t, side="BUY", qty=qty, price=fill, fee=fee, note=note)

    def sell(self, pf: Portfolio, qty: float, px: float, t: int, note: str) -> Trade:
        now = time.time()
        self._rate_limit(now)
        if qty <= 0:
            raise ExchangeError("sell qty<=0")
        if qty > pf.asset_qty + 1e-12:
            raise RiskError("insufficient asset qty")
        fill = _apply_slippage("SELL", px, self.ctx.fees, self.ctx.rng)
        notional = qty * fill
        if notional < self.ctx.risk.min_order_usd:
            raise RiskError("order too small")
        fee = _fee_for("SELL", notional, self.ctx.fees, passive=False)
        proceeds = notional - fee
        pf.cash += proceeds
        pf.asset_qty -= qty
        pf.fees_paid += fee

        # realized pnl
        pnl = (fill - pf.avg_entry) * qty
        pf.realized_pnl += pnl
        if pf.asset_qty <= 1e-12:
            pf.asset_qty = 0.0
            pf.avg_entry = 0.0

        return Trade(t=t, side="SELL", qty=qty, price=fill, fee=fee, note=note)


# ---------------------------
# Strategy framework
# ---------------------------


@dataclasses.dataclass
class Signal:
    t: int
    action: str  # "BUY" | "SELL" | "HOLD"
    strength: float
    reason: str
    meta: dict[str, t.Any] = dataclasses.field(default_factory=dict)


class Strategy:
    name: str = "base"

    def warmup(self) -> int:
        return 50

    def on_candle(self, i: int, candles: list[Candle], pf: Portfolio) -> Signal:
        raise NotImplementedError


class LoomMomentum(Strategy):
    name = "loom_momentum"

    def __init__(self, fast: int = 12, slow: int = 48, rsi_n: int = 14) -> None:
        self.fast = fast
        self.slow = slow
        self.rsi_n = rsi_n

    def warmup(self) -> int:
        return max(self.fast, self.slow, self.rsi_n) + 5

    def on_candle(self, i: int, candles: list[Candle], pf: Portfolio) -> Signal:
        closes = [c.c for c in candles[: i + 1]]
        ef = ema(closes, self.fast)[i]
        es = ema(closes, self.slow)[i]
        r = rsi(closes, self.rsi_n)[i]
        px = closes[i]

        if math.isnan(ef) or math.isnan(es) or math.isnan(r):
            return Signal(t=candles[i].t, action="HOLD", strength=0.0, reason="warmup")

        macd = ef - es
        macd_n = _safe_div(macd, px, default=0.0)
        score = _clamp(macd_n * 120.0 + (r - 50.0) / 50.0, -2.0, 2.0)

        if score > 0.55 and r < 72:
            return Signal(t=candles[i].t, action="BUY", strength=min(1.0, score / 2.0), reason="trend_up",
                          meta={"macd": macd, "rsi": r})
        if score < -0.55 and r > 28:
            return Signal(t=candles[i].t, action="SELL", strength=min(1.0, abs(score) / 2.0), reason="trend_down",
                          meta={"macd": macd, "rsi": r})
        return Signal(t=candles[i].t, action="HOLD", strength=0.0, reason="no_edge", meta={"macd": macd, "rsi": r})


class LoomMeanRevert(Strategy):
    name = "loom_meanrevert"

    def __init__(self, win: int = 40, z: float = 1.10, take: float = 0.65) -> None:
        self.win = win
        self.z = z
        self.take = take

    def warmup(self) -> int:
        return self.win + 5

    def on_candle(self, i: int, candles: list[Candle], pf: Portfolio) -> Signal:
        closes = [c.c for c in candles[: i + 1]]
        z = zscore(closes, self.win)[i]
        px = closes[i]
        if math.isnan(z):
            return Signal(t=candles[i].t, action="HOLD", strength=0.0, reason="warmup")
        # mean reversion: buy when oversold, sell when overbought.
        if z <= -self.z and pf.asset_qty <= 0:
            strength = _clamp(abs(z) / (self.z * 2.0), 0.0, 1.0)
            return Signal(t=candles[i].t, action="BUY", strength=strength, reason="oversold",
                          meta={"z": z, "px": px})
        if z >= self.z and pf.asset_qty > 0:
            strength = _clamp(abs(z) / (self.z * 2.0), 0.0, 1.0)
            return Signal(t=candles[i].t, action="SELL", strength=strength, reason="overbought",
                          meta={"z": z, "px": px})
        # take-profit / stop-ish using entry vs px
        if pf.asset_qty > 0 and pf.avg_entry > 0:
            up = (px / pf.avg_entry) - 1.0
            if up >= self.take:
                return Signal(t=candles[i].t, action="SELL", strength=1.0, reason="take_profit",
                              meta={"up": up, "px": px})
        return Signal(t=candles[i].t, action="HOLD", strength=0.0, reason="no_edge", meta={"z": z, "px": px})


class LoomBreakout(Strategy):
    name = "loom_breakout"

    def __init__(self, n: int = 60, atr_n: int = 14, k: float = 0.85) -> None:
        self.n = n
        self.atr_n = atr_n
        self.k = k

    def warmup(self) -> int:
        return max(self.n, self.atr_n) + 5

    def on_candle(self, i: int, candles: list[Candle], pf: Portfolio) -> Signal:
        if i < self.warmup():
            return Signal(t=candles[i].t, action="HOLD", strength=0.0, reason="warmup")
        window = candles[i - self.n : i]
        hi = max(c.h for c in window)
        lo = min(c.l for c in window)
        px = candles[i].c
        a = atr(candles[: i + 1], self.atr_n)[i]
        if math.isnan(a) or a <= 0:
            return Signal(t=candles[i].t, action="HOLD", strength=0.0, reason="no_atr")
        band = self.k * a
        if px > hi + band and pf.asset_qty <= 0:
            s = _clamp((px - hi) / max(1e-9, px), 0.0, 1.0)
            return Signal(t=candles[i].t, action="BUY", strength=s, reason="breakout_up", meta={"hi": hi, "atr": a})
        if px < lo - band and pf.asset_qty > 0:
            s = _clamp((lo - px) / max(1e-9, lo), 0.0, 1.0)
            return Signal(t=candles[i].t, action="SELL", strength=s, reason="breakout_down", meta={"lo": lo, "atr": a})
        return Signal(t=candles[i].t, action="HOLD", strength=0.0, reason="range", meta={"hi": hi, "lo": lo, "atr": a})


STRATEGY_REGISTRY: dict[str, t.Callable[[], Strategy]] = {
    LoomMomentum.name: lambda: LoomMomentum(),
    LoomMeanRevert.name: lambda: LoomMeanRevert(),
    LoomBreakout.name: lambda: LoomBreakout(),
}


def make_strategy(name: str) -> Strategy:
    if name not in STRATEGY_REGISTRY:
        raise StrategyError(f"unknown strategy: {name}")
    return STRATEGY_REGISTRY[name]()


# ---------------------------
# Risk manager
# ---------------------------


@dataclasses.dataclass
class RiskState:
    start_equity: float
    day_start_ts: int
    day_min_equity: float
    day_max_equity: float
    day_realized: float = 0.0
    day_trades: int = 0
    killed: bool = False


class RiskManager:
    def __init__(self, cfg: RiskConfig, log: Logger) -> None:
        self.cfg = cfg
        self.log = log

    def init_state(self, equity: float, t0: int) -> RiskState:
        return RiskState(start_equity=equity, day_start_ts=t0, day_min_equity=equity, day_max_equity=equity)

    def on_step(self, st: RiskState, pf: Portfolio, px: float, tcur: int) -> None:
        eq = pf.equity(px)
        pf.update_peak(px)
        dd = pf.drawdown(px)
        if dd >= self.cfg.kill_switch_drawdown_pct:
            pf.dd_killed = True
            st.killed = True
            self.log.warn("risk", "kill-switch drawdown", drawdown=_fmt_pct(dd), eq=_fmt_money(eq))

        # reset day if needed
        if (tcur - st.day_start_ts) >= 86400:
            st.day_start_ts = tcur
            st.start_equity = eq
            st.day_min_equity = eq
            st.day_max_equity = eq
            st.day_realized = 0.0
            st.day_trades = 0

        st.day_min_equity = min(st.day_min_equity, eq)
        st.day_max_equity = max(st.day_max_equity, eq)

        day_loss = max(0.0, (st.start_equity - eq) / max(1e-9, st.start_equity))
        if day_loss >= self.cfg.max_daily_loss_pct:
            st.killed = True
            self.log.warn("risk", "max daily loss reached", day_loss=_fmt_pct(day_loss), eq=_fmt_money(eq))

    def validate_order(self, pf: Portfolio, px: float, side: str, qty: float) -> None:
        if pf.dd_killed:
            raise RiskError("portfolio kill-switch active")
        if qty <= 0:
            raise RiskError("qty<=0")
        eq = pf.equity(px)
        if eq <= 0:
            raise RiskError("equity<=0")

        # position size cap
        pos_value = pf.asset_qty * px
        if side.upper() == "BUY":
            new_pos_value = (pf.asset_qty + qty) * px
        else:
            new_pos_value = max(0.0, (pf.asset_qty - qty) * px)
        if new_pos_value / eq > self.cfg.max_pos_pct + 1e-9:
            raise RiskError("max position percent exceeded")


# ---------------------------
# Engine (backtest / paper)
# ---------------------------


@dataclasses.dataclass
class EngineConfig:
    symbol: str
    strategy_name: str
    seed: int
    start_cash: float = 10_000.0
    fees: Fees = dataclasses.field(default_factory=Fees)
    risk: RiskConfig = dataclasses.field(default_factory=RiskConfig)
    verbose: bool = False


@dataclasses.dataclass
class EngineResult:
    run_id: str
    kind: str
    symbol: str
    strategy: str
    seed: int
    candles: list[Candle]
    trades: list[Trade]
    metrics: dict[str, float]
    summary: dict[str, t.Any]


class NurjaEngine:
    def __init__(self, cfg: EngineConfig, log: Logger) -> None:
        self.cfg = cfg
        self.log = log
        self.rng = random.Random(cfg.seed ^ 0x6A09E667F3BCC909)

        self.strategy = make_strategy(cfg.strategy_name)
        self.pf = Portfolio(cash=float(cfg.start_cash), peak_equity=float(cfg.start_cash))
        self.ctx = ExecutionContext(fees=cfg.fees, risk=cfg.risk, log=log, rng=self.rng)
        self.ex = PaperExchange(self.ctx)
        self.rm = RiskManager(cfg.risk, log)

    def _qty_for_strength(self, px: float, strength: float) -> float:
        strength = _clamp(strength, 0.0, 1.0)
        eq = self.pf.equity(px)
        max_pos_value = eq * self.cfg.risk.max_pos_pct
        target_value = max_pos_value * (0.25 + 0.75 * strength)
        target_qty = target_value / max(1e-9, px)
        # don't exceed cash in buy
        return max(0.0, target_qty)

    def _compute_metrics(self, candles: list[Candle], trades: list[Trade]) -> dict[str, float]:
        closes = [c.c for c in candles]
        if not closes:
            return {}
        end_px = closes[-1]
        eq = self.pf.equity(end_px)
        start = float(self.cfg.start_cash)
        ret = (eq / start) - 1.0 if start > 0 else 0.0

        # equity curve sampling
        eq_curve: list[float] = []
        cash = float(self.cfg.start_cash)
        qty = 0.0
        avg_entry = 0.0
        fee_paid = 0.0
        tr_i = 0
        for i, cd in enumerate(candles):
            while tr_i < len(trades) and trades[tr_i].t <= cd.t:
                tr = trades[tr_i]
                notional = tr.qty * tr.price
                if tr.side == "BUY":
                    total = notional + tr.fee
                    cash -= total
                    fee_paid += tr.fee
                    new_qty = qty + tr.qty
                    avg_entry = (avg_entry * qty + tr.price * tr.qty) / max(1e-12, new_qty)
                    qty = new_qty
                else:
                    proceeds = notional - tr.fee
                    cash += proceeds
                    fee_paid += tr.fee
                    qty -= tr.qty
                    if qty <= 1e-12:
                        qty = 0.0
                        avg_entry = 0.0
                tr_i += 1
            eq_curve.append(cash + qty * cd.c)

        # drawdown
        peak = -1e18
        max_dd = 0.0
        for e in eq_curve:
            if e > peak:
                peak = e
            if peak > 0:
                dd = max(0.0, 1.0 - (e / peak))
                max_dd = max(max_dd, dd)

        # returns stats
        rets: list[float] = []
        for i in range(1, len(eq_curve)):
            rets.append(_safe_div(eq_curve[i] - eq_curve[i - 1], eq_curve[i - 1], 0.0))
        mu = statistics.fmean(rets) if rets else 0.0
        sd = statistics.pstdev(rets) if len(rets) >= 2 else 0.0
        sharpe = _safe_div(mu, sd, 0.0) * math.sqrt(365 * 24 * 60)  # if 1-min candles, rough annualization

        win = 0
        loss = 0
        # very rough trade p&l approximation
        last_buy: Trade | None = None
        for tr in trades:
            if tr.side == "BUY":
                last_buy = tr
            elif tr.side == "SELL" and last_buy is not None:
                pnl = (tr.price - last_buy.price) * min(tr.qty, last_buy.qty) - (tr.fee + last_buy.fee)
                if pnl >= 0:
                    win += 1
                else:
                    loss += 1
                last_buy = None

        return {
            "equity_end": float(eq),
            "return": float(ret),
            "fees_paid": float(self.pf.fees_paid),
            "trades": float(len(trades)),
            "max_drawdown": float(max_dd),
            "sharpe_like": float(sharpe),
            "wins": float(win),
            "losses": float(loss),
        }

    def run(self, candles: list[Candle], kind: str) -> EngineResult:
        _assert_candles(candles)
        warm = self.strategy.warmup()
        trades: list[Trade] = []
        st = self.rm.init_state(self.pf.equity(candles[0].c), candles[0].t)

        for i, cd in enumerate(candles):
            px = cd.c
            self.rm.on_step(st, self.pf, px, cd.t)
            if st.killed:
                self.log.warn("engine", "risk killed run", i=i, t=cd.t)
                break
            if i < warm:
                continue
            sig = self.strategy.on_candle(i, candles, self.pf)
            if sig.action == "HOLD":
                continue

            strength = _clamp(sig.strength, 0.0, 1.0)
            qty = self._qty_for_strength(px, strength)
            if qty <= 0:
                continue

            try:
                self.rm.validate_order(self.pf, px, sig.action, qty)
                if sig.action == "BUY":
                    tr = self.ex.buy(self.pf, qty=qty, px=px, t=cd.t, note=sig.reason)
                else:
                    # sell full position proportionally to strength
                    sell_qty = min(self.pf.asset_qty, max(0.0, self.pf.asset_qty * (0.35 + 0.65 * strength)))
                    if sell_qty <= 0:
                        continue
                    tr = self.ex.sell(self.pf, qty=sell_qty, px=px, t=cd.t, note=sig.reason)
                trades.append(tr)
            except (RiskError, ExchangeError) as e:
                self.log.warn("engine", "order rejected", i=i, reason=str(e), action=sig.action)

        metrics = self._compute_metrics(candles, trades)
        summary = {
            "app": APP_NAME,
            "version": APP_VERSION,
            "kind": kind,
            "symbol": self.cfg.symbol,
            "strategy": self.cfg.strategy_name,
            "seed": self.cfg.seed,
            "created_at": _now_utc().isoformat(),
            "end_equity": metrics.get("equity_end", float("nan")),
            "return": metrics.get("return", float("nan")),
            "max_drawdown": metrics.get("max_drawdown", float("nan")),
            "trades": int(metrics.get("trades", 0.0)),
        }
        run_id = _rand_id("run")
        return EngineResult(
            run_id=run_id,
            kind=kind,
            symbol=self.cfg.symbol,
            strategy=self.cfg.strategy_name,
            seed=self.cfg.seed,
            candles=candles,
            trades=trades,
            metrics=metrics,
            summary=summary,
        )


# ---------------------------
# Reporting & exporting
# ---------------------------


def _mkdirp(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def export_run(result: EngineResult, db: NurjaDB, log: Logger, export: bool = True) -> str:
    cfg = {
        "symbol": result.symbol,
        "strategy": result.strategy,
        "seed": result.seed,
        "kind": result.kind,
        "version": APP_VERSION,
    }
    db.save_run(
        run_id=result.run_id,
        kind=result.kind,
        symbol=result.symbol,
        strategy=result.strategy,
        seed=result.seed,
        config=cfg,
        summary=result.summary,
        candles=result.candles,
        trades=result.trades,
        metrics=result.metrics,
    )

    if not export:
        return result.run_id

    _mkdirp(EXPORT_DIR)
    base = os.path.join(EXPORT_DIR, result.run_id)
    # summary
    with open(base + "_summary.json", "w", encoding="utf-8") as f:
        json.dump(result.summary, f, indent=2, sort_keys=True, default=str)
    with open(base + "_metrics.json", "w", encoding="utf-8") as f:
        json.dump(result.metrics, f, indent=2, sort_keys=True, default=str)
    # candles CSV
    with open(base + "_candles.csv", "w", encoding="utf-8") as f:
        f.write("t,o,h,l,c,v\n")
        for cd in result.candles:
            f.write(f"{cd.t},{cd.o:.8f},{cd.h:.8f},{cd.l:.8f},{cd.c:.8f},{cd.v:.4f}\n")
    # trades CSV
    with open(base + "_trades.csv", "w", encoding="utf-8") as f:
        f.write("t,side,qty,price,fee,note\n")
        for tr in result.trades:
            f.write(f"{tr.t},{tr.side},{tr.qty:.10f},{tr.price:.8f},{tr.fee:.8f},{tr.note}\n")

    log.info("export", "exported run", run_id=result.run_id, dir=EXPORT_DIR)
    return result.run_id


def render_report(result: EngineResult) -> str:
    m = result.metrics
    end_eq = float(m.get("equity_end", float("nan")))
    ret = float(m.get("return", float("nan")))
    dd = float(m.get("max_drawdown", float("nan")))
    fees_paid = float(m.get("fees_paid", float("nan")))
    trades = int(m.get("trades", 0.0))
    sharpe = float(m.get("sharpe_like", float("nan")))
    wins = int(m.get("wins", 0.0))
    losses = int(m.get("losses", 0.0))
    total = max(1, wins + losses)
    winrate = wins / total

    lines = []
    lines.append(f"{APP_NAME} report")
    lines.append("-" * 60)
    lines.append(f"run_id:    {result.run_id}")
    lines.append(f"kind:      {result.kind}")
    lines.append(f"symbol:    {result.symbol}")
    lines.append(f"strategy:  {result.strategy}")
    lines.append(f"seed:      {result.seed}")
    lines.append("-" * 60)
    lines.append(f"end eq:    {_fmt_money(end_eq)}")
    lines.append(f"return:    {_fmt_pct(ret)}")
    lines.append(f"max dd:    {_fmt_pct(dd)}")
    lines.append(f"fees:      {_fmt_money(fees_paid)}")
    lines.append(f"trades:    {trades}")
    lines.append(f"winrate*:  {_fmt_pct(winrate)}")
    lines.append(f"sharpe*:   {sharpe:.2f}")
    lines.append("-" * 60)
    lines.append("* heuristic metrics for this simplified execution model")
    return "\n".join(lines)


# ---------------------------
# CLI helpers
# ---------------------------


def _parse_int(x: str) -> int:
    try:
        return int(x)
    except Exception as e:
        raise argparse.ArgumentTypeError(str(e))


def _parse_float(x: str) -> float:
    try:
        return float(x)
    except Exception as e:
        raise argparse.ArgumentTypeError(str(e))


def _seed_or_random(seed: int | None) -> int:
    if seed is None:
        # a seed that changes, but still printable
        return int.from_bytes(secrets.token_bytes(8), "big") ^ int(time.time())
    return int(seed)


def _print_runs(rows: list[sqlite3.Row]) -> None:
    if not rows:
        print("No runs saved yet.")
        return
    print("Saved runs:")
    for r in rows:
        at = dt.datetime.fromtimestamp(int(r["created_at"])).strftime("%Y-%m-%d %H:%M:%S")
        print(f"- {r['id']}  {at}  {r['kind']:<7}  {r['symbol']:<10}  {r['strategy']:<14}  seed={r['seed']}")


def _sparkline(xs: list[float], width: int = 42) -> str:
    if not xs:
        return ""
    lo = min(xs)
    hi = max(xs)
    if abs(hi - lo) < 1e-12:
        return "─" * min(width, len(xs))
    chars = "▁▂▃▄▅▆▇█"
    step = max(1, len(xs) // width)
    sampled = xs[::step]
    out = []
    for x in sampled[:width]:
        z = (x - lo) / (hi - lo)
        idx = int(_clamp(z, 0.0, 0.999999) * len(chars))
        out.append(chars[idx])
    return "".join(out)


def _equity_curve(result: EngineResult) -> list[float]:
    # replay quickly from trades
    cash = 10_000.0
    qty = 0.0
    curve: list[float] = []
    trades = sorted(result.trades, key=lambda t0: t0.t)
    ti = 0
    for cd in result.candles:
        while ti < len(trades) and trades[ti].t <= cd.t:
            tr = trades[ti]
            notional = tr.qty * tr.price
            if tr.side == "BUY":
                cash -= notional + tr.fee
                qty += tr.qty
            else:
                cash += notional - tr.fee
                qty -= tr.qty
            ti += 1
        curve.append(cash + qty * cd.c)
    return curve


def _quickstart(log: Logger, db: NurjaDB) -> int:
    seed = _seed_or_random(None)
    symbol = "NURJ/USD"
    strat = "loom_momentum"
    mg = MarketGenConfig(symbol=symbol, timeframe_sec=60, n=720, seed=seed, start_price=100.0)
    candles = SyntheticMarket(mg, log).generate()

    cfg = EngineConfig(symbol=symbol, strategy_name=strat, seed=seed, verbose=log.verbose)
    eng = NurjaEngine(cfg, log)
    res = eng.run(candles, kind="backtest")
    export_run(res, db, log, export=True)
    print(render_report(res))
    curve = _equity_curve(res)
    print("equity:", _sparkline(curve))
    return 0


def _backtest(args: argparse.Namespace, log: Logger, db: NurjaDB) -> int:
    seed = _seed_or_random(args.seed)
    mg = MarketGenConfig(
        symbol=args.symbol,
        timeframe_sec=args.tf,
        n=int((args.days * 86400) / max(1, args.tf)),
        seed=seed,
        start_price=args.start_price,
        drift=args.drift,
        vol=args.vol,
        jump_prob=args.jump_prob,
        jump_sigma=args.jump_sigma,
        mean_revert=args.mean_revert,
        volume_base=args.volume_base,
    )
    candles = SyntheticMarket(mg, log).generate()

    fees = Fees(maker_bps=args.maker_bps, taker_bps=args.taker_bps, slippage_bps=args.slippage_bps)
    risk = RiskConfig(
        max_pos_pct=args.max_pos_pct,
        max_daily_loss_pct=args.max_daily_loss_pct,
        kill_switch_drawdown_pct=args.kill_switch_dd,
        min_order_usd=args.min_order_usd,
        max_orders_per_hour=args.max_oph,
        cooldown_sec=args.cooldown,
    )
    cfg = EngineConfig(symbol=args.symbol, strategy_name=args.strategy, seed=seed, start_cash=args.cash, fees=fees, risk=risk, verbose=log.verbose)
    eng = NurjaEngine(cfg, log)
    t0 = time.time()
    res = eng.run(candles, kind="backtest")
    dur = time.time() - t0

    export_run(res, db, log, export=not args.no_export)
    print(render_report(res))
    print("runtime:", _human_time(dur))
    curve = _equity_curve(res)
    print("equity:", _sparkline(curve))
    return 0


def _paper(args: argparse.Namespace, log: Logger, db: NurjaDB) -> int:
    seed = _seed_or_random(args.seed)
    mg = MarketGenConfig(
        symbol=args.symbol,
        timeframe_sec=args.tf,
        n=int((args.minutes * 60) / max(1, args.tf)),
        seed=seed,
        start_price=args.start_price,
        drift=args.drift,
        vol=args.vol,
        jump_prob=args.jump_prob,
        jump_sigma=args.jump_sigma,
        mean_revert=args.mean_revert,
        volume_base=args.volume_base,
    )
    candles = SyntheticMarket(mg, log).generate()

    fees = Fees(maker_bps=args.maker_bps, taker_bps=args.taker_bps, slippage_bps=args.slippage_bps)
    risk = RiskConfig(
        max_pos_pct=args.max_pos_pct,
        max_daily_loss_pct=args.max_daily_loss_pct,
        kill_switch_drawdown_pct=args.kill_switch_dd,
        min_order_usd=args.min_order_usd,
        max_orders_per_hour=args.max_oph,
        cooldown_sec=args.cooldown,
    )
    cfg = EngineConfig(symbol=args.symbol, strategy_name=args.strategy, seed=seed, start_cash=args.cash, fees=fees, risk=risk, verbose=log.verbose)
