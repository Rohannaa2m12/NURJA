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

