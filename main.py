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
