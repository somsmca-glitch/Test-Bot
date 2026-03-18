#!/usr/bin/env python3
"""
Indian Stock Market Screener — Telegram Bot (Fully Dynamic)
=============================================================
All stock symbols are fetched live from NSE India APIs.
No symbols are hardcoded. Data refreshes on every request.

Install:
  pip install python-telegram-bot yfinance pandas ta requests

Run:
  export TELEGRAM_BOT_TOKEN="your_token"
  python indian_stock_screener_bot.py
"""

from __future__ import annotations

import os
import logging
import asyncio
import csv
import heapq
import requests
import pandas as pd
import yfinance as yf
import ta
import time
from typing import Optional
from datetime import datetime
from zoneinfo import ZoneInfo
import gc
from threading import Lock
from collections import OrderedDict
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, ContextTypes,
)

# ─── CONFIG ───────────────────────────────────────────────────────────────────


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "6039703460:AAFOJDkr5BT5ffhIQ2UCiVuWeZXnHxG2W4M")

UNIVERSE_NIFTY50 = "nifty50"
UNIVERSE_ALL = "all"

UNIVERSE_LABELS = {
    UNIVERSE_NIFTY50: "Nifty 50",
    UNIVERSE_ALL: "All Stocks",
}

MIN_PRICE = float(os.getenv("MIN_PRICE", "100") or "100")
RESULT_LIMIT = int(os.getenv("RESULT_LIMIT", "5") or "5")
YF_CHUNK_SIZE = int(os.getenv("YF_CHUNK_SIZE", "50") or "50")
HEAVY_CONCURRENCY = int(os.getenv("HEAVY_CONCURRENCY", "1") or "1")
HEAVY_WORK_SEM = asyncio.Semaphore(max(1, HEAVY_CONCURRENCY))


def price_ok(price) -> bool:
    try:
        # "Above 100" means strictly greater than the threshold.
        return float(price) > MIN_PRICE
    except (TypeError, ValueError):
        return False

IST = ZoneInfo("Asia/Kolkata")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

# Optional proxy support (useful if you have a static-IP proxy).
# Example: set `NSE_PROXY=http://user:pass@1.2.3.4:8080`
NSE_PROXY = os.getenv("NSE_PROXY", "").strip()
NSE_TRUST_ENV = os.getenv("NSE_TRUST_ENV", "1").strip().lower() not in ("0", "false", "no", "off")

# Simple in-memory LRU cache: {key: (data, fetched_at_monotonic)}
_CACHE: "OrderedDict[str, tuple[object, float]]" = OrderedDict()
_CACHE_LOCK = Lock()
CACHE_TTL = 600  # seconds
CACHE_MAX = int(os.getenv("CACHE_MAX", "2048") or "2048")


def cache_get(key: str, ttl_seconds: int = CACHE_TTL):
    now = time.monotonic()
    with _CACHE_LOCK:
        entry = _CACHE.get(key)
        if not entry:
            return None
        value, fetched_at = entry
        if ttl_seconds > 0 and (now - fetched_at) >= ttl_seconds:
            _CACHE.pop(key, None)
            return None
        _CACHE.move_to_end(key)
        return value


def cache_set(key: str, value):
    with _CACHE_LOCK:
        _CACHE[key] = (value, time.monotonic())
        _CACHE.move_to_end(key)
        if CACHE_MAX > 0:
            while len(_CACHE) > CACHE_MAX:
                _CACHE.popitem(last=False)


@contextmanager
def log_timing(label: str):
    if os.getenv("PROFILE_TIMINGS", "").strip().lower() not in ("1", "true", "yes", "on"):
        yield
        return
    t0 = time.perf_counter()
    try:
        yield
    finally:
        logger.info("[timing] %s: %.3fs", label, time.perf_counter() - t0)


# ─── KEYBOARDS ────────────────────────────────────────────────────────────────

HOME_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📊 Technical"), KeyboardButton("📈 Fundamental"), KeyboardButton("📌 OI Data")],
        [KeyboardButton("📉 Top 5 Losers"), KeyboardButton("🚀 Top 5 Gainers")],
        [KeyboardButton("🚀 Active Value"), KeyboardButton("🚀 Active Vol")],
    ],
    resize_keyboard=True,
)

TECHNICAL_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🧺 Nifty 50"), KeyboardButton("🌐 All Stocks")],
        [KeyboardButton("📉 10 SMA"), KeyboardButton("📈 Prev Week High")],
        [KeyboardButton("🔵 RSI Oversold"), KeyboardButton("🔴 RSI Overbought")],
        [KeyboardButton("📌 OI Data"), KeyboardButton("🏠 Home")],
    ],
    resize_keyboard=True,
)

FUNDAMENTAL_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🔹 Small Cap"), KeyboardButton("🔷 Mid Cap"),KeyboardButton("📄 Stock Info")],
        [KeyboardButton("📌 OI Data"), KeyboardButton("🏠 Home")],
    ],
    resize_keyboard=True,
)

OI_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📌 Top CE OI"), KeyboardButton("📌 Top PE OI"),KeyboardButton("OI Analysis")],
        [KeyboardButton("🏠 Home")],
    ],
    resize_keyboard=True,
)


# ─── NSE SESSION ─────────────────────────────────────────────────────────────

def nse_session() -> requests.Session:
    """Return a cookie-primed NSE session."""
    global _NSE_SESSION, _NSE_SESSION_PRIMED_AT
    now = time.monotonic()
    with _NSE_SESSION_LOCK:
        if _NSE_SESSION is None or (now - _NSE_SESSION_PRIMED_AT) > NSE_SESSION_TTL:
            s = requests.Session()
            s.headers.update(NSE_HEADERS)
            s.trust_env = NSE_TRUST_ENV
            if NSE_PROXY:
                s.proxies.update({"http": NSE_PROXY, "https": NSE_PROXY})

            retry = Retry(
                total=3,
                backoff_factor=0.3,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET"]),
            )
            adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
            s.mount("https://", adapter)
            s.mount("http://", adapter)

            try:
                s.get("https://www.nseindia.com", timeout=10)
            except Exception:
                pass

            _NSE_SESSION = s
            _NSE_SESSION_PRIMED_AT = now

        return _NSE_SESSION


_NSE_SESSION: Optional[requests.Session] = None
_NSE_SESSION_LOCK = Lock()
_NSE_SESSION_PRIMED_AT = 0.0
NSE_SESSION_TTL = int(os.getenv("NSE_SESSION_TTL", "1800") or "1800")  # 30 minutes


def nse_fresh_session() -> requests.Session:
    """
    Creates a new session with NSE headers + retry adapter and primes cookies.
    Useful when a long-lived session gets blocked/stale.
    """
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    s.trust_env = NSE_TRUST_ENV
    if NSE_PROXY:
        s.proxies.update({"http": NSE_PROXY, "https": NSE_PROXY})
    retry = Retry(
        total=2,
        backoff_factor=0.3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    try:
        s.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    return s


def reset_nse_session():
    global _NSE_SESSION, _NSE_SESSION_PRIMED_AT
    with _NSE_SESSION_LOCK:
        try:
            if _NSE_SESSION is not None:
                _NSE_SESSION.close()
        except Exception:
            pass
        _NSE_SESSION = None
        _NSE_SESSION_PRIMED_AT = 0.0


def prime_nse_option_chain_session(session: requests.Session):
    """
    NSE sometimes requires cookies set by visiting the option-chain page before the JSON API works.
    """
    try:
        session.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    try:
        session.get("https://www.nseindia.com/option-chain", timeout=10)
    except Exception:
        pass


def option_chain_headers(symbol: str) -> dict:
    symbol = (symbol or "NIFTY").strip().upper()
    return {
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.nseindia.com/option-chain?symbol={symbol}",
        "Origin": "https://www.nseindia.com",
        "X-Requested-With": "XMLHttpRequest",
        # Common browser-ish headers (helps on some networks / NSE edge cases)
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "sec-ch-ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
    }


# ─── DYNAMIC SYMBOL FETCHERS ─────────────────────────────────────────────────

def fetch_index_symbols(index_slug: str, cache_key: str, limit: int = 50) -> list:
    """
    Fetch constituent symbols for any NSE index via the live API.
    index_slug examples: 'NIFTY%2050', 'NIFTY%20SMALLCAP%20100', 'NIFTY%20MIDCAP%20100'
    """
    cached = cache_get(cache_key)
    if cached:
        return cached

    try:
        session = nse_session()
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={index_slug}"
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        symbols = [
            item["symbol"] + ".NS"
            for item in data
            if item.get("symbol") and "NIFTY" not in item.get("symbol", "")
        ][:limit]

        if symbols:
            cache_set(cache_key, symbols)
            logger.info(f"[NSE] {cache_key}: {len(symbols)} symbols fetched")
            return symbols

    except Exception as e:
        logger.warning(f"[NSE] Failed to fetch {index_slug}: {e}")

    return []


def get_nifty50_symbols() -> list:
    return fetch_index_symbols("NIFTY%2050", "nifty50")


def get_smallcap_symbols() -> list:
    return fetch_index_symbols("NIFTY%20SMALLCAP%20100", "smallcap")


def get_midcap_symbols() -> list:
    return fetch_index_symbols("NIFTY%20MIDCAP%20100", "midcap")

def get_all_nse_equity_symbols(limit: Optional[int] = None) -> list:
    """
    Fetch all NSE equity symbols (EQ series) from the public CSV.
    Returns yfinance-compatible tickers with '.NS' suffix.
    """
    cache_key = "nse_equity_list_eq"
    cached = cache_get(cache_key, ttl_seconds=24 * 60 * 60)
    if cached:
        return cached[:limit] if limit else cached

    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    try:
        session = nse_session()
        tickers: list[str] = []
        seen: set[str] = set()
        with session.get(url, timeout=20, stream=True) as resp:
            resp.raise_for_status()
            reader = csv.DictReader(resp.iter_lines(decode_unicode=True))
            for row in reader:
                sym = (row.get("SYMBOL") or "").strip()
                if not sym:
                    continue
                series = (row.get("SERIES") or "").strip().upper()
                if series and series != "EQ":
                    continue
                if sym in seen:
                    continue
                seen.add(sym)
                tickers.append(f"{sym}.NS")
                if limit and len(tickers) >= limit:
                    break

        # Cache only the full list (not truncated) to avoid surprises.
        if tickers and not limit:
            cache_set(cache_key, tickers)
        return tickers
    except Exception as e:
        logger.warning(f"[NSE] Failed to fetch equity list: {e}")
        return []


def get_symbols_for_universe(universe: str) -> list:
    if universe == UNIVERSE_ALL:
        max_symbols_env = os.getenv("MAX_SYMBOLS", "").strip()
        limit = int(max_symbols_env) if max_symbols_env.isdigit() else None
        return get_all_nse_equity_symbols(limit=limit)
    return get_nifty50_symbols()


# ─── LIVE NSE MARKET DATA ────────────────────────────────────────────────────

def fetch_nse_live_quotes(index_slug: str = "NIFTY%2050") -> list:
    """
    Fetch full live quote data (price, change, high, low, volume)
    directly from NSE for all constituents of an index.
    """
    cached = cache_get(f"live_{index_slug}")
    if cached:
        return cached

    try:
        session = nse_session()
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={index_slug}"
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        raw = resp.json().get("data", [])

        quotes = []
        for item in raw:
            sym = item.get("symbol", "")
            if not sym or "NIFTY" in sym:
                continue
            try:
                quotes.append({
                    "symbol": sym,
                    "price":  round(float(item.get("lastPrice", 0)), 2),
                    "change": round(float(item.get("pChange", 0)), 2),
                    "open":   round(float(item.get("open", 0)), 2),
                    "high":   round(float(item.get("dayHigh", 0)), 2),
                    "low":    round(float(item.get("dayLow", 0)), 2),
                    "volume": int(item.get("totalTradedVolume", 0)),
                    "52w_high": round(float(item.get("yearHigh", 0)), 2),
                    "52w_low":  round(float(item.get("yearLow", 0)), 2),
                })
            except (ValueError, TypeError):
                continue

        cache_set(f"live_{index_slug}", quotes)
        logger.info(f"[NSE] Live quotes fetched: {len(quotes)} stocks")
        return quotes

    except Exception as e:
        logger.warning(f"[NSE] Live quotes failed: {e}")
        return []


# ─── OPTIONS OI (NIFTY) ───────────────────────────────────────────────────────

OI_CACHE_TTL = int(os.getenv("OI_CACHE_TTL", "60") or "60")  # seconds
OI_TOP_N = int(os.getenv("OI_TOP_N", "5") or "5")
INDEX_CACHE_TTL = int(os.getenv("INDEX_CACHE_TTL", "30") or "30")  # seconds


def fetch_nifty50_index_snapshot() -> Optional[dict]:
    """
    Lightweight index snapshot used for "% change" next to OI data.
    Uses NSE `allIndices` because it returns the index row directly.
    """
    cache_key = "idx_nifty50_snap"
    cached = cache_get(cache_key, ttl_seconds=INDEX_CACHE_TTL)
    if cached is not None:
        return cached

    try:
        session = nse_session()
        url = "https://www.nseindia.com/api/allIndices"
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        payload = resp.json() or {}
        rows = payload.get("data") or payload.get("dataList") or []
        for r in rows:
            name = (r.get("index") or r.get("indexName") or "").strip().upper()
            if name == "NIFTY 50":
                snap = {
                    "last": r.get("last") or r.get("lastPrice"),
                    "change": r.get("change"),
                    "pChange": r.get("pChange"),
                    "time": r.get("timeVal") or r.get("timestamp"),
                }
                cache_set(cache_key, snap)
                return snap
    except Exception as e:
        logger.warning(f"[NSE] allIndices snapshot failed: {e}")

    return None


def fetch_nse_derivatives_nextapi(symbol: str = "NIFTY") -> Optional[dict]:
    """
    NSE NextApi endpoint (as provided by user) for derivatives/option data.
    """
    cache_key = f"oc_nextapi_{symbol}"
    cached = cache_get(cache_key, ttl_seconds=OI_CACHE_TTL)
    if cached is not None:
        return cached

    symbol = (symbol or "NIFTY").strip().upper()
    url = (
        "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
        f"?functionName=getSymbolDerivativesData&symbol={symbol}"
    )
    last_err = None
    for attempt in (1, 2, 3):
        try:
            if attempt == 3:
                session = nse_fresh_session()
                prime_nse_option_chain_session(session)
            else:
                session = nse_session()
                if attempt == 2:
                    prime_nse_option_chain_session(session)
            hdrs = option_chain_headers(symbol)
            resp = session.get(
                url,
                timeout=15,
                headers=hdrs,
            )
            if resp.status_code in (401, 403):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception:
                snippet = (resp.text or "")[:160].replace("\n", " ").replace("\r", " ")
                ctype = (resp.headers.get("content-type") or "").lower()
                raise ValueError(f"Non-JSON response (status={resp.status_code}, ctype={ctype or 'unknown'}): {snippet}")
            if not isinstance(data, dict):
                raise ValueError(f"Unexpected option-chain payload type: {type(data).__name__}")

            cache_set(cache_key, data)
            return data
        except Exception as e:
            last_err = e
            if attempt == 1:
                reset_nse_session()
                continue
            if attempt == 2:
                reset_nse_session()
                continue
            break

    logger.warning(f"[NSE] NextApi derivatives failed ({symbol}): {last_err}")
    return {"error": "fetch_failed", "detail": str(last_err), "symbol": symbol}


def fetch_nse_option_chain_indices(symbol: str = "NIFTY") -> Optional[dict]:
    """
    NSE classic option-chain API (fallback).
    """
    cache_key = f"oc_idx_{symbol}"
    cached = cache_get(cache_key, ttl_seconds=OI_CACHE_TTL)
    if cached is not None:
        return cached

    symbol = (symbol or "NIFTY").strip().upper()
    url = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
    last_err = None
    for attempt in (1, 2, 3):
        try:
            if attempt == 3:
                session = nse_fresh_session()
                prime_nse_option_chain_session(session)
            else:
                session = nse_session()
                if attempt == 2:
                    prime_nse_option_chain_session(session)

            resp = session.get(url, timeout=15, headers=option_chain_headers(symbol))
            if resp.status_code in (401, 403):
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            try:
                data = resp.json()
            except Exception:
                snippet = (resp.text or "")[:160].replace("\n", " ").replace("\r", " ")
                ctype = (resp.headers.get("content-type") or "").lower()
                raise ValueError(f"Non-JSON response (status={resp.status_code}, ctype={ctype or 'unknown'}): {snippet}")
            if not isinstance(data, dict):
                raise ValueError(f"Unexpected option-chain payload type: {type(data).__name__}")

            # NSE sometimes returns only "filtered" (or returns a message/error object).
            if "records" not in data and isinstance(data.get("filtered"), dict):
                data = dict(data)
                data["records"] = data["filtered"]

            if "records" not in data:
                cache_set(cache_key, data)
                if not data:
                    raise ValueError("Empty JSON payload (likely blocked)")
                msg = data.get("message") or data.get("error") or data.get("msg") or f"keys={list(data)[:10]}"
                raise ValueError(f"Missing records in payload ({msg})")

            cache_set(cache_key, data)
            return data
        except Exception as e:
            last_err = e
            if attempt in (1, 2):
                reset_nse_session()
                continue
            break

    logger.warning(f"[NSE] Option chain failed ({symbol}): {last_err}")
    return {"error": "fetch_failed", "detail": str(last_err), "symbol": symbol}


def nifty_oi_summary(top_n: int = OI_TOP_N) -> Optional[dict]:
    def _to_int(v) -> int:
        try:
            if v is None:
                return 0
            if isinstance(v, bool):
                return int(v)
            if isinstance(v, int):
                return v
            if isinstance(v, float):
                return int(v)
            if isinstance(v, str):
                s = v.replace(",", "").strip()
                if not s or s.upper() in ("NA", "N/A") or s in ("-", "—"):
                    return 0
                return int(float(s))
            return int(v)
        except Exception:
            return 0

    def _to_float(v) -> float:
        try:
            if v is None:
                return 0.0
            if isinstance(v, bool):
                return float(int(v))
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                s = v.replace(",", "").strip()
                if not s or s.upper() in ("NA", "N/A") or s in ("-", "—"):
                    return 0.0
                return float(s)
            return float(v)
        except Exception:
            return 0.0

    def _select_band(rows: list, target: float, count: int, direction: str) -> list:
        """
        Select `count` strikes starting from nearest to target.
        direction: "up" or "down"
        """
        if not rows:
            return []
        strikes = sorted({float(r["strike"]) for r in rows if r.get("strike") is not None})
        if not strikes:
            return []
        # find nearest strike to target
        nearest = min(strikes, key=lambda s: abs(s - target))
        try:
            idx = strikes.index(nearest)
        except ValueError:
            idx = 0
        # Filter to 100-interval strikes if possible
        filt = [s for s in strikes if (int(round(s)) % 100) == 0]
        if filt:
            strikes = filt
            try:
                idx = strikes.index(min(strikes, key=lambda s: abs(s - target)))
            except ValueError:
                idx = 0
        if direction == "up":
            sel = strikes[idx: idx + count]
        else:
            start = max(0, idx - (count - 1))
            sel = strikes[start: idx + 1]
            sel = list(reversed(sel))
        # map back to rows with CE/PE dict
        by_strike = {float(r["strike"]): r for r in rows if r.get("strike") is not None}
        return [by_strike[s] for s in sel if s in by_strike]

    idx = fetch_nifty50_index_snapshot()

    def _walk(obj):
        stack = [obj]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                yield cur
                for v in cur.values():
                    stack.append(v)
            elif isinstance(cur, list):
                for v in cur:
                    stack.append(v)

    def _parse_expiry(s: str):
        if not s or not isinstance(s, str):
            return None
        s = s.strip()
        for fmt in ("%d-%b-%Y", "%d-%b-%y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass
        return None

    def _extract_from_option_chain_indices(payload: dict):
        records = payload.get("records") or {}
        if not isinstance(records, dict):
            return None
        chain = records.get("data")
        if not isinstance(chain, list) or not chain:
            filtered = payload.get("filtered")
            if isinstance(filtered, dict) and isinstance(filtered.get("data"), list):
                chain = filtered.get("data")
            else:
                chain = []
        if not chain:
            return None
        ts_ = records.get("timestamp") or ""
        und_ = records.get("underlyingValue")
        return chain, ts_, und_

    def _extract_from_nextapi(payload: dict):
        # Find contracts either as rows with CE/PE or as flat dicts with optionType/strikePrice.
        candidates = []
        for d in _walk(payload):
            if "strikePrice" in d and ("CE" in d or "PE" in d or "optionType" in d or "instrumentType" in d):
                candidates.append(d)
            elif "optionType" in d and ("openInterest" in d or "changeinOpenInterest" in d) and ("strikePrice" in d or "strike" in d):
                candidates.append(d)

        if not candidates:
            return None

        # If these are already CE/PE rows, return them as-is.
        if any(("CE" in c or "PE" in c) and "strikePrice" in c for c in candidates):
            rows = [c for c in candidates if "strikePrice" in c and ("CE" in c or "PE" in c)]
            ts_ = None
            und_ = None
            for d in _walk(payload):
                if und_ is None and any(k in d for k in ("underlyingValue", "underlying")):
                    und_ = d.get("underlyingValue") or d.get("underlying")
                if ts_ is None and any(k in d for k in ("timestamp", "time", "timeStamp", "lastUpdateTime")):
                    ts_ = d.get("timestamp") or d.get("time") or d.get("timeStamp") or d.get("lastUpdateTime")
            return rows, ts_ or "", und_

        # Otherwise treat as flat contracts and aggregate by strike.
        flat = []
        for c in candidates:
            if "optionType" in c and ("strikePrice" in c or "strike" in c):
                flat.append(c)
        if not flat:
            return None

        # Filter to nearest expiry if possible.
        expiry_dates = []
        for c in flat:
            exp = c.get("expiryDate") or c.get("expiry") or c.get("expirydate")
            d = _parse_expiry(exp) if isinstance(exp, str) else None
            if d:
                expiry_dates.append(d)
        sel_exp = min(expiry_dates) if expiry_dates else None

        by_strike = {}
        for c in flat:
            exp = c.get("expiryDate") or c.get("expiry") or c.get("expirydate")
            d = _parse_expiry(exp) if isinstance(exp, str) else None
            if sel_exp and d and d != sel_exp:
                continue
            strike = c.get("strikePrice") or c.get("strike")
            if strike is None:
                continue
            try:
                strike_val = float(strike)
            except Exception:
                continue
            otype = (c.get("optionType") or c.get("otype") or "").strip().upper()
            if otype not in ("CE", "PE", "CALL", "PUT"):
                inst = str(c.get("instrumentType") or "").upper()
                if "CE" in inst:
                    otype = "CE"
                elif "PE" in inst:
                    otype = "PE"
            side = "CE" if otype in ("CE", "CALL") else ("PE" if otype in ("PE", "PUT") else None)
            if not side:
                continue
            row = by_strike.setdefault(strike_val, {"strikePrice": strike_val})
            row[side] = c

        rows = list(by_strike.values())
        ts_ = None
        und_ = None
        for d in _walk(payload):
            if und_ is None and any(k in d for k in ("underlyingValue", "underlying", "underlyingIndex")):
                und_ = d.get("underlyingValue") or d.get("underlying") or d.get("underlyingIndex")
            if ts_ is None and any(k in d for k in ("timestamp", "time", "timeStamp", "lastUpdateTime")):
                ts_ = d.get("timestamp") or d.get("time") or d.get("timeStamp") or d.get("lastUpdateTime")
        return rows, ts_ or "", und_

    oc = fetch_nse_derivatives_nextapi("NIFTY")
    source = "nextapi"
    if not oc or (isinstance(oc, dict) and oc.get("error")):
        oc = fetch_nse_option_chain_indices("NIFTY")
        source = "optionchain"

    if not oc:
        return {"error": "fetch_failed", "index": idx}
    if isinstance(oc, dict) and oc.get("error"):
        out = dict(oc)
        out["index"] = idx
        return out

    extracted = _extract_from_nextapi(oc) if source == "nextapi" else _extract_from_option_chain_indices(oc)
    if not extracted:
        return {"error": "empty_chain", "index": idx, "source": source, "keys": list(oc)[:10] if isinstance(oc, dict) else None}
    chain, timestamp, underlying = extracted
    if not timestamp and isinstance(idx, dict):
        timestamp = idx.get("time") or ""
    if underlying is None and isinstance(idx, dict):
        underlying = idx.get("last")

    total_ce_oi = 0
    total_pe_oi = 0
    total_ce_coi = 0
    total_pe_coi = 0

    ce_rows = []
    pe_rows = []

    def _pick(d: dict, keys: list[str]):
        for k in keys:
            if k in d:
                return d.get(k)
        return None

    for row in chain:
        strike = row.get("strikePrice")
        if strike is None:
            continue

        ce = row.get("CE") or row.get("ce")
        if isinstance(ce, dict):
            oi = _to_int(_pick(ce, ["openInterest", "oi", "open_int"]))
            coi = _to_int(_pick(ce, ["changeinOpenInterest", "changeInOpenInterest", "changeInOI", "coi"]))
            ltp = _to_float(_pick(ce, ["lastPrice", "ltp", "last"]))
            pchg = _to_float(_pick(ce, ["pChange", "pchange", "pctChange", "%Change"]))
            total_ce_oi += oi
            total_ce_coi += coi
            ce_rows.append(
                {
                    "strike": strike,
                    "oi": oi,
                    "coi": coi,
                    "coi_pct": (coi / oi * 100.0) if oi else 0.0,
                    "ltp": ltp,
                    "pchg": pchg,
                }
            )

        pe = row.get("PE") or row.get("pe")
        if isinstance(pe, dict):
            oi = _to_int(_pick(pe, ["openInterest", "oi", "open_int"]))
            coi = _to_int(_pick(pe, ["changeinOpenInterest", "changeInOpenInterest", "changeInOI", "coi"]))
            ltp = _to_float(_pick(pe, ["lastPrice", "ltp", "last"]))
            pchg = _to_float(_pick(pe, ["pChange", "pchange", "pctChange", "%Change"]))
            total_pe_oi += oi
            total_pe_coi += coi
            pe_rows.append(
                {
                    "strike": strike,
                    "oi": oi,
                    "coi": coi,
                    "coi_pct": (coi / oi * 100.0) if oi else 0.0,
                    "ltp": ltp,
                    "pchg": pchg,
                }
            )

    underlying_val = _to_float(underlying)
    band_ce = _select_band(ce_rows, underlying_val + 100.0, 5, "up")
    band_pe = _select_band(pe_rows, underlying_val - 100.0, 5, "down")

    if total_ce_oi == 0 and total_pe_oi == 0:
        return {"error": "no_oi_fields", "index": idx, "timestamp": timestamp, "underlying": underlying}
    if total_ce_oi == 0 and total_pe_oi > 0:
        return {"error": "missing_call_oi", "index": idx, "timestamp": timestamp, "underlying": underlying}
    if total_pe_oi == 0 and total_ce_oi > 0:
        return {"error": "missing_put_oi", "index": idx, "timestamp": timestamp, "underlying": underlying}

    top_ce = sorted(ce_rows, key=lambda x: x["oi"], reverse=True)[: max(1, top_n)]
    top_pe = sorted(pe_rows, key=lambda x: x["oi"], reverse=True)[: max(1, top_n)]

    pcr_oi = (total_pe_oi / total_ce_oi) if total_ce_oi else None
    pcr_coi = (total_pe_coi / total_ce_coi) if total_ce_coi else None

    return {
        "timestamp": timestamp,
        "underlying": underlying,
        "index": idx,
        "source": source,
        "pcr_oi": pcr_oi,
        "pcr_coi": pcr_coi,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
        "total_ce_coi": total_ce_coi,
        "total_pe_coi": total_pe_coi,
        "top_ce": top_ce,
        "top_pe": top_pe,
        "band_ce": band_ce,
        "band_pe": band_pe,
    }


# ─── OHLCV FOR INDICATORS ────────────────────────────────────────────────────

def fetch_ohlcv_bulk(symbols: list, period: str = "6mo") -> dict:
    """Bulk download OHLCV from yfinance for indicator computation."""
    if not symbols:
        return {}
    result = {}
    for sym, df in iter_ohlcv(symbols, period=period, interval="1d"):
        if df is None or df.empty or len(df) <= 20:
            continue
        result[sym] = df
    return result


def _chunks(items: list, size: int):
    if size <= 0:
        size = 50
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _heap_top_n_push(heap: list, score: float, item: dict, n: int):
    if n <= 0:
        return
    entry = (float(score), item)
    if len(heap) < n:
        heapq.heappush(heap, entry)
        return
    if entry[0] > heap[0][0]:
        heapq.heapreplace(heap, entry)


YF_CACHE_TTL = int(os.getenv("YF_CACHE_TTL", "120") or "120")  # seconds
YF_CACHE_MAX = int(os.getenv("YF_CACHE_MAX", "2") or "2")      # cached chunks
YF_THREADS = os.getenv("YF_THREADS", "0").strip().lower() not in ("0", "false", "no", "off")
YF_GC_EVERY = int(os.getenv("YF_GC_EVERY", "0") or "0")        # 0 = never; else every N chunks

_YF_CACHE: dict = {}
_YF_CACHE_LOCK = Lock()
_YF_CACHE_ORDER: list[tuple] = []


def _yf_cache_get(key):
    if YF_CACHE_TTL <= 0:
        return None
    with _YF_CACHE_LOCK:
        entry = _YF_CACHE.get(key)
        if not entry:
            return None
        df, fetched_at = entry
        if (time.monotonic() - fetched_at) >= YF_CACHE_TTL:
            _YF_CACHE.pop(key, None)
            try:
                _YF_CACHE_ORDER.remove(key)
            except ValueError:
                pass
            return None
        return df


def _yf_cache_set(key, df):
    if YF_CACHE_MAX <= 0 or YF_CACHE_TTL <= 0:
        return
    with _YF_CACHE_LOCK:
        if key in _YF_CACHE:
            _YF_CACHE[key] = (df, time.monotonic())
            return
        _YF_CACHE[key] = (df, time.monotonic())
        _YF_CACHE_ORDER.append(key)
        while len(_YF_CACHE_ORDER) > YF_CACHE_MAX:
            old = _YF_CACHE_ORDER.pop(0)
            _YF_CACHE.pop(old, None)


def _yf_download_cached(tickers: list[str], period: str, interval: str):
    tickers_sorted = tuple(sorted(tickers))
    key = ("yf", tickers_sorted, period, interval, bool(YF_THREADS))
    cached = _yf_cache_get(key)
    if cached is not None:
        return cached

    with log_timing(f"yfinance.download {len(tickers_sorted)} {period} {interval}"):
        df = yf.download(
            " ".join(tickers_sorted),
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=YF_THREADS,
        )
    _yf_cache_set(key, df)
    return df


def iter_ohlcv(symbols: list, period: str, interval: str = "1d"):
    """
    Stream OHLCV data from yfinance in chunks to avoid large peak memory usage.
    Yields (symbol, df) where symbol includes '.NS' suffix.
    """
    if not symbols:
        return

    chunk_count = 0
    for chunk in _chunks(symbols, YF_CHUNK_SIZE):
        chunk_count += 1
        try:
            df = _yf_download_cached(chunk, period=period, interval=interval)
        except Exception as e:
            logger.error(f"Bulk OHLCV error (chunk): {e}")
            continue

        try:
            if len(chunk) == 1:
                sym = chunk[0]
                if df is not None and not df.empty:
                    yield sym, df
            else:
                for sym in chunk:
                    try:
                        sub = df[sym].dropna(how="all")
                        yield sym, sub
                    except Exception:
                        yield sym, None
        finally:
            # Help GC release large intermediate frames sooner on low-memory hosts.
            del df
            if YF_GC_EVERY and (chunk_count % YF_GC_EVERY == 0):
                gc.collect()


# ─── SCREENERS ───────────────────────────────────────────────────────────────


def screen_sma(sma_period: int, symbols: Optional[list] = None) -> list:
    """
    Screener:
    - For 10 SMA: close is above SMA(10) AND close is >= 20% below the recent high (pullback)
    - For other periods: close is above SMA(period)
    - Volume: today's volume is above the average volume of the previous 14 trading days

    Recent high is computed from the last ~1y of daily candles fetched from yfinance (adjusted OHLCV).
    """
    symbols = symbols or get_nifty50_symbols()
    if not symbols:
        return []

    results = []

    # 1y is needed for 10-SMA pullback high; for 100-SMA, 6mo is often sufficient.
    period = "1y" if sma_period == 10 else os.getenv("SMA_PERIOD", "6mo").strip() or "6mo"
    for sym, df in iter_ohlcv(symbols, period=period, interval="1d"):
        if df is None or df.empty:
            continue
        try:
            close = df["Close"].squeeze()
            if len(close) < sma_period + 2:
                continue

            sma = close.rolling(window=sma_period).mean()
            if pd.isna(sma.iloc[-1]):
                continue

            last_close = float(close.iloc[-1])
            prev_close = float(close.iloc[-2])
            last_sma = float(sma.iloc[-1])

            if not price_ok(last_close):
                continue

            if last_close <= last_sma:
                continue

            # Volume filter: last volume must be above avg of previous 14 sessions (exclude current).
            vol_series = df.get("Volume")
            if vol_series is None:
                continue
            vol_series = vol_series.squeeze()
            if len(vol_series) < 15:
                continue
            last_vol = float(vol_series.iloc[-1])
            avg_vol14 = float(vol_series.iloc[-15:-1].mean())
            if pd.isna(last_vol) or pd.isna(avg_vol14) or avg_vol14 <= 0:
                continue
            if last_vol <= avg_vol14:
                continue

            # Apply the pullback constraint only for the 10-SMA screener.
            recent_high = None
            drawdown = None
            if sma_period == 10:
                # Use the adjusted "High" series for a practical "recent high".
                high_series = df.get("High")
                if high_series is None:
                    continue
                recent_high = float(high_series.iloc[-252:].max())  # ~1 trading year
                if not recent_high or pd.isna(recent_high):
                    continue

                drawdown = (recent_high - last_close) / recent_high  # 0.20 = 20% down from high
                if drawdown < 0.20:
                    continue

            change = ((last_close - prev_close) / prev_close) * 100 if prev_close else 0.0
            row = {
                "symbol": sym.replace(".NS", ""),
                "price": round(last_close, 2),
                "sma": round(last_sma, 2),
                "change": round(float(change), 2),
                "vol_ratio": round(last_vol / avg_vol14, 2),
            }
            if sma_period == 10 and recent_high is not None and drawdown is not None:
                row["recent_high"] = round(float(recent_high), 2)
                row["drawdown_pct"] = round(float(drawdown) * 100.0, 1)

            results.append(row)
        except Exception as e:
            logger.warning(f"SMA error {sym}: {e}")

    if sma_period == 10:
        # Prefer deeper pullbacks first; then day change.
        results.sort(key=lambda x: (x.get("drawdown_pct", 0), x.get("change", 0)), reverse=True)
    else:
        results.sort(key=lambda x: x.get("change", 0), reverse=True)
    return results[:RESULT_LIMIT]


def screen_close_above_prev_week(symbols: Optional[list] = None) -> list:
    """
    Screener:
    - Latest daily close is strictly above the *previous completed week's* High.
    - Volume: today's volume is above the average volume of the previous 14 trading days.

    Notes:
    - "Previous week" uses Fri-ending weekly buckets ("W-FRI") so Mon–Fri is one week.
    - If the current week is in-progress (Mon–Thu), the previous completed week is the 2nd last bucket.
    """
    symbols = symbols or get_nifty50_symbols()
    if not symbols:
        return []

    results = []
    period = os.getenv("PREV_WEEK_PERIOD", "6mo").strip() or "6mo"

    for sym, df in iter_ohlcv(symbols, period=period, interval="1d"):
        if df is None or df.empty:
            continue
        try:
            close_series = df.get("Close")
            high_series = df.get("High")
            if close_series is None or high_series is None:
                continue

            close_series = close_series.squeeze()
            high_series = high_series.squeeze()
            if len(close_series) < 15 or len(high_series) < 15:
                continue

            last_close = float(close_series.iloc[-1])
            prev_close = float(close_series.iloc[-2])
            if not price_ok(last_close):
                continue

            # Volume filter: last volume must be above avg of previous 14 sessions (exclude current).
            vol_series = df.get("Volume")
            if vol_series is None:
                continue
            vol_series = vol_series.squeeze()
            if len(vol_series) < 15:
                continue
            last_vol = float(vol_series.iloc[-1])
            avg_vol14 = float(vol_series.iloc[-15:-1].mean())
            if pd.isna(last_vol) or pd.isna(avg_vol14) or avg_vol14 <= 0:
                continue
            if last_vol <= avg_vol14:
                continue

            weekly = pd.DataFrame({"High": high_series, "Close": close_series}).dropna(how="any")
            weekly.index = pd.to_datetime(weekly.index, errors="coerce")
            weekly = weekly.dropna(axis=0, subset=["High", "Close"])
            if getattr(weekly.index, "tz", None) is not None:
                weekly.index = weekly.index.tz_convert(None)

            weekly = weekly.resample("W-FRI").agg({"High": "max", "Close": "last"}).dropna(how="any")
            if len(weekly) < 2:
                continue
            prev_week_high = float(weekly["High"].iloc[-2])
            if pd.isna(prev_week_high) or prev_week_high <= 0:
                continue

            if last_close <= prev_week_high:
                continue

            change = ((last_close - prev_close) / prev_close) * 100 if prev_close else 0.0
            results.append(
                {
                    "symbol": sym.replace(".NS", ""),
                    "price": round(last_close, 2),
                    "prev_week_high": round(prev_week_high, 2),
                    "change": round(float(change), 2),
                    "vol_ratio": round(last_vol / avg_vol14, 2),
                }
            )
        except Exception as e:
            logger.warning(f"Prev-week error {sym}: {e}")

    results.sort(key=lambda x: x.get("change", 0), reverse=True)
    return results[:RESULT_LIMIT]

def fmt_activity(results: list, kind: str, universe_label: str) -> str:
    if kind == "value":
        title = "🚀 Active Value"
        key_label = "Value"
        sort_key = "value"
    else:
        title = "🚀 Active Vol"
        key_label = "Vol"
        sort_key = "volume"

    if not results:
        return f"❌ Could not fetch {title} data."

    lines = [f"*{title}* — {universe_label}\n"]
    for i, r in enumerate(results[:RESULT_LIMIT], 1):
        price = r.get("price", "N/A")
        volume = r.get("volume")
        value = r.get("value")

        if isinstance(volume, (int, float)):
            vol_txt = f"{int(volume):,}"
        else:
            vol_txt = "N/A"

        if isinstance(value, (int, float)):
            val_txt = f"₹{float(value):,.0f}"
        else:
            val_txt = "N/A"

        lines.append(f"{i}. *{r.get('symbol','')}*  ₹{price}  Vol `{vol_txt}`  Val `{val_txt}`")
    lines.append(f"\n_NSE/yfinance · {ts()}_")
    return "\n".join(lines)


def screen_rsi(oversold: bool = True, symbols: Optional[list] = None) -> list:
    """
    1. Fetch live Nifty 50 symbols from NSE
    2. Download OHLCV from yfinance
    3. Compute RSI(14) — return oversold < 30 or overbought > 70
    """
    symbols = symbols or get_nifty50_symbols()
    if not symbols:
        return []

    results = []

    for sym, df in iter_ohlcv(symbols, period="6mo", interval="1d"):
        if df is None or df.empty:
            continue
        try:
            close      = df["Close"].squeeze()
            last_close = float(close.iloc[-1])
            if not price_ok(last_close):
                continue

            rsi_series = ta.momentum.RSIIndicator(close=close, window=14).rsi()
            rsi_val    = float(rsi_series.iloc[-1])
            if pd.isna(rsi_val):
                continue
            triggered = (rsi_val < 30) if oversold else (rsi_val > 70)
            if triggered:
                change = ((close.iloc[-1] - close.iloc[-2]) / close.iloc[-2]) * 100
                results.append({
                    "symbol": sym.replace(".NS", ""),
                    "price":  round(last_close, 2),
                    "rsi":    round(rsi_val, 1),
                    "change": round(float(change), 2),
                })
        except Exception as e:
            logger.warning(f"RSI error {sym}: {e}")

    results.sort(key=lambda x: x["rsi"], reverse=not oversold)
    return results[:RESULT_LIMIT]


def screen_top_movers(top_n: int = RESULT_LIMIT, gainers: bool = True) -> list:
    """
    Fetch live NSE quotes → sort by % change → return top N.
    Falls back to yfinance OHLCV if NSE API is unavailable.
    """
    quotes = fetch_nse_live_quotes("NIFTY%2050")

    if quotes:
        quotes = [q for q in quotes if price_ok(q.get("price"))]
        quotes.sort(key=lambda x: x["change"], reverse=gainers)
        return quotes[:top_n]

    # yfinance fallback
    symbols = get_nifty50_symbols()
    if not symbols:
        return []

    fallback = []
    for sym, df in iter_ohlcv(symbols, period="5d", interval="1d"):
        if df is None or df.empty:
            continue
        try:
            c0 = float(df["Close"].iloc[-1])
            c1 = float(df["Close"].iloc[-2])
            if not price_ok(c0):
                continue
            fallback.append({
                "symbol": sym.replace(".NS", ""),
                "price":  round(c0, 2),
                "change": round(((c0 - c1) / c1) * 100, 2),
            })
        except Exception:
            pass

    fallback.sort(key=lambda x: x["change"], reverse=gainers)
    return fallback[:top_n]
def screen_active_vol(symbols: list, universe: str) -> list:
    """
    Top active volume.
    - Nifty 50: uses NSE live quotes (fast)
    - All Stocks: uses yfinance last day volume (chunked)
    """
    heap = []

    if universe == UNIVERSE_NIFTY50:
        quotes = fetch_nse_live_quotes("NIFTY%2050")
        for q in quotes:
            try:
                price = float(q.get("price", 0))
                volume = float(q.get("volume", 0))
            except (TypeError, ValueError):
                continue
            if not price_ok(price) or volume <= 0:
                continue
            traded_value = price * volume
            item = {
                "symbol": str(q.get("symbol", "")).strip(),
                "price": round(price, 2),
                "volume": int(volume),
                "value": traded_value,
            }
            _heap_top_n_push(heap, volume, item, RESULT_LIMIT)
    else:
        for sym, df in iter_ohlcv(symbols, period="5d", interval="1d"):
            if df is None or df.empty:
                continue
            try:
                close = float(df["Close"].iloc[-1])
                volume = float(df["Volume"].iloc[-1])
            except Exception:
                continue
            if not price_ok(close) or volume <= 0:
                continue
            traded_value = close * volume
            item = {
                "symbol": sym.replace(".NS", ""),
                "price": round(close, 2),
                "volume": int(volume),
                "value": traded_value,
            }
            _heap_top_n_push(heap, volume, item, RESULT_LIMIT)

    return [it for _, it in sorted(heap, key=lambda x: x[0], reverse=True)]


def screen_active_value(symbols: list, universe: str) -> list:
    """
    Top active traded value (approx): price * volume.
    - Nifty 50: uses NSE live quotes (fast)
    - All Stocks: uses yfinance last day close * volume (chunked)
    """
    heap = []

    if universe == UNIVERSE_NIFTY50:
        quotes = fetch_nse_live_quotes("NIFTY%2050")
        for q in quotes:
            try:
                price = float(q.get("price", 0))
                volume = float(q.get("volume", 0))
            except (TypeError, ValueError):
                continue
            if not price_ok(price) or volume <= 0:
                continue
            traded_value = price * volume
            item = {
                "symbol": str(q.get("symbol", "")).strip(),
                "price": round(price, 2),
                "volume": int(volume),
                "value": traded_value,
            }
            _heap_top_n_push(heap, traded_value, item, RESULT_LIMIT)
    else:
        for sym, df in iter_ohlcv(symbols, period="5d", interval="1d"):
            if df is None or df.empty:
                continue
            try:
                close = float(df["Close"].iloc[-1])
                volume = float(df["Volume"].iloc[-1])
            except Exception:
                continue
            if not price_ok(close) or volume <= 0:
                continue
            traded_value = close * volume
            item = {
                "symbol": sym.replace(".NS", ""),
                "price": round(close, 2),
                "volume": int(volume),
                "value": traded_value,
            }
            _heap_top_n_push(heap, traded_value, item, RESULT_LIMIT)

    return [it for _, it in sorted(heap, key=lambda x: x[0], reverse=True)]


def screen_fundamentals(cap_type: str) -> list:
    """
    1. Fetch live small/mid cap index symbols from NSE
    2. Pull fundamental data per ticker via yfinance
    """
    symbols = get_smallcap_symbols() if cap_type == "small" else get_midcap_symbols()
    if not symbols:
        return []

    fund_ttl = int(os.getenv("FUND_CACHE_TTL", "21600") or "21600")  # 6 hours
    workers = int(os.getenv("FUND_WORKERS", "5") or "5")

    def fetch_one(sym: str):
        cached = cache_get(f"fund_{sym}", ttl_seconds=fund_ttl)
        if cached is not None:
            return cached

        try:
            with log_timing(f"yfinance.info {sym}"):
                info = yf.Ticker(sym).info
            if not info:
                return None

            name = info.get("shortName") or info.get("longName") or sym.replace(".NS", "")
            price = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            if not price_ok(price):
                return None
            mktcap = info.get("marketCap") or 0
            pe = info.get("trailingPE")
            pb = info.get("priceToBook")
            roe = info.get("returnOnEquity")
            eps = info.get("trailingEps")
            div = info.get("dividendYield")
            sector = info.get("sector") or "—"

            row = {
                "symbol": sym.replace(".NS", ""),
                "name": name[:22],
                "sector": sector[:18],
                "price": round(price, 2) if price else "N/A",
                "mktcap": f"₹{mktcap/1e9:.1f}B" if mktcap else "N/A",
                "pe": round(pe, 1) if pe else "N/A",
                "pb": round(pb, 2) if pb else "N/A",
                "roe": f"{roe*100:.1f}%" if roe else "N/A",
                "eps": round(eps, 2) if eps else "N/A",
                "div": f"{div*100:.2f}%" if div else "N/A",
            }
            cache_set(f"fund_{sym}", row)
            return row
        except Exception as e:
            logger.warning(f"Fundamental error {sym}: {e}")
            return None

    results_by_pos: list[tuple[int, dict]] = []
    with log_timing(f"fundamentals {cap_type} ({len(symbols)} tickers)"):
        with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
            futures = {ex.submit(fetch_one, sym): i for i, sym in enumerate(symbols)}
            for fut in as_completed(futures):
                pos = futures[fut]
                row = fut.result()
                if row:
                    results_by_pos.append((pos, row))

    results_by_pos.sort(key=lambda x: x[0])
    results = [row for _, row in results_by_pos]
    return results[:RESULT_LIMIT]


def md_escape(text) -> str:
    if text is None:
        return ""
    s = str(text)
    for ch in ("\\", "_", "*", "`", "["):
        s = s.replace(ch, f"\\{ch}")
    return s


FO_CACHE_TTL = int(os.getenv("FO_CACHE_TTL", "3600") or "3600")


def _normalize_name(text: str) -> str:
    return "".join(ch.lower() for ch in text if ch.isalnum())


def fetch_fo_securities_details() -> list:
    """
    Fetch NSE "Securities in F&O" list with symbols and names for matching.
    """
    cache_key = "fo_securities_list"
    cached = cache_get(cache_key, ttl_seconds=FO_CACHE_TTL)
    if cached is not None:
        return cached

    try:
        session = nse_session()
        url = "https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O"
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        raw = resp.json().get("data", [])
        out = []
        for item in raw:
            sym = item.get("symbol")
            name = item.get("companyName") or item.get("company") or item.get("name")
            if sym:
                out.append({"symbol": str(sym).strip().upper(), "name": str(name or "").strip()})
        cache_set(cache_key, out)
        return out
    except Exception:
        return []


def match_symbol_from_fo(query: str) -> Optional[str]:
    qn = _normalize_name(query or "")
    if not qn:
        return None
    items = fetch_fo_securities_details()
    # First try exact symbol match
    for it in items:
        if _normalize_name(it.get("symbol", "")) == qn:
            return it.get("symbol")
    # Then try name contains
    for it in items:
        name = _normalize_name(it.get("name", ""))
        if qn and name and qn in name:
            return it.get("symbol")
    return None


def nse_autocomplete_symbol(query: str) -> Optional[str]:
    """
    Best-effort NSE autocomplete: lets users type a company name and we pick a symbol.
    Returns something like 'RELIANCE' (without .NS).
    """
    try:
        session = nse_session()
        url = "https://www.nseindia.com/api/search/autocomplete"
        resp = session.get(url, params={"q": query}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        candidates = []
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict):
            candidates = data.get("symbols") or data.get("data") or data.get("items") or []
        for c in candidates or []:
            if not isinstance(c, dict):
                continue
            sym = c.get("symbol") or c.get("symbolCode") or c.get("underlying") or c.get("underlyingSymbol")
            if sym:
                return str(sym).strip().upper()
    except Exception:
        return None
    return None


def fetch_nse_equity_quote(symbol: str) -> Optional[dict]:
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return None
    try:
        session = nse_session()
        url = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
        resp = session.get(
            url,
            timeout=15,
            headers={
                "Referer": f"https://www.nseindia.com/get-quotes/equity?symbol={symbol}",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/plain, */*",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def stock_info(query: str) -> Optional[dict]:
    """
    Single-stock info lookup. User can provide symbol (RELIANCE) or a company name.
    """
    q = (query or "").strip()
    if not q:
        return None

    symbol = q.upper().replace(".NS", "").strip()
    if " " in q or len(symbol) > 12:
        sym2 = nse_autocomplete_symbol(q)
        if sym2:
            symbol = sym2
        else:
            sym3 = match_symbol_from_fo(q)
            if sym3:
                symbol = sym3

    ticker = f"{symbol}.NS"
    info = {"symbol": symbol}

    nse = fetch_nse_equity_quote(symbol)
    if isinstance(nse, dict):
        price_info = nse.get("priceInfo") or {}
        meta = nse.get("metadata") or {}
        if isinstance(price_info, dict):
            info["price"] = price_info.get("lastPrice")
            info["change_pct"] = price_info.get("pChange")
            info["open"] = price_info.get("open")
            info["high"] = price_info.get("intraDayHighLow", {}).get("max") if isinstance(price_info.get("intraDayHighLow"), dict) else price_info.get("dayHigh")
            info["low"] = price_info.get("intraDayHighLow", {}).get("min") if isinstance(price_info.get("intraDayHighLow"), dict) else price_info.get("dayLow")
            info["52w_high"] = price_info.get("weekHighLow", {}).get("max") if isinstance(price_info.get("weekHighLow"), dict) else None
            info["52w_low"] = price_info.get("weekHighLow", {}).get("min") if isinstance(price_info.get("weekHighLow"), dict) else None
        if isinstance(meta, dict):
            info["name"] = meta.get("companyName") or meta.get("symbol") or info.get("name")
            info["industry"] = meta.get("industry") or info.get("industry")
            info["last_update"] = meta.get("lastUpdateTime") or meta.get("lastUpdate") or info.get("last_update")

    try:
        yf_t = yf.Ticker(ticker)
        fast = getattr(yf_t, "fast_info", None)
        if isinstance(fast, dict):
            info.setdefault("price", fast.get("last_price") or fast.get("lastPrice"))
            if "change_pct" not in info:
                prev_close = fast.get("previous_close") or fast.get("previousClose")
                last_price = fast.get("last_price") or fast.get("lastPrice")
                try:
                    if prev_close and last_price:
                        info["change_pct"] = (float(last_price) - float(prev_close)) / float(prev_close) * 100.0
                except Exception:
                    pass
            info.setdefault("open", fast.get("open"))
            info.setdefault("high", fast.get("day_high") or fast.get("dayHigh"))
            info.setdefault("low", fast.get("day_low") or fast.get("dayLow"))
            info.setdefault("52w_high", fast.get("year_high") or fast.get("yearHigh"))
            info.setdefault("52w_low", fast.get("year_low") or fast.get("yearLow"))
            info.setdefault("volume", fast.get("last_volume") or fast.get("lastVolume"))
            info.setdefault("mktcap", fast.get("market_cap") or fast.get("marketCap"))

        yfi = yf_t.info or {}
        if isinstance(yfi, dict) and yfi:
            info.setdefault("name", yfi.get("shortName") or yfi.get("longName"))
            info.setdefault("sector", yfi.get("sector"))
            info.setdefault("industry", yfi.get("industry"))
            info.setdefault("pe", yfi.get("trailingPE") or yfi.get("forwardPE"))
            info.setdefault("pb", yfi.get("priceToBook"))
            info.setdefault("eps", yfi.get("trailingEps"))
            info.setdefault("roe", yfi.get("returnOnEquity"))
            info.setdefault("div_yield", yfi.get("dividendYield"))
            info.setdefault("beta", yfi.get("beta"))
            info.setdefault("currency", yfi.get("currency"))
    except Exception:
        pass

    return info


# ─── FORMATTERS ──────────────────────────────────────────────────────────────

def ts() -> str:
    return datetime.now(IST).strftime("%d %b %Y %H:%M IST")


def fmt_sma(results: list, period: int, universe_label: str = "Nifty 50") -> str:
    if not results:
        if period == 10:
            return f"❌ No {universe_label} stocks meet: pullback ≥20% from recent high + close above 10-Day SMA."
        return f"❌ No {universe_label} stocks above {period}-Day SMA right now."

    title = f"📊 *Pullback ≥20% + Above {period}-Day SMA* — {len(results)} stocks" if period == 10 else f"📊 *Above {period}-Day SMA* — {len(results)} stocks"
    lines = [f"{title}\n"]
    for r in results[:RESULT_LIMIT]:
        dot = "🟢" if r["change"] >= 0 else "🔴"
        dd = f"  DD `{r['drawdown_pct']:.1f}%`" if period == 10 and "drawdown_pct" in r else ""
        vx = f"  Vx`{r['vol_ratio']}`" if "vol_ratio" in r else ""
        lines.append(f"{dot} *{r['symbol']}*  ₹{r['price']}  `{r['change']:+.2f}%`  SMA={r['sma']}{dd}{vx}")
    lines.append(f"\n_{universe_label} · NSE Live · {ts()}_")
    return "\n".join(lines)


def fmt_prev_week(results: list, universe_label: str = "Nifty 50") -> str:
    if not results:
        return f"❌ No {universe_label} stocks with close above previous week's High right now."

    title = f"📊 *Close > Previous Week High* — {len(results)} stocks"
    lines = [f"{title}\n"]
    for r in results[:RESULT_LIMIT]:
        dot = "🟢" if r["change"] >= 0 else "🔴"
        vx = f"  Vx`{r['vol_ratio']}`" if "vol_ratio" in r else ""
        lines.append(
            f"{dot} *{r['symbol']}*  ₹{r['price']}  `{r['change']:+.2f}%`  PWH={r['prev_week_high']}{vx}"
        )
    lines.append(f"\n_{universe_label} · NSE Live · {ts()}_")
    return "\n".join(lines)


def fmt_nifty_oi(summary: Optional[dict]) -> str:
    if not summary:
        return "❌ Could not fetch NIFTY OI data right now (NSE may be blocking/slow). Try again in 30–60s."
    if isinstance(summary, dict) and summary.get("error"):
        err = summary.get("error")
        detail = summary.get("detail")
        detail_txt = ""
        if detail:
            detail_txt = f"\n`{str(detail)[:180]}`"
        return f"❌ Could not fetch NIFTY OI data: `{err}` (try again in 30–60s){detail_txt}"

    underlying = summary.get("underlying")
    if underlying is None:
        underlying = "N/A"
    timestamp = summary.get("timestamp") or ts()
    idx = summary.get("index") or {}
    idx_pchg = idx.get("pChange")
    pcr_oi = summary.get("pcr_oi")
    pcr_coi = summary.get("pcr_coi")

    hdr = "📌 *NIFTY OI Dashboard*"
    src = summary.get("source")
    if src:
        hdr = f"{hdr} _({md_escape(src)})_"
    lines = [hdr]
    idx_txt = ""
    if idx_pchg is not None:
        try:
            v = float(idx_pchg)
            arrow = "🔺" if v >= 0 else "🔻"
            idx_txt = f"  {arrow} *%Chg* `{v:+.2f}%`"
        except (TypeError, ValueError):
            idx_txt = ""

    if isinstance(pcr_oi, (int, float)) and isinstance(pcr_coi, (int, float)):
        lines.append(f"📈 *Underlying* `{underlying}`{idx_txt}")
        lines.append(f"⚖️ *PCR*  OI `{pcr_oi:.2f}`  ΔOI `{pcr_coi:.2f}`")
    else:
        lines.append(f"📈 *Underlying* `{underlying}`{idx_txt}")
        lines.append(f"⚖️ *PCR*  OI `{pcr_oi if pcr_oi is not None else 'N/A'}`  ΔOI `{pcr_coi if pcr_coi is not None else 'N/A'}`")

    top_ce = (summary.get("top_ce") or [])[:OI_TOP_N]
    top_pe = (summary.get("top_pe") or [])[:OI_TOP_N]

    def bar(oi: int, max_oi: int, width: int = 20) -> str:
        if max_oi <= 0:
            return ""
        n = int(round((oi / max_oi) * width))
        return "#" * max(0, n)

    band_ce = summary.get("band_ce") or top_ce
    band_pe = summary.get("band_pe") or top_pe

    max_oi_ce = max((int(r.get("oi") or 0) for r in band_ce), default=0)
    max_oi_pe = max((int(r.get("oi") or 0) for r in band_pe), default=0)

    lines.append("\nTop CE OI (from NIFTY +100, 5 strikes)")
    for r in band_ce:
        strike = r.get("strike")
        oi = int(r.get("oi") or 0)
        lines.append(f"{str(strike):>7} | {bar(oi, max_oi_ce):<20} {oi}")

    lines.append("\nTop PE OI (from NIFTY -100, 5 strikes)")
    for r in band_pe:
        strike = r.get("strike")
        oi = int(r.get("oi") or 0)
        lines.append(f"{str(strike):>7} | {bar(oi, max_oi_pe):<20} {oi}")

    lines.append(f"\nTime: {timestamp}")
    return "\n".join(lines)


def fmt_nifty_oi_analysis(summary: Optional[dict]) -> str:
    if not summary:
        return "OI analysis unavailable (no data)."
    if isinstance(summary, dict) and summary.get("error"):
        return fmt_nifty_oi(summary)

    pcr_oi = summary.get("pcr_oi")
    pcr_coi = summary.get("pcr_coi")
    idx = summary.get("index") or {}
    idx_pchg = idx.get("pChange")
    underlying = summary.get("underlying") or "N/A"

    if not isinstance(pcr_oi, (int, float)) or not isinstance(pcr_coi, (int, float)):
        return "OI analysis unavailable (insufficient OI data)."

    bullish_score = 0
    bearish_score = 0

    if pcr_oi >= 1.20:
        bullish_score += 1
    elif pcr_oi <= 0.80:
        bearish_score += 1

    if pcr_coi >= 1.10:
        bullish_score += 1
    elif pcr_coi <= 0.90:
        bearish_score += 1

    if isinstance(idx_pchg, (int, float)):
        if idx_pchg >= 0.20:
            bullish_score += 1
        elif idx_pchg <= -0.20:
            bearish_score += 1

    def score_to_label(score: int) -> str:
        if score >= 3:
            return "High"
        if score == 2:
            return "Medium"
        if score == 1:
            return "Low"
        return "Very Low"

    bullish = score_to_label(bullish_score)
    bearish = score_to_label(bearish_score)
    if bullish_score > bearish_score:
        bias = "Bullish"
    elif bearish_score > bullish_score:
        bias = "Bearish"
    else:
        bias = "Neutral"

    # Convert score difference into a simple probability estimate.
    net = bullish_score - bearish_score
    bullish_pct = max(10, min(90, 50 + net * 10))
    bearish_pct = 100 - bullish_pct

    lines = [
        "OI Analysis (NIFTY)",
        f"Underlying: {underlying}",
        f"PCR(OI): {pcr_oi:.2f}  PCR(Delta OI): {pcr_coi:.2f}",
        f"Index %Chg: {idx_pchg if idx_pchg is not None else 'N/A'}",
        f"Bullish chance: {bullish} ({bullish_pct}%)",
        f"Bearish chance: {bearish} ({bearish_pct}%)",
        f"Bias: {bias}",
        "Note: This is a simple OI/PCR heuristic, not financial advice.",
    ]
    return "\n".join(lines)


def fmt_nifty_oi_side(summary: Optional[dict], side: str) -> str:
    if not summary:
        return fmt_nifty_oi(summary)
    if isinstance(summary, dict) and summary.get("error"):
        return fmt_nifty_oi(summary)

    side = (side or "").strip().upper()
    if side not in ("CE", "PE"):
        side = "CE"

    underlying = summary.get("underlying")
    if underlying is None:
        underlying = "N/A"
    timestamp = summary.get("timestamp") or ts()
    idx = summary.get("index") or {}
    idx_pchg = idx.get("pChange")
    pcr_oi = summary.get("pcr_oi")
    pcr_coi = summary.get("pcr_coi")

    idx_txt = ""
    if idx_pchg is not None:
        try:
            v = float(idx_pchg)
            arrow = "🔺" if v >= 0 else "🔻"
            idx_txt = f"  {arrow} *%Chg* `{v:+.2f}%`"
        except (TypeError, ValueError):
            idx_txt = ""

    title = "📌 *NIFTY Top CE OI*" if side == "CE" else "📌 *NIFTY Top PE OI*"
    lines = [title]
    if isinstance(pcr_oi, (int, float)) and isinstance(pcr_coi, (int, float)):
        lines.append(f"📈 *Underlying* `{underlying}`{idx_txt}")
        lines.append(f"⚖️ *PCR*  OI `{pcr_oi:.2f}`  ΔOI `{pcr_coi:.2f}`")
    else:
        lines.append(f"📈 *Underlying* `{underlying}`{idx_txt}")
        lines.append(f"⚖️ *PCR*  OI `{pcr_oi if pcr_oi is not None else 'N/A'}`  ΔOI `{pcr_coi if pcr_coi is not None else 'N/A'}`")

    def bar(oi: int, max_oi: int, width: int = 20) -> str:
        if max_oi <= 0:
            return ""
        n = int(round((oi / max_oi) * width))
        return "#" * max(0, n)

    band = summary.get("band_ce") if side == "CE" else summary.get("band_pe")
    rows = (band or [])[:5]
    max_oi = max((int(r.get("oi") or 0) for r in rows), default=0)
    for r in rows:
        strike = r.get("strike")
        oi = int(r.get("oi") or 0)
        lines.append(f"{str(strike):>7} | {bar(oi, max_oi):<20} {oi}")

    lines.append(f"\nTime: {timestamp}")
    return "\n".join(lines)


def fmt_rsi(results: list, oversold: bool, universe_label: str = "Nifty 50") -> str:
    label = "Oversold  RSI < 30 🔵" if oversold else "Overbought  RSI > 70 🔴"
    if not results:
        return f"❌ No stocks in the {label} zone right now."
    lines = [f"*RSI Signal — {label}*\n_{len(results)} found_\n"]
    for r in results[:RESULT_LIMIT]:
        dot = "🟢" if r["change"] >= 0 else "🔴"
        lines.append(f"{dot} *{r['symbol']}*  ₹{r['price']}  `{r['change']:+.2f}%`  RSI `{r['rsi']}`")
    lines.append(f"\n_{universe_label} · NSE Live · {ts()}_")
    return "\n".join(lines)


def fmt_movers(results: list, gainers: bool) -> str:
    title = f"🚀 Top {RESULT_LIMIT} Gainers" if gainers else f"📉 Top {RESULT_LIMIT} Losers"
    if not results:
        return f"❌ Could not fetch {title} data."
    lines = [f"*{title}* — Nifty 50\n"]
    for i, r in enumerate(results, 1):
        dot = "🟢" if r["change"] >= 0 else "🔴"
        extra = f"  H:{r['high']}  L:{r['low']}" if "high" in r else ""
        lines.append(f"{i}. {dot} *{r['symbol']}*  ₹{r['price']}  `{r['change']:+.2f}%`{extra}")
    lines.append(f"\n_NSE Live · {ts()}_")
    return "\n".join(lines)


def fmt_fundamentals(results: list, cap_type: str) -> str:
    emoji = "🔹" if cap_type == "small" else "🔷"
    label = "Small Cap (Nifty Smallcap 100)" if cap_type == "small" else "Mid Cap (Nifty Midcap 100)"
    if not results:
        return f"❌ Could not fetch {label} data."
    lines = [f"{emoji} *{label}*\n"]
    for r in results[:10]:
        lines.append(
            f"*{r['symbol']}*  _{r['name']}_\n"
            f"  Sector: {r['sector']}\n"
            f"  Price: ₹{r['price']}  MCap: {r['mktcap']}\n"
            f"  P/E: {r['pe']}  P/B: {r['pb']}  ROE: {r['roe']}\n"
            f"  EPS: {r['eps']}  Div: {r['div']}\n"
        )
    lines.append(f"_NSE Live · {ts()}_")
    return "\n".join(lines)


def fmt_stock_info(info: Optional[dict]) -> str:
    if not info or not isinstance(info, dict) or not info.get("symbol"):
        return "❌ Stock not found. Please send an NSE symbol like `RELIANCE`, `TCS`, `INFY`."

    sym = md_escape(info.get("symbol"))
    name = md_escape(info.get("name") or "")
    sector = md_escape(info.get("sector") or "—")
    industry = md_escape(info.get("industry") or "—")

    def n(v, suffix: str = ""):
        if v is None or v == "":
            return "N/A"
        try:
            if isinstance(v, (int, float)):
                return f"{v:,.2f}{suffix}".replace(",", "")
            return f"{v}{suffix}"
        except Exception:
            return str(v)

    price = info.get("price")
    chg = info.get("change_pct")
    open_ = info.get("open")
    high = info.get("high")
    low = info.get("low")
    w52h = info.get("52w_high")
    w52l = info.get("52w_low")
    vol = info.get("volume")
    mcap = info.get("mktcap")
    pe = info.get("pe")
    pb = info.get("pb")
    eps = info.get("eps")
    roe = info.get("roe")
    divy = info.get("div_yield")
    beta = info.get("beta")
    last_update = info.get("last_update") or ts()

    lines = [f"📄 *Stock Info — {sym}*"]
    if name:
        lines.append(f"_{name}_")
    if price is not None:
        chg_txt = f"  `{float(chg):+.2f}%`" if isinstance(chg, (int, float)) else (f"  `{chg}`" if chg is not None else "")
        lines.append(f"Price: ₹`{n(price)}`{chg_txt}")
    lines.append(f"O/H/L: `{n(open_)}` / `{n(high)}` / `{n(low)}`")
    lines.append(f"52W H/L: `{n(w52h)}` / `{n(w52l)}`")
    if vol is not None:
        lines.append(f"Volume: `{n(vol)}`")
    if mcap is not None:
        lines.append(f"MCap: `{n(mcap)}`")

    lines.append(f"P/E: `{n(pe)}`  P/B: `{n(pb)}`  EPS: `{n(eps)}`")
    lines.append(f"ROE: `{n(roe)}`  DivY: `{n(divy)}`  Beta: `{n(beta)}`")
    lines.append(f"Sector: {sector}")
    lines.append(f"Industry: {industry}")
    lines.append(f"\n_NSE/yfinance · {md_escape(last_update)}_")
    return "\n".join(lines)


# ─── HANDLERS ────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🇮🇳 *Indian Stock Market Screener*\n\n"
        "All data is fetched *live from NSE* \n\n"
        "📊 *Technical* — SMA & RSI on live Nifty 50 constituents\n"
        "📈 *Fundamental* — Live small/mid cap snapshots\n"
        "📌 *OI Data* — NIFTY option-chain Top OI + PCR\n"
        f"🚀 *Gainers* / 📉 *Losers* — Top {RESULT_LIMIT} NSE movers",
        parse_mode="Markdown",
        reply_markup=HOME_KEYBOARD,
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # ── Menu navigation (no data fetch) ──
    if text in ("🏠 Home", "/start"):
        context.user_data.pop("awaiting_stock_info", None)
        await update.message.reply_text(
            "🏠 *Home*", parse_mode="Markdown", reply_markup=HOME_KEYBOARD
        )
        return

    if text == "📊 Technical":
        context.user_data.pop("awaiting_stock_info", None)
        await update.message.reply_text(
            "📊 *Technical Screener* — Choose a signal:",
            parse_mode="Markdown",
            reply_markup=TECHNICAL_KEYBOARD,
        )
        return

    if text == "🧺 Nifty 50":
        context.user_data["universe"] = UNIVERSE_NIFTY50
        await update.message.reply_text(
            "Universe set to *Nifty 50*.",
            parse_mode="Markdown",
            reply_markup=TECHNICAL_KEYBOARD,
        )
        return

    if text == "🌐 All Stocks":
        context.user_data["universe"] = UNIVERSE_ALL
        await update.message.reply_text(
            "Universe set to *All Stocks*.\n"
            "Note: this can be slow; optional `MAX_SYMBOLS=500` to cap the universe.",
            parse_mode="Markdown",
            reply_markup=TECHNICAL_KEYBOARD,
        )
        return

    if text == "📈 Fundamental":
        context.user_data.pop("awaiting_stock_info", None)
        await update.message.reply_text(
            "📈 *Fundamental Screener* — Choose a cap size:",
            parse_mode="Markdown",
            reply_markup=FUNDAMENTAL_KEYBOARD,
        )
        return

    if text == "📄 Stock Info":
        context.user_data["awaiting_stock_info"] = True
        await update.message.reply_text(
            "📄 *Stock Info*\n\nSend the stock *symbol* (example: `RELIANCE`, `TCS`, `INFY`) or company name.",
            parse_mode="Markdown",
            reply_markup=FUNDAMENTAL_KEYBOARD,
        )
        return

    if text == "📌 OI Data":
        context.user_data.pop("awaiting_stock_info", None)
        await update.message.reply_text(
            "📌 *OI Data* — Choose:",
            parse_mode="Markdown",
            reply_markup=OI_KEYBOARD,
        )
        return

    if context.user_data.get("awaiting_stock_info"):
        context.user_data.pop("awaiting_stock_info", None)
        thinking = await update.message.reply_text("⏳ Fetching stock info…")
        loop = asyncio.get_running_loop()
        try:
            async with HEAVY_WORK_SEM:
                info = await loop.run_in_executor(None, lambda: stock_info(text))
            msg = fmt_stock_info(info)
            await thinking.delete()
            await update.message.reply_text(
                msg, parse_mode="Markdown", reply_markup=FUNDAMENTAL_KEYBOARD
            )
        except Exception:
            await thinking.delete()
            await update.message.reply_text(
                "⚠️ Error fetching stock info. Try again with an NSE symbol like `RELIANCE`.",
                parse_mode="Markdown",
                reply_markup=FUNDAMENTAL_KEYBOARD,
            )
        return

    # ── Data-fetching actions ──
    t0 = time.perf_counter()
    thinking = await update.message.reply_text("⏳ Fetching live data from NSE…")
    loop = asyncio.get_running_loop()

    try:
        async with HEAVY_WORK_SEM:
            if text in ("📌 Nifty OI", "📌 Nifty OI Summary", "📌 Top CE OI", "📌 Top PE OI", "OI Analysis"):
                summary = await loop.run_in_executor(None, nifty_oi_summary)
                if text == "OI Analysis":
                    msg = fmt_nifty_oi_analysis(summary)
                elif text == "📌 Top PE OI":
                    msg = fmt_nifty_oi_side(summary, "PE")
                elif text == "📌 Top CE OI":
                    msg = fmt_nifty_oi_side(summary, "CE")
                else:
                    msg = fmt_nifty_oi(summary)
            else:
                universe = (context.user_data.get("universe") or UNIVERSE_NIFTY50).lower()
                universe_label = UNIVERSE_LABELS.get(universe, "Nifty 50")
                symbols = None

                needs_symbols = text in (
                    "📉 10 SMA",
                    "📈 100 SMA",
                    "📈 Prev Week High",
                    "🔵 RSI Oversold",
                    "🔴 RSI Overbought",
                    "🚀 Active Value",
                    "🚀 Actve Value",
                    "🚀 Active Val",
                    "🚀 Actve Val",
                    "🚀 Active Vol",
                )
                if needs_symbols:
                    with log_timing(f"symbols {universe}"):
                        symbols = get_symbols_for_universe(universe)

                if text == "📉 10 SMA":
                    data = await loop.run_in_executor(None, lambda: screen_sma(10, symbols=symbols))
                    msg  = fmt_sma(data, 10, universe_label=universe_label)

                elif text in ("📈 100 SMA", "📈 Prev Week High"):
                    data = await loop.run_in_executor(None, lambda: screen_close_above_prev_week(symbols=symbols))
                    msg  = fmt_prev_week(data, universe_label=universe_label)

                elif text == "🔵 RSI Oversold":
                    data = await loop.run_in_executor(None, lambda: screen_rsi(oversold=True, symbols=symbols))
                    msg  = fmt_rsi(data, oversold=True, universe_label=universe_label)

                elif text == "🔴 RSI Overbought":
                    data = await loop.run_in_executor(None, lambda: screen_rsi(oversold=False, symbols=symbols))
                    msg  = fmt_rsi(data, oversold=False, universe_label=universe_label)

                elif text == "📉 Top 5 Losers":
                    data = await loop.run_in_executor(None, lambda: screen_top_movers(RESULT_LIMIT, gainers=False))
                    msg  = fmt_movers(data, gainers=False)

                elif text == "🚀 Top 5 Gainers":
                    data = await loop.run_in_executor(None, lambda: screen_top_movers(RESULT_LIMIT, gainers=True))
                    msg  = fmt_movers(data, gainers=True)

                elif text in ("🚀 Active Value", "🚀 Actve Value", "🚀 Active Val", "🚀 Actve Val"):
                    data = await loop.run_in_executor(None, lambda: screen_active_value(symbols, universe))
                    msg  = fmt_activity(data, "value", universe_label)

                elif text == "🚀 Active Vol":
                    data = await loop.run_in_executor(None, lambda: screen_active_vol(symbols, universe))
                    msg  = fmt_activity(data, "vol", universe_label)

                elif text == "🔹 Small Cap":
                    data = await loop.run_in_executor(None, lambda: screen_fundamentals("small"))
                    msg  = fmt_fundamentals(data, "small")

                elif text == "🔷 Mid Cap":
                    data = await loop.run_in_executor(None, lambda: screen_fundamentals("mid"))
                    msg  = fmt_fundamentals(data, "mid")

                else:
                    await thinking.delete()
                    await update.message.reply_text(
                        "❓ Use the menu buttons to navigate.", reply_markup=HOME_KEYBOARD
                    )
                    return

        if os.getenv("SHOW_RESPONSE_TIME", "").strip().lower() in ("1", "true", "yes", "on"):
            msg += f"\n\n_⏱ {time.perf_counter() - t0:.2f}s_"

        await thinking.delete()
        # Split if message exceeds Telegram's 4096 char limit
        for chunk in [msg[i:i+4000] for i in range(0, len(msg), 4000)]:
            await update.message.reply_text(chunk, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Handler error: {e}", exc_info=True)
        await thinking.delete()
        await update.message.reply_text(
            "⚠️ Error fetching data. Please try again.",
            reply_markup=HOME_KEYBOARD,
        )


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        print("ERROR: Set TELEGRAM_BOT_TOKEN environment variable first.")
        return

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("Indian Stock Screener Bot running - symbols fetched live from NSE")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
