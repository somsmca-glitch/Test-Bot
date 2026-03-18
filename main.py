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

import os
import logging
import asyncio
import requests
import pandas as pd
import yfinance as yf
import ta
from datetime import datetime
from zoneinfo import ZoneInfo
from io import StringIO

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

# Simple in-memory cache: {key: (data, fetched_at)}
_CACHE: dict = {}
CACHE_TTL = 600  # 10 minutes


def cache_get(key: str, ttl_seconds: int = CACHE_TTL):
    entry = _CACHE.get(key)
    if entry and (datetime.now() - entry[1]).seconds < ttl_seconds:
        return entry[0]
    return None


def cache_set(key: str, value):
    _CACHE[key] = (value, datetime.now())


# ─── KEYBOARDS ────────────────────────────────────────────────────────────────

HOME_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("📊 Technical"), KeyboardButton("📈 Fundamental")],
        [KeyboardButton("📉 Top 5 Losers"), KeyboardButton("🚀 Top 5 Gainers")],
    ],
    resize_keyboard=True,
)

TECHNICAL_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🧺 Nifty 50"), KeyboardButton("🌐 All Stocks")],
        [KeyboardButton("📉 10 SMA"), KeyboardButton("📈 100 SMA")],
        [KeyboardButton("🔵 RSI Oversold"), KeyboardButton("🔴 RSI Overbought")],
        [KeyboardButton("🏠 Home")],
    ],
    resize_keyboard=True,
)

FUNDAMENTAL_KEYBOARD = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🔹 Small Cap"), KeyboardButton("🔷 Mid Cap")],
        [KeyboardButton("🏠 Home")],
    ],
    resize_keyboard=True,
)


# ─── NSE SESSION ─────────────────────────────────────────────────────────────

def nse_session() -> requests.Session:
    """Return a cookie-primed NSE session."""
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    try:
        s.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    return s


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

def get_all_nse_equity_symbols(limit: int | None = None) -> list:
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
        resp = session.get(url, timeout=20)
        resp.raise_for_status()

        df = pd.read_csv(StringIO(resp.text))
        if "SYMBOL" not in df.columns:
            return []

        if "SERIES" in df.columns:
            df = df[df["SERIES"].astype(str).str.upper().eq("EQ")]

        symbols = (
            df["SYMBOL"]
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        tickers = [f"{sym}.NS" for sym in symbols]
        cache_set(cache_key, tickers)
        return tickers[:limit] if limit else tickers
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


# ─── OHLCV FOR INDICATORS ────────────────────────────────────────────────────

def fetch_ohlcv_bulk(symbols: list, period: str = "6mo") -> dict:
    """Bulk download OHLCV from yfinance for indicator computation."""
    if not symbols:
        return {}
    try:
        tickers_str = " ".join(symbols)
        df = yf.download(
            tickers_str,
            period=period,
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        result = {}
        if len(symbols) == 1:
            if not df.empty and len(df) > 20:
                result[symbols[0]] = df
        else:
            for sym in symbols:
                try:
                    sub = df[sym].dropna(how="all")
                    if not sub.empty and len(sub) > 20:
                        result[sym] = sub
                except KeyError:
                    pass
        return result
    except Exception as e:
        logger.error(f"Bulk OHLCV error: {e}")
        return {}


# ─── SCREENERS ───────────────────────────────────────────────────────────────

def screen_sma(sma_period: int, symbols: list | None = None) -> list:
    """
    Screener:
    - For 10 SMA: close is above SMA(10) AND close is >= 20% below the recent high (pullback)
    - For other periods: close is above SMA(period)

    Recent high is computed from the last ~1y of daily candles fetched from yfinance (adjusted OHLCV).
    """
    symbols = symbols or get_nifty50_symbols()
    if not symbols:
        return []

    raw = fetch_ohlcv_bulk(symbols, period="1y")
    results = []

    for sym, df in raw.items():
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
    return results


def screen_rsi(oversold: bool = True, symbols: list | None = None) -> list:
    """
    1. Fetch live Nifty 50 symbols from NSE
    2. Download OHLCV from yfinance
    3. Compute RSI(14) — return oversold < 30 or overbought > 70
    """
    symbols = symbols or get_nifty50_symbols()
    if not symbols:
        return []

    raw = fetch_ohlcv_bulk(symbols, period="6mo")
    results = []

    for sym, df in raw.items():
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
    return results


def screen_top_movers(top_n: int = 5, gainers: bool = True) -> list:
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

    raw = fetch_ohlcv_bulk(symbols, period="5d")
    fallback = []
    for sym, df in raw.items():
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


def screen_fundamentals(cap_type: str) -> list:
    """
    1. Fetch live small/mid cap index symbols from NSE
    2. Pull fundamental data per ticker via yfinance
    """
    symbols = get_smallcap_symbols() if cap_type == "small" else get_midcap_symbols()
    if not symbols:
        return []

    results = []
    for sym in symbols:
        try:
            info = yf.Ticker(sym).info
            if not info:
                continue

            name   = info.get("shortName") or info.get("longName") or sym.replace(".NS", "")
            price  = info.get("currentPrice") or info.get("regularMarketPrice") or 0
            if not price_ok(price):
                continue
            mktcap = info.get("marketCap") or 0
            pe     = info.get("trailingPE")
            pb     = info.get("priceToBook")
            roe    = info.get("returnOnEquity")
            eps    = info.get("trailingEps")
            div    = info.get("dividendYield")
            sector = info.get("sector") or "—"

            results.append({
                "symbol": sym.replace(".NS", ""),
                "name":   name[:22],
                "sector": sector[:18],
                "price":  round(price, 2) if price else "N/A",
                "mktcap": f"₹{mktcap/1e9:.1f}B" if mktcap else "N/A",
                "pe":     round(pe, 1) if pe else "N/A",
                "pb":     round(pb, 2) if pb else "N/A",
                "roe":    f"{roe*100:.1f}%" if roe else "N/A",
                "eps":    round(eps, 2) if eps else "N/A",
                "div":    f"{div*100:.2f}%" if div else "N/A",
            })
        except Exception as e:
            logger.warning(f"Fundamental error {sym}: {e}")

    return results


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
    for r in results[:15]:
        dot = "🟢" if r["change"] >= 0 else "🔴"
        dd = f"  DD `{r['drawdown_pct']:.1f}%`" if period == 10 and "drawdown_pct" in r else ""
        lines.append(f"{dot} *{r['symbol']}*  ₹{r['price']}  `{r['change']:+.2f}%`  SMA={r['sma']}{dd}")
    lines.append(f"\n_{universe_label} · NSE Live · {ts()}_")
    return "\n".join(lines)


def fmt_rsi(results: list, oversold: bool, universe_label: str = "Nifty 50") -> str:
    label = "Oversold  RSI < 30 🔵" if oversold else "Overbought  RSI > 70 🔴"
    if not results:
        return f"❌ No stocks in the {label} zone right now."
    lines = [f"*RSI Signal — {label}*\n_{len(results)} found_\n"]
    for r in results[:15]:
        dot = "🟢" if r["change"] >= 0 else "🔴"
        lines.append(f"{dot} *{r['symbol']}*  ₹{r['price']}  `{r['change']:+.2f}%`  RSI `{r['rsi']}`")
    lines.append(f"\n_{universe_label} · NSE Live · {ts()}_")
    return "\n".join(lines)


def fmt_movers(results: list, gainers: bool) -> str:
    title = "🚀 Top 5 Gainers" if gainers else "📉 Top 5 Losers"
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


# ─── HANDLERS ────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🇮🇳 *Indian Stock Market Screener*\n\n"
        "All data is fetched *live from NSE* — no hardcoded symbols.\n\n"
        "📊 *Technical* — SMA & RSI on live Nifty 50 constituents\n"
        "📈 *Fundamental* — Live small/mid cap snapshots\n"
        "🚀 *Gainers* / 📉 *Losers* — Real-time NSE movers",
        parse_mode="Markdown",
        reply_markup=HOME_KEYBOARD,
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()

    # ── Menu navigation (no data fetch) ──
    if text in ("🏠 Home", "/start"):
        await update.message.reply_text(
            "🏠 *Home*", parse_mode="Markdown", reply_markup=HOME_KEYBOARD
        )
        return

    if text == "📊 Technical":
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
        await update.message.reply_text(
            "📈 *Fundamental Screener* — Choose a cap size:",
            parse_mode="Markdown",
            reply_markup=FUNDAMENTAL_KEYBOARD,
        )
        return

    # ── Data-fetching actions ──
    thinking = await update.message.reply_text("⏳ Fetching live data from NSE…")
    loop = asyncio.get_running_loop()

    try:
        universe = (context.user_data.get("universe") or UNIVERSE_NIFTY50).lower()
        symbols = get_symbols_for_universe(universe)
        universe_label = UNIVERSE_LABELS.get(universe, "Nifty 50")

        if text == "📉 10 SMA":
            data = await loop.run_in_executor(None, lambda: screen_sma(10, symbols=symbols))
            msg  = fmt_sma(data, 10, universe_label=universe_label)

        elif text == "📈 100 SMA":
            data = await loop.run_in_executor(None, lambda: screen_sma(100, symbols=symbols))
            msg  = fmt_sma(data, 100, universe_label=universe_label)

        elif text == "🔵 RSI Oversold":
            data = await loop.run_in_executor(None, lambda: screen_rsi(oversold=True, symbols=symbols))
            msg  = fmt_rsi(data, oversold=True, universe_label=universe_label)

        elif text == "🔴 RSI Overbought":
            data = await loop.run_in_executor(None, lambda: screen_rsi(oversold=False, symbols=symbols))
            msg  = fmt_rsi(data, oversold=False, universe_label=universe_label)

        elif text == "📉 Top 5 Losers":
            data = await loop.run_in_executor(None, lambda: screen_top_movers(5, gainers=False))
            msg  = fmt_movers(data, gainers=False)

        elif text == "🚀 Top 5 Gainers":
            data = await loop.run_in_executor(None, lambda: screen_top_movers(5, gainers=True))
            msg  = fmt_movers(data, gainers=True)

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
