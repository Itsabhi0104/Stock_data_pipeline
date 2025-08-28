#!/usr/bin/env python3
"""
scripts/fetch_and_upsert.py

Provider-flexible fetcher that upserts snapshot rows into `stock_price`
and appends to `stock_price_history`.

Provider selection via environment:
  MARKET_PROVIDER = "alphavantage" | "yfinance" | "yahoo"   (default: "yfinance")

If using Alpha Vantage:
  set ALPHAVANTAGE_API_KEY env var.

DB config via env:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
SYMBOLS env: comma-separated (SYMBOLS)
"""
from __future__ import annotations

import os
import time
import random
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from psycopg2.extras import execute_values

# optional yfinance
try:
    import yfinance as yf
    HAVE_YFINANCE = True
except Exception:
    HAVE_YFINANCE = False

# === Config (env overrides) ===
MARKET_PROVIDER = os.environ.get("MARKET_PROVIDER", "yfinance").lower()
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()

REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "10"))
REQUEST_DELAY_SECONDS = float(os.environ.get("REQUEST_DELAY_SECONDS", "2.0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "5"))
BACKOFF_FACTOR = float(os.environ.get("BACKOFF_FACTOR", "1.5"))

DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_PORT = int(os.environ.get("DB_PORT", 5432))
DB_NAME = os.environ.get("DB_NAME", "stocksdb")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")

SYMBOLS_ENV = os.environ.get("SYMBOLS", "AAPL,MSFT,GOOGL")
DEFAULT_SYMBOLS = [s.strip().upper() for s in SYMBOLS_ENV.split(",") if s.strip()]

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger("fetch_and_upsert")

# HTTP session & retry for non-yfinance providers
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; StockPipeline/1.0; +https://example.local/)",
    "Accept": "application/json",
})
retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    backoff_factor=1,
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


# === Provider implementations ===

def _safe_float(v: Optional[Any]) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Optional[Any]) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(float(v))
    except Exception:
        return None


def _parse_percent_string(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).strip().rstrip("%"))
    except Exception:
        return None


def _fetch_alphavantage(symbols: List[str]) -> List[Dict[str, Any]]:
    if not ALPHAVANTAGE_API_KEY:
        raise RuntimeError("ALPHAVANTAGE_API_KEY is not set but MARKET_PROVIDER=alphavantage")

    results = []
    for symbol in symbols:
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            attempt += 1
            try:
                url = "https://www.alphavantage.co/query"
                params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": ALPHAVANTAGE_API_KEY}
                resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                data = resp.json() if resp.text else {}
                if "Note" in data:
                    wait = BACKOFF_FACTOR * attempt + random.uniform(0, 1) + REQUEST_DELAY_SECONDS
                    logger.warning("AlphaVantage note for %s: %s. Waiting %.1fs", symbol, data.get("Note"), wait)
                    time.sleep(wait)
                    continue
                gq = data.get("Global Quote") or {}
                if not gq:
                    logger.warning("AlphaVantage empty Global Quote for %s: %s", symbol, data)
                    time.sleep(BACKOFF_FACTOR * attempt)
                    continue
                mapped = {
                    "symbol": gq.get("01. symbol"),
                    "price": _safe_float(gq.get("05. price")),
                    "change": _safe_float(gq.get("09. change")),
                    "percent_change": _parse_percent_string(gq.get("10. change percent")),
                    "volume": _safe_int(gq.get("06. volume")),
                    "raw": gq,
                }
                results.append(mapped)
                time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 0.5))
                break
            except requests.RequestException as exc:
                wait = BACKOFF_FACTOR * attempt + random.uniform(0, 1)
                logger.warning("AlphaVantage network error %s: %s (attempt %d). Retrying in %.1fs", symbol, exc, attempt, wait)
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed to fetch {symbol} from AlphaVantage after {MAX_ATTEMPTS} attempts")
    return results


def _fetch_yfinance(symbols: List[str]) -> List[Dict[str, Any]]:
    if not HAVE_YFINANCE:
        raise RuntimeError("yfinance not installed. Add 'yfinance' to requirements and rebuild the image.")
    results = []
    for symbol in symbols:
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            attempt += 1
            try:
                t = yf.Ticker(symbol)
                hist = t.history(period="2d", interval="1m")
                price = None; vol = None; change = None; pct = None
                if hist is not None and not hist.empty:
                    last = hist.iloc[-1]
                    price = float(last["Close"]) if "Close" in last.index else None
                    vol = int(last["Volume"]) if "Volume" in last.index and not (last["Volume"] is None) else None
                    if len(hist) >= 2:
                        prev = hist.iloc[-2]
                        change = (price - float(prev["Close"])) if price is not None else None
                        if prev["Close"]:
                            pct = (change / float(prev["Close"]) * 100) if (change is not None) and prev["Close"] else None
                if price is None:
                    info = t.info or {}
                    price = info.get("regularMarketPrice") or info.get("previousClose")
                    vol = vol or info.get("volume") or info.get("regularMarketVolume")
                    change = change or info.get("regularMarketChange")
                    pct = pct or info.get("regularMarketChangePercent")
                mapped = {
                    "symbol": symbol,
                    "price": None if price is None else float(price),
                    "change": None if change is None else float(change),
                    "percent_change": None if pct is None else float(pct),
                    "volume": None if vol is None else int(vol),
                    "raw": {},
                }
                results.append(mapped)
                time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 0.5))
                break
            except Exception as exc:
                wait = BACKOFF_FACTOR * attempt + random.uniform(0, 1)
                logger.warning("yfinance error for %s: %s (attempt %d). Retrying in %.1fs", symbol, exc, attempt, wait)
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed to fetch {symbol} via yfinance after {MAX_ATTEMPTS} attempts")
    return results


def _fetch_yahoo_fallback(symbols: List[str]) -> List[Dict[str, Any]]:
    results = []
    for i in range(0, len(symbols), max(1, BATCH_SIZE)):
        batch = symbols[i:i + BATCH_SIZE]
        symbol_param = ",".join(batch)
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            attempt += 1
            try:
                url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={requests.utils.requote_uri(symbol_param)}"
                resp = session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 429:
                    wait = BACKOFF_FACTOR * attempt + random.uniform(0, 1)
                    logger.warning("Yahoo 429 for %s: waiting %.1fs (attempt %d)", symbol_param, wait, attempt)
                    time.sleep(wait)
                    continue
                if resp.status_code == 401:
                    raise RuntimeError("Yahoo returned 401 Unauthorized — automated access is blocked without crumb/cookie")
                resp.raise_for_status()
                data = resp.json()
                results.extend(data.get("quoteResponse", {}).get("result", []))
                time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 0.5))
                break
            except requests.RequestException as exc:
                wait = BACKOFF_FACTOR * attempt + random.uniform(0, 1)
                logger.warning("Yahoo network error %s: %s (attempt %d). Retrying in %.1fs", symbol_param, exc, attempt, wait)
                time.sleep(wait)
        else:
            raise RuntimeError(f"Failed to fetch quotes for {symbol_param} from Yahoo after {MAX_ATTEMPTS} attempts")
    return results


def fetch_quotes(symbols: List[str]) -> List[Dict[str, Any]]:
    logger.info("Provider=%s | fetching %d symbols", MARKET_PROVIDER, len(symbols))
    if MARKET_PROVIDER == "alphavantage":
        return _fetch_alphavantage(symbols)
    if MARKET_PROVIDER == "yfinance":
        return _fetch_yfinance(symbols)
    return _fetch_yahoo_fallback(symbols)


# === DB logic ===

def parse_quote_item(item: Dict[str, Any]) -> Dict[str, Any]:
    # normalized output
    if "symbol" in item and ("price" in item or "raw" in item):
        return {
            "symbol": item.get("symbol"),
            "price": _safe_float(item.get("price")),
            "change": _safe_float(item.get("change")),
            "percent_change": _safe_float(item.get("percent_change")),
            "volume": _safe_int(item.get("volume")),
        }
    sym = item.get("symbol") or item.get("shortName")
    price = item.get("regularMarketPrice") or item.get("ask") or item.get("previousClose")
    change = item.get("regularMarketChange")
    pct = item.get("regularMarketChangePercent")
    vol = item.get("regularMarketVolume") or item.get("volume")
    return {
        "symbol": sym,
        "price": _safe_float(price),
        "change": _safe_float(change),
        "percent_change": _safe_float(pct),
        "volume": _safe_int(vol),
    }


def get_db_connection():
    return psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)


def upsert_quotes_to_db(quotes: List[Dict[str, Any]]):
    if not quotes:
        logger.info("No quotes to upsert.")
        return

    snapshot_values = []
    history_values = []
    for q in quotes:
        parsed = parse_quote_item(q)
        sym = parsed["symbol"]
        if not sym:
            continue
        snapshot_values.append((sym, parsed["price"], parsed["change"], parsed["percent_change"], parsed["volume"]))
        history_values.append((sym, parsed["price"], parsed["change"], parsed["percent_change"], parsed["volume"]))

    if not snapshot_values:
        logger.info("No valid parsed quotes to write.")
        return

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ts = datetime.utcnow()
                upsert_sql = """
                INSERT INTO stock_price (symbol, price, change, percent_change, volume, fetched_at)
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE
                  SET price = EXCLUDED.price,
                      change = EXCLUDED.change,
                      percent_change = EXCLUDED.percent_change,
                      volume = EXCLUDED.volume,
                      fetched_at = EXCLUDED.fetched_at;
                """
                upsert_tuples = [ (sym, price, change, pct, vol, ts) for (sym, price, change, pct, vol) in snapshot_values ]
                execute_values(cur, upsert_sql, upsert_tuples)
                logger.info("Upserted %d rows into stock_price", len(upsert_tuples))

                insert_history_sql = """
                INSERT INTO stock_price_history (symbol, price, change, percent_change, volume, fetched_at)
                VALUES %s
                """
                history_tuples = [ (sym, price, change, pct, vol, ts) for (sym, price, change, pct, vol) in history_values ]
                execute_values(cur, insert_history_sql, history_tuples)
                logger.info("Inserted %d rows into stock_price_history", len(history_tuples))
    finally:
        conn.close()


def fetch_and_store(symbols: List[str]):
    logger.info("Starting fetch_and_store for symbols: %s", symbols)
    quotes = fetch_quotes(symbols)
    if not quotes:
        logger.warning("No quotes fetched.")
        return
    upsert_quotes_to_db(quotes)
    logger.info("fetch_and_store completed for %d symbols", len(symbols))


if __name__ == "__main__":
    try:
        symbols_env = os.environ.get("SYMBOLS")
        if symbols_env:
            symbols = [s.strip().upper() for s in symbols_env.split(",") if s.strip()]
        else:
            symbols = DEFAULT_SYMBOLS
        if not symbols:
            logger.error("No symbols provided. Set SYMBOLS env var.")
            raise SystemExit(1)
        fetch_and_store(symbols)
    except Exception as exc:
        logger.exception("Fatal error in fetch_and_upsert: %s", exc)
        raise
