#!/usr/bin/env python3
"""
scripts/fetch_and_upsert.py

Enhanced stock data fetcher with improved error handling and multiple fallback mechanisms.
Supports yfinance with better rate limiting and fallback strategies.

Provider selection via environment:
  MARKET_PROVIDER = "yfinance" | "alphavantage" | "yahoo_direct"   (default: "yfinance")

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
from datetime import datetime, timedelta
import json

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

REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "30"))
REQUEST_DELAY_SECONDS = float(os.environ.get("REQUEST_DELAY_SECONDS", "5.0"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1"))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))
BACKOFF_FACTOR = float(os.environ.get("BACKOFF_FACTOR", "2.0"))

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

# Enhanced HTTP session with better headers and retry logic
def create_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=2,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

session = create_session()

# === Helper functions ===

def _safe_float(v: Optional[Any]) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def _safe_int(v: Optional[Any]) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(float(v))
    except (ValueError, TypeError):
        return None


def _parse_percent_string(s: Optional[str]) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(str(s).strip().rstrip("%"))
    except (ValueError, TypeError):
        return None


# === Enhanced Provider implementations ===

def _fetch_yfinance_enhanced(symbols: List[str]) -> List[Dict[str, Any]]:
    """Enhanced yfinance fetcher with better error handling and fallbacks"""
    if not HAVE_YFINANCE:
        raise RuntimeError("yfinance not installed. Add 'yfinance' to requirements and rebuild the image.")
    
    results = []
    for symbol in symbols:
        attempt = 0
        success = False
        
        while attempt < MAX_ATTEMPTS and not success:
            attempt += 1
            try:
                logger.info(f"Fetching {symbol} (attempt {attempt}/{MAX_ATTEMPTS})")
                
                # Configure yfinance with longer timeout and retry
                ticker = yf.Ticker(symbol)
                
                # Method 1: Try to get current data from info
                try:
                    info = ticker.info
                    if info and len(info) > 5:  # Basic check for valid data
                        price = (info.get("regularMarketPrice") or 
                                info.get("currentPrice") or 
                                info.get("previousClose"))
                        change = info.get("regularMarketChange")
                        percent_change = info.get("regularMarketChangePercent")
                        volume = (info.get("regularMarketVolume") or 
                                info.get("volume") or 
                                info.get("averageVolume"))
                        
                        if price is not None:
                            mapped = {
                                "symbol": symbol,
                                "price": _safe_float(price),
                                "change": _safe_float(change),
                                "percent_change": _safe_float(percent_change),
                                "volume": _safe_int(volume),
                                "raw": {"method": "info", "source": "yfinance"},
                            }
                            results.append(mapped)
                            logger.info(f"Successfully fetched {symbol} using info method: price=${price}")
                            success = True
                            time.sleep(REQUEST_DELAY_SECONDS)
                            continue
                except Exception as e:
                    logger.warning(f"Info method failed for {symbol}: {e}")
                
                # Method 2: Try historical data as fallback
                try:
                    hist = ticker.history(period="5d", timeout=REQUEST_TIMEOUT)
                    if not hist.empty:
                        last_row = hist.iloc[-1]
                        price = float(last_row["Close"])
                        volume = int(last_row["Volume"]) if not pd.isna(last_row["Volume"]) else None
                        
                        # Calculate change if we have enough data
                        change = None
                        percent_change = None
                        if len(hist) >= 2:
                            prev_close = float(hist.iloc[-2]["Close"])
                            change = price - prev_close
                            percent_change = (change / prev_close) * 100 if prev_close != 0 else None
                        
                        mapped = {
                            "symbol": symbol,
                            "price": _safe_float(price),
                            "change": _safe_float(change),
                            "percent_change": _safe_float(percent_change),
                            "volume": _safe_int(volume),
                            "raw": {"method": "history", "source": "yfinance"},
                        }
                        results.append(mapped)
                        logger.info(f"Successfully fetched {symbol} using history method: price=${price}")
                        success = True
                        time.sleep(REQUEST_DELAY_SECONDS)
                        continue
                except Exception as e:
                    logger.warning(f"History method failed for {symbol}: {e}")
                
                # If we reach here, both methods failed
                if attempt < MAX_ATTEMPTS:
                    wait_time = BACKOFF_FACTOR ** attempt + random.uniform(1, 3)
                    logger.warning(f"All methods failed for {symbol} (attempt {attempt}). Retrying in {wait_time:.1f}s")
                    time.sleep(wait_time)
                
            except Exception as exc:
                if attempt < MAX_ATTEMPTS:
                    wait_time = BACKOFF_FACTOR ** attempt + random.uniform(1, 3)
                    logger.warning(f"Exception fetching {symbol}: {exc} (attempt {attempt}). Retrying in {wait_time:.1f}s")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {symbol} after {MAX_ATTEMPTS} attempts: {exc}")
        
        if not success:
            logger.error(f"Failed to fetch {symbol} after all attempts")
            # Add a placeholder entry with None values to avoid breaking the pipeline
            results.append({
                "symbol": symbol,
                "price": None,
                "change": None,
                "percent_change": None,
                "volume": None,
                "raw": {"error": "failed_all_attempts", "source": "yfinance"},
            })
    
    return results


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
                
                if resp.status_code != 200:
                    logger.warning(f"AlphaVantage HTTP {resp.status_code} for {symbol}")
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(BACKOFF_FACTOR ** attempt)
                        continue
                    else:
                        break
                
                data = resp.json() if resp.text else {}
                if "Note" in data:
                    wait = BACKOFF_FACTOR ** attempt + random.uniform(0, 2) + REQUEST_DELAY_SECONDS
                    logger.warning("AlphaVantage rate limit for %s: %s. Waiting %.1fs", symbol, data.get("Note"), wait)
                    time.sleep(wait)
                    continue
                    
                gq = data.get("Global Quote") or {}
                if not gq:
                    logger.warning("AlphaVantage empty Global Quote for %s: %s", symbol, data)
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(BACKOFF_FACTOR ** attempt)
                        continue
                    else:
                        break
                        
                mapped = {
                    "symbol": gq.get("01. symbol"),
                    "price": _safe_float(gq.get("05. price")),
                    "change": _safe_float(gq.get("09. change")),
                    "percent_change": _parse_percent_string(gq.get("10. change percent")),
                    "volume": _safe_int(gq.get("06. volume")),
                    "raw": gq,
                }
                results.append(mapped)
                time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 1))
                break
                
            except requests.RequestException as exc:
                wait = BACKOFF_FACTOR ** attempt + random.uniform(0, 2)
                logger.warning("AlphaVantage network error %s: %s (attempt %d). Retrying in %.1fs", symbol, exc, attempt, wait)
                if attempt < MAX_ATTEMPTS:
                    time.sleep(wait)
        else:
            logger.error(f"Failed to fetch {symbol} from AlphaVantage after {MAX_ATTEMPTS} attempts")
            # Add placeholder
            results.append({
                "symbol": symbol,
                "price": None,
                "change": None,
                "percent_change": None,
                "volume": None,
                "raw": {"error": "failed_all_attempts", "source": "alphavantage"},
            })
    return results


def _fetch_yahoo_direct(symbols: List[str]) -> List[Dict[str, Any]]:
    """Direct Yahoo Finance API with enhanced headers"""
    results = []
    for symbol in symbols:
        attempt = 0
        while attempt < MAX_ATTEMPTS:
            attempt += 1
            try:
                # Use the more reliable Yahoo Finance endpoint
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache"
                }
                
                resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                
                if resp.status_code == 429:
                    wait = BACKOFF_FACTOR ** attempt + random.uniform(5, 10)
                    logger.warning("Yahoo 429 for %s: waiting %.1fs (attempt %d)", symbol, wait, attempt)
                    time.sleep(wait)
                    continue
                    
                if resp.status_code != 200:
                    logger.warning(f"Yahoo HTTP {resp.status_code} for {symbol}")
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(BACKOFF_FACTOR ** attempt)
                        continue
                    else:
                        break
                
                data = resp.json()
                chart = data.get("chart", {})
                result = chart.get("result", [])
                
                if not result:
                    logger.warning(f"No chart data for {symbol}")
                    if attempt < MAX_ATTEMPTS:
                        time.sleep(BACKOFF_FACTOR ** attempt)
                        continue
                    else:
                        break
                
                quote_data = result[0]
                meta = quote_data.get("meta", {})
                indicators = quote_data.get("indicators", {})
                quote = indicators.get("quote", [{}])[0] if indicators.get("quote") else {}
                
                # Extract current price and other data
                current_price = meta.get("regularMarketPrice") or meta.get("previousClose")
                volume = meta.get("regularMarketVolume")
                
                mapped = {
                    "symbol": symbol,
                    "price": _safe_float(current_price),
                    "change": _safe_float(meta.get("regularMarketChange")),
                    "percent_change": _safe_float(meta.get("regularMarketChangePercent")),
                    "volume": _safe_int(volume),
                    "raw": {"method": "chart", "source": "yahoo_direct"},
                }
                results.append(mapped)
                logger.info(f"Successfully fetched {symbol} via Yahoo direct: price=${current_price}")
                time.sleep(REQUEST_DELAY_SECONDS + random.uniform(0, 2))
                break
                
            except Exception as exc:
                wait = BACKOFF_FACTOR ** attempt + random.uniform(0, 2)
                logger.warning("Yahoo direct error %s: %s (attempt %d). Retrying in %.1fs", symbol, exc, attempt, wait)
                if attempt < MAX_ATTEMPTS:
                    time.sleep(wait)
        else:
            logger.error(f"Failed to fetch {symbol} from Yahoo direct after {MAX_ATTEMPTS} attempts")
            results.append({
                "symbol": symbol,
                "price": None,
                "change": None,
                "percent_change": None,
                "volume": None,
                "raw": {"error": "failed_all_attempts", "source": "yahoo_direct"},
            })
    
    return results


def fetch_quotes(symbols: List[str]) -> List[Dict[str, Any]]:
    logger.info("Provider=%s | fetching %d symbols", MARKET_PROVIDER, len(symbols))
    
    try:
        if MARKET_PROVIDER == "alphavantage":
            return _fetch_alphavantage(symbols)
        elif MARKET_PROVIDER == "yahoo_direct":
            return _fetch_yahoo_direct(symbols)
        else:  # default to yfinance
            return _fetch_yfinance_enhanced(symbols)
    except Exception as e:
        logger.error(f"Primary provider {MARKET_PROVIDER} failed: {e}")
        
        # Fallback strategy
        if MARKET_PROVIDER != "yfinance" and HAVE_YFINANCE:
            logger.info("Falling back to yfinance")
            try:
                return _fetch_yfinance_enhanced(symbols)
            except Exception as fallback_e:
                logger.error(f"Fallback to yfinance also failed: {fallback_e}")
        
        # Return empty list if all methods fail
        logger.error("All fetch methods failed, returning empty results")
        return []


# === Enhanced DB logic ===

def parse_quote_item(item: Dict[str, Any]) -> Dict[str, Any]:
    # Check if it's our normalized format
    if "symbol" in item and ("price" in item or "raw" in item):
        return {
            "symbol": item.get("symbol"),
            "price": _safe_float(item.get("price")),
            "change": _safe_float(item.get("change")),
            "percent_change": _safe_float(item.get("percent_change")),
            "volume": _safe_int(item.get("volume")),
        }
    
    # Handle raw Yahoo Finance format
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
    max_retries = 5
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                host=DB_HOST,
                port=DB_PORT,
                connect_timeout=10
            )
            return conn
        except psycopg2.Error as e:
            logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise


def upsert_quotes_to_db(quotes: List[Dict[str, Any]]):
    if not quotes:
        logger.info("No quotes to upsert.")
        return

    # Filter out quotes with missing essential data
    valid_quotes = []
    for q in quotes:
        parsed = parse_quote_item(q)
        if parsed["symbol"] and parsed["price"] is not None:
            valid_quotes.append(parsed)
        else:
            logger.warning(f"Skipping invalid quote: symbol={parsed['symbol']}, price={parsed['price']}")

    if not valid_quotes:
        logger.warning("No valid quotes to write to database.")
        return

    snapshot_values = []
    history_values = []
    
    for parsed in valid_quotes:
        sym = parsed["symbol"]
        snapshot_values.append((
            sym,
            parsed["price"],
            parsed["change"],
            parsed["percent_change"],
            parsed["volume"]
        ))
        history_values.append((
            sym,
            parsed["price"],
            parsed["change"],
            parsed["percent_change"],
            parsed["volume"]
        ))

    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                ts = datetime.utcnow()
                
                # Upsert into stock_price (current snapshot)
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
                upsert_tuples = [
                    (sym, price, change, pct, vol, ts)
                    for (sym, price, change, pct, vol) in snapshot_values
                ]
                execute_values(cur, upsert_sql, upsert_tuples)
                logger.info("Upserted %d rows into stock_price", len(upsert_tuples))

                # Insert into stock_price_history (historical record)
                insert_history_sql = """
                INSERT INTO stock_price_history (symbol, price, change, percent_change, volume, fetched_at)
                VALUES %s
                """
                history_tuples = [
                    (sym, price, change, pct, vol, ts)
                    for (sym, price, change, pct, vol) in history_values
                ]
                execute_values(cur, insert_history_sql, history_tuples)
                logger.info("Inserted %d rows into stock_price_history", len(history_tuples))
                
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        conn.close()


def fetch_and_store(symbols: List[str]):
    logger.info("Starting fetch_and_store for symbols: %s", symbols)
    
    try:
        quotes = fetch_quotes(symbols)
        if not quotes:
            logger.error("No quotes fetched from any provider.")
            return
            
        logger.info(f"Fetched {len(quotes)} quotes")
        upsert_quotes_to_db(quotes)
        logger.info("fetch_and_store completed successfully for %d symbols", len(symbols))
        
    except Exception as e:
        logger.exception(f"Error in fetch_and_store: {e}")
        raise


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
            
        logger.info(f"Starting with symbols: {symbols}")
        fetch_and_store(symbols)
        
    except Exception as exc:
        logger.exception("Fatal error in fetch_and_upsert: %s", exc)
        raise