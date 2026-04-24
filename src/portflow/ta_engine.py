# src/portflow/ta_engine.py
# Pure-Python TA compute layer for the Portflow watchlist.
# Pulls candles from Binance, computes RSI/ATR/EMA-stack/volume-ratio,
# and upserts results into watchlist_ta_cache.

import logging
import time
from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import requests
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

from src.portflow.db import get_portflow_conn

logger = logging.getLogger(__name__)

TIMEFRAMES = ["15m", "1h", "4h", "1d", "1w"]
BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
KLINE_LIMIT = 40
RSI_HISTORY_CANDLES = {"1w": 10, "1d": 30, "1h": 20, "15m": 10, "4h": 10}


def _kline_limit(tf: str) -> int:
    if tf == "1d":
        return 220
    if tf == "1w":
        return 30
    if tf == "1h":
        return 60
    if tf == "15m":
        # Binance pre-announces ~96 future 15m candles (24h × 4).
        # Fetching 200 guarantees at least 100 past closed candles after filtering.
        return 200
    return KLINE_LIMIT
RSI_PERIOD = 14
ATR_PERIOD = 14
EMA_PERIODS = [20, 50, 200]
VOL_MA_PERIOD = 20

_STABLES = {"USDT", "USDC", "DAI", "BUSD", "TUSD"}


def build_symbol(ticker: str) -> Optional[str]:
    t = ticker.upper()
    if t in _STABLES:
        return None
    return f"{t}USDT"


def fetch_klines(symbol: str, interval: str, limit: int) -> Tuple[Optional[pd.DataFrame], int]:
    try:
        resp = requests.get(
            BINANCE_KLINES_URL,
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10,
        )
    except requests.RequestException as e:
        logger.warning("Binance request failed for %s %s: %s", symbol, interval, e)
        return None, 0

    if resp.status_code == 400:
        return None, 0

    if resp.status_code != 200:
        logger.warning("Binance HTTP %s for %s %s", resp.status_code, symbol, interval)
        return None, 0

    weight_used = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", "0") or 0)

    try:
        rows = resp.json()
    except ValueError as e:
        logger.warning("Binance JSON decode failed for %s %s: %s", symbol, interval, e)
        return None, weight_used

    if not rows:
        return None, weight_used

    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_vol", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore",
        ],
    )
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)

    return df, weight_used


def derive_rsi_direction(rsi: float, rsi_prev1: float, rsi_prev2: float) -> str:
    slope = rsi - rsi_prev2
    if slope > 3:
        return "RISING"
    if slope > 1:
        return "CLIMBING"
    if slope < -3:
        return "DROPPING"
    if slope < -1:
        return "FALLING"
    return "FLAT"


def _round(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and (value != value):  # NaN check
        return None
    return round(float(value), 4)


def compute_ta(df: pd.DataFrame, timeframe: str) -> dict:
    out = {
        "rsi": None,
        "rsi_prev1": None,
        "rsi_prev2": None,
        "rsi_direction": None,
        "atr": None,
        "atr_pct": None,
        "ema_stack": None,
        "vol_ratio": None,
        "rsi_history": [],
    }

    rsi_series = RSIIndicator(close=df["close"], window=RSI_PERIOD, fillna=False).rsi()
    if rsi_series is not None and len(rsi_series.dropna()) >= 3:
        rsi = float(rsi_series.iloc[-1])
        rsi_prev1 = float(rsi_series.iloc[-2])
        rsi_prev2 = float(rsi_series.iloc[-3])
        out["rsi"] = _round(rsi)
        out["rsi_prev1"] = _round(rsi_prev1)
        out["rsi_prev2"] = _round(rsi_prev2)
        out["rsi_direction"] = derive_rsi_direction(rsi, rsi_prev1, rsi_prev2)

        # Last N closed-candle RSI values.
        # Binance pre-announces future candles at the tail of every klines response
        # (up to ~7 hours worth, proportionally more for shorter timeframes).
        # Scanning the entire df and filtering by close_time > now_ms is the only
        # reliable way to discard every forming/future candle regardless of how many
        # Binance appends. We then take exactly the last N valid closed candles.
        n_history = RSI_HISTORY_CANDLES.get(timeframe, 10)
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        history = []
        for idx, row in df.iterrows():
            close_time_ms = int(row["close_time"])
            if close_time_ms > now_ms:
                continue  # candle still forming or future placeholder — skip
            rsi_val = rsi_series.iloc[idx] if idx in rsi_series.index else None
            if rsi_val is None or rsi_val != rsi_val:  # NaN (RSI warmup period)
                continue
            close_iso = datetime.utcfromtimestamp(close_time_ms / 1000).isoformat() + "Z"
            history.append((close_iso, round(float(rsi_val), 4)))
        out["rsi_history"] = history[-n_history:]  # keep exactly the last N closes

    if timeframe in ("1h", "1d"):
        atr_series = AverageTrueRange(
            high=df["high"], low=df["low"], close=df["close"],
            window=ATR_PERIOD, fillna=False,
        ).average_true_range()
        if atr_series is not None and len(atr_series.dropna()) > 0:
            atr = float(atr_series.iloc[-1])
            close = float(df["close"].iloc[-1])
            out["atr"] = _round(atr)
            if close:
                out["atr_pct"] = _round((atr / close) * 100)

        ema20 = EMAIndicator(close=df["close"], window=20, fillna=False).ema_indicator()
        ema50 = EMAIndicator(close=df["close"], window=50, fillna=False).ema_indicator()
        ema200 = EMAIndicator(close=df["close"], window=200, fillna=False).ema_indicator()
        if (
            len(ema20.dropna()) and len(ema50.dropna()) and len(ema200.dropna())
        ):
            e20 = float(ema20.iloc[-1])
            e50 = float(ema50.iloc[-1])
            e200 = float(ema200.iloc[-1])
            close = float(df["close"].iloc[-1])
            if close > e20 > e50 > e200:
                out["ema_stack"] = "BULLISH"
            elif close < e20 < e50 < e200:
                out["ema_stack"] = "BEARISH"
            else:
                out["ema_stack"] = "MIXED"

    if timeframe == "1h":
        if len(df) >= VOL_MA_PERIOD:
            vol_avg = float(df["volume"].iloc[-VOL_MA_PERIOD:].mean())
            if vol_avg:
                out["vol_ratio"] = _round(float(df["volume"].iloc[-1]) / vol_avg)

    return out


def upsert_ta_cache(ticker: str, timeframe: str, ta: dict) -> None:
    computed_at = datetime.utcnow().isoformat() + "Z"
    conn = get_portflow_conn()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO watchlist_ta_cache
                (ticker, timeframe, rsi, rsi_prev1, rsi_prev2, rsi_direction,
                 atr, atr_pct, ema_stack, vol_ratio, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                timeframe,
                ta.get("rsi"),
                ta.get("rsi_prev1"),
                ta.get("rsi_prev2"),
                ta.get("rsi_direction"),
                ta.get("atr"),
                ta.get("atr_pct"),
                ta.get("ema_stack"),
                ta.get("vol_ratio"),
                computed_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_rsi_history(ticker: str, timeframe: str, pairs: list) -> None:
    if not pairs:
        return
    n_keep = RSI_HISTORY_CANDLES.get(timeframe, 10)
    conn = get_portflow_conn()
    try:
        conn.executemany(
            """
            INSERT OR REPLACE INTO watchlist_rsi_history
                (ticker, timeframe, candle_close_time, rsi_value)
            VALUES (?, ?, ?, ?)
            """,
            [(ticker, timeframe, ct, rv) for ct, rv in pairs],
        )
        # Trim rows older than the N most recent closes for this (ticker, timeframe).
        conn.execute(
            """
            DELETE FROM watchlist_rsi_history
            WHERE ticker = ? AND timeframe = ?
              AND candle_close_time NOT IN (
                  SELECT candle_close_time
                  FROM watchlist_rsi_history
                  WHERE ticker = ? AND timeframe = ?
                  ORDER BY candle_close_time DESC
                  LIMIT ?
              )
            """,
            (ticker, timeframe, ticker, timeframe, n_keep),
        )
        conn.commit()
    finally:
        conn.close()


def _unsupported_payload() -> dict:
    return {
        "rsi": None,
        "rsi_prev1": None,
        "rsi_prev2": None,
        "rsi_direction": "UNSUPPORTED",
        "atr": None,
        "atr_pct": None,
        "ema_stack": None,
        "vol_ratio": None,
        "rsi_history": [],
    }


def bootstrap_ticker(ticker: str) -> dict:
    ticker = ticker.upper()
    symbol = build_symbol(ticker)

    for timeframe in TIMEFRAMES:
        if symbol is None:
            upsert_ta_cache(ticker, timeframe, _unsupported_payload())
            continue

        df, _ = fetch_klines(symbol, timeframe, _kline_limit(timeframe))
        if df is None:
            upsert_ta_cache(ticker, timeframe, _unsupported_payload())
            continue

        ta = compute_ta(df, timeframe)
        upsert_ta_cache(ticker, timeframe, ta)
        upsert_rsi_history(ticker, timeframe, ta.get("rsi_history", []))
        time.sleep(0.06)

    logger.info("Bootstrapped TA cache for %s", ticker)
    return {"ticker": ticker, "status": "ok", "timeframes_computed": len(TIMEFRAMES)}


def refresh_all_tickers() -> dict:
    conn = get_portflow_conn()
    try:
        rows = conn.execute("SELECT DISTINCT ticker FROM watchlist_tickers").fetchall()
    finally:
        conn.close()

    tickers = [r["ticker"] for r in rows]
    weight_budget = 0
    processed = 0
    stopped_early = False

    for ticker in tickers:
        symbol = build_symbol(ticker)
        if symbol is None:
            continue

        for timeframe in TIMEFRAMES:
            if weight_budget > 5000:
                logger.warning("Rate limit budget reached, stopping cycle")
                stopped_early = True
                break

            df, weight = fetch_klines(symbol, timeframe, _kline_limit(timeframe))
            weight_budget += weight
            if df is None:
                continue

            ta = compute_ta(df, timeframe)
            upsert_ta_cache(ticker, timeframe, ta)
            upsert_rsi_history(ticker, timeframe, ta.get("rsi_history", []))
            time.sleep(0.05)

        if stopped_early:
            break

        processed += 1

    logger.info(
        "Portflow refresh complete: tickers_processed=%s weight_used=%s",
        processed, weight_budget,
    )
    return {"tickers_processed": processed, "weight_used": weight_budget}
