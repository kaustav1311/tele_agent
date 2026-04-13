# src/api/routes/signals.py

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from sqlmodel import create_engine

import os
import traceback
import sqlite3

ET  = ZoneInfo("America/New_York")
IST = ZoneInfo("Asia/Kolkata")

router = APIRouter()

TIMEFRAMES = ["15m", "1h", "4h", "daily"]


def _get_conn():
    """Raw sqlite3 connection — avoids SQLModel exec() param binding quirks."""
    db_path = os.environ.get("DB_PATH", "data/signals.db")
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row  # access columns by name, not index
    return conn


def _parse_ts(val) -> datetime | None:
    """Parse a timestamp value that may be a string, datetime, or None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        # SQLite stores as 'YYYY-MM-DD HH:MM:SS.ffffff' or ISO format
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _format_signal(row) -> dict:
    """Convert a named-column sqlite3.Row to API response dict."""
    ts_utc = _parse_ts(row["timestamp"])
    fetched_at = _parse_ts(row["metrics_fetched_at"])
    created_at = _parse_ts(row["created_at"])

    return {
        "message_id":           row["message_id"],
        "ticker":               row["ticker"],
        "pair":                 row["pair"],
        "activity_type":        row["activity_type"],
        "activity_raw":         row["activity_raw"],
        "boost":                row["boost"],
        "alerts_this_hour":     row["alerts_this_hour"],
        "alerts_tier":          row["alerts_tier"],
        "price_at_signal":      row["price_at_signal"],
        "change_24h":           row["change_24h"],
        "timestamp_utc":        ts_utc.isoformat() if ts_utc else None,
        "timestamp_et":         ts_utc.astimezone(ET).isoformat() if ts_utc else None,
        "timestamp_ist":        ts_utc.astimezone(IST).isoformat() if ts_utc else None,
        "created_at":           created_at.isoformat() if created_at else None,
        "call_count":           row["call_count"],
        "live_price":           row["live_price"],
        "metrics_fetched_at":   fetched_at.isoformat() if fetched_at else None,
    }


SQL = """
    WITH latest AS (
        SELECT ticker, MAX(timestamp) AS max_ts
        FROM signals
        WHERE timestamp >= ?
        GROUP BY ticker
    )
    SELECT
        s.message_id, s.ticker, s.pair,
        s.activity_type, s.activity_raw,
        s.boost, s.alerts_this_hour, s.alerts_tier,
        s.price_at_signal, s.change_24h,
        s.timestamp, s.created_at,
        (
            SELECT COUNT(*) FROM signals s2
            WHERE s2.ticker = s.ticker
            AND s2.timestamp >= ?
        ) AS call_count,
        m.price         AS live_price,
        m.fetched_at    AS metrics_fetched_at
    FROM signals s
    JOIN latest l ON s.ticker = l.ticker AND s.timestamp = l.max_ts
    LEFT JOIN metrics_cache m ON m.ticker = s.ticker
    ORDER BY s.boost DESC, s.timestamp DESC
"""


@router.get("/signals/summary")
def signals_summary():
    try:
        from src.api.utils.candles import get_candle_start

        now_utc = datetime.now(timezone.utc)
        candle_starts = {tf: get_candle_start(tf) for tf in TIMEFRAMES}

        conn = _get_conn()
        try:
            windows = {}
            for tf in TIMEFRAMES:
                # SQLite uses ? placeholders — pass candle_start twice (outer WHERE + subquery WHERE)
                candle_start_str = candle_starts[tf].strftime("%Y-%m-%d %H:%M:%S")
                cursor = conn.execute(SQL, (candle_start_str, candle_start_str))
                rows = cursor.fetchall()
                windows[tf] = [_format_signal(r) for r in rows]
        finally:
            conn.close()

        return {
            "fetched_at_utc": now_utc.isoformat(),
            "candle_starts":  {tf: v.isoformat() for tf, v in candle_starts.items()},
            "windows":        windows,
        }

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))