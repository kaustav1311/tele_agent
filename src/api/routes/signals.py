# src/api/routes/signals.py

from datetime import datetime, timezone, timedelta
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


def _format_signal(row, has_conflicting_activity: bool = False) -> dict:
    """Convert a named-column sqlite3.Row to API response dict."""
    ts_utc = _parse_ts(row["timestamp"])
    fetched_at = _parse_ts(row["metrics_fetched_at"])
    created_at = _parse_ts(row["created_at"])

    return {
        "message_id":               row["message_id"],
        "ticker":                   row["ticker"],
        "pair":                     row["pair"],
        "activity_type":            row["activity_type"],
        "activity_raw":             row["activity_raw"],
        "boost":                    row["boost"],
        "alerts_this_hour":         row["alerts_this_hour"],
        "alerts_tier":              row["alerts_tier"],
        "price_at_signal":          row["price_at_signal"],
        "change_24h":               row["change_24h"],
        "timestamp_utc":            ts_utc.isoformat() if ts_utc else None,
        "timestamp_et":             ts_utc.astimezone(ET).isoformat() if ts_utc else None,
        "timestamp_ist":            ts_utc.astimezone(IST).isoformat() if ts_utc else None,
        "created_at":               created_at.isoformat() if created_at else None,
        "call_count":               row["call_count"],
        "has_conflicting_activity": has_conflicting_activity,
        "live_price":               row["live_price"],
        "metrics_fetched_at":       fetched_at.isoformat() if fetched_at else None,
    }


SQL = """
    WITH ranked_signals AS (
        SELECT
            s.*,
            ROW_NUMBER() OVER (PARTITION BY s.ticker, s.activity_type ORDER BY s.timestamp DESC) as rn
        FROM signals s
        WHERE s.timestamp >= ?
    )
    SELECT
        rs.message_id, rs.ticker, rs.pair,
        rs.activity_type, rs.activity_raw,
        rs.boost, rs.alerts_this_hour, rs.alerts_tier,
        rs.price_at_signal, rs.change_24h,
        rs.timestamp, rs.created_at,
        (SELECT COUNT(*) FROM signals
         WHERE ticker = rs.ticker
         AND activity_type = rs.activity_type
         AND timestamp >= ?) AS call_count,
        m.price         AS live_price,
        m.fetched_at    AS metrics_fetched_at
    FROM ranked_signals rs
    LEFT JOIN metrics_cache m ON m.ticker = rs.ticker
    WHERE rs.rn = 1
    ORDER BY rs.boost DESC, rs.timestamp DESC
"""


def get_streak_days(ticker: str, conn) -> int:
    """Calculate the number of consecutive days with signals for a ticker.

    Uses the latest day-ending group and counts consecutive days back.
    """
    query = """
    WITH numbered AS (
        SELECT et_day,
               julianday(et_day) - ROW_NUMBER() OVER (ORDER BY et_day DESC) as grp
        FROM daily_signal_summary WHERE ticker = ?
    )
    SELECT COUNT(*) as streak FROM numbered
    WHERE grp = (SELECT grp FROM numbered ORDER BY et_day DESC LIMIT 1)
    """
    row = conn.execute(query, (ticker,)).fetchone()
    return row["streak"] if row else 0


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
                candle_start_str = candle_starts[tf].strftime("%Y-%m-%d %H:%M:%S")
                cursor = conn.execute(SQL, (candle_start_str, candle_start_str))
                rows = cursor.fetchall()

                # Build set of tickers that have both BUY and SELL in this timeframe
                activity_types_by_ticker = {}
                for r in rows:
                    ticker = r["ticker"]
                    activity = r["activity_type"]
                    if ticker not in activity_types_by_ticker:
                        activity_types_by_ticker[ticker] = set()
                    activity_types_by_ticker[ticker].add(activity)

                # Format signals with conflicting_activity flag
                formatted_rows = []
                for r in rows:
                    ticker = r["ticker"]
                    has_conflict = len(activity_types_by_ticker[ticker]) > 1
                    formatted_rows.append(_format_signal(r, has_conflicting_activity=has_conflict))

                windows[tf] = formatted_rows
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


@router.get("/signals/prev-day")
def signals_prev_day():
    """
    Get signal summary for the previous ET calendar day.

    Returns a list of tickers that had signals on prev_et_day, sorted by
    max_boost descending. Includes first call details and streak calculation.
    """
    try:
        conn = _get_conn()
        try:
            # Get current ET date and compute prev_et_day
            now_et = datetime.now(ET)
            today_et = now_et.date()
            prev_et_day = (today_et - timedelta(days=1)).isoformat()

            # Query daily_signal_summary for prev_et_day
            cursor = conn.execute(
                "SELECT * FROM daily_signal_summary WHERE et_day = ?",
                (prev_et_day,)
            )
            summary_rows = cursor.fetchall()

            result = []

            for summary in summary_rows:
                ticker = summary["ticker"]

                # Fetch first signal record by message_id
                cursor = conn.execute(
                    "SELECT * FROM signals WHERE message_id = ?",
                    (summary["first_message_id"],)
                )
                first_row = cursor.fetchone()

                if not first_row:
                    continue

                # Parse timestamp to ET time string
                first_ts = _parse_ts(first_row["timestamp"])
                first_time_et = first_ts.astimezone(ET).strftime("%H:%M") if first_ts else None

                first_price = float(first_row["price_at_signal"])
                first_activity = first_row["activity_type"]

                item = {
                    "ticker": ticker,
                    "et_day": prev_et_day,
                    "call_count": summary["signal_count"],
                    "streak_days": get_streak_days(ticker, conn),
                    "max_boost": summary["max_boost"],
                    "first_call": {
                        "time_et": first_time_et,
                        "price": first_price,
                        "activity_type": first_activity,
                        "activity_raw": first_row["activity_raw"],
                        "boost": first_row["boost"]
                    }
                }

                result.append(item)

            # Sort by max_boost descending
            result.sort(key=lambda x: x["max_boost"], reverse=True)

            return result

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))