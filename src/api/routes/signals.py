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

TIMEFRAMES = ["5m", "15m", "1h", "4h", "daily", "1hr_rolling"]


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
        "first_call_price":         row["first_call_price"],
        "last_call_price":          row["last_call_price"],
        "first_call_time_et":       row["first_call_time_et"],
        "last_call_time_et":        row["last_call_time_et"],
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
        dc.first_call_price,
        dc.last_call_price,
        dc.first_call_time_et,
        dc.last_call_time_et
    FROM ranked_signals rs
    LEFT JOIN daily_calls dc ON dc.ticker = rs.ticker
        AND dc.activity_type = rs.activity_type
        AND dc.et_day = ?
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
        today_et_day = datetime.now(ET).date().isoformat()
        candle_starts = {tf: get_candle_start(tf) for tf in TIMEFRAMES}

        conn = _get_conn()
        try:
            windows = {}
            for tf in TIMEFRAMES:
                candle_start_str = candle_starts[tf].strftime("%Y-%m-%d %H:%M:%S")
                cursor = conn.execute(SQL, (candle_start_str, candle_start_str, today_et_day))
                rows = cursor.fetchall()

                # Build set of tickers that have both BUY and SELL in this timeframe
                activity_types_by_ticker = {}
                for r in rows:
                    ticker = r["ticker"]
                    activity = r["activity_type"]
                    if ticker not in activity_types_by_ticker:
                        activity_types_by_ticker[ticker] = set()
                    activity_types_by_ticker[ticker].add(activity)

                # Format signals with conflicting_activity flag and streak_days
                formatted_rows = []
                streak_cache = {}
                for r in rows:
                    ticker = r["ticker"]
                    has_conflict = len(activity_types_by_ticker[ticker]) > 1
                    if ticker not in streak_cache:
                        streak_cache[ticker] = get_streak_days(ticker, conn)
                    formatted = _format_signal(r, has_conflicting_activity=has_conflict)
                    formatted["streak_days"] = streak_cache[ticker]
                    formatted_rows.append(formatted)

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
def signals_prev_day(
    sort: str = "avg_boost",
    activity_type: str = "ALL",
    boost_min: int | None = None,
):
    """
    Get signal summary for the previous ET calendar day, sourced from daily_calls.

    Returns per-(ticker × activity_type) rows with call prices, timing, and performance metrics.

    Query parameters:
    - sort: "avg_boost" (default) | "first_call_time" | "call_count" | "intraday_drift"
    - activity_type: "BUY" | "SELL" | "ALL" (default)
    - boost_min: Optional. Filter rows where max_boost >= boost_min

    Response shape:
    {
        "et_day": "2026-04-13",
        "summary": {
            "total_signals": 42,
            "total_ticker_directions": 12,
            "buy_tickers": 8,
            "sell_tickers": 4,
            "avg_boost_day": 5.8
        },
        "tickers": [...]
    }
    """
    try:
        conn = _get_conn()
        try:
            # ET-correct date computation
            prev_et_day = (datetime.now(ET).date() - timedelta(days=1)).isoformat()

            # Query daily_calls for prev_et_day with optional filters
            where_clause = "WHERE dc.et_day = ?"
            params = [prev_et_day]

            if activity_type in ("BUY", "SELL"):
                where_clause += " AND dc.activity_type = ?"
                params.append(activity_type)

            if boost_min is not None:
                where_clause += " AND dc.max_boost >= ?"
                params.append(boost_min)

            daily_calls_rows = conn.execute(
                f"""
                SELECT
                    dc.ticker, dc.activity_type, dc.call_count, dc.max_boost,
                    dc.first_call_price, dc.last_call_price,
                    dc.first_call_time_et, dc.last_call_time_et,
                    dc.intraday_drift_pct,
                    dc.direction_correct,
                    dc.first_call_efficiency_pct,
                    dc.last_call_efficiency_pct,
                    dc.eod_price,
                    dc.mcap_tier,
                    dc.dq_first_price_missing, dc.dq_last_price_missing, dc.dq_eod_missing
                FROM daily_calls dc
                {where_clause}
                """
                , params
            ).fetchall()

            # Compute avg_boost per (ticker, activity_type) from signals
            boost_query_where = "WHERE DATE(datetime(s.timestamp, '-4 hours')) = ?"
            boost_params = [prev_et_day]

            if activity_type in ("BUY", "SELL"):
                boost_query_where += " AND s.activity_type = ?"
                boost_params.append(activity_type)

            avg_boost_rows = conn.execute(
                f"""
                SELECT s.ticker, s.activity_type,
                    ROUND(AVG(s.boost), 2) as avg_boost
                FROM signals s
                {boost_query_where}
                GROUP BY s.ticker, s.activity_type
                """
                , boost_params
            ).fetchall()

            # Build avg_boost lookup
            avg_boost_map = {}
            for row in avg_boost_rows:
                key = (row["ticker"], row["activity_type"])
                avg_boost_map[key] = row["avg_boost"]

            # Compute header stats
            total_signals = conn.execute(
                f"SELECT COUNT(*) as cnt FROM signals s {boost_query_where}",
                boost_params
            ).fetchone()["cnt"]

            buy_count = sum(1 for r in daily_calls_rows if r["activity_type"] == "BUY")
            sell_count = sum(1 for r in daily_calls_rows if r["activity_type"] == "SELL")

            # Calculate avg_boost for day
            if daily_calls_rows:
                day_boost_sum = sum(
                    avg_boost_map.get((r["ticker"], r["activity_type"]), 0)
                    for r in daily_calls_rows
                )
                day_boost_count = len(daily_calls_rows)
                avg_boost_day = round(day_boost_sum / day_boost_count, 2) if day_boost_count > 0 else 0.0
            else:
                avg_boost_day = 0.0

            # Build ticker rows
            tickers = []
            for dc_row in daily_calls_rows:
                ticker = dc_row["ticker"]
                act_type = dc_row["activity_type"]
                avg_boost = avg_boost_map.get((ticker, act_type), None)

                tickers.append({
                    "ticker": ticker,
                    "activity_type": act_type,
                    "call_count": dc_row["call_count"],
                    "avg_boost": avg_boost,
                    "first_call_price": dc_row["first_call_price"],
                    "last_call_price": dc_row["last_call_price"],
                    "first_call_time_et": dc_row["first_call_time_et"],
                    "last_call_time_et": dc_row["last_call_time_et"],
                    "intraday_drift_pct": dc_row["intraday_drift_pct"],
                    "direction_correct": dc_row["direction_correct"],
                    "first_call_efficiency_pct": dc_row["first_call_efficiency_pct"],
                    "last_call_efficiency_pct": dc_row["last_call_efficiency_pct"],
                    "eod_price": dc_row["eod_price"],
                    "mcap_tier": dc_row["mcap_tier"],
                    "dq_flags": {
                        "first_price_missing": bool(dc_row["dq_first_price_missing"]),
                        "last_price_missing": bool(dc_row["dq_last_price_missing"]),
                        "eod_missing": bool(dc_row["dq_eod_missing"]),
                    }
                })

            # Apply sorting
            if sort == "first_call_time":
                tickers.sort(key=lambda x: x["first_call_time_et"] or "")
            elif sort == "call_count":
                tickers.sort(key=lambda x: x["call_count"], reverse=True)
            elif sort == "intraday_drift":
                tickers.sort(key=lambda x: abs(x["intraday_drift_pct"] or 0), reverse=True)
            else:  # avg_boost (default)
                tickers.sort(key=lambda x: (x["avg_boost"] or 0), reverse=True)

            return {
                "et_day": prev_et_day,
                "summary": {
                    "total_signals": total_signals,
                    "total_ticker_directions": len(daily_calls_rows),
                    "buy_tickers": buy_count,
                    "sell_tickers": sell_count,
                    "avg_boost_day": avg_boost_day,
                },
                "tickers": tickers,
            }

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))