# src/api/routes/analytics.py
# Analytics endpoints — heatmap/hourly, windows/4h, filters.

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text
import os
import sqlite3
import traceback

ET = ZoneInfo("America/New_York")

router = APIRouter()


def _get_conn():
    """Raw sqlite3 connection for parameterized queries."""
    db_path = os.environ.get("DB_PATH", "data/signals.db")
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_ts(val) -> datetime | None:
    """Parse a timestamp value that may be a string, datetime, or None."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _get_et_hour(ts_utc: datetime) -> int:
    """Convert UTC datetime to ET hour (0-23)."""
    if not ts_utc:
        return 0
    et_dt = ts_utc.astimezone(ET)
    return et_dt.hour


def _get_et_day_and_hour(ts_utc: datetime) -> tuple[str, int]:
    """Convert UTC datetime to (YYYY-MM-DD in ET, hour in ET)."""
    if not ts_utc:
        return ("", 0)
    et_dt = ts_utc.astimezone(ET)
    et_day = et_dt.strftime("%Y-%m-%d")
    return (et_day, et_dt.hour)


def _get_4h_window(hour: int) -> str:
    """Convert hour (0-23) to 4-hour window string."""
    window_start = (hour // 4) * 4
    window_end = window_start + 4
    return f"{window_start:02d}-{window_end:02d}"


@router.get("/analytics/heatmap/hourly")
def analytics_heatmap_hourly(
    range: str = Query("7d"),
    mcap_tier: str = Query("all"),
):
    """
    Returns signal count per ET hour (0–23) aggregated over a date range,
    optionally filtered by MCap tier.

    Query parameters:
    - range: "1d" (today) | "7d" (last 7 days, default) | "all" (full DB history)
    - mcap_tier: "micro" | "small" | "mid" | "large" | "unknown" | "all" (default)

    Response includes 24 hourly buckets with buy/sell split and avg boost.
    """
    try:
        if range not in ("1d", "7d", "all"):
            range = "7d"

        if mcap_tier not in ("micro", "small", "mid", "large", "unknown", "all"):
            mcap_tier = "all"

        conn = _get_conn()
        try:
            # Compute range_start in UTC
            now_et = datetime.now(ET)
            if range == "1d":
                range_start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
            elif range == "7d":
                range_start_et = (now_et - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
            else:  # "all"
                range_start = datetime.min.replace(tzinfo=timezone.utc)
                range_start = range_start.isoformat()

            if range != "all":
                range_start = range_start_et.astimezone(timezone.utc).isoformat()

            # Build query with optional mcap_tier filter
            if mcap_tier == "all":
                sql = """
                    SELECT
                        CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INTEGER) as hour_et,
                        COUNT(*) as count,
                        SUM(CASE WHEN s.activity_type='BUY'  THEN 1 ELSE 0 END) as buy_count,
                        SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                        ROUND(AVG(CASE WHEN s.boost IS NOT NULL THEN s.boost ELSE 0 END), 2) as avg_boost
                    FROM signals s
                    WHERE s.timestamp >= ?
                    GROUP BY hour_et
                    ORDER BY hour_et
                """
                params = (range_start,)
            else:
                sql = """
                    SELECT
                        CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INTEGER) as hour_et,
                        COUNT(*) as count,
                        SUM(CASE WHEN s.activity_type='BUY'  THEN 1 ELSE 0 END) as buy_count,
                        SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                        ROUND(AVG(CASE WHEN s.boost IS NOT NULL THEN s.boost ELSE 0 END), 2) as avg_boost
                    FROM signals s
                    LEFT JOIN daily_calls dc ON dc.ticker = s.ticker
                        AND dc.et_day = DATE(datetime(s.timestamp, '-4 hours'))
                        AND dc.activity_type = s.activity_type
                    WHERE s.timestamp >= ? AND dc.mcap_tier = ?
                    GROUP BY hour_et
                    ORDER BY hour_et
                """
                params = (range_start, mcap_tier)

            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()

            # Initialize 24-hour buckets
            hours = []
            hour_map = {row["hour_et"]: row for row in rows}

            for hour in range(24):
                if hour in hour_map:
                    r = hour_map[hour]
                    hours.append({
                        "hour_et": hour,
                        "count": r["count"],
                        "buy_count": r["buy_count"],
                        "sell_count": r["sell_count"],
                        "avg_boost": r["avg_boost"],
                    })
                else:
                    hours.append({
                        "hour_et": hour,
                        "count": 0,
                        "buy_count": 0,
                        "sell_count": 0,
                        "avg_boost": None,
                    })

            # Compute dataset_days and total stats
            stats_sql = """
                SELECT
                    COUNT(DISTINCT DATE(datetime(s.timestamp, '-4 hours'))) as dataset_days,
                    COUNT(*) as total_signals,
                    COUNT(DISTINCT s.ticker) as unique_tickers
                FROM signals s
                WHERE s.timestamp >= ?
            """

            if mcap_tier != "all":
                stats_sql = """
                    SELECT
                        COUNT(DISTINCT DATE(datetime(s.timestamp, '-4 hours'))) as dataset_days,
                        COUNT(*) as total_signals,
                        COUNT(DISTINCT s.ticker) as unique_tickers
                    FROM signals s
                    LEFT JOIN daily_calls dc ON dc.ticker = s.ticker
                        AND dc.et_day = DATE(datetime(s.timestamp, '-4 hours'))
                        AND dc.activity_type = s.activity_type
                    WHERE s.timestamp >= ? AND dc.mcap_tier = ?
                """

            stats_row = conn.execute(
                stats_sql,
                params if mcap_tier != "all" else (range_start,)
            ).fetchone()

            return {
                "range": range,
                "mcap_tier": mcap_tier,
                "dataset_days": stats_row["dataset_days"] if stats_row else 0,
                "total_signals": stats_row["total_signals"] if stats_row else 0,
                "unique_tickers": stats_row["unique_tickers"] if stats_row else 0,
                "hours": hours,
            }

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Analytics heatmap error: {str(e)}")


@router.get("/analytics/windows/4h")
def analytics_windows_4h(
    mcap_tier: str = Query("all"),
):
    """
    Returns per-4H-window aggregates of signal volume and boost.
    Performance metrics (win_rate, avg_return_pct) are currently blocked pending EOD price backfill.

    Query parameters:
    - mcap_tier: "micro" | "small" | "mid" | "large" | "unknown" | "all" (default)

    Response includes 6 windows (00-04, 04-08, etc.) with volume metrics.
    Performance fields (win_rate, avg_return_pct) return null with pending_backfill=true.
    """
    try:
        if mcap_tier not in ("micro", "small", "mid", "large", "unknown", "all"):
            mcap_tier = "all"

        conn = _get_conn()
        try:
            # Query signals for volume metrics (available now)
            if mcap_tier == "all":
                sql = """
                    SELECT
                        (CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INT) / 4) * 4 as window_start,
                        COUNT(*) as signal_count,
                        SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                        SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                        ROUND(AVG(CASE WHEN s.boost IS NOT NULL THEN s.boost ELSE 0 END), 2) as avg_boost
                    FROM signals s
                    GROUP BY window_start
                    ORDER BY window_start
                """
                params = ()
            else:
                sql = """
                    SELECT
                        (CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INT) / 4) * 4 as window_start,
                        COUNT(*) as signal_count,
                        SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                        SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                        ROUND(AVG(CASE WHEN s.boost IS NOT NULL THEN s.boost ELSE 0 END), 2) as avg_boost
                    FROM signals s
                    LEFT JOIN daily_calls dc ON dc.ticker = s.ticker
                        AND dc.et_day = DATE(datetime(s.timestamp, 'localtime', '-4 hours'))
                        AND dc.activity_type = s.activity_type
                    WHERE dc.mcap_tier = ?
                    GROUP BY window_start
                    ORDER BY window_start
                """
                params = (mcap_tier,)

            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()

            # Build window data
            window_counts = {}
            for row in rows:
                window_start = row["window_start"]
                window_counts[window_start] = row["signal_count"]

            # Compute call_volume_multiplier
            avg_count = sum(window_counts.values()) / 6 if window_counts else 1
            if avg_count == 0:
                avg_count = 1

            # Build results
            result_windows = []
            for window_start in [0, 4, 8, 12, 16, 20]:
                window_str = f"{window_start:02d}-{window_start+4:02d}"
                row = next((r for r in rows if r["window_start"] == window_start), None)

                if row:
                    signal_count = row["signal_count"]
                    buy_count = row["buy_count"]
                    sell_count = row["sell_count"]
                    avg_boost = row["avg_boost"]
                else:
                    signal_count = 0
                    buy_count = 0
                    sell_count = 0
                    avg_boost = None

                call_volume_multiplier = signal_count / avg_count if avg_count > 0 else 0

                result_windows.append({
                    "label": window_str,
                    "hour_start": window_start,
                    "signal_count": signal_count,
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "avg_boost": avg_boost,
                    "call_volume_multiplier": round(call_volume_multiplier, 2),
                    "win_rate": None,  # BLOCKED — pending EOD backfill
                    "avg_return_pct": None,  # BLOCKED — pending EOD backfill
                })

            return {
                "mcap_tier": mcap_tier,
                "pending_backfill": True,  # EOD prices not yet backfilled
                "windows": result_windows,
            }

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Analytics 4h windows error: {str(e)}")


@router.get("/analytics/filters")
def analytics_filters():
    """
    Returns distinct filter option lists for Analytics widgets.
    Prevents hardcoding filter values in frontend.

    Response includes:
    - mcap_tiers: distinct non-null mcap_tier values from daily_calls
    - dataset_start_et, dataset_end_et: earliest/latest et_day in daily_calls
    - total_days: count of distinct et_day in daily_calls
    """
    try:
        conn = _get_conn()
        try:
            # Get distinct mcap_tiers
            tiers_cursor = conn.execute(
                "SELECT DISTINCT mcap_tier FROM daily_calls WHERE mcap_tier IS NOT NULL ORDER BY mcap_tier"
            )
            mcap_tiers = [row["mcap_tier"] for row in tiers_cursor.fetchall()]

            # Get dataset date range
            range_cursor = conn.execute(
                "SELECT MIN(et_day) as start_day, MAX(et_day) as end_day, COUNT(DISTINCT et_day) as total_days FROM daily_calls"
            )
            range_row = range_cursor.fetchone()

            return {
                "mcap_tiers": mcap_tiers,
                "dataset_start_et": range_row["start_day"] if range_row else None,
                "dataset_end_et": range_row["end_day"] if range_row else None,
                "total_days": range_row["total_days"] if range_row else 0,
            }

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Analytics filters error: {str(e)}")


@router.get("/analytics/dataset-info")
def dataset_info():
    """
    Returns metadata about the signals dataset:
    - total_signals: total count of signals
    - unique_tickers: count of distinct tickers
    - first_signal_at_utc: earliest signal timestamp (UTC)
    - last_signal_at_utc: latest signal timestamp (UTC)
    - first_signal_at_ist: earliest signal timestamp (IST)
    - last_signal_at_ist: latest signal timestamp (IST)
    """
    try:
        conn = _get_conn()
        try:
            query = """
            SELECT
                COUNT(*) as total_signals,
                COUNT(DISTINCT ticker) as unique_tickers,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp
            FROM signals
            """
            cursor = conn.execute(query)
            row = cursor.fetchone()

            if not row:
                return {
                    "total_signals": 0,
                    "unique_tickers": 0,
                    "first_signal_at_utc": None,
                    "last_signal_at_utc": None,
                    "first_signal_at_ist": None,
                    "last_signal_at_ist": None,
                }

            first_ts = _parse_ts(row["first_timestamp"])
            last_ts = _parse_ts(row["last_timestamp"])

            IST = ZoneInfo("Asia/Kolkata")

            return {
                "total_signals": row["total_signals"],
                "unique_tickers": row["unique_tickers"],
                "first_signal_at_utc": first_ts.isoformat() if first_ts else None,
                "last_signal_at_utc": last_ts.isoformat() if last_ts else None,
                "first_signal_at_ist": first_ts.astimezone(IST).isoformat() if first_ts else None,
                "last_signal_at_ist": last_ts.astimezone(IST).isoformat() if last_ts else None,
            }

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
