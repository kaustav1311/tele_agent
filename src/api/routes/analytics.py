# src/api/routes/analytics.py
# Analytics endpoints — heatmap, cap-windows, pair-correlation.

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

from fastapi import APIRouter, Query
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


@router.get("/analytics/heatmap")
def analytics_heatmap():
    """
    Returns signal frequency and avg boost by hour-of-day (ET), broken down
    by activity_type. Always returns 24 hours (0-23), with zeros for empty hours.
    """
    try:
        conn = _get_conn()
        try:
            # Fetch all signals with timestamp and activity_type
            cursor = conn.execute(
                "SELECT timestamp, activity_type, boost FROM signals ORDER BY timestamp"
            )
            rows = cursor.fetchall()

            # Initialize 24-hour buckets
            buckets = {}
            for hour in range(24):
                buckets[hour] = {
                    "hour_et": hour,
                    "total_signals": 0,
                    "avg_boost": 0.0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "unknown_count": 0,
                    "dominant_activity": None,
                }

            # Aggregate signals into buckets
            boost_sum = defaultdict(float)
            boost_count = defaultdict(int)

            for row in rows:
                ts_utc = _parse_ts(row["timestamp"])
                if not ts_utc:
                    continue

                hour = _get_et_hour(ts_utc)
                activity = row["activity_type"] or "UNKNOWN"
                boost = row["boost"]

                buckets[hour]["total_signals"] += 1

                if activity == "BUY":
                    buckets[hour]["buy_count"] += 1
                elif activity == "SELL":
                    buckets[hour]["sell_count"] += 1
                else:
                    buckets[hour]["unknown_count"] += 1

                if boost is not None:
                    boost_sum[hour] += boost
                    boost_count[hour] += 1

            # Calculate averages and dominant activity
            for hour in range(24):
                if boost_count[hour] > 0:
                    buckets[hour]["avg_boost"] = round(boost_sum[hour] / boost_count[hour], 2)

                counts = [
                    ("BUY", buckets[hour]["buy_count"]),
                    ("SELL", buckets[hour]["sell_count"]),
                    ("UNKNOWN", buckets[hour]["unknown_count"]),
                ]
                max_count = max(counts, key=lambda x: x[1])
                if max_count[1] > 0:
                    buckets[hour]["dominant_activity"] = max_count[0]

            hours = [buckets[h] for h in range(24)]
            return {"hours": hours}

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        return {"hours": []}


@router.get("/analytics/cap-windows")
def analytics_cap_windows(
    cap_tier: str = Query("all"),
    days: int = Query(30, ge=1, le=90),
):
    """
    Analyzes which 4-hour ET windows perform best by market cap tier.
    Returns 6 windows with signal stats.
    """
    try:
        if cap_tier not in ["small", "mid", "large", "all"]:
            cap_tier = "all"

        conn = _get_conn()
        try:
            cutoff_time = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Build the mcap tier classification query
            if cap_tier == "all":
                where_clause = "s.timestamp >= ?"
                params = (cutoff_time,)
            elif cap_tier == "small":
                where_clause = "s.timestamp >= ? AND (m.mcap IS NULL OR m.mcap < 500000000)"
                params = (cutoff_time,)
            elif cap_tier == "mid":
                where_clause = "s.timestamp >= ? AND m.mcap >= 500000000 AND m.mcap < 5000000000"
                params = (cutoff_time,)
            else:  # large
                where_clause = "s.timestamp >= ? AND m.mcap >= 5000000000"
                params = (cutoff_time,)

            sql = f"""
                SELECT
                    s.timestamp,
                    s.activity_type,
                    s.boost,
                    s.alerts_tier,
                    s.ticker,
                    m.mcap
                FROM signals s
                LEFT JOIN metrics_cache m ON s.ticker = m.ticker
                WHERE {where_clause}
                ORDER BY s.timestamp
            """

            cursor = conn.execute(sql, params)
            rows = cursor.fetchall()

            # Initialize 6 windows
            windows_data = {}
            for window_str in ["00-04", "04-08", "08-12", "12-16", "16-20", "20-24"]:
                windows_data[window_str] = {
                    "window": window_str,
                    "signal_count": 0,
                    "boost_sum": 0.0,
                    "boost_count": 0,
                    "hot_fire_count": 0,
                    "unique_tickers": set(),
                    "activity_counts": defaultdict(int),
                }

            # Aggregate signals into windows
            for row in rows:
                ts_utc = _parse_ts(row["timestamp"])
                if not ts_utc:
                    continue

                et_dt = ts_utc.astimezone(ET)
                hour = et_dt.hour
                window_str = _get_4h_window(hour)

                activity = row["activity_type"] or "UNKNOWN"
                boost = row["boost"]
                alerts_tier = row["alerts_tier"]
                ticker = row["ticker"]

                windows_data[window_str]["signal_count"] += 1
                windows_data[window_str]["unique_tickers"].add(ticker)
                windows_data[window_str]["activity_counts"][activity] += 1

                if boost is not None:
                    windows_data[window_str]["boost_sum"] += boost
                    windows_data[window_str]["boost_count"] += 1

                if alerts_tier in ("HOT", "FIRE"):
                    windows_data[window_str]["hot_fire_count"] += 1

            # Finalize stats
            result_windows = []
            for window_str in ["00-04", "04-08", "08-12", "12-16", "16-20", "20-24"]:
                data = windows_data[window_str]
                avg_boost = 0.0
                if data["boost_count"] > 0:
                    avg_boost = round(data["boost_sum"] / data["boost_count"], 2)

                hot_fire_pct = 0.0
                if data["signal_count"] > 0:
                    hot_fire_pct = round(100.0 * data["hot_fire_count"] / data["signal_count"], 1)

                dominant_activity = "UNKNOWN"
                if data["activity_counts"]:
                    dominant_activity = max(data["activity_counts"].items(), key=lambda x: x[1])[0]

                result_windows.append({
                    "window": window_str,
                    "signal_count": data["signal_count"],
                    "avg_boost": avg_boost,
                    "hot_fire_pct": hot_fire_pct,
                    "unique_tickers": len(data["unique_tickers"]),
                    "dominant_activity": dominant_activity,
                })

            return {
                "cap_tier": cap_tier,
                "windows": result_windows,
            }

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        return {"cap_tier": cap_tier, "windows": []}


@router.get("/analytics/pair-correlation")
def analytics_pair_correlation(
    min_co_occurrences: int = Query(3, ge=1),
    days: int = Query(30, ge=1, le=90),
):
    """
    Finds ticker pairs that are frequently signaled within the same 4H ET window
    on the same day, suggesting correlation.
    """
    try:
        conn = _get_conn()
        try:
            cutoff_time = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

            # Fetch all signals with timestamp and ticker
            sql = """
                SELECT timestamp, ticker, activity_type
                FROM signals
                WHERE timestamp >= ?
                ORDER BY timestamp
            """
            cursor = conn.execute(sql, (cutoff_time,))
            rows = cursor.fetchall()

            # Build buckets: (et_day, 4h_window) -> list of (ticker, activity_type)
            bucket_signals = defaultdict(list)

            for row in rows:
                ts_utc = _parse_ts(row["timestamp"])
                if not ts_utc:
                    continue

                et_dt = ts_utc.astimezone(ET)
                et_day = et_dt.strftime("%Y-%m-%d")
                hour = et_dt.hour
                window = _get_4h_window(hour)

                bucket_key = (et_day, window)
                ticker = row["ticker"]
                activity = row["activity_type"] or "UNKNOWN"

                bucket_signals[bucket_key].append((ticker, activity))

            # Count co-occurrences and track shared windows
            co_occur = defaultdict(int)
            shared_windows = defaultdict(list)
            activity_matches = defaultdict(int)
            activity_total = defaultdict(int)

            for bucket_key, tickers_in_bucket in bucket_signals.items():
                # Remove duplicates per ticker per bucket
                unique_tickers = list(set(t[0] for t in tickers_in_bucket))

                # For each pair in this bucket, increment co-occurrence
                for i in range(len(unique_tickers)):
                    for j in range(i + 1, len(unique_tickers)):
                        ta = unique_tickers[i]
                        tb = unique_tickers[j]
                        pair_key = tuple(sorted([ta, tb]))

                        co_occur[pair_key] += 1
                        shared_windows[pair_key].append(f"{bucket_key[0]} {bucket_key[1]}")

                        # Check if same activity type
                        activity_a = next((x[1] for x in tickers_in_bucket if x[0] == ta), None)
                        activity_b = next((x[1] for x in tickers_in_bucket if x[0] == tb), None)

                        activity_total[pair_key] += 1
                        if activity_a and activity_b and activity_a == activity_b:
                            activity_matches[pair_key] += 1

            # Filter by min_co_occurrences and sort
            result_pairs = []
            for pair_key, count in co_occur.items():
                if count >= min_co_occurrences:
                    same_activity_pct = 0.0
                    if activity_total[pair_key] > 0:
                        same_activity_pct = round(
                            100.0 * activity_matches[pair_key] / activity_total[pair_key],
                            1
                        )

                    # Keep last 3 shared windows
                    windows_list = shared_windows[pair_key][-3:]

                    result_pairs.append({
                        "ticker_a": pair_key[0],
                        "ticker_b": pair_key[1],
                        "co_occurrences": count,
                        "shared_windows": windows_list,
                        "same_activity_pct": same_activity_pct,
                    })

            # Sort by co_occurrences descending
            result_pairs.sort(key=lambda x: x["co_occurrences"], reverse=True)

            return {"pairs": result_pairs}

        finally:
            conn.close()

    except Exception as e:
        traceback.print_exc()
        return {"pairs": []}


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
