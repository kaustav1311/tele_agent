# src/api/routes/analytics.py
# Analytics endpoints — summary cards, trends, accuracy, scatter, leaderboard.

import os
import sqlite3
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn():
    db_path = os.environ.get("DB_PATH", "data/signals.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def _range_start(range_: str, today_et: str) -> str | None:
    """Return lower-bound et_day string for range, or None for 'all'."""
    if range_ == "1d":
        d = datetime.strptime(today_et, "%Y-%m-%d") - timedelta(days=1)
        return d.strftime("%Y-%m-%d")
    if range_ == "7d":
        d = datetime.strptime(today_et, "%Y-%m-%d") - timedelta(days=7)
        return d.strftime("%Y-%m-%d")
    if range_ == "30d":
        d = datetime.strptime(today_et, "%Y-%m-%d") - timedelta(days=30)
        return d.strftime("%Y-%m-%d")
    return None  # "all"


def _days_in_range(range_: str) -> int:
    return {"1d": 1, "7d": 7, "30d": 30}.get(range_, 0)


def _validate_range(r: str) -> str:
    return r if r in ("1d", "7d", "30d", "all") else "7d"


# ---------------------------------------------------------------------------
# 1. GET /api/analytics/filters
# ---------------------------------------------------------------------------

@router.get("/analytics/filters")
def analytics_filters():
    try:
        today = _today_et()
        conn = _get_conn()
        try:
            tiers = [
                row["mcap_tier"]
                for row in conn.execute(
                    "SELECT DISTINCT mcap_tier FROM daily_calls WHERE mcap_tier IS NOT NULL ORDER BY mcap_tier"
                ).fetchall()
            ]

            bounds = conn.execute(
                "SELECT MIN(et_day) as start_day, MAX(et_day) as end_day, "
                "COUNT(DISTINCT et_day) as total_days FROM daily_calls WHERE et_day < ?",
                (today,),
            ).fetchone()

            range_counts = {}
            for r in ("1d", "7d", "30d", "all"):
                rs = _range_start(r, today)
                if rs is not None:
                    cnt = conn.execute(
                        "SELECT COUNT(*) FROM daily_calls WHERE et_day >= ? AND et_day < ?",
                        (rs, today),
                    ).fetchone()[0]
                else:
                    cnt = conn.execute(
                        "SELECT COUNT(*) FROM daily_calls WHERE et_day < ?",
                        (today,),
                    ).fetchone()[0]
                range_counts[r] = cnt

            return {
                "mcap_tiers": tiers,
                "dataset_start_et": bounds["start_day"] if bounds else None,
                "dataset_end_et": bounds["end_day"] if bounds else None,
                "total_days": bounds["total_days"] if bounds else 0,
                "range_counts": range_counts,
            }
        finally:
            conn.close()
    except Exception:
        logger.exception("analytics/filters error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 2. GET /api/analytics/summary-cards?range=7d
# ---------------------------------------------------------------------------

@router.get("/analytics/summary-cards")
def analytics_summary_cards(range: str = Query("7d")):
    try:
        range = _validate_range(range)
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        try:
            # Signals query
            if rs is not None:
                sig_rows = conn.execute(
                    """
                    SELECT activity_type,
                           COUNT(*) as cnt,
                           COUNT(DISTINCT ticker) as unique_tickers,
                           ROUND(AVG(boost), 2) as avg_boost
                    FROM signals
                    WHERE date(datetime(timestamp, '-4 hours')) >= ?
                      AND date(datetime(timestamp, '-4 hours')) < ?
                    GROUP BY activity_type
                    """,
                    (rs, today),
                ).fetchall()
                date_from = rs
            else:
                sig_rows = conn.execute(
                    """
                    SELECT activity_type,
                           COUNT(*) as cnt,
                           COUNT(DISTINCT ticker) as unique_tickers,
                           ROUND(AVG(boost), 2) as avg_boost
                    FROM signals
                    WHERE date(datetime(timestamp, '-4 hours')) < ?
                    GROUP BY activity_type
                    """,
                    (today,),
                ).fetchall()
                bounds = conn.execute(
                    "SELECT MIN(et_day) FROM daily_calls WHERE et_day < ?", (today,)
                ).fetchone()
                date_from = bounds[0] if bounds else today

            sig_map = {r["activity_type"]: r for r in sig_rows}
            buy_row = sig_map.get("BUY")
            sell_row = sig_map.get("SELL")

            total_buy = buy_row["cnt"] if buy_row else 0
            total_sell = sell_row["cnt"] if sell_row else 0
            buy_sell_ratio = round(total_buy / total_sell, 2) if total_sell else None

            # avg_calls_per_ticker from daily_calls
            if rs is not None:
                dc_rows = conn.execute(
                    """
                    SELECT activity_type, ROUND(AVG(call_count), 2) as avg_calls
                    FROM daily_calls
                    WHERE et_day >= ? AND et_day < ?
                    GROUP BY activity_type
                    """,
                    (rs, today),
                ).fetchall()
            else:
                dc_rows = conn.execute(
                    """
                    SELECT activity_type, ROUND(AVG(call_count), 2) as avg_calls
                    FROM daily_calls
                    WHERE et_day < ?
                    GROUP BY activity_type
                    """,
                    (today,),
                ).fetchall()

            dc_map = {r["activity_type"]: r for r in dc_rows}

            total_signals = total_buy + total_sell
            days = _days_in_range(range) if range != "all" else None
            if days:
                velocity = round(total_signals / (days * 24.0), 1)
            else:
                velocity = None

            # date_to = yesterday et
            date_to = (
                datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")

            return {
                "range": range,
                "date_from": date_from,
                "date_to": date_to,
                "avg_boost_buy": buy_row["avg_boost"] if buy_row else None,
                "avg_boost_sell": sell_row["avg_boost"] if sell_row else None,
                "avg_calls_per_ticker_buy": dc_map["BUY"]["avg_calls"] if "BUY" in dc_map else None,
                "avg_calls_per_ticker_sell": dc_map["SELL"]["avg_calls"] if "SELL" in dc_map else None,
                "total_buy_signals": total_buy,
                "total_sell_signals": total_sell,
                "buy_sell_ratio": buy_sell_ratio,
                "unique_tickers_buy": buy_row["unique_tickers"] if buy_row else 0,
                "unique_tickers_sell": sell_row["unique_tickers"] if sell_row else 0,
                "signal_velocity_per_hour": velocity,
            }
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/summary-cards error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 3. GET /api/analytics/timezone-activity?range=7d
# ---------------------------------------------------------------------------

@router.get("/analytics/timezone-activity")
def analytics_timezone_activity(range: str = Query("7d")):
    try:
        range = _validate_range(range)
        today = _today_et()
        rs = _range_start(range, today)
        params = (rs, today) if rs is not None else (today,)
        where_lower = "AND date(datetime(timestamp, '-4 hours')) >= ?" if rs is not None else ""

        conn = _get_conn()
        try:
            buy_sql = f"""
                SELECT CAST(strftime('%H', datetime(first_signal_time, '-4 hours')) AS INT) as hour_et,
                       COUNT(*) as cnt
                FROM (
                    SELECT ticker, MIN(timestamp) as first_signal_time
                    FROM signals
                    WHERE activity_type = 'BUY'
                      {where_lower}
                      AND date(datetime(timestamp, '-4 hours')) < ?
                    GROUP BY ticker, date(datetime(timestamp, '-4 hours'))
                )
                GROUP BY hour_et
            """
            sell_sql = f"""
                SELECT CAST(strftime('%H', datetime(first_signal_time, '-4 hours')) AS INT) as hour_et,
                       COUNT(*) as cnt
                FROM (
                    SELECT ticker, MIN(timestamp) as first_signal_time
                    FROM signals
                    WHERE activity_type = 'SELL'
                      {where_lower}
                      AND date(datetime(timestamp, '-4 hours')) < ?
                    GROUP BY ticker, date(datetime(timestamp, '-4 hours'))
                )
                GROUP BY hour_et
            """

            buy_map = {r["hour_et"]: r["cnt"] for r in conn.execute(buy_sql, params).fetchall()}
            sell_map = {r["hour_et"]: r["cnt"] for r in conn.execute(sell_sql, params).fetchall()}

            hours = [
                {
                    "hour_et": h,
                    "buy_new_tickers": buy_map.get(h, 0),
                    "sell_new_tickers": sell_map.get(h, 0),
                }
                for h in range(24)
            ]

            return {"range": range, "hours": hours}
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/timezone-activity error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 4. GET /api/analytics/daily-trend?range=7d
# ---------------------------------------------------------------------------

@router.get("/analytics/daily-trend")
def analytics_daily_trend(range: str = Query("7d")):
    try:
        range = _validate_range(range)
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        try:
            if rs is not None:
                vol_rows = conn.execute(
                    """
                    SELECT date(datetime(timestamp, '-4 hours')) as et_day,
                           SUM(CASE WHEN activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                           SUM(CASE WHEN activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                           ROUND(AVG(CASE WHEN activity_type='BUY' THEN boost END), 2) as avg_boost_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' THEN boost END), 2) as avg_boost_sell
                    FROM signals
                    WHERE date(datetime(timestamp, '-4 hours')) >= ?
                      AND date(datetime(timestamp, '-4 hours')) < ?
                    GROUP BY et_day ORDER BY et_day
                    """,
                    (rs, today),
                ).fetchall()

                perf_rows = conn.execute(
                    """
                    SELECT et_day,
                           ROUND(AVG(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_sell,
                           ROUND(AVG(CASE WHEN activity_type='BUY' THEN first_call_efficiency_pct END), 2) as avg_eff_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' THEN first_call_efficiency_pct END), 2) as avg_eff_sell,
                           MAX(CASE WHEN dq_eod_missing=0 THEN 1 ELSE 0 END) as eod_filled
                    FROM daily_calls
                    WHERE et_day >= ? AND et_day < ?
                    GROUP BY et_day
                    """,
                    (rs, today),
                ).fetchall()
            else:
                vol_rows = conn.execute(
                    """
                    SELECT date(datetime(timestamp, '-4 hours')) as et_day,
                           SUM(CASE WHEN activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                           SUM(CASE WHEN activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                           ROUND(AVG(CASE WHEN activity_type='BUY' THEN boost END), 2) as avg_boost_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' THEN boost END), 2) as avg_boost_sell
                    FROM signals
                    WHERE date(datetime(timestamp, '-4 hours')) < ?
                    GROUP BY et_day ORDER BY et_day
                    """,
                    (today,),
                ).fetchall()

                perf_rows = conn.execute(
                    """
                    SELECT et_day,
                           ROUND(AVG(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_sell,
                           ROUND(AVG(CASE WHEN activity_type='BUY' THEN first_call_efficiency_pct END), 2) as avg_eff_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' THEN first_call_efficiency_pct END), 2) as avg_eff_sell,
                           MAX(CASE WHEN dq_eod_missing=0 THEN 1 ELSE 0 END) as eod_filled
                    FROM daily_calls
                    WHERE et_day < ?
                    GROUP BY et_day
                    """,
                    (today,),
                ).fetchall()

            perf_map = {r["et_day"]: r for r in perf_rows}

            days = []
            for v in vol_rows:
                day = v["et_day"]
                p = perf_map.get(day)
                buy_count = v["buy_count"]
                sell_count = v["sell_count"]
                buy_sell_ratio = (
                    round(buy_count / sell_count, 2) if sell_count else None
                )
                days.append({
                    "et_day": day,
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "avg_boost_buy": v["avg_boost_buy"],
                    "avg_boost_sell": v["avg_boost_sell"],
                    "buy_sell_ratio": buy_sell_ratio,
                    "win_rate_buy": p["win_rate_buy"] if p else None,
                    "win_rate_sell": p["win_rate_sell"] if p else None,
                    "avg_efficiency_buy": p["avg_eff_buy"] if p else None,
                    "avg_efficiency_sell": p["avg_eff_sell"] if p else None,
                    "eod_filled": bool(p["eod_filled"]) if p else False,
                })

            pending_backfill = any(not d["eod_filled"] for d in days)

            return {"range": range, "pending_backfill": pending_backfill, "days": days}
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/daily-trend error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 5. GET /api/analytics/boost-hour-scatter?range=7d&activity_type=ALL
# ---------------------------------------------------------------------------

@router.get("/analytics/boost-hour-scatter")
def analytics_boost_hour_scatter(
    range: str = Query("7d"),
    activity_type: str = Query("ALL"),
):
    try:
        range = _validate_range(range)
        if activity_type not in ("BUY", "SELL", "ALL"):
            activity_type = "ALL"
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        try:
            # Build WHERE clauses
            if rs is not None:
                where_date = (
                    "date(datetime(timestamp, '-4 hours')) >= ? "
                    "AND date(datetime(timestamp, '-4 hours')) < ?"
                )
                base_params: list = [rs, today]
            else:
                where_date = "date(datetime(timestamp, '-4 hours')) < ?"
                base_params = [today]

            if activity_type != "ALL":
                where_at = "AND activity_type = ?"
                count_params = base_params + [activity_type]
            else:
                where_at = ""
                count_params = base_params

            count_sql = (
                f"SELECT COUNT(*) FROM signals "
                f"WHERE {where_date} {where_at} AND boost IS NOT NULL"
            )
            total_before_cap = conn.execute(count_sql, count_params).fetchone()[0]

            scatter_sql = (
                f"SELECT CAST(strftime('%H', datetime(timestamp, '-4 hours')) AS INT) as hour_et, "
                f"boost, activity_type "
                f"FROM signals "
                f"WHERE {where_date} {where_at} AND boost IS NOT NULL "
                f"ORDER BY RANDOM() LIMIT 1500"
            )
            rows = conn.execute(scatter_sql, count_params).fetchall()

            points = [
                {"hour_et": r["hour_et"], "boost": r["boost"], "activity_type": r["activity_type"]}
                for r in rows
            ]

            return {
                "range": range,
                "activity_type": activity_type,
                "total_before_cap": total_before_cap,
                "capped": total_before_cap > 1500,
                "points": points,
            }
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/boost-hour-scatter error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 6. GET /api/analytics/streak-leaderboard
# ---------------------------------------------------------------------------

@router.get("/analytics/streak-leaderboard")
def analytics_streak_leaderboard():
    try:
        today = _today_et()
        conn = _get_conn()
        try:
            # All (ticker, et_day) sorted DESC for streak computation
            summary_rows = conn.execute(
                "SELECT ticker, et_day FROM daily_signal_summary "
                "WHERE et_day < ? ORDER BY ticker, et_day DESC",
                (today,),
            ).fetchall()

            # Group et_days per ticker
            from collections import defaultdict
            ticker_days: dict[str, list[str]] = defaultdict(list)
            for r in summary_rows:
                ticker_days[r["ticker"]].append(r["et_day"])

            # Compute streak: consecutive days ending at the most recent
            def _streak(days: list[str]) -> int:
                if not days:
                    return 0
                streak = 1
                for i in range(1, len(days)):
                    prev = datetime.strptime(days[i - 1], "%Y-%m-%d")
                    curr = datetime.strptime(days[i], "%Y-%m-%d")
                    if (prev - curr).days == 1:
                        streak += 1
                    else:
                        break
                return streak

            streaks = {t: _streak(ds) for t, ds in ticker_days.items()}
            top15 = sorted(streaks.items(), key=lambda x: x[1], reverse=True)[:15]

            # Total calls + avg boost
            stats_rows = conn.execute(
                "SELECT ticker, COUNT(*) as total_calls, ROUND(AVG(boost), 2) as avg_boost "
                "FROM signals GROUP BY ticker"
            ).fetchall()
            stats_map = {r["ticker"]: r for r in stats_rows}

            # Last seen
            last_seen_rows = conn.execute(
                "SELECT ticker, MAX(et_day) as last_seen FROM daily_signal_summary "
                "WHERE et_day < ? GROUP BY ticker",
                (today,),
            ).fetchall()
            last_seen_map = {r["ticker"]: r["last_seen"] for r in last_seen_rows}

            leaderboard = []
            for rank, (ticker, streak_days) in enumerate(top15, start=1):
                s = stats_map.get(ticker)
                leaderboard.append({
                    "rank": rank,
                    "ticker": ticker,
                    "streak_days": streak_days,
                    "total_calls_all_time": s["total_calls"] if s else 0,
                    "avg_boost_all_time": s["avg_boost"] if s else None,
                    "last_seen_et": last_seen_map.get(ticker),
                })

            return {"leaderboard": leaderboard}
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/streak-leaderboard error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 7. GET /api/analytics/accuracy-by-day?range=7d
# ---------------------------------------------------------------------------

@router.get("/analytics/accuracy-by-day")
def analytics_accuracy_by_day(range: str = Query("7d")):
    try:
        range = _validate_range(range)
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        try:
            params = (rs, today) if rs is not None else (today,)
            where = "et_day >= ? AND et_day < ?" if rs is not None else "et_day < ?"

            rows = conn.execute(
                f"""
                SELECT et_day,
                       ROUND(AVG(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL
                           THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_buy,
                       ROUND(AVG(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL
                           THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_sell,
                       ROUND(AVG(CASE WHEN activity_type='BUY' THEN first_call_efficiency_pct END), 2) as avg_eff_buy,
                       ROUND(AVG(CASE WHEN activity_type='SELL' THEN first_call_efficiency_pct END), 2) as avg_eff_sell,
                       SUM(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL THEN 1 ELSE 0 END) as sample_size_buy,
                       SUM(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL THEN 1 ELSE 0 END) as sample_size_sell
                FROM daily_calls
                WHERE dq_eod_missing=0 AND {where}
                GROUP BY et_day ORDER BY et_day
                """,
                params,
            ).fetchall()

            pending_cnt = conn.execute(
                f"SELECT COUNT(*) FROM daily_calls WHERE dq_eod_missing=1 AND {where}",
                params,
            ).fetchone()[0]

            has_data = len(rows) > 0

            days = [
                {
                    "et_day": r["et_day"],
                    "win_rate_buy": r["win_rate_buy"],
                    "win_rate_sell": r["win_rate_sell"],
                    "avg_efficiency_buy": r["avg_eff_buy"],
                    "avg_efficiency_sell": r["avg_eff_sell"],
                    "sample_size_buy": r["sample_size_buy"],
                    "sample_size_sell": r["sample_size_sell"],
                }
                for r in rows
            ]

            return {
                "range": range,
                "pending_backfill": pending_cnt > 0,
                "has_data": has_data,
                "days": days,
            }
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/accuracy-by-day error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 8. GET /api/analytics/accuracy-by-mcap?range=7d
# ---------------------------------------------------------------------------

@router.get("/analytics/accuracy-by-mcap")
def analytics_accuracy_by_mcap(range: str = Query("7d")):
    try:
        range = _validate_range(range)
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        try:
            params = (rs, today) if rs is not None else (today,)
            where = "et_day >= ? AND et_day < ?" if rs is not None else "et_day < ?"

            rows = conn.execute(
                f"""
                SELECT mcap_tier,
                       ROUND(AVG(CAST(direction_correct AS FLOAT))*100, 1) as win_rate,
                       ROUND(AVG(first_call_efficiency_pct), 2) as avg_efficiency_pct,
                       COUNT(*) as sample_size
                FROM daily_calls
                WHERE dq_eod_missing=0
                  AND mcap_tier IS NOT NULL
                  AND {where}
                GROUP BY mcap_tier ORDER BY mcap_tier
                """,
                params,
            ).fetchall()

            pending_cnt = conn.execute(
                f"SELECT COUNT(*) FROM daily_calls WHERE dq_eod_missing=1 AND {where}",
                params,
            ).fetchone()[0]

            tiers = [
                {
                    "mcap_tier": r["mcap_tier"],
                    "win_rate": r["win_rate"],
                    "avg_efficiency_pct": r["avg_efficiency_pct"],
                    "sample_size": r["sample_size"],
                }
                for r in rows
            ]

            return {
                "range": range,
                "pending_backfill": pending_cnt > 0,
                "has_data": len(tiers) > 0,
                "tiers": tiers,
            }
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/accuracy-by-mcap error")
        raise HTTPException(status_code=500, detail="Analytics error")
