# src/api/routes/analytics.py
# Analytics endpoints — summary cards, trends, accuracy, hourly volume, leaderboard, today.

import os
import builtins
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
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


def _validate_mcap_tier(t: str) -> str:
    return t if t in ("micro", "small", "mid", "large", "unknown", "all") else "all"


# Module-level flag to ensure _ensure_twa_column runs only once
_twa_column_ensured = False


def _ensure_twa_column(conn):
    """Ensure time_weighted_accuracy column exists in daily_calls table."""
    global _twa_column_ensured
    if _twa_column_ensured:
        return
    try:
        conn.execute(
            "ALTER TABLE daily_calls ADD COLUMN "
            "time_weighted_accuracy REAL DEFAULT NULL"
        )
        conn.commit()
    except Exception:
        pass  # column already exists
    _twa_column_ensured = True


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
def analytics_summary_cards(range: str = Query("7d"), mcap_tier: str = Query("all")):
    try:
        range = _validate_range(range)
        mcap_tier = _validate_mcap_tier(mcap_tier)
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        try:
            # Signals query
            tier_join = ""
            tier_extra_params: tuple = ()
            if mcap_tier != "all":
                tier_join = (
                    " JOIN daily_calls dc ON dc.ticker = s.ticker "
                    "AND dc.et_day = date(datetime(s.timestamp, '-4 hours')) "
                    "AND dc.mcap_tier = ?"
                )
                tier_extra_params = (mcap_tier,)

            if rs is not None:
                sig_rows = conn.execute(
                    f"""
                    SELECT s.activity_type,
                           COUNT(*) as cnt,
                           COUNT(DISTINCT s.ticker) as unique_tickers,
                           ROUND(AVG(s.boost), 2) as avg_boost
                    FROM signals s{tier_join}
                    WHERE date(datetime(s.timestamp, '-4 hours')) >= ?
                      AND date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY s.activity_type
                    """,
                    tier_extra_params + (rs, today),
                ).fetchall()
                date_from = rs
            else:
                sig_rows = conn.execute(
                    f"""
                    SELECT s.activity_type,
                           COUNT(*) as cnt,
                           COUNT(DISTINCT s.ticker) as unique_tickers,
                           ROUND(AVG(s.boost), 2) as avg_boost
                    FROM signals s{tier_join}
                    WHERE date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY s.activity_type
                    """,
                    tier_extra_params + (today,),
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
            buy_sell_ratio = builtins.round(total_buy / total_sell, 2) if total_sell else None

            # avg_calls_per_ticker from daily_calls
            dc_tier_clause = " AND mcap_tier = ?" if mcap_tier != "all" else ""
            dc_tier_params: tuple = (mcap_tier,) if mcap_tier != "all" else ()

            if rs is not None:
                dc_rows = conn.execute(
                    f"""
                    SELECT activity_type, ROUND(AVG(call_count), 2) as avg_calls
                    FROM daily_calls
                    WHERE et_day >= ? AND et_day < ?{dc_tier_clause}
                    GROUP BY activity_type
                    """,
                    (rs, today) + dc_tier_params,
                ).fetchall()
            else:
                dc_rows = conn.execute(
                    f"""
                    SELECT activity_type, ROUND(AVG(call_count), 2) as avg_calls
                    FROM daily_calls
                    WHERE et_day < ?{dc_tier_clause}
                    GROUP BY activity_type
                    """,
                    (today,) + dc_tier_params,
                ).fetchall()

            dc_map = {r["activity_type"]: r for r in dc_rows}

            total_signals = total_buy + total_sell
            days = _days_in_range(range) if range != "all" else None
            if days:
                velocity = builtins.round(total_signals / (days * 24.0), 1)
            else:
                velocity = None

            # date_to = yesterday et
            date_to = (
                datetime.strptime(today, "%Y-%m-%d") - timedelta(days=1)
            ).strftime("%Y-%m-%d")

            return {
                "range": range,
                "mcap_tier": mcap_tier,
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
def analytics_timezone_activity(range: str = Query("7d"), mcap_tier: str = Query("all")):
    try:
        range = _validate_range(range)
        mcap_tier = _validate_mcap_tier(mcap_tier)
        today = _today_et()
        rs = _range_start(range, today)

        # First-seen subquery uses unqualified `timestamp` (it's an aggregate on signals).
        # For mcap_tier filtering, we JOIN daily_calls inside the inner query.
        tier_join_inner = ""
        tier_extra_params: tuple = ()
        if mcap_tier != "all":
            tier_join_inner = (
                " JOIN daily_calls dc ON dc.ticker = s.ticker "
                "AND dc.et_day = date(datetime(s.timestamp, '-4 hours')) "
                "AND dc.mcap_tier = ?"
            )
            tier_extra_params = (mcap_tier,)

        if rs is not None:
            inner_params = tier_extra_params + (rs, today)
            where_lower_inner = "AND date(datetime(s.timestamp, '-4 hours')) >= ?"
        else:
            inner_params = tier_extra_params + (today,)
            where_lower_inner = ""

        conn = _get_conn()
        try:
            # --- First-seen (unique tickers per day, bucketed by hour of first signal) ---
            buy_sql = f"""
                SELECT CAST(strftime('%H', datetime(first_signal_time, '-4 hours')) AS INT) as hour_et,
                       COUNT(*) as cnt
                FROM (
                    SELECT s.ticker, MIN(s.timestamp) as first_signal_time
                    FROM signals s{tier_join_inner}
                    WHERE s.activity_type = 'BUY'
                      {where_lower_inner}
                      AND date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY s.ticker, date(datetime(s.timestamp, '-4 hours'))
                )
                GROUP BY hour_et
            """
            sell_sql = f"""
                SELECT CAST(strftime('%H', datetime(first_signal_time, '-4 hours')) AS INT) as hour_et,
                       COUNT(*) as cnt
                FROM (
                    SELECT s.ticker, MIN(s.timestamp) as first_signal_time
                    FROM signals s{tier_join_inner}
                    WHERE s.activity_type = 'SELL'
                      {where_lower_inner}
                      AND date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY s.ticker, date(datetime(s.timestamp, '-4 hours'))
                )
                GROUP BY hour_et
            """

            buy_new_map = {r["hour_et"]: r["cnt"] for r in conn.execute(buy_sql, inner_params).fetchall()}
            sell_new_map = {r["hour_et"]: r["cnt"] for r in conn.execute(sell_sql, inner_params).fetchall()}

            # --- Total signal counts per hour (regardless of new/repeat) ---
            tier_join_total = tier_join_inner  # identical JOIN structure
            if rs is not None:
                total_sql = f"""
                    SELECT
                      CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INT) as hour_et,
                      SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_total,
                      SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_total
                    FROM signals s{tier_join_total}
                    WHERE date(datetime(s.timestamp, '-4 hours')) >= ?
                      AND date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY hour_et
                """
                total_params = tier_extra_params + (rs, today)
            else:
                total_sql = f"""
                    SELECT
                      CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INT) as hour_et,
                      SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_total,
                      SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_total
                    FROM signals s{tier_join_total}
                    WHERE date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY hour_et
                """
                total_params = tier_extra_params + (today,)

            buy_total_map: dict = {}
            sell_total_map: dict = {}
            for r in conn.execute(total_sql, total_params).fetchall():
                buy_total_map[r["hour_et"]] = r["buy_total"] or 0
                sell_total_map[r["hour_et"]] = r["sell_total"] or 0

            hours = []
            for h in builtins.range(24):
                buy_total = buy_total_map.get(h, 0)
                sell_total = sell_total_map.get(h, 0)
                bsr = builtins.round(buy_total / sell_total, 2) if sell_total else None
                hours.append({
                    "hour_et": h,
                    "buy_new_tickers": buy_new_map.get(h, 0),
                    "sell_new_tickers": sell_new_map.get(h, 0),
                    "buy_total": buy_total,
                    "sell_total": sell_total,
                    "buy_sell_ratio": bsr,
                })

            return {"range": range, "mcap_tier": mcap_tier, "hours": hours}
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
def analytics_daily_trend(range: str = Query("7d"), mcap_tier: str = Query("all")):
    try:
        range = _validate_range(range)
        mcap_tier = _validate_mcap_tier(mcap_tier)
        today = _today_et()
        rs = _range_start(range, today)

        # JOIN to daily_calls on signals side when mcap_tier != "all"
        tier_join = ""
        tier_extra_params: tuple = ()
        if mcap_tier != "all":
            tier_join = (
                " JOIN daily_calls dc ON dc.ticker = s.ticker "
                "AND dc.et_day = date(datetime(s.timestamp, '-4 hours')) "
                "AND dc.mcap_tier = ?"
            )
            tier_extra_params = (mcap_tier,)

        # Filter clause for daily_calls-only queries
        dc_tier_clause = " AND mcap_tier = ?" if mcap_tier != "all" else ""
        dc_tier_params: tuple = (mcap_tier,) if mcap_tier != "all" else ()

        conn = _get_conn()
        try:
            if rs is not None:
                vol_rows = conn.execute(
                    f"""
                    SELECT date(datetime(s.timestamp, '-4 hours')) as et_day,
                           SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                           SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                           ROUND(AVG(CASE WHEN s.activity_type='BUY' THEN s.boost END), 2) as avg_boost_buy,
                           ROUND(AVG(CASE WHEN s.activity_type='SELL' THEN s.boost END), 2) as avg_boost_sell
                    FROM signals s{tier_join}
                    WHERE date(datetime(s.timestamp, '-4 hours')) >= ?
                      AND date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY et_day ORDER BY et_day
                    """,
                    tier_extra_params + (rs, today),
                ).fetchall()

                perf_rows = conn.execute(
                    f"""
                    SELECT et_day,
                           ROUND(AVG(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_sell,
                           ROUND(AVG(CASE WHEN activity_type='BUY' THEN first_call_efficiency_pct END), 2) as avg_eff_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' THEN first_call_efficiency_pct END), 2) as avg_eff_sell,
                           MAX(CASE WHEN dq_eod_missing=0 THEN 1 ELSE 0 END) as eod_filled
                    FROM daily_calls
                    WHERE et_day >= ? AND et_day < ?{dc_tier_clause}
                    GROUP BY et_day
                    """,
                    (rs, today) + dc_tier_params,
                ).fetchall()
            else:
                vol_rows = conn.execute(
                    f"""
                    SELECT date(datetime(s.timestamp, '-4 hours')) as et_day,
                           SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                           SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                           ROUND(AVG(CASE WHEN s.activity_type='BUY' THEN s.boost END), 2) as avg_boost_buy,
                           ROUND(AVG(CASE WHEN s.activity_type='SELL' THEN s.boost END), 2) as avg_boost_sell
                    FROM signals s{tier_join}
                    WHERE date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY et_day ORDER BY et_day
                    """,
                    tier_extra_params + (today,),
                ).fetchall()

                perf_rows = conn.execute(
                    f"""
                    SELECT et_day,
                           ROUND(AVG(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL
                               THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_sell,
                           ROUND(AVG(CASE WHEN activity_type='BUY' THEN first_call_efficiency_pct END), 2) as avg_eff_buy,
                           ROUND(AVG(CASE WHEN activity_type='SELL' THEN first_call_efficiency_pct END), 2) as avg_eff_sell,
                           MAX(CASE WHEN dq_eod_missing=0 THEN 1 ELSE 0 END) as eod_filled
                    FROM daily_calls
                    WHERE et_day < ?{dc_tier_clause}
                    GROUP BY et_day
                    """,
                    (today,) + dc_tier_params,
                ).fetchall()

            perf_map = {r["et_day"]: r for r in perf_rows}

            days = []
            for v in vol_rows:
                day = v["et_day"]
                p = perf_map.get(day)
                buy_count = v["buy_count"]
                sell_count = v["sell_count"]
                buy_sell_ratio = (
                    builtins.round(buy_count / sell_count, 2) if sell_count else None
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

            return {
                "range": range,
                "mcap_tier": mcap_tier,
                "pending_backfill": pending_backfill,
                "days": days,
            }
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/daily-trend error")
        raise HTTPException(status_code=500, detail="Analytics error")


# ---------------------------------------------------------------------------
# 5. GET /api/analytics/boost-hour-scatter?range=7d
# (Route path retained; handler now returns hourly stacked BUY/SELL volume.)
# ---------------------------------------------------------------------------

@router.get("/analytics/boost-hour-scatter")
def analytics_hourly_volume(range: str = Query("7d"), mcap_tier: str = Query("all")):
    try:
        range = _validate_range(range)
        mcap_tier = _validate_mcap_tier(mcap_tier)
        today = _today_et()
        rs = _range_start(range, today)

        tier_join = ""
        tier_extra_params: tuple = ()
        if mcap_tier != "all":
            tier_join = (
                " JOIN daily_calls dc ON dc.ticker = s.ticker "
                "AND dc.et_day = date(datetime(s.timestamp, '-4 hours')) "
                "AND dc.mcap_tier = ?"
            )
            tier_extra_params = (mcap_tier,)

        conn = _get_conn()
        try:
            if rs is not None:
                sql = f"""
                    SELECT
                      CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INT) as hour_et,
                      SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                      SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count
                    FROM signals s{tier_join}
                    WHERE date(datetime(s.timestamp, '-4 hours')) >= ?
                      AND date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY hour_et
                """
                params: tuple = tier_extra_params + (rs, today)
            else:
                sql = f"""
                    SELECT
                      CAST(strftime('%H', datetime(s.timestamp, '-4 hours')) AS INT) as hour_et,
                      SUM(CASE WHEN s.activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                      SUM(CASE WHEN s.activity_type='SELL' THEN 1 ELSE 0 END) as sell_count
                    FROM signals s{tier_join}
                    WHERE date(datetime(s.timestamp, '-4 hours')) < ?
                    GROUP BY hour_et
                """
                params = tier_extra_params + (today,)

            raw = {r["hour_et"]: r for r in conn.execute(sql, params).fetchall()}

            hours = []
            for h in builtins.range(24):
                r = raw.get(h)
                bc = (r["buy_count"] if r else 0) or 0
                sc = (r["sell_count"] if r else 0) or 0
                total = bc + sc
                buy_pct = builtins.round(bc / total * 100, 1) if total else 0.0
                sell_pct = builtins.round(sc / total * 100, 1) if total else 0.0
                bsr = builtins.round(bc / sc, 2) if sc else None
                hours.append({
                    "hour_et": h,
                    "buy_count": bc,
                    "sell_count": sc,
                    "total": total,
                    "buy_pct": buy_pct,
                    "sell_pct": sell_pct,
                    "buy_sell_ratio": bsr,
                })

            return {"range": range, "mcap_tier": mcap_tier, "hours": hours}
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/hourly-volume error")
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
                for i in builtins.range(1, len(days)):
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
def analytics_accuracy_by_day(range: str = Query("7d"), mcap_tier: str = Query("all")):
    try:
        range = _validate_range(range)
        mcap_tier = _validate_mcap_tier(mcap_tier)
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        _ensure_twa_column(conn)
        try:
            base_params = (rs, today) if rs is not None else (today,)
            where = "et_day >= ? AND et_day < ?" if rs is not None else "et_day < ?"
            if mcap_tier != "all":
                where = f"{where} AND mcap_tier = ?"
                params = base_params + (mcap_tier,)
            else:
                params = base_params

            rows = conn.execute(
                f"""
                SELECT et_day,
                       ROUND(AVG(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL
                           THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_buy,
                       ROUND(AVG(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL
                           THEN CAST(direction_correct AS FLOAT) END)*100, 1) as win_rate_sell,
                       ROUND(AVG(CASE WHEN activity_type='BUY' THEN first_call_efficiency_pct END), 2) as avg_eff_buy,
                       ROUND(AVG(CASE WHEN activity_type='SELL' THEN first_call_efficiency_pct END), 2) as avg_eff_sell,
                       ROUND(AVG(CASE WHEN activity_type='BUY' THEN time_weighted_accuracy END), 3) as twa_buy,
                       ROUND(AVG(CASE WHEN activity_type='SELL' THEN time_weighted_accuracy END), 3) as twa_sell,
                       SUM(CASE WHEN activity_type='BUY' AND direction_correct IS NOT NULL THEN 1 ELSE 0 END) as sample_size_buy,
                       SUM(CASE WHEN activity_type='SELL' AND direction_correct IS NOT NULL THEN 1 ELSE 0 END) as sample_size_sell
                FROM daily_calls
                WHERE dq_eod_missing=0 AND {where}
                GROUP BY et_day ORDER BY et_day
                """,
                params,
            ).fetchall()

            # pending_backfill: a day counts as "pending" only when it has BOTH
            # filled and unfilled rows (partial backfill). Days with no filled
            # rows at all are surfaced as "no data" instead of "pending".
            # Days that have ANY filled row
            filled_days = {r[0] for r in conn.execute(
                f"SELECT DISTINCT et_day FROM daily_calls WHERE dq_eod_missing=0 AND {where}",
                params
            ).fetchall()}

            # Days that ALSO have unfilled rows (partial)
            partial_days_cnt = len([d for d in conn.execute(
                f"SELECT DISTINCT et_day FROM daily_calls WHERE dq_eod_missing=1 AND {where}",
                params
            ).fetchall() if d[0] in filled_days])

            pending_backfill = partial_days_cnt > 0

            has_data = len(rows) > 0

            days = [
                {
                    "et_day": r["et_day"],
                    "win_rate_buy": r["win_rate_buy"],
                    "win_rate_sell": r["win_rate_sell"],
                    "avg_efficiency_buy": r["avg_eff_buy"],
                    "avg_efficiency_sell": r["avg_eff_sell"],
                    "twa_buy": r["twa_buy"],
                    "twa_sell": r["twa_sell"],
                    "sample_size_buy": r["sample_size_buy"],
                    "sample_size_sell": r["sample_size_sell"],
                }
                for r in rows
            ]

            return {
                "range": range,
                "mcap_tier": mcap_tier,
                "pending_backfill": pending_backfill,
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
def analytics_accuracy_by_mcap(range: str = Query("7d"), mcap_tier: str = Query("all")):
    try:
        range = _validate_range(range)
        mcap_tier = _validate_mcap_tier(mcap_tier)
        today = _today_et()
        rs = _range_start(range, today)

        conn = _get_conn()
        _ensure_twa_column(conn)
        try:
            base_params = (rs, today) if rs is not None else (today,)
            where = "et_day >= ? AND et_day < ?" if rs is not None else "et_day < ?"
            if mcap_tier != "all":
                where = f"{where} AND mcap_tier = ?"
                params = base_params + (mcap_tier,)
            else:
                params = base_params

            rows = conn.execute(
                f"""
                SELECT mcap_tier,
                       ROUND(AVG(CAST(direction_correct AS FLOAT))*100, 1) as win_rate,
                       ROUND(AVG(first_call_efficiency_pct), 2) as avg_efficiency_pct,
                       ROUND(AVG(time_weighted_accuracy), 3) as twa,
                       COUNT(*) as sample_size
                FROM daily_calls
                WHERE dq_eod_missing=0
                  AND mcap_tier IS NOT NULL
                  AND {where}
                GROUP BY mcap_tier ORDER BY mcap_tier
                """,
                params,
            ).fetchall()

            # Same partial-backfill semantics as accuracy-by-day.
            # Days that have ANY filled row
            filled_days = {r[0] for r in conn.execute(
                f"SELECT DISTINCT et_day FROM daily_calls WHERE dq_eod_missing=0 AND {where}",
                params
            ).fetchall()}

            # Days that ALSO have unfilled rows (partial)
            partial_days_cnt = len([d for d in conn.execute(
                f"SELECT DISTINCT et_day FROM daily_calls WHERE dq_eod_missing=1 AND {where}",
                params
            ).fetchall() if d[0] in filled_days])

            pending_backfill = partial_days_cnt > 0

            tiers = [
                {
                    "mcap_tier": r["mcap_tier"],
                    "win_rate": r["win_rate"],
                    "avg_efficiency_pct": r["avg_efficiency_pct"],
                    "twa": r["twa"],
                    "sample_size": r["sample_size"],
                }
                for r in rows
            ]

            return {
                "range": range,
                "mcap_tier": mcap_tier,
                "pending_backfill": pending_backfill,
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


# ---------------------------------------------------------------------------
# 9. GET /api/analytics/today
# Live intraday signal stats — sourced from signals table only.
# ---------------------------------------------------------------------------

@router.get("/analytics/today")
def analytics_today():
    try:
        today = _today_et()
        conn = _get_conn()
        try:
            rows = conn.execute(
                """
                SELECT
                  CAST(strftime('%H', datetime(timestamp, '-4 hours')) AS INT) as hour_et,
                  SUM(CASE WHEN activity_type='BUY' THEN 1 ELSE 0 END) as buy_count,
                  SUM(CASE WHEN activity_type='SELL' THEN 1 ELSE 0 END) as sell_count,
                  ROUND(AVG(CASE WHEN activity_type='BUY' THEN boost END), 2) as avg_boost_buy,
                  ROUND(AVG(CASE WHEN activity_type='SELL' THEN boost END), 2) as avg_boost_sell
                FROM signals
                WHERE date(datetime(timestamp, '-4 hours')) = ?
                GROUP BY hour_et ORDER BY hour_et
                """,
                (today,),
            ).fetchall()

            by_hour = []
            total_buy = 0
            total_sell = 0
            # Weighted sums for overall boost averages across the whole day.
            w_boost_buy_num = 0.0
            w_boost_buy_den = 0
            w_boost_sell_num = 0.0
            w_boost_sell_den = 0
            most_active_hour = None
            most_active_count = -1

            for r in rows:
                bc = r["buy_count"] or 0
                sc = r["sell_count"] or 0
                bsr = builtins.round(bc / sc, 2) if sc else None
                by_hour.append({
                    "hour_et": r["hour_et"],
                    "buy_count": bc,
                    "sell_count": sc,
                    "buy_sell_ratio": bsr,
                    "avg_boost_buy": r["avg_boost_buy"],
                    "avg_boost_sell": r["avg_boost_sell"],
                })
                total_buy += bc
                total_sell += sc
                if r["avg_boost_buy"] is not None and bc:
                    w_boost_buy_num += r["avg_boost_buy"] * bc
                    w_boost_buy_den += bc
                if r["avg_boost_sell"] is not None and sc:
                    w_boost_sell_num += r["avg_boost_sell"] * sc
                    w_boost_sell_den += sc
                hour_total = bc + sc
                if hour_total > most_active_count:
                    most_active_count = hour_total
                    most_active_hour = r["hour_et"]

            # Unique ticker counts for BUY vs SELL today.
            uniq_rows = conn.execute(
                """
                SELECT activity_type, COUNT(DISTINCT ticker) as cnt
                FROM signals
                WHERE date(datetime(timestamp, '-4 hours')) = ?
                GROUP BY activity_type
                """,
                (today,),
            ).fetchall()
            uniq_map = {r["activity_type"]: r["cnt"] for r in uniq_rows}

            hours_active = len(by_hour)
            total_signals = total_buy + total_sell
            velocity = (
                builtins.round(total_signals / hours_active, 2) if hours_active else 0
            )

            avg_boost_buy = (
                builtins.round(w_boost_buy_num / w_boost_buy_den, 2)
                if w_boost_buy_den else None
            )
            avg_boost_sell = (
                builtins.round(w_boost_sell_num / w_boost_sell_den, 2)
                if w_boost_sell_den else None
            )
            buy_sell_ratio = (
                builtins.round(total_buy / total_sell, 2) if total_sell else None
            )

            summary = {
                "total_buy_signals": total_buy,
                "total_sell_signals": total_sell,
                "unique_tickers_buy": uniq_map.get("BUY", 0),
                "unique_tickers_sell": uniq_map.get("SELL", 0),
                "avg_boost_buy": avg_boost_buy,
                "avg_boost_sell": avg_boost_sell,
                "buy_sell_ratio": buy_sell_ratio,
                "signal_velocity_per_hour": velocity,
                "most_active_hour_et": most_active_hour,
                "hours_active": hours_active,
            }

            return {
                "et_day": today,
                "as_of_utc": datetime.now(timezone.utc).isoformat(),
                "summary": summary,
                "by_hour": by_hour,
            }
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception:
        logger.exception("analytics/today error")
        raise HTTPException(status_code=500, detail="Analytics error")
