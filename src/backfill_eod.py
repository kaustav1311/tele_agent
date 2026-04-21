import os
import logging
import sqlite3
import time
from datetime import datetime, timezone, timedelta

import httpx
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def run_eod_backfill(et_day: str) -> dict:
    """
    Fetch EOD close prices for all tickers in daily_calls for et_day.
    Updates eod_price, eod_fetched_at, first_call_efficiency_pct,
    last_call_efficiency_pct, direction_correct, time_weighted_accuracy,
    dq_eod_missing.
    Does NOT overwrite intraday_drift_pct.
    Returns {et_day, updated, failed, skipped_no_price, skipped_dq_first_missing}
    """
    load_dotenv()

    db_path = os.environ.get("DB_PATH", "data/signals.db")
    api_key = os.environ.get("CRYPTOCOMPARE_API_KEY", "")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Lazily ensure the time_weighted_accuracy column exists.
    # Safe to run on every invocation (idempotent via try/except).
    try:
        conn.execute(
            "ALTER TABLE daily_calls ADD COLUMN time_weighted_accuracy REAL DEFAULT NULL"
        )
        conn.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            raise

    updated = 0
    failed = 0
    skipped_no_price = 0
    skipped_dq_first_missing = 0

    try:
        rows = conn.execute(
            """
            SELECT id, ticker, activity_type, first_call_price, last_call_price,
                   first_call_time_et, dq_first_price_missing
            FROM daily_calls
            WHERE et_day = ? AND dq_eod_missing = 1
            """,
            (et_day,),
        ).fetchall()

        tickers = list({row["ticker"] for row in rows})

        # toTs = midnight UTC on the day after et_day
        next_day = datetime.strptime(et_day, "%Y-%m-%d") + timedelta(days=1)
        to_ts = int(next_day.replace(tzinfo=timezone.utc).timestamp())

        prices: dict[str, float | None] = {}
        for i, ticker in enumerate(tickers):
            if i > 0:
                time.sleep(0.12)
            try:
                params: dict = {
                    "fsym": ticker,
                    "tsym": "USD",
                    "limit": 1,
                    "toTs": to_ts,
                }
                headers = {"authorization": api_key} if api_key else {}

                resp = httpx.get(
                    "https://min-api.cryptocompare.com/data/histoday",
                    params=params,
                    headers=headers,
                    timeout=10,
                )
                data = resp.json()
                if data.get("Response") == "Success":
                    prices[ticker] = data["Data"][0]["close"]
                else:
                    prices[ticker] = None
            except Exception:
                logger.warning("Failed to fetch EOD price for ticker %s", ticker)
                prices[ticker] = None

        fetched_at = datetime.now(timezone.utc).isoformat()

        for row in rows:
            try:
                ticker = row["ticker"]
                eod_price = prices.get(ticker)

                if eod_price is None:
                    skipped_no_price += 1
                    continue

                if row["dq_first_price_missing"] == 1:
                    skipped_dq_first_missing += 1
                    conn.execute(
                        """
                        UPDATE daily_calls
                        SET eod_price=?, eod_fetched_at=?, dq_eod_missing=0
                        WHERE id=?
                        """,
                        (eod_price, fetched_at, row["id"]),
                    )
                    continue

                first_price = row["first_call_price"]
                last_price = row["last_call_price"]
                first_call_time_et = row["first_call_time_et"]

                first_eff = (
                    round((eod_price - first_price) / first_price * 100, 4)
                    if first_price
                    else None
                )
                last_eff = (
                    round((eod_price - last_price) / last_price * 100, 4)
                    if last_price
                    else None
                )

                if first_price is None:
                    direction_correct = None
                elif row["activity_type"] == "BUY":
                    direction_correct = 1 if eod_price > first_price else 0
                elif row["activity_type"] == "SELL":
                    direction_correct = 1 if eod_price < first_price else 0
                else:
                    direction_correct = None

                # Time-weighted accuracy: rewards early correct calls, penalizes
                # late wrong calls least. first_call_time_et is "HH:MM" ET.
                try:
                    hour = int(first_call_time_et[:2]) if first_call_time_et else 12
                except (ValueError, TypeError):
                    hour = 12
                hours_remaining = 24 - hour
                weight = hours_remaining / 24.0
                if direction_correct is not None:
                    time_weighted_accuracy = round(
                        weight if direction_correct == 1 else (1.0 - weight), 4
                    )
                else:
                    time_weighted_accuracy = None

                conn.execute(
                    """
                    UPDATE daily_calls
                    SET eod_price=?, eod_fetched_at=?,
                        first_call_efficiency_pct=?, last_call_efficiency_pct=?,
                        direction_correct=?, time_weighted_accuracy=?,
                        dq_eod_missing=0
                    WHERE id=?
                    """,
                    (
                        eod_price,
                        fetched_at,
                        first_eff,
                        last_eff,
                        direction_correct,
                        time_weighted_accuracy,
                        row["id"],
                    ),
                )
                updated += 1

            except Exception:
                logger.warning("Failed to update row id=%s for ticker %s", row["id"], row["ticker"])
                failed += 1
                continue

        conn.commit()

    finally:
        conn.close()

    logger.info(
        "EOD backfill %s: updated=%d failed=%d skipped_no_price=%d",
        et_day, updated, failed, skipped_no_price,
    )

    return {
        "et_day": et_day,
        "updated": updated,
        "failed": failed,
        "skipped_no_price": skipped_no_price,
        "skipped_dq_first_missing": skipped_dq_first_missing,
    }
