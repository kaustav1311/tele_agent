# src/api/routes/signals.py
# GET /signals/summary — returns all 4 timeframe windows in one payload.

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends
from sqlmodel import Session, select, text

from src.schema import Signal, get_session, get_engine
from src.api.utils.candles import get_candle_start

import os

ET  = ZoneInfo("America/New_York")
IST = ZoneInfo("Asia/Kolkata")

router = APIRouter()

TIMEFRAMES = ["15m", "1h", "4h", "daily"]


def _get_db():
    engine = get_engine(os.environ.get("DB_PATH", "data/signals.db"))
    with get_session(engine) as session:
        yield session


def _format_signal(sig: Signal) -> dict:
    """Convert a Signal row to API response dict with multi-tz timestamps."""
    ts_utc = sig.timestamp
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)

    return {
        "message_id":       sig.message_id,
        "ticker":           sig.ticker,
        "pair":             sig.pair,
        "activity_type":    sig.activity_type,
        "activity_raw":     sig.activity_raw,
        "boost":            sig.boost,
        "alerts_this_hour": sig.alerts_this_hour,
        "alerts_tier":      sig.alerts_tier,
        "price_at_signal":  sig.price_at_signal,
        "change_24h":       sig.change_24h,
        "timestamp_utc":    ts_utc.isoformat(),
        "timestamp_et":     ts_utc.astimezone(ET).isoformat(),
        "timestamp_ist":    ts_utc.astimezone(IST).isoformat(),
    }


@router.get("/signals/summary")
def signals_summary(db: Session = Depends(_get_db)):
    now_utc     = datetime.now(timezone.utc)
    candle_starts = {tf: get_candle_start(tf) for tf in TIMEFRAMES}

    windows = {}
    for tf in TIMEFRAMES:
        # Pure SQL filtering + sorting — no Python-side filtering
        statement = (
            select(Signal)
            .where(Signal.timestamp >= candle_starts[tf])
            .order_by(Signal.boost.desc(), Signal.timestamp.desc())
        )
        rows = db.exec(statement).all()
        windows[tf] = [_format_signal(r) for r in rows]

    return {
        "fetched_at_utc": now_utc.isoformat(),
        "candle_starts":  {tf: v.isoformat() for tf, v in candle_starts.items()},
        "windows":        windows,
    }