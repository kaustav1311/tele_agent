# src/schema.py
# SQLModel table definitions for Signal Agent.
# Four tables: signals, metrics_cache, unparsed_messages, daily_signal_summary.

from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo
from sqlmodel import Field, SQLModel


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# signals
# One row per successfully parsed Telegram message.
# ---------------------------------------------------------------------------

class Signal(SQLModel, table=True):
    __tablename__ = "signals"

    # Primary key — Telegram's own message ID, guaranteed unique per channel
    message_id:       int            = Field(primary_key=True)

    # Parsed token fields
    ticker:           str            = Field(index=True)        # e.g. "VIRTUAL"
    pair:             str            = Field()                  # e.g. "VIRTUALUSDT"

    # Price at time of signal
    price_at_signal:  Optional[float] = Field(default=None)
    change_24h:       Optional[float] = Field(default=None)    # e.g. 2.0 (not 0.02)

    # Activity
    activity_raw:     str            = Field()                  # verbatim, minus leading emoji
    activity_type:    str            = Field(index=True)        # "BUY" | "SELL" | "UNKNOWN"

    # Boost score (0–10)
    boost:            Optional[int]  = Field(default=None)

    # Alerts
    alerts_this_hour: Optional[int]  = Field(default=None)
    alerts_tier:      Optional[str]  = Field(default=None)      # "NORMAL" | "HOT" | "FIRE"

    # Media flag
    has_media:        bool           = Field(default=False)

    # Sender (for anomaly detection)
    sender_id:        Optional[int]  = Field(default=None)

    # Timestamps
    timestamp:        datetime       = Field(index=True)        # when TG sent the message
    created_at:       datetime       = Field(default_factory=utcnow)  # when we stored it


# ---------------------------------------------------------------------------
# metrics_cache
# MCap + tier lookup table. One row per ticker.
# Upserted via POST /metrics/refresh (CryptoCompare, fetches MCap only).
# Role: Provide mcap + mcap_tier for daily_calls enrichment + analytics.
# ---------------------------------------------------------------------------

class MetricsCache(SQLModel, table=True):
    __tablename__ = "metrics_cache"

    ticker:       str             = Field(primary_key=True)  # e.g. "VIRTUAL"

    price:        Optional[float] = Field(default=None)      # USD spot price
    volume_24h:   Optional[float] = Field(default=None)      # 24h volume USD
    mcap:         Optional[float] = Field(default=None)      # Market cap in USD
    rank:         Optional[int]   = Field(default=None)      # CoinGecko/CC rank
    circ_supply:  Optional[float] = Field(default=None)      # circulating supply
    mcap_tier:    Optional[str]   = Field(default=None)      # "micro"|"small"|"mid"|"large"|"unknown"

    fetched_at:   Optional[datetime] = Field(default=None)   # last successful MCap fetch


# ---------------------------------------------------------------------------
# unparsed_messages
# Messages that failed parsing — stored for audit and future regex improvement.
# ---------------------------------------------------------------------------

class UnparsedMessage(SQLModel, table=True):
    __tablename__ = "unparsed_messages"

    message_id:  int            = Field(primary_key=True)
    raw_text:    str            = Field()
    sender_id:   Optional[int]  = Field(default=None)
    timestamp:   datetime       = Field()
    reason:      str            = Field()       # e.g. "NO_TICKER" | "NO_ACTIVITY_EMOJI" | "PARSE_EXCEPTION"
    created_at:  datetime       = Field(default_factory=utcnow)


# ---------------------------------------------------------------------------
# daily_signal_summary
# Materialized summary of signals grouped by (ticker, et_day).
# Rebuilt on startup, then incrementally updated on each new signal.
# ---------------------------------------------------------------------------

class DailySignalSummary(SQLModel, table=True):
    __tablename__ = "daily_signal_summary"

    ticker:             str            = Field(primary_key=True)  # e.g. "VIRTUAL"
    et_day:             str            = Field(primary_key=True)  # YYYY-MM-DD in ET timezone

    first_message_id:   int            = Field()                  # FK → signals.message_id
    last_message_id:    int            = Field()                  # FK → signals.message_id

    first_price:        Optional[float] = Field(default=None)
    last_price:         Optional[float] = Field(default=None)

    first_activity:     Optional[str]  = Field(default=None)
    last_activity:      Optional[str]  = Field(default=None)

    first_time_et:      Optional[str]  = Field(default=None)     # HH:MM format
    last_time_et:       Optional[str]  = Field(default=None)     # HH:MM format

    signal_count:       int            = Field(default=1)
    max_boost:          Optional[int]  = Field(default=None)


# ---------------------------------------------------------------------------
# daily_calls
# Per-(ticker, et_day, activity_type) aggregation with pricing and efficiency metrics.
# Populated by listener as signals arrive, enriched by CoinGecko EOD job.
# ---------------------------------------------------------------------------

class DailyCall(SQLModel, table=True):
    __tablename__ = "daily_calls"

    # Primary key (auto-increment) + unique constraint on dimensions
    id:                         int     = Field(primary_key=True)

    # Dimensions
    ticker:                     str     = Field(index=True)            # e.g. "VIRTUAL"
    et_day:                     str     = Field(index=True)            # YYYY-MM-DD in ET
    activity_type:              str     = Field(index=True)            # "BUY" | "SELL"

    # First call anchor
    first_call_msg_id:          int     = Field()                      # FK → signals.message_id
    first_call_price:           Optional[float] = Field(default=None)
    first_call_time_et:         Optional[str]   = Field(default=None)  # HH:MM

    # Last call anchor
    last_call_msg_id:           int     = Field()                      # FK → signals.message_id
    last_call_price:            Optional[float] = Field(default=None)
    last_call_time_et:          Optional[str]   = Field(default=None)  # HH:MM

    # Day aggregates
    call_count:                 int     = Field(default=1)
    max_boost:                  Optional[int]   = Field(default=None)

    # EOD price — fetched separately via CoinGecko, null until filled
    eod_price:                  Optional[float] = Field(default=None)
    eod_fetched_at:             Optional[str]   = Field(default=None)  # ISO UTC datetime

    # MCap — snapshotted from metrics_cache at backfill time
    mcap_at_call:               Optional[float] = Field(default=None)
    mcap_tier:                  Optional[str]   = Field(default=None)  # "micro" | "small" | "mid" | "large" | "unknown"

    # Computed efficiency (Python fills these after eod_price arrives)
    first_call_efficiency_pct:  Optional[float] = Field(default=None)  # (eod - first_price) / first_price * 100
    last_call_efficiency_pct:   Optional[float] = Field(default=None)  # (eod - last_price) / last_price * 100
    intraday_drift_pct:         Optional[float] = Field(default=None)  # (last_price - first_price) / first_price * 100
    hours_remaining_at_first_call: Optional[float] = Field(default=None)  # hours from first_call_time_et to 23:59 ET (not an average)
    direction_correct:          Optional[int]   = Field(default=None)  # 1 | 0 | NULL (bool, null if eod missing)

    # Data quality flags
    dq_first_price_missing:     int     = Field(default=0)
    dq_last_price_missing:      int     = Field(default=0)
    dq_eod_missing:             int     = Field(default=1)             # starts as 1, cleared when eod fetched
    dq_mcap_missing:            int     = Field(default=0)

    # Unique constraint enforced at DB level (one row per ticker, day, direction)


# ---------------------------------------------------------------------------
# Daily summary helpers
# ---------------------------------------------------------------------------

from sqlmodel import create_engine, Session, select, func
from sqlalchemy import text as sql_text


def _get_et_day(timestamp: datetime) -> str:
    """Convert UTC timestamp to ET date string (YYYY-MM-DD)."""
    et_tz = ZoneInfo("America/New_York")
    et_dt = timestamp.astimezone(et_tz)
    return et_dt.strftime("%Y-%m-%d")


def _get_et_time(timestamp: datetime) -> str:
    """Convert UTC timestamp to ET time string (HH:MM)."""
    et_tz = ZoneInfo("America/New_York")
    et_dt = timestamp.astimezone(et_tz)
    return et_dt.strftime("%H:%M")


def rebuild_daily_calls(engine) -> int:
    """
    Delete all rows from daily_calls and re-aggregate from scratch.
    Creates one row per (ticker, et_day, activity_type) with first/last call metrics.
    Returns the number of rows inserted.
    """
    session = Session(engine)
    try:
        # Delete all existing rows
        session.exec(sql_text("DELETE FROM daily_calls"))
        session.commit()

        # Fetch all signals, grouped by (ticker, et_day, activity_type)
        all_signals = session.exec(select(Signal).order_by(Signal.timestamp)).all()

        # Group by (ticker, et_day, activity_type)
        groups = {}
        for sig in all_signals:
            et_day = _get_et_day(sig.timestamp)
            key = (sig.ticker, et_day, sig.activity_type)
            if key not in groups:
                groups[key] = []
            groups[key].append(sig)

        # Create a daily_calls row for each group
        daily_calls_rows = []
        for (ticker, et_day, activity_type), signals in groups.items():
            # Signals are already sorted by timestamp
            first_sig = signals[0]
            last_sig = signals[-1]

            # Compute intraday_drift_pct
            intraday_drift_pct = None
            if (first_sig.price_at_signal and last_sig.price_at_signal and
                first_sig.price_at_signal != 0):
                intraday_drift_pct = round(
                    (last_sig.price_at_signal - first_sig.price_at_signal)
                    / first_sig.price_at_signal * 100, 2
                )

            daily_call = DailyCall(
                ticker=ticker,
                et_day=et_day,
                activity_type=activity_type,
                first_call_msg_id=first_sig.message_id,
                first_call_price=first_sig.price_at_signal,
                first_call_time_et=_get_et_time(first_sig.timestamp),
                last_call_msg_id=last_sig.message_id,
                last_call_price=last_sig.price_at_signal,
                last_call_time_et=_get_et_time(last_sig.timestamp),
                call_count=len(signals),
                max_boost=max((s.boost for s in signals if s.boost is not None), default=None),
                intraday_drift_pct=intraday_drift_pct,
                dq_first_price_missing=(1 if first_sig.price_at_signal is None else 0),
                dq_last_price_missing=(1 if last_sig.price_at_signal is None else 0),
                dq_eod_missing=1,
                dq_mcap_missing=0,
            )
            daily_calls_rows.append(daily_call)

        # Bulk insert
        for dc_row in daily_calls_rows:
            session.add(dc_row)
        session.commit()

        row_count = len(daily_calls_rows)
        return row_count

    finally:
        session.close()


def backfill_missing_daily_calls(engine) -> int:
    """
    Inserts daily_calls rows for any (ticker, et_day, activity_type) combinations
    that exist in signals but have no corresponding daily_calls row.
    Safe to run at any time — only inserts missing rows, never overwrites.
    Returns count of rows inserted.
    """
    session = Session(engine)
    try:
        # Find all (ticker, et_day, activity_type) groups in signals
        all_signals = session.exec(
            select(Signal)
            .where(Signal.activity_type.in_(["BUY", "SELL"]))
            .order_by(Signal.timestamp)
        ).all()

        # Group by (ticker, et_day, activity_type)
        groups: dict = {}
        for sig in all_signals:
            et_day = _get_et_day(sig.timestamp)
            key = (sig.ticker, et_day, sig.activity_type)
            if key not in groups:
                groups[key] = []
            groups[key].append(sig)

        # Check which groups are missing from daily_calls
        existing = session.exec(
            sql_text("SELECT ticker, et_day, activity_type FROM daily_calls")
        ).all()
        existing_keys = {(r[0], r[1], r[2]) for r in existing}

        inserted = 0
        for (ticker, et_day, activity_type), signals in groups.items():
            if (ticker, et_day, activity_type) in existing_keys:
                continue  # already exists, skip

            first_sig = signals[0]
            last_sig = signals[-1]

            daily_call = DailyCall(
                ticker=ticker,
                et_day=et_day,
                activity_type=activity_type,
                first_call_msg_id=first_sig.message_id,
                first_call_price=first_sig.price_at_signal,
                first_call_time_et=_get_et_time(first_sig.timestamp),
                last_call_msg_id=last_sig.message_id,
                last_call_price=last_sig.price_at_signal,
                last_call_time_et=_get_et_time(last_sig.timestamp),
                call_count=len(signals),
                max_boost=max(
                    (s.boost for s in signals if s.boost is not None), default=None
                ),
                intraday_drift_pct=(
                    round(
                        (last_sig.price_at_signal - first_sig.price_at_signal)
                        / first_sig.price_at_signal * 100,
                        4,
                    )
                    if first_sig.price_at_signal
                    and last_sig.price_at_signal
                    and first_sig.price_at_signal != 0
                    else None
                ),
                dq_first_price_missing=(1 if first_sig.price_at_signal is None else 0),
                dq_last_price_missing=(1 if last_sig.price_at_signal is None else 0),
                dq_eod_missing=1,
                dq_mcap_missing=0,
            )
            session.add(daily_call)
            inserted += 1

        session.commit()
        return inserted

    finally:
        session.close()


def rebuild_daily_summary(engine) -> int:
    """
    Delete all rows from daily_signal_summary and re-aggregate from scratch.
    Returns the number of rows inserted.
    """
    session = Session(engine)
    try:
        # Delete all existing rows
        session.exec(sql_text("DELETE FROM daily_signal_summary"))
        session.commit()

        # Fetch all signals, grouped by (ticker, et_day)
        all_signals = session.exec(select(Signal).order_by(Signal.timestamp)).all()

        # Group by (ticker, et_day)
        groups = {}
        for sig in all_signals:
            et_day = _get_et_day(sig.timestamp)
            key = (sig.ticker, et_day)
            if key not in groups:
                groups[key] = []
            groups[key].append(sig)

        # Create a summary row for each group
        summaries = []
        for (ticker, et_day), signals in groups.items():
            # Signals are already sorted by timestamp
            first_sig = signals[0]
            last_sig = signals[-1]

            summary = DailySignalSummary(
                ticker=ticker,
                et_day=et_day,
                first_message_id=first_sig.message_id,
                last_message_id=last_sig.message_id,
                first_price=first_sig.price_at_signal,
                last_price=last_sig.price_at_signal,
                first_activity=first_sig.activity_type,
                last_activity=last_sig.activity_type,
                first_time_et=_get_et_time(first_sig.timestamp),
                last_time_et=_get_et_time(last_sig.timestamp),
                signal_count=len(signals),
                max_boost=max((s.boost for s in signals if s.boost is not None), default=None),
            )
            summaries.append(summary)

        # Bulk insert
        for summary in summaries:
            session.add(summary)
        session.commit()

        row_count = len(summaries)
        return row_count

    finally:
        session.close()


def upsert_daily_summary_for(engine, signal: Signal) -> None:
    """
    After a new signal is inserted, upsert the daily summary for (ticker, et_day).
    Re-calculates all fields by querying only that slice from signals.
    """
    session = Session(engine)
    try:
        et_day = _get_et_day(signal.timestamp)
        ticker = signal.ticker

        # Fetch all signals for this (ticker, et_day)
        signals = session.exec(
            select(Signal)
            .where(Signal.ticker == ticker)
            .order_by(Signal.timestamp)
        ).all()

        # Filter to only this et_day
        same_day_signals = [
            s for s in signals
            if _get_et_day(s.timestamp) == et_day
        ]

        if not same_day_signals:
            # Should not happen, but be safe
            return

        # Find first and last by timestamp
        first_sig = min(same_day_signals, key=lambda s: s.timestamp)
        last_sig = max(same_day_signals, key=lambda s: s.timestamp)

        # Recalculate summary fields
        summary = DailySignalSummary(
            ticker=ticker,
            et_day=et_day,
            first_message_id=first_sig.message_id,
            last_message_id=last_sig.message_id,
            first_price=first_sig.price_at_signal,
            last_price=last_sig.price_at_signal,
            first_activity=first_sig.activity_type,
            last_activity=last_sig.activity_type,
            first_time_et=_get_et_time(first_sig.timestamp),
            last_time_et=_get_et_time(last_sig.timestamp),
            signal_count=len(same_day_signals),
            max_boost=max((s.boost for s in same_day_signals if s.boost is not None), default=None),
        )

        # Use INSERT OR REPLACE (upsert)
        session.merge(summary)
        session.commit()

    finally:
        session.close()


def upsert_daily_calls_for(engine, signal: Signal) -> None:
    """
    After a new signal is inserted, upsert the daily_calls row for (ticker, et_day, activity_type).
    Re-calculates all fields (first/last prices, times, call_count, max_boost) by querying
    only that slice from signals.

    daily_calls is per-(ticker, et_day, activity_type) and tracks the day's call metrics.
    """
    session = Session(engine)
    try:
        et_day = _get_et_day(signal.timestamp)
        ticker = signal.ticker
        activity_type = signal.activity_type

        # Fetch all signals for this (ticker, et_day, activity_type)
        same_type_signals = session.exec(
            select(Signal)
            .where(Signal.ticker == ticker)
            .where(Signal.activity_type == activity_type)
            .order_by(Signal.timestamp)
        ).all()

        # Filter to only this et_day
        same_day_signals = [
            s for s in same_type_signals
            if _get_et_day(s.timestamp) == et_day
        ]

        if not same_day_signals:
            # Should not happen, but be safe
            return

        # Find first and last by timestamp
        first_sig = min(same_day_signals, key=lambda s: s.timestamp)
        last_sig = max(same_day_signals, key=lambda s: s.timestamp)

        # Check if row already exists for this (ticker, et_day, activity_type)
        existing = session.exec(
            select(DailyCall)
            .where(DailyCall.ticker == ticker)
            .where(DailyCall.et_day == et_day)
            .where(DailyCall.activity_type == activity_type)
        ).first()

        # Recalculate daily_calls fields
        # Compute intraday_drift_pct
        intraday_drift_pct = None
        if (first_sig.price_at_signal and last_sig.price_at_signal and
            first_sig.price_at_signal != 0):
            intraday_drift_pct = round(
                (last_sig.price_at_signal - first_sig.price_at_signal)
                / first_sig.price_at_signal * 100, 2
            )

        if existing:
            # Update existing row
            existing.first_call_msg_id = first_sig.message_id
            existing.first_call_price = first_sig.price_at_signal
            existing.first_call_time_et = _get_et_time(first_sig.timestamp)
            existing.last_call_msg_id = last_sig.message_id
            existing.last_call_price = last_sig.price_at_signal
            existing.last_call_time_et = _get_et_time(last_sig.timestamp)
            existing.call_count = len(same_day_signals)
            existing.max_boost = max((s.boost for s in same_day_signals if s.boost is not None), default=None)
            existing.intraday_drift_pct = intraday_drift_pct
            existing.dq_first_price_missing = (1 if first_sig.price_at_signal is None else 0)
            existing.dq_last_price_missing = (1 if last_sig.price_at_signal is None else 0)
            session.add(existing)
        else:
            # Create new row
            daily_call = DailyCall(
                ticker=ticker,
                et_day=et_day,
                activity_type=activity_type,
                first_call_msg_id=first_sig.message_id,
                first_call_price=first_sig.price_at_signal,
                first_call_time_et=_get_et_time(first_sig.timestamp),
                last_call_msg_id=last_sig.message_id,
                last_call_price=last_sig.price_at_signal,
                last_call_time_et=_get_et_time(last_sig.timestamp),
                call_count=len(same_day_signals),
                max_boost=max((s.boost for s in same_day_signals if s.boost is not None), default=None),
                intraday_drift_pct=intraday_drift_pct,
                dq_first_price_missing=(1 if first_sig.price_at_signal is None else 0),
                dq_last_price_missing=(1 if last_sig.price_at_signal is None else 0),
                dq_eod_missing=1,
                dq_mcap_missing=0,
            )
            session.add(daily_call)

        session.commit()

    finally:
        session.close()


# ---------------------------------------------------------------------------
# DB init helper — call once on startup
# ---------------------------------------------------------------------------

def get_engine(db_path: str = "data/signals.db"):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    SQLModel.metadata.create_all(engine)
    return engine


def get_session(engine):
    return Session(engine)