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
# One row per ticker — CoinGecko enrichment, upserted on manual refresh.
# ---------------------------------------------------------------------------

class MetricsCache(SQLModel, table=True):
    __tablename__ = "metrics_cache"

    ticker:       str            = Field(primary_key=True)  # e.g. "VIRTUAL"

    price:        Optional[float] = Field(default=None)
    volume_24h:   Optional[float] = Field(default=None)
    mcap:         Optional[float] = Field(default=None)
    rank:         Optional[int]   = Field(default=None)
    circ_supply:  Optional[float] = Field(default=None)

    fetched_at:   Optional[datetime] = Field(default=None)  # last successful CoinGecko pull


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


# ---------------------------------------------------------------------------
# DB init helper — call once on startup
# ---------------------------------------------------------------------------

def get_engine(db_path: str = "data/signals.db"):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    SQLModel.metadata.create_all(engine)
    return engine


def get_session(engine):
    return Session(engine)