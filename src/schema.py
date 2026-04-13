# src/schema.py
# SQLModel table definitions for Signal Agent.
# Three tables: signals, metrics_cache, unparsed_messages.

from datetime import datetime, timezone
from typing import Optional
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
# DB init helper — call once on startup
# ---------------------------------------------------------------------------

from sqlmodel import create_engine, Session

def get_engine(db_path: str = "data/signals.db"):
    engine = create_engine(f"sqlite:///{db_path}", echo=False)
    SQLModel.metadata.create_all(engine)
    return engine


def get_session(engine):
    return Session(engine)