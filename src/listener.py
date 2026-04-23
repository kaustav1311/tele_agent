# src/listener.py
# Real-time Telethon listener + startup backfill.
# Parses every new message and writes to SQLite.
# On parse failure → unparsed_messages. On DB failure → log and continue.

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from sqlmodel import select
from telethon import TelegramClient, events
from zoneinfo import ZoneInfo
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from src.parser import parse, ParserError
from src.backfill_eod import run_eod_backfill
from src.portflow.db import init_portflow_db
from src.portflow.state_engine import refresh_all_states
from src.portflow.ta_engine import refresh_all_tickers
from src.schema import (
    Signal,
    UnparsedMessage,
    get_engine,
    get_session,
    rebuild_daily_summary,
    rebuild_daily_calls,
    backfill_missing_daily_calls,
    upsert_daily_summary_for,
    upsert_daily_calls_for,
)

load_dotenv()

logger = logging.getLogger(__name__)

API_ID       = int(os.environ["TG_API_ID"])
API_HASH     = os.environ["TG_API_HASH"]
CHANNEL_ID   = int(os.environ["TG_CHANNEL_ID"])
SESSION_FILE = Path(__file__).parent.parent / "session"
DB_PATH      = Path(__file__).parent.parent / "data" / "signals.db"

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_max_message_id(session) -> int:
    """Return highest known message_id across both tables."""
    sig_max = session.exec(
        select(Signal.message_id).order_by(Signal.message_id.desc()).limit(1)
    ).first()
    unp_max = session.exec(
        select(UnparsedMessage.message_id).order_by(UnparsedMessage.message_id.desc()).limit(1)
    ).first()
    return max(sig_max or 0, unp_max or 0)


def _store_signal(session, engine, parsed: dict, meta: dict):
    """Write a parsed signal to DB. Skips if message_id already exists.
    After successful insert, updates daily_signal_summary for this signal."""
    existing = session.get(Signal, meta["message_id"])
    if existing:
        return

    signal = Signal(
        message_id       = meta["message_id"],
        timestamp        = meta["timestamp"],
        sender_id        = meta.get("sender_id"),
        **parsed,
    )
    try:
        session.add(signal)
        session.commit()
        logger.info(f"Stored signal {meta['message_id']} | {parsed['ticker']} | {parsed['activity_type']}")

        # Update daily_signal_summary (all dates)
        upsert_daily_summary_for(engine, signal)

        # Update daily_calls for all signals (idempotent upsert)
        upsert_daily_calls_for(engine, signal)

    except Exception as e:
        session.rollback()
        logger.error(f"DB write failed for signal {meta['message_id']}: {e}")


def _store_unparsed(session, meta: dict, reason: str):
    """Write a failed message to unparsed_messages. Skips if already exists."""
    existing = session.get(UnparsedMessage, meta["message_id"])
    if existing:
        return

    record = UnparsedMessage(
        message_id = meta["message_id"],
        raw_text   = meta["text"],
        sender_id  = meta.get("sender_id"),
        timestamp  = meta["timestamp"],
        reason     = reason,
    )
    try:
        session.add(record)
        session.commit()
        logger.warning(f"Unparsed {meta['message_id']}: {reason}")
    except Exception as e:
        session.rollback()
        logger.error(f"DB write failed for unparsed {meta['message_id']}: {e}")


# ---------------------------------------------------------------------------
# Message handler (shared by backfill + live listener)
# ---------------------------------------------------------------------------

def _handle_message(session, engine, msg_id: int, timestamp: datetime, text: str,
                    has_media: bool, sender_id: int):
    meta = {
        "message_id": msg_id,
        "timestamp":  timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc),
        "text":       text,
        "has_media":  has_media,
        "sender_id":  sender_id,
    }

    if not text.strip():
        _store_unparsed(session, meta, reason="EMPTY_TEXT")
        return

    try:
        parsed = parse(text, has_media=has_media)
        _store_signal(session, engine, parsed, meta)
    except ParserError as e:
        _store_unparsed(session, meta, reason=e.reason)
    except Exception as e:
        logger.error(f"Unexpected parse error on {msg_id}: {e}")
        _store_unparsed(session, meta, reason="PARSE_EXCEPTION")


# ---------------------------------------------------------------------------
# Backfill: pull missed messages since last known message_id
# ---------------------------------------------------------------------------




async def backfill(client: TelegramClient, session, engine, channel):
    db_max = _get_max_message_id(session)
    env_min = int(os.environ.get("BACKFILL_MIN_ID", "0"))
    min_id = max(db_max, env_min)
    if min_id > 0:
        logger.info(f"Resuming from message_id {min_id}")
    else:
        logger.info("Full backfill — no prior messages found")
     # DEBUG
    test = await client.get_messages(channel, limit=3)
    logger.info(f"DEBUG test fetch: {[m.id for m in test]}")
    count = 0
    async for msg in client.iter_messages(channel, min_id=min_id, limit=None):
        _handle_message(
            session,
            engine,
            msg_id     = msg.id,
            timestamp  = msg.date,
            text       = msg.text or "",
            has_media  = msg.media is not None,
            sender_id  = msg.sender_id,
        )
        count += 1

    logger.info(f"Backfill complete: {count} messages processed")


# ---------------------------------------------------------------------------
# EOD backfill helper
# ---------------------------------------------------------------------------

def _prev_et_day() -> str:
    """Returns the previous ET calendar day as YYYY-MM-DD string."""
    et_now = datetime.now(ZoneInfo("America/New_York"))
    prev = et_now - timedelta(days=1)
    return prev.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Main: start listener with backfill
# ---------------------------------------------------------------------------

async def main():
    logging.basicConfig(
        level  = logging.INFO,
        format = "%(asctime)s [%(levelname)s] %(message)s",
    )

    engine  = get_engine(str(DB_PATH))
    session = get_session(engine)

    async with TelegramClient(str(SESSION_FILE), API_ID, API_HASH) as client:
        await client.get_dialogs()                          # warm entity cache
        channel = await client.get_input_entity(CHANNEL_ID)

        # Backfill missed messages before going live
        await backfill(client, session, engine, channel)

        # Rebuild daily_signal_summary from all signals
        summary_rows = rebuild_daily_summary(engine)
        logger.info(f"daily_signal_summary rebuilt: {summary_rows} rows")

        # Rebuild daily_calls from all signals
        calls_rows = rebuild_daily_calls(engine)
        logger.info(f"daily_calls rebuilt: {calls_rows} rows")

        # Fill any gaps — catches days where listener was not running
        gap_rows = backfill_missing_daily_calls(engine)
        logger.info(f"daily_calls gap fill: {gap_rows} new rows inserted")

        # -----------------------------------------------------------------------
        # APScheduler: EOD backfill fires at 00:05 ET every day
        # -----------------------------------------------------------------------
        scheduler = BackgroundScheduler(timezone="America/New_York")
        scheduler.add_job(
            lambda: run_eod_backfill(_prev_et_day()),
            CronTrigger(hour=0, minute=5, timezone="America/New_York"),
            id="eod_backfill_daily",
            replace_existing=True,
            misfire_grace_time=300,   # allow up to 5min late if listener was down
        )

        # Portflow watchlist TA refresh (separate SQLite file).
        # Init here too so listener can run before API has ever started.
        init_portflow_db()
        scheduler.add_job(
            refresh_all_tickers,
            trigger="interval",
            minutes=10,
            id="portflow_ta_refresh",
            replace_existing=True,
            next_run_time=datetime.now(timezone.utc) + timedelta(minutes=2),
        )

        # Portflow RSI state refresh — hourly, offset +3min after TA refresh
        # so state eval always sees fresh watchlist_rsi_history rows.
        scheduler.add_job(
            refresh_all_states,
            trigger="interval",
            hours=1,
            id="portflow_state_refresh",
            replace_existing=True,
            next_run_time=datetime.now(timezone.utc) + timedelta(minutes=5),
        )

        scheduler.start()
        logger.info(
            "APScheduler started: EOD backfill at 00:05 ET, "
            "portflow TA refresh every 10min, state refresh hourly"
        )

        # Live listener — fires on every new message in channel
        @client.on(events.NewMessage(chats=channel))
        async def on_new_message(event):
            msg = event.message
            _handle_message(
                session,
                engine,
                msg_id    = msg.id,
                timestamp = msg.date,
                text      = msg.text or "",
                has_media = msg.media is not None,
                sender_id = msg.sender_id,
            )

        logger.info("Listener live. Waiting for new messages...")
        try:
            await client.run_until_disconnected()
        except asyncio.CancelledError:
            logger.info("Listener stopped.")
        finally:
            session.close()
            scheduler.shutdown(wait=False)
            logger.info("APScheduler stopped.")


if __name__ == "__main__":
    asyncio.run(main())