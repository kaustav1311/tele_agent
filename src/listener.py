# src/listener.py
# Real-time Telethon listener + startup backfill.
# Parses every new message and writes to SQLite.
# On parse failure → unparsed_messages. On DB failure → log and continue.

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from sqlmodel import select
from telethon import TelegramClient, events

from src.parser import parse, ParserError
from src.schema import Signal, UnparsedMessage, get_engine, get_session

load_dotenv()

logger = logging.getLogger(__name__)

API_ID       = int(os.environ["TG_API_ID"])
API_HASH     = os.environ["TG_API_HASH"]
CHANNEL_ID   = int(os.environ["TG_CHANNEL_ID"])
SESSION_FILE = Path(__file__).parent.parent / "session"
DB_PATH      = Path(__file__).parent.parent / "data" / "signals.db"

BACKFILL_LIMIT = 500


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


def _store_signal(session, parsed: dict, meta: dict):
    """Write a parsed signal to DB. Skips if message_id already exists."""
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

def _handle_message(session, msg_id: int, timestamp: datetime, text: str,
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
        _store_signal(session, parsed, meta)
    except ParserError as e:
        _store_unparsed(session, meta, reason=e.reason)
    except Exception as e:
        logger.error(f"Unexpected parse error on {msg_id}: {e}")
        _store_unparsed(session, meta, reason="PARSE_EXCEPTION")


# ---------------------------------------------------------------------------
# Backfill: pull missed messages since last known message_id
# ---------------------------------------------------------------------------

async def backfill(client: TelegramClient, session, channel):
    min_id = _get_max_message_id(session)
    logger.info(f"Backfill starting from message_id > {min_id}")

    count = 0
    async for msg in client.iter_messages(channel, limit=BACKFILL_LIMIT, min_id=min_id):
        _handle_message(
            session,
            msg_id     = msg.id,
            timestamp  = msg.date,
            text       = msg.text or "",
            has_media  = msg.media is not None,
            sender_id  = msg.sender_id,
        )
        count += 1

    logger.info(f"Backfill complete: {count} messages processed")


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
        channel = await client.get_entity(CHANNEL_ID)

        # Backfill missed messages before going live
        await backfill(client, session, channel)

        # Live listener — fires on every new message in channel
        @client.on(events.NewMessage(chats=channel))
        async def on_new_message(event):
            msg = event.message
            _handle_message(
                session,
                msg_id    = msg.id,
                timestamp = msg.date,
                text      = msg.text or "",
                has_media = msg.media is not None,
                sender_id = msg.sender_id,
            )

        logger.info("Listener live. Waiting for new messages...")
        await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())