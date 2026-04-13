# src/fetch_raw.py
# One-off raw message pull for exploratory analysis.
# Idempotent: only fetches messages newer than max known message_id.

import asyncio
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

API_ID       = int(os.environ["TG_API_ID"])
API_HASH     = os.environ["TG_API_HASH"]
CHANNEL_ID   = int(os.environ["TG_CHANNEL_ID"])
SESSION_FILE = Path(__file__).parent.parent / "session"   # → E:\Telethon\session.session

RAW_OUTPUT   = Path(__file__).parent.parent / "data" / "raw_messages.json"
MAX_MESSAGES = 500
HOURS_BACK   = 4


def load_existing() -> tuple[list[dict], int]:
    """Return (existing_records, max_message_id). Creates data/ dir if needed."""
    RAW_OUTPUT.parent.mkdir(exist_ok=True)
    if not RAW_OUTPUT.exists():
        return [], 0
    with RAW_OUTPUT.open("r", encoding="utf-8") as f:
        records = json.load(f)
    max_id = max((r["message_id"] for r in records), default=0)
    return records, max_id


async def fetch(min_id: int) -> list[dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=HOURS_BACK)
    new_records = []

    async with TelegramClient(str(SESSION_FILE), API_ID, API_HASH) as client:
      await client.get_dialogs()          # warms entity cache
      channel = await client.get_entity(CHANNEL_ID)
      
      async for msg in client.iter_messages(
          channel,
          limit=MAX_MESSAGES,
          min_id=min_id,
      ):
            if msg.date < cutoff:
                break            # messages arrive newest-first; stop at horizon

            new_records.append({
                "message_id": msg.id,
                "timestamp":  msg.date.isoformat(),
                "text":       msg.text or "",
                "has_media":  msg.media is not None,
                "sender_id":  msg.sender_id,
            })

    return new_records


async def main():
    existing, max_id = load_existing()
    print(f"Existing records: {len(existing)} | Fetching newer than message_id={max_id}")

    new_records = await fetch(min_id=max_id)
    print(f"New messages fetched: {len(new_records)}")

    if not new_records:
        print("Nothing new. Exiting.")
        return

    combined = existing + new_records
    with RAW_OUTPUT.open("w", encoding="utf-8") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(combined)} total records → {RAW_OUTPUT}")


if __name__ == "__main__":
    asyncio.run(main())