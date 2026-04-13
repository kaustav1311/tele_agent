# src/parser.py
# Parses raw Telegram message text into a structured dict.
# Returns parsed dict on success, raises ParserError on failure.

import re
from dataclasses import dataclass
from typing import Optional


class ParserError(Exception):
    """Raised when a message cannot be fully parsed. Carries a reason code."""
    def __init__(self, reason: str, detail: str = ""):
        self.reason = reason          # short code for unparsed_messages.reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}")


# ---------------------------------------------------------------------------
# Alerts tier mapping
# ---------------------------------------------------------------------------

def _alerts_tier(count: int) -> str:
    if count >= 10:
        return "FIRE"
    if count >= 5:
        return "HOT"
    return "NORMAL"


# ---------------------------------------------------------------------------
# Core parser
# ---------------------------------------------------------------------------

# Handles both plain "$TICKER" and markdown "**$TICKER**" formats
_RE_HEADER  = re.compile(r'\*{0,2}\$(\w+)\*{0,2}\s*\|\s*\*{0,2}#(\w+)\*{0,2}')
_RE_PRICE   = re.compile(r'Price:\s*([\d.]+)')
_RE_CHANGE  = re.compile(r'([+-][\d.]+)%')          # captures sign + value
_RE_BOOST   = re.compile(r'Boost:\s*\*{0,2}(\d+)\*{0,2}\s*/\s*10')
_RE_ALERTS  = re.compile(r'Alerts in this hour:\s*(\d+)')
_EMOJI_BUY  = '🟢'
_EMOJI_SELL = '🔴'


def parse(text: str, has_media: bool = False) -> dict:
    """
    Parse a single signal message text.

    Returns a dict with keys matching Signal model fields
    (excluding message_id, sender_id, timestamp, created_at — caller fills those).

    Raises ParserError with a reason code if critical fields are missing.
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # --- Header: ticker + pair ---
    header_match = None
    for line in lines[:3]:              # header is always in first 3 lines
        header_match = _RE_HEADER.search(line)
        if header_match:
            break
    if not header_match:
        raise ParserError("NO_TICKER", repr(text[:80]))

    ticker = header_match.group(1).upper()
    pair   = header_match.group(2).upper()

    # --- Price ---
    price_at_signal: Optional[float] = None
    for line in lines:
        m = _RE_PRICE.search(line)
        if m:
            try:
                price_at_signal = float(m.group(1))
            except ValueError:
                pass
            break

    # --- Change 24h ---
    change_24h: Optional[float] = None
    for line in lines:
        if 'Price:' in line:
            m = _RE_CHANGE.search(line)
            if m:
                try:
                    change_24h = float(m.group(1))
                except ValueError:
                    pass
            break

    # --- Activity: type from emoji, raw text stripped of emoji + extra whitespace ---
    activity_type = "UNKNOWN"
    activity_raw  = ""
    for line in lines:
        if _EMOJI_BUY in line:
            activity_type = "BUY"
            activity_raw  = line.replace(_EMOJI_BUY, "").strip()
            break
        if _EMOJI_SELL in line:
            activity_type = "SELL"
            activity_raw  = line.replace(_EMOJI_SELL, "").strip()
            break

    if activity_type == "UNKNOWN":
        raise ParserError("NO_ACTIVITY_EMOJI", repr(text[:80]))

    # --- Boost ---
    boost: Optional[int] = None
    for line in lines:
        m = _RE_BOOST.search(line)
        if m:
            try:
                boost = int(m.group(1))
            except ValueError:
                pass
            break

    # --- Alerts ---
    alerts_this_hour: Optional[int] = None
    alerts_tier: Optional[str]      = None
    for line in lines:
        m = _RE_ALERTS.search(line)
        if m:
            try:
                alerts_this_hour = int(m.group(1))
                alerts_tier      = _alerts_tier(alerts_this_hour)
            except ValueError:
                pass
            break

    return {
        "ticker":           ticker,
        "pair":             pair,
        "price_at_signal":  price_at_signal,
        "change_24h":       change_24h,
        "activity_raw":     activity_raw,
        "activity_type":    activity_type,
        "boost":            boost,
        "alerts_this_hour": alerts_this_hour,
        "alerts_tier":      alerts_tier,
        "has_media":        has_media,
    }