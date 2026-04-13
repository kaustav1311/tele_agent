# src/api/utils/candles.py
# ET-aligned candle start times for each timeframe.
# Uses zoneinfo (stdlib, Python 3.9+) — no pytz dependency.

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def get_candle_start(timeframe: str) -> datetime:
    """
    Return the UTC datetime of the current ET candle start for a given timeframe.
    Timeframes: "15m", "1h", "4h", "daily"
    """
    now_et = datetime.now(ET)

    if timeframe == "15m":
        # Floor to nearest :00, :15, :30, :45
        floored_minute = (now_et.minute // 15) * 15
        candle_et = now_et.replace(minute=floored_minute, second=0, microsecond=0)

    elif timeframe == "1h":
        candle_et = now_et.replace(minute=0, second=0, microsecond=0)

    elif timeframe == "4h":
        # Resets at 00, 04, 08, 12, 16, 20 ET
        floored_hour = (now_et.hour // 4) * 4
        candle_et = now_et.replace(hour=floored_hour, minute=0, second=0, microsecond=0)

    elif timeframe == "daily":
        candle_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)

    else:
        raise ValueError(f"Unknown timeframe: {timeframe!r}. Must be one of: 15m, 1h, 4h, daily")

    # Return as UTC — all DB timestamps are UTC
    return candle_et.astimezone(timezone.utc)