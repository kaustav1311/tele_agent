# src/api/utils/candles.py
# ET-aligned candle start times for each timeframe.
# Uses zoneinfo (stdlib, Python 3.9+) — no pytz dependency.

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def get_candle_start(timeframe: str) -> datetime:
    """
    Return the UTC datetime of the current ET candle start for a given timeframe.
    Timeframes: "5m", "15m", "1h", "4h", "daily", "1hr_rolling", "15m_rolling", "4h_rolling", "1d_rolling"

    - 5m: floor to nearest :00/:05/:10/…/:55
    - 15m: floor to nearest :00/:15/:30/:45
    - 1h: floor to hour boundary
    - 4h: floor to 4-hour boundary (00, 04, 08, 12, 16, 20 ET)
    - daily: floor to ET midnight
    - 1hr_rolling: NOW - 3600 seconds (pure rolling, not candle-aligned)
    - 15m_rolling: NOW - 900 seconds (pure rolling, not candle-aligned)
    - 4h_rolling: NOW - 14400 seconds (pure rolling, not candle-aligned)
    - 1d_rolling: NOW - 86400 seconds (pure rolling, not candle-aligned)
    """
    now_utc = datetime.now(timezone.utc)
    now_et = now_utc.astimezone(ET)

    if timeframe == "5m":
        # Floor to nearest :00, :05, :10, :15, …, :55
        floored_minute = (now_et.minute // 5) * 5
        candle_et = now_et.replace(minute=floored_minute, second=0, microsecond=0)
        return candle_et.astimezone(timezone.utc)

    elif timeframe == "15m":
        # Floor to nearest :00, :15, :30, :45
        floored_minute = (now_et.minute // 15) * 15
        candle_et = now_et.replace(minute=floored_minute, second=0, microsecond=0)
        return candle_et.astimezone(timezone.utc)

    elif timeframe == "1h":
        candle_et = now_et.replace(minute=0, second=0, microsecond=0)
        return candle_et.astimezone(timezone.utc)

    elif timeframe == "4h":
        # Resets at 00, 04, 08, 12, 16, 20 ET
        floored_hour = (now_et.hour // 4) * 4
        candle_et = now_et.replace(hour=floored_hour, minute=0, second=0, microsecond=0)
        return candle_et.astimezone(timezone.utc)

    elif timeframe == "daily":
        candle_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
        return candle_et.astimezone(timezone.utc)

    elif timeframe == "1hr_rolling":
        # Pure rolling window: now - 3600 seconds (no candle boundary)
        return now_utc - __import__('datetime').timedelta(seconds=3600)

    elif timeframe == "15m_rolling":
        # Pure rolling window: now - 900 seconds (no candle boundary)
        return now_utc - __import__('datetime').timedelta(seconds=900)

    elif timeframe == "4h_rolling":
        # Pure rolling window: now - 14400 seconds (no candle boundary)
        return now_utc - __import__('datetime').timedelta(seconds=14400)

    elif timeframe == "1d_rolling":
        # Pure rolling window: now - 86400 seconds (no candle boundary)
        return now_utc - __import__('datetime').timedelta(seconds=86400)

    else:
        raise ValueError(f"Unknown timeframe: {timeframe!r}. Must be one of: 5m, 15m, 1h, 4h, daily, 1hr_rolling, 15m_rolling, 4h_rolling, 1d_rolling")