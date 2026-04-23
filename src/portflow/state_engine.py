# src/portflow/state_engine.py
# Derives a 9-state RSI zone machine per (ticker, timeframe) by replaying the
# last N closed-candle RSI values from watchlist_rsi_history, and upserts the
# derived state into watchlist_rsi_state.

import logging
from datetime import datetime
from typing import Optional

from src.portflow.db import get_portflow_conn

logger = logging.getLogger(__name__)

# Per-timeframe zone config (from plan_RSI_System.txt).
ZONE_CFG = {
    "1w": {"low": 40, "high": 60, "deep_low": 30, "deep_high": 70, "sustain": 2},
    "1d": {"low": 36, "high": 64, "deep_low": 25, "deep_high": 75, "sustain": 3},
    "1h": {"low": 30, "high": 70, "deep_low": 20, "deep_high": 80, "sustain": 4},
}

STATE_TIMEFRAMES = ["1w", "1d", "1h"]

LOW = "LOW"
HIGH = "HIGH"
MID = "MID"


def _classify(rsi: float, cfg: dict) -> str:
    if rsi <= cfg["low"]:
        return LOW
    if rsi >= cfg["high"]:
        return HIGH
    return MID


def _load_history(ticker: str, timeframe: str) -> list:
    conn = get_portflow_conn()
    try:
        rows = conn.execute(
            """
            SELECT candle_close_time, rsi_value
            FROM watchlist_rsi_history
            WHERE ticker = ? AND timeframe = ?
            ORDER BY candle_close_time ASC
            """,
            (ticker, timeframe),
        ).fetchall()
    finally:
        conn.close()
    return [(r["candle_close_time"], float(r["rsi_value"])) for r in rows]


def _build_segments(zones: list) -> list:
    if not zones:
        return []
    segments = []
    current_zone = zones[0]
    start = 0
    for i in range(1, len(zones)):
        if zones[i] != current_zone:
            segments.append({
                "zone": current_zone,
                "start": start,
                "end": i - 1,
                "length": i - start,
            })
            current_zone = zones[i]
            start = i
    segments.append({
        "zone": current_zone,
        "start": start,
        "end": len(zones) - 1,
        "length": len(zones) - start,
    })
    return segments


def _count_failed_attempts(segments: list) -> tuple:
    failed_bottom = 0
    failed_top = 0
    low_seen = 0
    high_seen = 0
    for seg in segments:
        if seg["zone"] == LOW:
            if low_seen > 0:
                failed_bottom += 1
            low_seen += 1
        elif seg["zone"] == HIGH:
            if high_seen > 0:
                failed_top += 1
            high_seen += 1
    return failed_bottom, failed_top


def _find_source_zone(segments: list):
    if len(segments) < 2:
        return None
    for i in range(len(segments) - 2, -1, -1):
        if segments[i]["zone"] in (LOW, HIGH):
            return segments[i]["zone"]
    return None


def _compute_extremum(history: list, segments: list, zone_type: str):
    rsi_values = []
    for seg in segments:
        if seg["zone"] == zone_type:
            for idx in range(seg["start"], seg["end"] + 1):
                rsi_values.append(history[idx][1])
    if not rsi_values:
        return None
    if zone_type == LOW:
        return min(rsi_values)
    if zone_type == HIGH:
        return max(rsi_values)
    return None


def _derive_state(history: list, cfg: dict) -> dict:
    """history is oldest→newest list of (close_time_iso, rsi)."""
    if not history:
        return {
            "state": "RANGE",
            "zone_entered_at": None,
            "zone_exited_at": None,
            "sustain_candles_count": 0,
            "failed_attempts_count": 0,
            "last_zone_extremum": None,
        }

    sustain_n = cfg["sustain"]
    zones = [_classify(rsi, cfg) for _, rsi in history]
    segments = _build_segments(zones)
    failed_bottom, failed_top = _count_failed_attempts(segments)

    last_seg = segments[-1]
    latest_zone = last_seg["zone"]
    source_zone = _find_source_zone(segments)

    state = "RANGE"
    zone_entered_at = None
    zone_exited_at = None
    sustain = last_seg["length"]
    failed = 0
    extremum = None

    if latest_zone == LOW:
        zone_entered_at = history[last_seg["start"]][0]
        extremum = _compute_extremum(history, segments, LOW)
        failed = failed_bottom
        state = "FAILED_BOTTOM" if failed_bottom > 0 else "LOW_ZONE"

    elif latest_zone == HIGH:
        zone_entered_at = history[last_seg["start"]][0]
        extremum = _compute_extremum(history, segments, HIGH)
        failed = failed_top
        state = "FAILED_TOP" if failed_top > 0 else "HIGH_ZONE"

    else:  # MID
        if source_zone == LOW:
            zone_exited_at = history[last_seg["start"]][0]
            extremum = _compute_extremum(history, segments, LOW)
            failed = failed_bottom
            state = "CONFIRMED_BULL" if sustain >= sustain_n else "EXITING_LOW"
        elif source_zone == HIGH:
            zone_exited_at = history[last_seg["start"]][0]
            extremum = _compute_extremum(history, segments, HIGH)
            failed = failed_top
            state = "CONFIRMED_BEAR" if sustain >= sustain_n else "EXITING_HIGH"
        else:
            state = "RANGE"

    return {
        "state": state,
        "zone_entered_at": zone_entered_at,
        "zone_exited_at": zone_exited_at,
        "sustain_candles_count": sustain,
        "failed_attempts_count": failed,
        "last_zone_extremum": extremum,
    }


def _upsert_state(ticker: str, timeframe: str, derived: dict) -> None:
    updated_at = datetime.utcnow().isoformat() + "Z"
    conn = get_portflow_conn()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO watchlist_rsi_state
                (ticker, timeframe, state,
                 zone_entered_at, zone_exited_at,
                 sustain_candles_count, failed_attempts_count,
                 last_zone_extremum, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ticker,
                timeframe,
                derived["state"],
                derived["zone_entered_at"],
                derived["zone_exited_at"],
                derived["sustain_candles_count"],
                derived["failed_attempts_count"],
                derived["last_zone_extremum"],
                updated_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def evaluate_state(ticker: str, timeframe: str) -> Optional[dict]:
    cfg = ZONE_CFG.get(timeframe)
    if cfg is None:
        return None
    history = _load_history(ticker, timeframe)
    derived = _derive_state(history, cfg)
    _upsert_state(ticker, timeframe, derived)
    return derived


def refresh_states_for_ticker(ticker: str) -> dict:
    ticker = ticker.upper()
    out = {}
    for tf in STATE_TIMEFRAMES:
        try:
            out[tf] = evaluate_state(ticker, tf)
        except Exception as e:
            logger.exception("evaluate_state failed for %s %s: %s", ticker, tf, e)
            out[tf] = None
    return out


def refresh_all_states() -> dict:
    conn = get_portflow_conn()
    try:
        rows = conn.execute("SELECT DISTINCT ticker FROM watchlist_tickers").fetchall()
    finally:
        conn.close()

    processed = 0
    for r in rows:
        refresh_states_for_ticker(r["ticker"])
        processed += 1
    logger.info("Portflow state refresh complete: tickers_processed=%s", processed)
    return {"tickers_processed": processed}
