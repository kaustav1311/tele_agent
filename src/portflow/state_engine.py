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
    latest_zone = zones[-1]
    latest_ct, latest_rsi = history[-1]

    # Find most recent zone transition (point where zone changes).
    # Walk backward looking for first index where zones[i] != zones[i+1].
    transition_idx = None  # index of the candle BEFORE the transition
    for i in range(len(zones) - 2, -1, -1):
        if zones[i] != zones[i + 1]:
            transition_idx = i
            break

    if transition_idx is None:
        # Whole window is one zone (no transition seen).
        if latest_zone == LOW:
            state = "LOW_ZONE"
            zone_entered_at = history[0][0]
            zone_exited_at = None
            sustain = len(history)
            extremum = min(r for _, r in history)
        elif latest_zone == HIGH:
            state = "HIGH_ZONE"
            zone_entered_at = history[0][0]
            zone_exited_at = None
            sustain = len(history)
            extremum = max(r for _, r in history)
        else:
            state = "RANGE"
            zone_entered_at = None
            zone_exited_at = None
            sustain = len(history)
            extremum = None
        return {
            "state": state,
            "zone_entered_at": zone_entered_at,
            "zone_exited_at": zone_exited_at,
            "sustain_candles_count": sustain,
            "failed_attempts_count": 0,
            "last_zone_extremum": extremum,
        }

    prev_zone = zones[transition_idx]       # zone before the transition
    curr_zone = zones[transition_idx + 1]   # zone the latest run started in
    transition_ct = history[transition_idx + 1][0]

    # Sustain = how many closes since the transition (including transition candle).
    sustain = len(zones) - (transition_idx + 1)

    # Count failed re-entries: times zone flipped back into prev_zone within window.
    failed = sum(
        1
        for i in range(transition_idx + 1, len(zones))
        if zones[i] == prev_zone
    )

    # Last extremum inside previous zone spell.
    prev_zone_rsis = [history[i][1] for i in range(transition_idx + 1) if zones[i] == prev_zone]
    if prev_zone == LOW and prev_zone_rsis:
        extremum = min(prev_zone_rsis)
    elif prev_zone == HIGH and prev_zone_rsis:
        extremum = max(prev_zone_rsis)
    else:
        extremum = None

    # Map to final state.
    if latest_zone == LOW:
        # Currently back in LOW after having been elsewhere — failed bottom if
        # the prior excursion exited LOW recently; else fresh LOW_ZONE.
        if prev_zone == LOW:
            state = "FAILED_BOTTOM"
        else:
            state = "LOW_ZONE"
        zone_entered_at = transition_ct if curr_zone == LOW else history[-1][0]
        zone_exited_at = None
    elif latest_zone == HIGH:
        if prev_zone == HIGH:
            state = "FAILED_TOP"
        else:
            state = "HIGH_ZONE"
        zone_entered_at = transition_ct if curr_zone == HIGH else history[-1][0]
        zone_exited_at = None
    else:  # latest_zone == MID
        if prev_zone == LOW:
            if sustain >= sustain_n:
                state = "CONFIRMED_BULL"
            else:
                state = "EXITING_LOW"
            zone_exited_at = transition_ct
        elif prev_zone == HIGH:
            if sustain >= sustain_n:
                state = "CONFIRMED_BEAR"
            else:
                state = "EXITING_HIGH"
            zone_exited_at = transition_ct
        else:
            state = "RANGE"
            zone_exited_at = None
        zone_entered_at = None

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
