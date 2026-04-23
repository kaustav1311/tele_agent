# src/portflow/badges.py
# Pure badge computer. Consumes two rows from watchlist_rsi_state (father, son)
# and returns {badge_name, icon, color}. No DB writes — computed fresh on read.

from typing import Optional

from src.portflow.db import get_portflow_conn


NEUTRAL_BADGE = {"badge_name": "NEUTRAL", "icon": "Minus", "color": "gray-500"}


def _badge(name: str, icon: str, color: str) -> dict:
    return {"badge_name": name, "icon": icon, "color": color}


def compute_pair_badge(father: Optional[dict], son: Optional[dict]) -> dict:
    if not father or not son:
        return NEUTRAL_BADGE

    f = father.get("state")
    s = son.get("state")
    f_failed = int(father.get("failed_attempts_count") or 0)

    # 1. BULL_ALIGNED
    if f == "CONFIRMED_BULL" and s == "CONFIRMED_BULL":
        return _badge("BULL_ALIGNED", "TrendingUp", "emerald-500")
    # 2. BEAR_ALIGNED
    if f == "CONFIRMED_BEAR" and s == "CONFIRMED_BEAR":
        return _badge("BEAR_ALIGNED", "TrendingDown", "red-500")
    # 3. EARLY_BULL
    if f == "EXITING_LOW" and s == "CONFIRMED_BULL":
        return _badge("EARLY_BULL", "ArrowUpRight", "emerald-400")
    # 4. EARLY_BEAR
    if f == "EXITING_HIGH" and s == "CONFIRMED_BEAR":
        return _badge("EARLY_BEAR", "ArrowDownRight", "red-400")
    # 5. BULL_FORMING
    if f == "LOW_ZONE" and s == "EXITING_LOW":
        return _badge("BULL_FORMING", "Sunrise", "amber-400")
    # 6. BEAR_FORMING
    if f == "HIGH_ZONE" and s == "EXITING_HIGH":
        return _badge("BEAR_FORMING", "Sunset", "amber-500")
    # 7. STRUCTURAL_BEAR — stuck in LOW with repeated failed rebounds.
    if f == "LOW_ZONE" and f_failed >= 2:
        return _badge("STRUCTURAL_BEAR", "TrendingDownIcon+AlertTriangle", "red-600")
    # 8. STRUCTURAL_BULL — stuck in HIGH with repeated failed pullbacks.
    if f == "HIGH_ZONE" and f_failed >= 2:
        return _badge("STRUCTURAL_BULL", "TrendingUpIcon+AlertTriangle", "emerald-600")
    # 9. DIVERGENCE — opposite CONFIRMED states across the pair.
    if (f == "CONFIRMED_BULL" and s == "CONFIRMED_BEAR") or (
        f == "CONFIRMED_BEAR" and s == "CONFIRMED_BULL"
    ):
        return _badge("DIVERGENCE", "Split", "purple-400")

    return NEUTRAL_BADGE


def _state_rows_for_ticker(ticker: str) -> dict:
    conn = get_portflow_conn()
    try:
        rows = conn.execute(
            """
            SELECT timeframe, state, zone_entered_at, zone_exited_at,
                   sustain_candles_count, failed_attempts_count,
                   last_zone_extremum, updated_at
            FROM watchlist_rsi_state
            WHERE ticker = ?
            """,
            (ticker,),
        ).fetchall()
    finally:
        conn.close()
    return {r["timeframe"]: dict(r) for r in rows}


def compute_macro_badge(ticker: str, states: Optional[dict] = None) -> dict:
    s = states if states is not None else _state_rows_for_ticker(ticker)
    return compute_pair_badge(s.get("1w"), s.get("1d"))


def compute_tactical_badge(ticker: str, states: Optional[dict] = None) -> dict:
    s = states if states is not None else _state_rows_for_ticker(ticker)
    return compute_pair_badge(s.get("1d"), s.get("1h"))


def compute_badges_for_ticker(ticker: str) -> dict:
    states = _state_rows_for_ticker(ticker)
    return {
        "macro": compute_macro_badge(ticker, states),
        "tactical": compute_tactical_badge(ticker, states),
    }
