# src/portflow/badges.py
# Pure badge computer. Consumes two rows from watchlist_rsi_state (father, son)
# and returns badge dict with full metadata. No DB writes — computed fresh on read.
#
# RULE PRIORITY ORDER (first match wins):
#   1. STRUCTURAL (failed_attempts >= 2 — overrides everything)
#   2. ALIGNED (both confirmed same direction, or father confirmed + son resting)
#   3. DIVERGENCE (confirmed opposite directions)
#   4. EARLY (one side transitioning, other confirmed or leading)
#   5. FORMING (early zone exits, tentative signals)
#   6. NEUTRAL (no actionable signal)

from typing import Optional

from src.portflow.db import get_portflow_conn


def _badge(name: str, icon: str, color: str,
           father: Optional[dict] = None, son: Optional[dict] = None) -> dict:
    """Build badge response with full metadata for frontend tooltip."""
    return {
        "badge_name": name,
        "icon": icon,
        "color": color,
        "father_state": father.get("state") if father else None,
        "son_state": son.get("state") if son else None,
        "failed_attempts": int(father.get("failed_attempts_count") or 0) if father else 0,
        "sustain_candles": int(father.get("sustain_candles_count") or 0) if father else 0,
    }


def compute_pair_badge(father: Optional[dict], son: Optional[dict]) -> dict:
    if not father or not son:
        return _badge("NEUTRAL", "Minus", "gray-500")

    f = father.get("state")
    s = son.get("state")
    f_failed = int(father.get("failed_attempts_count") or 0)

    # TIER 1: STRUCTURAL
    if f in ("LOW_ZONE", "FAILED_BOTTOM") and f_failed >= 2:
        return _badge("STRUCTURAL_BEAR", "TrendingDown+AlertTriangle", "red-600", father, son)
    if f in ("HIGH_ZONE", "FAILED_TOP") and f_failed >= 2:
        return _badge("STRUCTURAL_BULL", "TrendingUp+AlertTriangle", "emerald-600", father, son)

    # TIER 2: ALIGNED
    if f == "CONFIRMED_BULL":
        if s in ("CONFIRMED_BULL", "EXITING_LOW", "RANGE", "HIGH_ZONE", "FAILED_TOP"):
            return _badge("BULL_ALIGNED", "TrendingUp", "emerald-500", father, son)
        if s in ("CONFIRMED_BEAR", "EXITING_HIGH", "LOW_ZONE", "FAILED_BOTTOM"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)

    if f == "CONFIRMED_BEAR":
        if s in ("CONFIRMED_BEAR", "EXITING_HIGH", "RANGE", "LOW_ZONE", "FAILED_BOTTOM"):
            return _badge("BEAR_ALIGNED", "TrendingDown", "red-500", father, son)
        if s in ("CONFIRMED_BULL", "EXITING_LOW", "HIGH_ZONE", "FAILED_TOP"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)

    if f == "LOW_ZONE":
        if s in ("LOW_ZONE", "CONFIRMED_BEAR", "FAILED_BOTTOM"):
            return _badge("BEAR_ALIGNED", "TrendingDown", "red-500", father, son)
        if s in ("EXITING_LOW", "CONFIRMED_BULL"):
            return _badge("BULL_FORMING", "Sunrise", "amber-400", father, son)
        if s in ("EXITING_HIGH", "HIGH_ZONE"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)
        if s in ("RANGE", "FAILED_TOP"):
            return _badge("NEUTRAL", "Minus", "gray-500", father, son)

    if f == "HIGH_ZONE":
        if s in ("HIGH_ZONE", "CONFIRMED_BULL", "FAILED_TOP"):
            return _badge("BULL_ALIGNED", "TrendingUp", "emerald-500", father, son)
        if s in ("EXITING_HIGH", "CONFIRMED_BEAR"):
            return _badge("BEAR_FORMING", "Sunset", "amber-500", father, son)
        if s in ("EXITING_LOW", "LOW_ZONE"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)
        if s in ("RANGE", "FAILED_BOTTOM"):
            return _badge("NEUTRAL", "Minus", "gray-500", father, son)

    # TIER 3: EARLY
    if f == "EXITING_LOW":
        if s in ("CONFIRMED_BULL", "EXITING_LOW", "RANGE", "HIGH_ZONE", "FAILED_TOP"):
            return _badge("EARLY_BULL", "ArrowUpRight", "emerald-400", father, son)
        if s in ("LOW_ZONE", "FAILED_BOTTOM"):
            return _badge("BULL_FORMING", "Sunrise", "amber-400", father, son)
        if s in ("CONFIRMED_BEAR", "EXITING_HIGH"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)

    if f == "EXITING_HIGH":
        if s in ("CONFIRMED_BEAR", "EXITING_HIGH", "RANGE", "LOW_ZONE", "FAILED_BOTTOM"):
            return _badge("EARLY_BEAR", "ArrowDownRight", "red-400", father, son)
        if s in ("HIGH_ZONE", "FAILED_TOP"):
            return _badge("BEAR_FORMING", "Sunset", "amber-500", father, son)
        if s in ("CONFIRMED_BULL", "EXITING_LOW"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)

    if f == "RANGE":
        if s == "CONFIRMED_BULL":
            return _badge("EARLY_BULL", "ArrowUpRight", "emerald-400", father, son)
        if s == "CONFIRMED_BEAR":
            return _badge("EARLY_BEAR", "ArrowDownRight", "red-400", father, son)
        if s == "EXITING_LOW":
            return _badge("BULL_FORMING", "Sunrise", "amber-400", father, son)
        if s == "EXITING_HIGH":
            return _badge("BEAR_FORMING", "Sunset", "amber-500", father, son)
        if s == "HIGH_ZONE":
            return _badge("EARLY_BULL", "ArrowUpRight", "emerald-400", father, son)
        if s == "LOW_ZONE":
            return _badge("EARLY_BEAR", "ArrowDownRight", "red-400", father, son)
        if s == "FAILED_TOP":
            return _badge("EARLY_BULL", "ArrowUpRight", "emerald-400", father, son)
        if s == "FAILED_BOTTOM":
            return _badge("EARLY_BEAR", "ArrowDownRight", "red-400", father, son)
        if s == "RANGE":
            return _badge("NEUTRAL", "Minus", "gray-500", father, son)

    if f == "FAILED_BOTTOM":
        if s in ("CONFIRMED_BEAR", "LOW_ZONE", "FAILED_BOTTOM"):
            return _badge("BEAR_ALIGNED", "TrendingDown", "red-500", father, son)
        if s in ("EXITING_LOW", "CONFIRMED_BULL"):
            return _badge("BULL_FORMING", "Sunrise", "amber-400", father, son)
        if s in ("EXITING_HIGH", "HIGH_ZONE"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)
        if s in ("RANGE", "FAILED_TOP"):
            return _badge("NEUTRAL", "Minus", "gray-500", father, son)

    if f == "FAILED_TOP":
        if s in ("CONFIRMED_BULL", "HIGH_ZONE", "FAILED_TOP"):
            return _badge("BULL_ALIGNED", "TrendingUp", "emerald-500", father, son)
        if s in ("EXITING_HIGH", "CONFIRMED_BEAR"):
            return _badge("BEAR_FORMING", "Sunset", "amber-500", father, son)
        if s in ("EXITING_LOW", "LOW_ZONE"):
            return _badge("DIVERGENCE", "Split", "purple-400", father, son)
        if s in ("RANGE", "FAILED_BOTTOM"):
            return _badge("NEUTRAL", "Minus", "gray-500", father, son)

    return _badge("NEUTRAL", "Minus", "gray-500", father, son)


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
