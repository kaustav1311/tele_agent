# src/portflow/router.py
# FastAPI router for Portflow watchlist CRUD + TA read endpoints.
# Raw sqlite3 throughout — no SQLAlchemy / SQLModel.

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from src.portflow.badges import compute_badges_for_ticker
from src.portflow.db import get_portflow_conn
from src.portflow.state_engine import refresh_all_states, refresh_states_for_ticker
from src.portflow.ta_engine import TIMEFRAMES, bootstrap_ticker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/portflow", tags=["portflow"])


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class WatchlistCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=50)


class TickerCreate(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=20)
    coingecko_id: str = Field(..., min_length=1, max_length=100)
    symbol: str = Field(..., min_length=1, max_length=20)
    name: str = Field(..., min_length=1, max_length=100)
    image: Optional[str] = Field(default=None, max_length=500)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TA_COLUMNS = (
    "rsi", "rsi_prev1", "rsi_prev2", "rsi_direction",
    "atr", "atr_pct", "ema_stack", "vol_ratio", "computed_at",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def _ta_map_for_ticker(conn: sqlite3.Connection, ticker: str) -> dict:
    """Return {'15m': {...} | None, '1h': ..., '4h': ..., '1d': ...}."""
    rows = conn.execute(
        """
        SELECT timeframe, rsi, rsi_prev1, rsi_prev2, rsi_direction,
               atr, atr_pct, ema_stack, vol_ratio, computed_at
        FROM watchlist_ta_cache
        WHERE ticker = ?
        """,
        (ticker,),
    ).fetchall()
    by_tf = {r["timeframe"]: {col: r[col] for col in _TA_COLUMNS} for r in rows}
    return {tf: by_tf.get(tf) for tf in TIMEFRAMES}


def _ticker_row_to_dict(row: sqlite3.Row, ta: dict, badges: Optional[dict] = None) -> dict:
    return {
        "id": row["id"],
        "watchlist_id": row["watchlist_id"],
        "ticker": row["ticker"],
        "coingecko_id": row["coingecko_id"],
        "symbol": row["symbol"],
        "name": row["name"],
        "image": row["image"],
        "added_at": row["added_at"],
        "ta": ta,
        "badges": badges,
    }


def _watchlist_exists(conn: sqlite3.Connection, watchlist_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM watchlists WHERE id = ?", (watchlist_id,)
    ).fetchone()
    return row is not None


def _ticker_used_elsewhere(conn: sqlite3.Connection, ticker: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM watchlist_tickers WHERE ticker = ? LIMIT 1", (ticker,)
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Watchlist CRUD
# ---------------------------------------------------------------------------

@router.get("/watchlists")
def list_watchlists():
    conn = get_portflow_conn()
    try:
        rows = conn.execute(
            "SELECT id, name, created_at FROM watchlists ORDER BY created_at ASC"
        ).fetchall()
        result = []
        for r in rows:
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM watchlist_tickers WHERE watchlist_id = ?",
                (r["id"],),
            ).fetchone()["c"]
            result.append({
                "id": r["id"],
                "name": r["name"],
                "created_at": r["created_at"],
                "ticker_count": count,
            })
        return result
    finally:
        conn.close()


@router.post("/watchlists", status_code=status.HTTP_201_CREATED)
def create_watchlist(body: WatchlistCreate):
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="name must not be empty")

    created_at = _now_iso()
    conn = get_portflow_conn()
    try:
        existing = conn.execute(
            "SELECT 1 FROM watchlists WHERE name = ?", (name,)
        ).fetchone()
        if existing:
            raise HTTPException(status_code=409, detail="watchlist name already exists")
        try:
            cur = conn.execute(
                "INSERT INTO watchlists (name, created_at) VALUES (?, ?)",
                (name, created_at),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="watchlist name already exists")
        return {"id": cur.lastrowid, "name": name, "created_at": created_at}
    finally:
        conn.close()


@router.delete("/watchlists/{watchlist_id}")
def delete_watchlist(watchlist_id: int):
    conn = get_portflow_conn()
    try:
        if not _watchlist_exists(conn, watchlist_id):
            raise HTTPException(status_code=404, detail="watchlist not found")

        tickers_in_list = [
            r["ticker"] for r in conn.execute(
                "SELECT ticker FROM watchlist_tickers WHERE watchlist_id = ?",
                (watchlist_id,),
            ).fetchall()
        ]

        try:
            conn.execute("BEGIN")
            conn.execute(
                "DELETE FROM watchlist_tickers WHERE watchlist_id = ?", (watchlist_id,)
            )
            conn.execute("DELETE FROM watchlists WHERE id = ?", (watchlist_id,))
            for t in tickers_in_list:
                if not _ticker_used_elsewhere(conn, t):
                    conn.execute(
                        "DELETE FROM watchlist_ta_cache WHERE ticker = ?", (t,)
                    )
                    conn.execute(
                        "DELETE FROM watchlist_rsi_history WHERE ticker = ?", (t,)
                    )
                    conn.execute(
                        "DELETE FROM watchlist_rsi_state WHERE ticker = ?", (t,)
                    )
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            logger.exception("delete_watchlist failed: %s", e)
            raise HTTPException(status_code=500, detail="failed to delete watchlist")

        return {"deleted": True}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Ticker CRUD within a watchlist
# ---------------------------------------------------------------------------

@router.get("/watchlists/{watchlist_id}/tickers")
def list_tickers(watchlist_id: int):
    conn = get_portflow_conn()
    try:
        if not _watchlist_exists(conn, watchlist_id):
            raise HTTPException(status_code=404, detail="watchlist not found")

        rows = conn.execute(
            """
            SELECT id, watchlist_id, ticker, coingecko_id, symbol, name, image, added_at
            FROM watchlist_tickers
            WHERE watchlist_id = ?
            ORDER BY added_at ASC
            """,
            (watchlist_id,),
        ).fetchall()

        return [
            _ticker_row_to_dict(
                r,
                _ta_map_for_ticker(conn, r["ticker"]),
                compute_badges_for_ticker(r["ticker"]),
            )
            for r in rows
        ]
    finally:
        conn.close()


@router.post("/watchlists/{watchlist_id}/tickers", status_code=status.HTTP_201_CREATED)
def add_ticker(watchlist_id: int, body: TickerCreate):
    ticker = body.ticker.strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker must not be empty")

    added_at = _now_iso()
    conn = get_portflow_conn()
    try:
        if not _watchlist_exists(conn, watchlist_id):
            raise HTTPException(status_code=404, detail="watchlist not found")

        try:
            cur = conn.execute(
                """
                INSERT INTO watchlist_tickers
                    (watchlist_id, ticker, coingecko_id, symbol, name, image, added_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    watchlist_id, ticker, body.coingecko_id, body.symbol,
                    body.name, body.image, added_at,
                ),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="ticker already in this watchlist")

        new_id = cur.lastrowid
    finally:
        conn.close()

    try:
        bootstrap_ticker(ticker)
    except Exception as e:
        logger.exception("bootstrap_ticker failed for %s: %s", ticker, e)
        # Ticker row persists; TA cache will be refilled by the scheduler.

    conn = get_portflow_conn()
    try:
        row = conn.execute(
            """
            SELECT id, watchlist_id, ticker, coingecko_id, symbol, name, image, added_at
            FROM watchlist_tickers WHERE id = ?
            """,
            (new_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="failed to read inserted ticker")
        try:
            refresh_states_for_ticker(ticker)
        except Exception as e:
            logger.exception("refresh_states_for_ticker failed for %s: %s", ticker, e)
        return _ticker_row_to_dict(
            row,
            _ta_map_for_ticker(conn, ticker),
            compute_badges_for_ticker(ticker),
        )
    finally:
        conn.close()


@router.delete("/watchlists/{watchlist_id}/tickers/{ticker}")
def delete_ticker(watchlist_id: int, ticker: str):
    ticker = ticker.strip().upper()
    conn = get_portflow_conn()
    try:
        existing = conn.execute(
            "SELECT 1 FROM watchlist_tickers WHERE watchlist_id = ? AND ticker = ?",
            (watchlist_id, ticker),
        ).fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="ticker not found in this watchlist")

        try:
            conn.execute("BEGIN")
            conn.execute(
                "DELETE FROM watchlist_tickers WHERE watchlist_id = ? AND ticker = ?",
                (watchlist_id, ticker),
            )
            if not _ticker_used_elsewhere(conn, ticker):
                conn.execute(
                    "DELETE FROM watchlist_ta_cache WHERE ticker = ?", (ticker,)
                )
                conn.execute(
                    "DELETE FROM watchlist_rsi_history WHERE ticker = ?", (ticker,)
                )
                conn.execute(
                    "DELETE FROM watchlist_rsi_state WHERE ticker = ?", (ticker,)
                )
            conn.commit()
        except sqlite3.Error as e:
            conn.rollback()
            logger.exception("delete_ticker failed: %s", e)
            raise HTTPException(status_code=500, detail="failed to delete ticker")

        return {"deleted": True}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# TA status
# ---------------------------------------------------------------------------

@router.post("/ta/refresh-states")
def ta_refresh_states():
    return refresh_all_states()


@router.get("/ta/status")
def ta_status():
    conn = get_portflow_conn()
    try:
        row = conn.execute(
            """
            SELECT MIN(computed_at) AS oldest,
                   MAX(computed_at) AS newest,
                   COUNT(DISTINCT ticker) AS ticker_count,
                   COUNT(*) AS row_count
            FROM watchlist_ta_cache
            """
        ).fetchone()
        return {
            "oldest_computed_at": row["oldest"],
            "newest_computed_at": row["newest"],
            "ticker_count": row["ticker_count"] or 0,
            "row_count": row["row_count"] or 0,
        }
    finally:
        conn.close()
