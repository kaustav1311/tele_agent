# src/portflow/db.py
# Raw sqlite3 connection + schema init for the Portflow watchlist DB.
# Intentionally separate from data/signals.db.

import sqlite3
from pathlib import Path

PORTFLOW_DB_PATH = Path(__file__).parent.parent.parent / "data" / "portflow.db"


def get_portflow_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(PORTFLOW_DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def init_portflow_db() -> None:
    conn = get_portflow_conn()
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlists (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL,
                created_at  TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_tickers (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                watchlist_id  INTEGER NOT NULL REFERENCES watchlists(id),
                ticker        TEXT NOT NULL,
                coingecko_id  TEXT NOT NULL,
                symbol        TEXT NOT NULL,
                name          TEXT NOT NULL,
                image         TEXT,
                added_at      TEXT NOT NULL,
                UNIQUE(watchlist_id, ticker)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_rsi_history (
                ticker              TEXT NOT NULL,
                timeframe           TEXT NOT NULL,
                candle_close_time   TEXT NOT NULL,
                rsi_value           REAL NOT NULL,
                PRIMARY KEY (ticker, timeframe, candle_close_time)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_rsi_state (
                ticker                  TEXT NOT NULL,
                timeframe               TEXT NOT NULL,
                state                   TEXT NOT NULL,
                zone_entered_at         TEXT,
                zone_exited_at          TEXT,
                sustain_candles_count   INTEGER DEFAULT 0,
                failed_attempts_count   INTEGER DEFAULT 0,
                last_zone_extremum      REAL,
                updated_at              TEXT NOT NULL,
                PRIMARY KEY (ticker, timeframe)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_ta_cache (
                ticker          TEXT NOT NULL,
                timeframe       TEXT NOT NULL,
                rsi             REAL,
                rsi_prev1       REAL,
                rsi_prev2       REAL,
                rsi_direction   TEXT,
                atr             REAL,
                atr_pct         REAL,
                ema_stack       TEXT,
                vol_ratio       REAL,
                computed_at     TEXT NOT NULL,
                PRIMARY KEY (ticker, timeframe)
            )
        """)

        conn.commit()
    finally:
        conn.close()
