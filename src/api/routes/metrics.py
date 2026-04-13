# src/api/routes/metrics.py
# GET /metrics/{ticker} — returns metrics_cache row or 404
# POST /metrics/refresh  — stub, no real API calls yet

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select
import os

from src.schema import MetricsCache, get_session, get_engine

router = APIRouter()


def _get_db():
    engine = get_engine(os.environ.get("DB_PATH", "data/signals.db"))
    with get_session(engine) as session:
        yield session


@router.get("/metrics/{ticker}")
def get_metrics(ticker: str, db: Session = Depends(_get_db)):
    row = db.exec(
        select(MetricsCache).where(MetricsCache.ticker == ticker.upper())
    ).first()

    if not row:
        raise HTTPException(status_code=404, detail=f"No metrics cached for {ticker.upper()}")

    return {
        "ticker":      row.ticker,
        "price":       row.price,
        "volume_24h":  row.volume_24h,
        "mcap":        row.mcap,
        "rank":        row.rank,
        "circ_supply": row.circ_supply,
        "fetched_at":  row.fetched_at.isoformat() if row.fetched_at else None,
    }


@router.post("/metrics/refresh")
def refresh_metrics():
    # Stub — multi-API fallback chain (CoinGecko → Binance → CoinCap) wired in Phase 2
    return {
        "updated": 0,
        "failed":  0,
        "message": "stub — fetcher not yet implemented",
    }