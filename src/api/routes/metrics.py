# src/api/routes/metrics.py
# GET /metrics/{ticker} — returns MCap + tier from metrics_cache
# POST /metrics/refresh — fetches MCap from CryptoCompare, upserts metrics_cache

from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select, text
import os
import httpx

from src.schema import MetricsCache, get_session, get_engine

router = APIRouter()


def _get_db():
    engine = get_engine(os.environ.get("DB_PATH", "data/signals.db"))
    with get_session(engine) as session:
        yield session


def _compute_mcap_tier(mcap: float | None) -> str:
    """
    Classify MCap into tiers.
    - micro: < $50M
    - small: $50M - $500M
    - mid: $500M - $5B
    - large: >= $5B
    - unknown: null MCap
    """
    if mcap is None:
        return "unknown"
    if mcap < 50_000_000:
        return "micro"
    elif mcap < 500_000_000:
        return "small"
    elif mcap < 5_000_000_000:
        return "mid"
    else:
        return "large"


@router.get("/metrics/{ticker}")
def get_metrics(ticker: str, db: Session = Depends(_get_db)):
    """
    Get MCap + tier for a single ticker.
    Returns {ticker, mcap, mcap_tier, fetched_at} or 404.
    """
    row = db.exec(
        select(MetricsCache).where(MetricsCache.ticker == ticker.upper())
    ).first()

    if not row:
        raise HTTPException(status_code=404, detail=f"No metrics cached for {ticker.upper()}")

    return {
        "ticker":      row.ticker,
        "mcap":        row.mcap,
        "mcap_tier":   row.mcap_tier,
        "fetched_at":  row.fetched_at.isoformat() if row.fetched_at else None,
    }


@router.post("/metrics/refresh")
def refresh_metrics(
    all_tickers: bool = Query(False),
    db: Session = Depends(_get_db)
):
    """
    Fetch MCap for all distinct tickers and upsert into metrics_cache.

    Query parameters:
    - all_tickers: true → all distinct tickers ever (for backfill)
                   false (default) → last 24h only (live use)

    After upsert, MCap + tier are available for daily_calls enrichment.
    """
    now_utc = datetime.now(timezone.utc)

    # Query distinct tickers
    if all_tickers:
        query = text("SELECT DISTINCT ticker FROM signals ORDER BY ticker")
        result = db.exec(query).all()
    else:
        cutoff_utc = now_utc - timedelta(hours=24)
        query = text("""
            SELECT DISTINCT ticker FROM signals
            WHERE timestamp >= :cutoff_utc
            ORDER BY ticker
        """)
        result = db.exec(query.bindparams(cutoff_utc=cutoff_utc)).all()

    tickers = [row[0] for row in result] if result else []

    if not tickers:
        return {
            "updated": 0,
            "failed": 0,
            "fetched_at": now_utc.isoformat(),
        }

    # Fetch MCap from CryptoCompare only
    mcap_data = _fetch_mcap_from_cryptocompare(tickers)

    # Upsert into metrics_cache with computed mcap_tier
    updated = 0
    failed = 0

    for ticker, data in mcap_data.items():
        try:
            if data is None:
                failed += 1
                continue

            mcap = data.get("mcap")
            mcap_tier = _compute_mcap_tier(mcap)

            existing = db.exec(
                select(MetricsCache).where(MetricsCache.ticker == ticker)
            ).first()

            if existing:
                existing.price = data.get("price")
                existing.volume_24h = data.get("volume_24h")
                existing.mcap = mcap
                existing.rank = data.get("rank")
                existing.circ_supply = data.get("circ_supply")
                existing.mcap_tier = mcap_tier
                existing.fetched_at = now_utc
                db.add(existing)
            else:
                db.add(MetricsCache(
                    ticker=ticker,
                    price=data.get("price"),
                    volume_24h=data.get("volume_24h"),
                    mcap=mcap,
                    rank=data.get("rank"),
                    circ_supply=data.get("circ_supply"),
                    mcap_tier=mcap_tier,
                    fetched_at=now_utc,
                ))
            updated += 1
        except Exception:
            failed += 1

    db.commit()

    return {
        "updated": updated,
        "failed": failed,
        "fetched_at": now_utc.isoformat(),
    }


def _fetch_mcap_from_cryptocompare(tickers: list) -> dict:
    """
    Fetch market data for tickers from CryptoCompare pricemultifull endpoint.
    Returns {ticker: {price, volume_24h, mcap, rank, circ_supply}} or {ticker: None} on failure.
    """
    try:
        # SECURITY: NEVER prefix with VITE_ — that would expose the key in the Vite bundle at build time.
        api_key = os.environ.get("CRYPTOCOMPARE_API_KEY", "")
        url = "https://min-api.cryptocompare.com/data/pricemultifull"
        params = {
            "fsyms": ",".join(tickers),
            "tsyms": "USD",
        }
        if api_key:
            params["api_key"] = api_key

        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        result = {}
        raw = data.get("RAW", {})
        for ticker in tickers:
            coin_data = raw.get(ticker, {}).get("USD", {})
            result[ticker] = {
                "price": coin_data.get("PRICE"),
                "volume_24h": coin_data.get("VOLUME24HOUR"),
                "mcap": coin_data.get("MKTCAP"),
                "rank": coin_data.get("RANK"),
                "circ_supply": coin_data.get("SUPPLY"),
            }

        return result
    except Exception:
        return {ticker: None for ticker in tickers}
