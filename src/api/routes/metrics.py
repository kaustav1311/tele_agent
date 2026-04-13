# src/api/routes/metrics.py
# GET /metrics/{ticker} — returns metrics_cache row or 404
# POST /metrics/refresh — fetches from CoinGecko/CryptoCompare, upserts into metrics_cache

from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select, text
import os
import httpx

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
def refresh_metrics(db: Session = Depends(_get_db)):
    """
    Fetch live metrics from CoinGecko/CryptoCompare for all tickers with signals in last 24h.
    Upsert into metrics_cache and return update count.
    """
    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc - timedelta(hours=24)

    # Query distinct tickers from signals in last 24h
    query = text("""
        SELECT DISTINCT ticker
        FROM signals
        WHERE timestamp >= :cutoff_utc
        ORDER BY ticker
    """)
    result = db.exec(query, {"cutoff_utc": cutoff_utc}).all()
    tickers = [row[0] for row in result] if result else []

    if not tickers:
        return {
            "updated": 0,
            "failed": 0,
            "fetched_at": now_utc.isoformat(),
        }

    # Try CoinGecko first, fallback to CryptoCompare
    metrics_data = _fetch_from_coingecko(tickers)
    if not metrics_data:
        metrics_data = _fetch_from_cryptocompare(tickers)

    # Upsert into metrics_cache
    updated = 0
    failed = 0

    for ticker, data in metrics_data.items():
        try:
            if data is None:
                failed += 1
                continue

            existing = db.exec(
                select(MetricsCache).where(MetricsCache.ticker == ticker)
            ).first()

            if existing:
                existing.price = data.get("price")
                existing.volume_24h = data.get("volume_24h")
                existing.mcap = data.get("mcap")
                existing.rank = data.get("rank")
                existing.circ_supply = data.get("circ_supply")
                existing.fetched_at = now_utc
                db.add(existing)
            else:
                db.add(MetricsCache(
                    ticker=ticker,
                    price=data.get("price"),
                    volume_24h=data.get("volume_24h"),
                    mcap=data.get("mcap"),
                    rank=data.get("rank"),
                    circ_supply=data.get("circ_supply"),
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


def _fetch_from_coingecko(tickers: list) -> dict:
    """
    Fetch metrics from CoinGecko free API.
    Returns {ticker: {price, volume_24h, mcap, rank, circ_supply}} or empty dict on failure.
    """
    try:
        base_url = os.environ.get(
            "COINGECKO_BASE",
            "https://api.coingecko.com/api/v3"
        )
        # CoinGecko free API doesn't map tickers directly to coin IDs — would need a lookup.
        # For now, attempt a simple fallback approach: use lowercase ticker as ID hint.
        # In production, maintain a ticker → coin_id mapping.
        ids = ",".join([t.lower() for t in tickers])

        url = f"{base_url}/simple/price"
        params = {
            "ids": ids,
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
        }

        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        result = {}
        for ticker in tickers:
            coin_id = ticker.lower()
            if coin_id in data:
                coin_data = data[coin_id]
                result[ticker] = {
                    "price": coin_data.get("usd"),
                    "volume_24h": coin_data.get("usd_24h_vol"),
                    "mcap": coin_data.get("usd_market_cap"),
                    "rank": None,  # CoinGecko free doesn't include rank in this endpoint
                    "circ_supply": None,
                }
            else:
                result[ticker] = None

        return result
    except Exception:
        return {}


def _fetch_from_cryptocompare(tickers: list) -> dict:
    """
    Fetch metrics from CryptoCompare free API (fallback).
    Returns {ticker: {price, volume_24h, mcap, rank, circ_supply}} or empty dict on failure.
    """
    try:
        api_key = os.environ.get("VITE_CRYPTOCOMPARE_API_KEY", "")
        fsyms = ",".join(tickers)

        url = "https://min-api.cryptocompare.com/data/pricemultifull"
        params = {
            "fsyms": fsyms,
            "tsyms": "USD",
        }
        if api_key:
            params["api_key"] = api_key

        response = httpx.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        result = {}
        for ticker in tickers:
            if ticker in data.get("RAW", {}):
                coin_data = data["RAW"][ticker].get("USD", {})
                result[ticker] = {
                    "price": coin_data.get("PRICE"),
                    "volume_24h": coin_data.get("VOLUME24HOURTO"),
                    "mcap": coin_data.get("MKTCAP"),
                    "rank": None,
                    "circ_supply": None,
                }
            else:
                result[ticker] = None

        return result
    except Exception:
        return {}