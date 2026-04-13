# src/api/routes/health.py
# GET /health — liveness + signal count
# GET /filters — dynamic filter lists from DB

from fastapi import APIRouter, Depends
from sqlmodel import Session, select, func, text
import os

from src.schema import Signal, get_session, get_engine

router = APIRouter()


def _get_db():
    engine = get_engine(os.environ.get("DB_PATH", "data/signals.db"))
    with get_session(engine) as session:
        yield session


@router.get("/health")
def health(db: Session = Depends(_get_db)):
    count = db.exec(select(func.count()).select_from(Signal)).one()

    last_signal = db.exec(
        select(Signal.timestamp).order_by(Signal.timestamp.desc()).limit(1)
    ).first()

    return {
        "status":            "ok",
        "signals_count":     count,
        "last_signal_at_utc": last_signal.isoformat() if last_signal else None,
    }


@router.get("/filters")
def filters(db: Session = Depends(_get_db)):
    activity_types = db.exec(
        select(Signal.activity_type).distinct()
    ).all()

    activity_raw = db.exec(
        select(Signal.activity_raw).distinct()
    ).all()

    alerts_tiers = db.exec(
        select(Signal.alerts_tier).distinct()
    ).all()

    return {
        "activity_types": sorted([r for r in activity_types if r]),
        "activity_raw":   sorted([r for r in activity_raw if r]),
        "alerts_tiers":   sorted([r for r in alerts_tiers if r]),
    }