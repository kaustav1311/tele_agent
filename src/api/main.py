# src/api/main.py
# FastAPI app — includes all routers, CORS, auth middleware, WAL mode on startup.

import logging
import os
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlmodel import text

load_dotenv()

from src.schema import get_engine
from src.api.routes import signals, health, metrics, analytics, backfill


# ---------------------------------------------------------------------------
# WAL mode on startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.environ.get("AUTH_ENABLED", "true").lower() != "false" \
            and not os.environ.get("SIGNAL_API_KEY", ""):
        raise RuntimeError("SIGNAL_API_KEY must be set when AUTH_ENABLED=true")

    db_path = os.environ.get("DB_PATH", "data/signals.db")
    engine  = get_engine(db_path)
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL"))
        result = conn.execute(text("PRAGMA journal_mode")).scalar()
        if result != "wal":
            logger.warning("WAL mode not active — got: %s", result)
        else:
            logger.info("WAL mode confirmed")
        conn.commit()
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Signal Agent API", version="0.1.0", lifespan=lifespan)

# CORS — allow all in Phase 1 local; tighten to VPS origin in Phase 2
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Auth middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    # Skip auth if AUTH_ENABLED=false (local dev default)
    if os.environ.get("AUTH_ENABLED", "true").lower() == "false":
        return await call_next(request)

    # Always allow health check unauthenticated
    if request.url.path == "/health":
        return await call_next(request)

    token = request.headers.get("Authorization", "")
    expected = f"Bearer {os.environ.get('SIGNAL_API_KEY', '')}"

    if token != expected:
        return JSONResponse(status_code=401, content={"detail": "Unauthorized"})

    return await call_next(request)


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(signals.router, prefix="/api")
app.include_router(health.router)
app.include_router(metrics.router, prefix="/api")
app.include_router(analytics.router, prefix="/api")
app.include_router(backfill.router, prefix="/api")