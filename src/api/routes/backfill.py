# src/api/routes/backfill.py
# POST /api/backfill/eod — manual EOD backfill trigger (protected)
# Allows notebook or curl to trigger without restarting listener.

import re
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query, Request

from src.backfill_eod import run_eod_backfill

router = APIRouter()

_ET_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _default_prev_et_day() -> str:
    et_now = datetime.now(ZoneInfo("America/New_York"))
    return (et_now - timedelta(days=1)).strftime("%Y-%m-%d")


@router.post("/backfill/eod")
def trigger_eod_backfill(
    request: Request,
    et_day: str = Query(default=None, description="YYYY-MM-DD in ET. Defaults to yesterday ET."),
):
    """
    Manually trigger EOD price backfill for a given ET day.
    Protected by AUTH_ENABLED / SIGNAL_API_KEY middleware (already in main.py).

    - et_day must be in the past (not today or future)
    - et_day format: YYYY-MM-DD
    """
    # Resolve default
    if et_day is None:
        et_day = _default_prev_et_day()

    # Validate format
    if not _ET_DAY_RE.match(et_day):
        raise HTTPException(status_code=400, detail="et_day must be YYYY-MM-DD format")

    # Validate not today or future
    today_et = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    if et_day >= today_et:
        raise HTTPException(
            status_code=400,
            detail=f"et_day must be before today ({today_et}). Backfill is for completed days only."
        )

    try:
        result = run_eod_backfill(et_day)
        return {"status": "ok", **result}
    except Exception as e:
        # Do not expose raw exception detail — sanitise
        raise HTTPException(status_code=500, detail="Backfill job failed. Check server logs.")
