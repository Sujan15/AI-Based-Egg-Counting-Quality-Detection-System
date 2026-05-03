# api/shed_routes.py

# ============================================================
#  EggTrackAI  –  Shed Management API Routes
#  ─────────────────────────────────────────────────────────
#    POST /api/shed/switch        – switch active shed on a line
#    GET  /api/shed/active        – list currently active sheds
#    GET  /api/live-status        – live DB view (eggs this session)
#    GET  /api/report/daily       – daily summary query
#    GET  /api/report/hourly      – hourly summary query
# ============================================================

from __future__ import annotations
import logging
from datetime import date
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import services.db_writer as db_svc

logger = logging.getLogger("EggTrackAI.api")

shed_router = APIRouter(prefix="/api", tags=["Shed Management"])


# ── Pydantic Schemas ───────────────────────────────────────

class ShedSwitchRequest(BaseModel):
    line_id:       int
    new_shed_id:   str              # e.g. "SHD001"
    activated_by:  str = "operator"
    notes:         Optional[str] = None


class ShedSwitchResponse(BaseModel):
    success: bool
    message: str


# ── Endpoints ─────────────────────────────────────────────

@shed_router.post("/shed/switch", response_model=ShedSwitchResponse)
async def switch_shed(payload: ShedSwitchRequest):
    """
    Atomically switch the active shed on a conveyor line.
    Closes the current session and opens a new one in Oracle.
    No pipeline restart required.
    """
    writer = db_svc.get_writer()
    if writer is None:
        raise HTTPException(503, "DB writer not initialized")

    ok = writer.switch_shed(
        line_id=payload.line_id,
        new_shed_id=payload.new_shed_id,
        activated_by=payload.activated_by,
        notes=payload.notes or "",
    )
    if not ok:
        raise HTTPException(500, "Shed switch failed — check server logs")

    return ShedSwitchResponse(
        success=True,
        message=(
            f"Line {payload.line_id} switched to shed "
            f"{payload.new_shed_id} by {payload.activated_by}"
        ),
    )


@shed_router.get("/live-status")
async def live_status():
    """
    Returns current active shed and live egg count for every line.
    Sourced from Oracle view V_LIVE_LINE_STATUS.
    """
    writer = db_svc.get_writer()
    if writer is None:
        raise HTTPException(503, "DB writer not initialized")
    return {"lines": writer.get_live_status()}


@shed_router.get("/report/daily")
async def daily_report(
    report_date: Optional[date] = Query(
        default=None, description="YYYY-MM-DD (defaults to today)"
    ),
    shed_id: Optional[str] = Query(default=None),
    line_id: Optional[int] = Query(default=None),
):
    """
    Management daily production report.
    Queries DAILY_PRODUCTION_SUMMARY.
    """
    writer = db_svc.get_writer()
    if writer is None:
        raise HTTPException(503, "DB writer not initialized")

    target = report_date or date.today()

    try:
        pool = writer._get_pool()
        sql  = """
            SELECT
                TO_CHAR(SUMMARY_DATE, 'YYYY-MM-DD') AS SUMMARY_DATE,
                SHED_ID, LINE_ID, TOTAL_EGGS,
                SMALL_COUNT, STANDARD_COUNT, BIG_COUNT,
                CRACKED_COUNT, GOOD_COUNT, CRACK_RATE_PCT,
                SHIFT1_TOTAL, SHIFT2_TOTAL, SHIFT3_TOTAL
            FROM APPS.DAILY_PRODUCTION_SUMMARY
            WHERE SUMMARY_DATE = TO_DATE(:dt, 'YYYY-MM-DD')
              AND (:shed IS NULL OR SHED_ID  = :shed)
              AND (:line IS NULL OR LINE_ID  = :line)
            ORDER BY SHED_ID, LINE_ID
        """
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {
                "dt":   target.isoformat(),
                "shed": shed_id,
                "line": line_id,
            })
            cols = [d[0].lower() for d in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        return {"date": target.isoformat(), "records": rows, "count": len(rows)}

    except Exception as exc:
        logger.error("daily_report query failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Report query failed")


@shed_router.get("/report/hourly")
async def hourly_report(
    shed_id: Optional[str] = Query(default=None),
    line_id: Optional[int] = Query(default=None),
    hours:   int           = Query(default=24, ge=1, le=168),
):
    """
    Last N hours of hourly production summary.
    Queries HOURLY_PRODUCTION_SUMMARY.
    """
    writer = db_svc.get_writer()
    if writer is None:
        raise HTTPException(503, "DB writer not initialized")

    try:
        pool = writer._get_pool()
        sql  = """
            SELECT
                TO_CHAR(HOUR_SLOT, 'YYYY-MM-DD HH24:MI') AS HOUR_SLOT,
                SHED_ID, LINE_ID, SHIFT_ID, TOTAL_EGGS,
                SMALL_COUNT, STANDARD_COUNT, BIG_COUNT,
                CRACKED_COUNT, GOOD_COUNT, CRACK_RATE_PCT
            FROM APPS.HOURLY_PRODUCTION_SUMMARY
            WHERE HOUR_SLOT >= SYSTIMESTAMP - INTERVAL ':h' HOUR
              AND (:shed IS NULL OR SHED_ID = :shed)
              AND (:line IS NULL OR LINE_ID = :line)
            ORDER BY HOUR_SLOT DESC
        """.replace(":h", str(hours))

        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, {"shed": shed_id, "line": line_id})
            cols = [d[0].lower() for d in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        return {"hours_back": hours, "records": rows, "count": len(rows)}

    except Exception as exc:
        logger.error("hourly_report query failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Report query failed")
