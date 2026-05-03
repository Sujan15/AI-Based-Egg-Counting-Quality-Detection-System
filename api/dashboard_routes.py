# api/dashboard_routes.py

from __future__ import annotations
import logging
from fastapi import APIRouter, HTTPException, Query
import services.db_writer as db_svc

logger = logging.getLogger("EggTrackAI.api.dashboard")

dashboard_router = APIRouter(prefix="/api/dashboard", tags=["Dashboard"])


@dashboard_router.get("/hourly")
async def hourly_production(hours: int = Query(24, ge=1, le=168)):
    """
    Returns hourly aggregated egg counts for the last N hours.
    Queries the raw EGG_PRODUCTION_EVENT table in real time.
    """
    writer = db_svc.get_writer()
    if writer is None:
        raise HTTPException(503, "DB writer not initialized")

    pool = writer._get_pool()
    if pool is None:
        raise HTTPException(503, "Oracle unavailable")

    sql = f"""
        SELECT
            TO_CHAR(TRUNC(EVENT_TIME, 'HH'), 'YYYY-MM-DD HH24:MI') AS hour_slot,
            COUNT(*) AS total_eggs,
            SUM(CRACK_FLAG) AS cracked_count,
            ROUND(SUM(CRACK_FLAG) * 100.0 / NULLIF(COUNT(*), 0), 2) AS crack_rate_pct
        FROM APPS.EGG_PRODUCTION_EVENT
        WHERE EVENT_TIME >= SYSTIMESTAMP - INTERVAL '{hours}' HOUR
        GROUP BY TRUNC(EVENT_TIME, 'HH')
        ORDER BY TRUNC(EVENT_TIME, 'HH') ASC
    """

    try:
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [d[0].lower() for d in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        return rows
    except Exception as exc:
        logger.error("hourly_production query failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Failed to fetch hourly data")


@dashboard_router.get("/defect-trend")
async def defect_trend(hours: int = Query(24, ge=1, le=168)):
    """
    Returns hourly crack rates for the last N hours.
    Queries the raw EGG_PRODUCTION_EVENT table in real time.
    """
    writer = db_svc.get_writer()
    if writer is None:
        raise HTTPException(503, "DB writer not initialized")

    pool = writer._get_pool()
    if pool is None:
        raise HTTPException(503, "Oracle unavailable")

    sql = f"""
        SELECT
            TO_CHAR(TRUNC(EVENT_TIME, 'HH'), 'YYYY-MM-DD HH24:MI') AS hour_slot,
            ROUND(SUM(CRACK_FLAG) * 100.0 / NULLIF(COUNT(*), 0), 2) AS crack_rate_pct,
            COUNT(*) AS total_eggs,
            SUM(CRACK_FLAG) AS cracked_count
        FROM APPS.EGG_PRODUCTION_EVENT
        WHERE EVENT_TIME >= SYSTIMESTAMP - INTERVAL '{hours}' HOUR
        GROUP BY TRUNC(EVENT_TIME, 'HH')
        ORDER BY TRUNC(EVENT_TIME, 'HH') ASC
    """

    try:
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [d[0].lower() for d in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        return rows
    except Exception as exc:
        logger.error("defect_trend query failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Failed to fetch defect trend")


@dashboard_router.get("/shed-performance")
async def shed_performance():
    """
    Returns total eggs per active shed, aggregated from V_LIVE_LINE_STATUS.
    """
    writer = db_svc.get_writer()
    if writer is None:
        raise HTTPException(503, "DB writer not initialized")

    pool = writer._get_pool()
    if pool is None:
        raise HTTPException(503, "Oracle unavailable")

    sql = """
        SELECT
            SHED_ID,
            SUM(EGGS_THIS_SESSION) AS total_eggs,
            SUM(CRACKED_THIS_SESSION) AS cracked_eggs
        FROM APPS.V_LIVE_LINE_STATUS
        GROUP BY SHED_ID
        ORDER BY SHED_ID
    """

    try:
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            cols = [d[0].lower() for d in cursor.description]
            rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        return rows
    except Exception as exc:
        logger.error("shed_performance query failed: %s", exc, exc_info=True)
        raise HTTPException(500, "Failed to fetch shed performance data")