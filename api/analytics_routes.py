# api/analytics_routes.py
# ══════════════════════════════════════════════════════════════
#  Analytics API Routes — EggTrackAI
#  Queries: APPS.EGG_PRODUCTION_EVENT (size breakdown)
#           APPS.HOURLY_PRODUCTION_SUMMARY (hourly trend)
#           APPS.DAILY_PRODUCTION_SUMMARY  (shift data)
#  Size codes: SMALL | MEDIUM | HIGHER_MEDIUM | BIG | LARGE
#  Shifts    : 1=Morning (07-14) | 2=Night (14-07)
#  Sheds     : Line1→SHD001 | Line2→SHD004 | Line3→SHD007
# ══════════════════════════════════════════════════════════════

import logging
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from typing import Optional

import services.db_writer as db_svc

logger = logging.getLogger("EggTrackAI.analytics")

analytics_router = APIRouter(prefix="/api/analytics", tags=["analytics"])


# ─────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────

def _get_pool():
    """Borrow the Oracle pool from the running OracleDbWriter instance."""
    writer = db_svc.get_writer()
    if writer is None:
        return None
    return writer._get_pool()


def _rows(cursor) -> list:
    """Convert cursor result to list of dicts (lowercase keys)."""
    cols = [d[0].lower() for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _event_date_filter(
    period: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
):
    """
    Returns (sql_fragment, bind_dict) for EGG_PRODUCTION_EVENT.
    Dates are Oracle format DD-MON-YY  e.g. '12-APR-26'
    """
    if start_date and end_date:
        return (
            "TRUNC(e.EVENT_TIME) BETWEEN "
            "TO_DATE(:sd, 'DD-MON-YY') AND TO_DATE(:ed, 'DD-MON-YY')",
            {"sd": start_date, "ed": end_date},
        )
    p = int(period or 1)
    if p == 2:
        return "TRUNC(e.EVENT_TIME) >= TRUNC(SYSDATE) - 6", {}
    if p == 3:
        return "TRUNC(e.EVENT_TIME) >= TRUNC(SYSDATE) - 29", {}
    # default → today
    return "TRUNC(e.EVENT_TIME) = TRUNC(SYSDATE)", {}


def _daily_date_filter(
    period: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
):
    """Same but for DAILY_PRODUCTION_SUMMARY (SUMMARY_DATE column)."""
    if start_date and end_date:
        return (
            "SUMMARY_DATE BETWEEN "
            "TO_DATE(:sd, 'DD-MON-YY') AND TO_DATE(:ed, 'DD-MON-YY')",
            {"sd": start_date, "ed": end_date},
        )
    p = int(period or 1)
    if p == 2:
        return "SUMMARY_DATE >= TRUNC(SYSDATE) - 6", {}
    if p == 3:
        return "SUMMARY_DATE >= TRUNC(SYSDATE) - 29", {}
    return "SUMMARY_DATE = TRUNC(SYSDATE)", {}


def _hourly_date_filter(
    period: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
):
    """Same but for HOURLY_PRODUCTION_SUMMARY (HOUR_SLOT column)."""
    if start_date and end_date:
        return (
            "TRUNC(h.HOUR_SLOT) BETWEEN "
            "TO_DATE(:sd, 'DD-MON-YY') AND TO_DATE(:ed, 'DD-MON-YY')",
            {"sd": start_date, "ed": end_date},
        )
    p = int(period or 1)
    if p == 2:
        return "h.HOUR_SLOT >= TRUNC(SYSDATE) - 6", {}
    if p == 3:
        return "h.HOUR_SLOT >= TRUNC(SYSDATE) - 29", {}
    return "TRUNC(h.HOUR_SLOT) = TRUNC(SYSDATE)", {}


def _error(msg: str, status: int = 500):
    logger.error("Analytics API: %s", msg)
    return JSONResponse({"success": False, "error": msg}, status_code=status)


# ─────────────────────────────────────────────────────────────
#  1.  SHED SUMMARY
#      /api/analytics/shed-summary
#      Returns one row per active shed with full size breakdown
# ─────────────────────────────────────────────────────────────

@analytics_router.get("/shed-summary")
async def shed_summary(
    period:     Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    shed_id:    Optional[str] = Query(None),
    line_id:    Optional[str] = Query(None),
):
    pool = _get_pool()
    if pool is None:
        return _error("Oracle pool not available", 503)

    date_sql, params = _event_date_filter(period, start_date, end_date)

    extra = ""
    if shed_id:
        extra += " AND e.SHED_ID = :shed_id"
        params["shed_id"] = shed_id
    if line_id:
        extra += " AND e.LINE_ID = :line_id"
        params["line_id"] = int(line_id)

    sql = f"""
        SELECT
            e.SHED_ID,
            e.LINE_ID,
            COUNT(*)                                                       AS total_eggs,
            SUM(CASE WHEN e.SIZE_CODE = 'SMALL'         THEN 1 ELSE 0 END) AS small_count,
            SUM(CASE WHEN e.SIZE_CODE = 'MEDIUM'        THEN 1 ELSE 0 END) AS medium_count,
            SUM(CASE WHEN e.SIZE_CODE = 'HIGHER_MEDIUM' THEN 1 ELSE 0 END) AS higher_medium_count,
            SUM(CASE WHEN e.SIZE_CODE = 'BIG'           THEN 1 ELSE 0 END) AS big_count,
            SUM(CASE WHEN e.SIZE_CODE = 'LARGE'         THEN 1 ELSE 0 END) AS large_count,
            SUM(CASE WHEN e.SIZE_CODE = 'STANDARD'      THEN 1 ELSE 0 END) AS standard_count,
            SUM(e.CRACK_FLAG)                                              AS cracked_count,
            SUM(1 - e.CRACK_FLAG)                                         AS good_count,
            ROUND(SUM(e.CRACK_FLAG) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS crack_rate_pct
        FROM APPS.EGG_PRODUCTION_EVENT e
        WHERE {date_sql}{extra}
        GROUP BY e.SHED_ID, e.LINE_ID
        ORDER BY e.SHED_ID
    """

    try:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            data = _rows(cur)
        return {"success": True, "data": data}
    except Exception as exc:
        logger.error("shed_summary error: %s", exc, exc_info=True)
        return _error(str(exc))


# ─────────────────────────────────────────────────────────────
#  2.  CONVEYOR SUMMARY
#      /api/analytics/conveyor-summary
#      Returns one row per conveyor line
# ─────────────────────────────────────────────────────────────

@analytics_router.get("/conveyor-summary")
async def conveyor_summary(
    period:     Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    shed_id:    Optional[str] = Query(None),
    line_id:    Optional[str] = Query(None),
):
    pool = _get_pool()
    if pool is None:
        return _error("Oracle pool not available", 503)

    date_sql, params = _event_date_filter(period, start_date, end_date)

    extra = ""
    if shed_id:
        extra += " AND e.SHED_ID = :shed_id"
        params["shed_id"] = shed_id
    if line_id:
        extra += " AND e.LINE_ID = :line_id"
        params["line_id"] = int(line_id)

    sql = f"""
        SELECT
            e.LINE_ID,
            COUNT(*)                                                       AS total_eggs,
            SUM(CASE WHEN e.SIZE_CODE = 'SMALL'         THEN 1 ELSE 0 END) AS small_count,
            SUM(CASE WHEN e.SIZE_CODE = 'MEDIUM'        THEN 1 ELSE 0 END) AS medium_count,
            SUM(CASE WHEN e.SIZE_CODE = 'HIGHER_MEDIUM' THEN 1 ELSE 0 END) AS higher_medium_count,
            SUM(CASE WHEN e.SIZE_CODE = 'BIG'           THEN 1 ELSE 0 END) AS big_count,
            SUM(CASE WHEN e.SIZE_CODE = 'LARGE'         THEN 1 ELSE 0 END) AS large_count,
            SUM(CASE WHEN e.SIZE_CODE = 'STANDARD'      THEN 1 ELSE 0 END) AS standard_count,
            SUM(e.CRACK_FLAG)                                              AS cracked_count,
            SUM(1 - e.CRACK_FLAG)                                         AS good_count,
            ROUND(SUM(e.CRACK_FLAG) * 100.0 / NULLIF(COUNT(*), 0), 2)    AS crack_rate_pct
        FROM APPS.EGG_PRODUCTION_EVENT e
        WHERE {date_sql}{extra}
        GROUP BY e.LINE_ID
        ORDER BY e.LINE_ID
    """

    try:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            data = _rows(cur)
        return {"success": True, "data": data}
    except Exception as exc:
        logger.error("conveyor_summary error: %s", exc, exc_info=True)
        return _error(str(exc))


# ─────────────────────────────────────────────────────────────
#  3.  HOURLY TREND
#      /api/analytics/hourly-trend
#      Uses HOURLY_PRODUCTION_SUMMARY (pre-aggregated every 5 min)
#      Falls back to EGG_PRODUCTION_EVENT if summary is empty
# ─────────────────────────────────────────────────────────────

@analytics_router.get("/hourly-trend")
async def hourly_trend(
    period:     Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    shed_id:    Optional[str] = Query(None),
    line_id:    Optional[str] = Query(None),
):
    pool = _get_pool()
    if pool is None:
        return _error("Oracle pool not available", 503)

    date_sql, params = _hourly_date_filter(period, start_date, end_date)

    extra = ""
    if shed_id:
        extra += " AND h.SHED_ID = :shed_id"
        params["shed_id"] = shed_id
    if line_id:
        extra += " AND h.LINE_ID = :line_id"
        params["line_id"] = int(line_id)

    sql = f"""
        SELECT
            TO_CHAR(h.HOUR_SLOT, 'YYYY-MM-DD HH24:MI') AS hour_slot,
            SUM(h.TOTAL_EGGS)    AS total_eggs,
            SUM(h.CRACKED_COUNT) AS cracked_count,
            ROUND(
                SUM(h.CRACKED_COUNT) * 100.0 / NULLIF(SUM(h.TOTAL_EGGS), 0),
            2)                   AS crack_rate_pct
        FROM APPS.HOURLY_PRODUCTION_SUMMARY h
        WHERE {date_sql}{extra}
        GROUP BY h.HOUR_SLOT
        ORDER BY h.HOUR_SLOT
    """

    try:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            data = _rows(cur)

        # Fallback: if hourly summary is empty, aggregate from events
        if not data:
            date_sql2, params2 = _event_date_filter(period, start_date, end_date)
            extra2 = ""
            if shed_id:
                extra2 += " AND e.SHED_ID = :shed_id"
                params2["shed_id"] = shed_id
            if line_id:
                extra2 += " AND e.LINE_ID = :line_id"
                params2["line_id"] = int(line_id)

            sql2 = f"""
                SELECT
                    TO_CHAR(TRUNC(e.EVENT_TIME, 'HH'), 'YYYY-MM-DD HH24:MI') AS hour_slot,
                    COUNT(*)                                                   AS total_eggs,
                    SUM(e.CRACK_FLAG)                                          AS cracked_count,
                    ROUND(SUM(e.CRACK_FLAG)*100.0/NULLIF(COUNT(*),0),2)        AS crack_rate_pct
                FROM APPS.EGG_PRODUCTION_EVENT e
                WHERE {date_sql2}{extra2}
                GROUP BY TRUNC(e.EVENT_TIME, 'HH')
                ORDER BY TRUNC(e.EVENT_TIME, 'HH')
            """
            with pool.acquire() as conn:
                cur = conn.cursor()
                cur.execute(sql2, params2)
                data = _rows(cur)

        return {"success": True, "data": data}
    except Exception as exc:
        logger.error("hourly_trend error: %s", exc, exc_info=True)
        return _error(str(exc))


# ─────────────────────────────────────────────────────────────
#  4.  CRACK ANALYSIS
#      /api/analytics/crack-analysis
#      Crack rate per shed per day (with 7-day rolling avg)
# ─────────────────────────────────────────────────────────────

@analytics_router.get("/crack-analysis")
async def crack_analysis(
    period:     Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    shed_id:    Optional[str] = Query(None),
    line_id:    Optional[str] = Query(None),
):
    pool = _get_pool()
    if pool is None:
        return _error("Oracle pool not available", 503)

    date_sql, params = _daily_date_filter(period, start_date, end_date)

    extra = ""
    if shed_id:
        extra += " AND d.SHED_ID = :shed_id"
        params["shed_id"] = shed_id
    if line_id:
        extra += " AND d.LINE_ID = :line_id"
        params["line_id"] = int(line_id)

    sql = f"""
        SELECT
            TO_CHAR(d.SUMMARY_DATE, 'YYYY-MM-DD') AS summary_date,
            d.SHED_ID,
            d.LINE_ID,
            d.TOTAL_EGGS,
            d.CRACKED_COUNT,
            d.CRACK_RATE_PCT,
            AVG(d.CRACK_RATE_PCT) OVER (
                PARTITION BY d.SHED_ID
                ORDER BY d.SUMMARY_DATE
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS rolling_7day_crack_rate
        FROM APPS.DAILY_PRODUCTION_SUMMARY d
        WHERE {date_sql}{extra}
        ORDER BY d.SUMMARY_DATE DESC, d.SHED_ID
    """

    try:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            data = _rows(cur)

        if not data:
            date_sql2, params2 = _event_date_filter(period, start_date, end_date)
            extra2 = ""
            if shed_id:
                extra2 += " AND e.SHED_ID = :shed_id"
                params2["shed_id"] = shed_id
            if line_id:
                extra2 += " AND e.LINE_ID = :line_id"
                params2["line_id"] = int(line_id)
            sql2 = f"""
                SELECT
                    e.SHED_ID,
                    e.LINE_ID,
                    COUNT(*)                                                AS total_eggs,
                    SUM(e.CRACK_FLAG)                                       AS cracked_count,
                    ROUND(SUM(e.CRACK_FLAG)*100.0/NULLIF(COUNT(*),0), 2)   AS crack_rate_pct
                FROM APPS.EGG_PRODUCTION_EVENT e
                WHERE {date_sql2}{extra2}
                GROUP BY e.SHED_ID, e.LINE_ID
                ORDER BY e.SHED_ID
            """
            with pool.acquire() as conn:
                cur = conn.cursor()
                cur.execute(sql2, params2)
                data = _rows(cur)

        return {"success": True, "data": data}
    except Exception as exc:
        logger.error("crack_analysis error: %s", exc, exc_info=True)
        return _error(str(exc))


# ─────────────────────────────────────────────────────────────
#  5.  SHIFT REPORT
#      /api/analytics/shift-report
#      Morning (Shift 1) vs Night (Shift 2) breakdown per shed
# ─────────────────────────────────────────────────────────────

@analytics_router.get("/shift-report")
async def shift_report(
    period:     Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    shed_id:    Optional[str] = Query(None),
    line_id:    Optional[str] = Query(None),
):
    pool = _get_pool()
    if pool is None:
        return _error("Oracle pool not available", 503)

    date_sql, params = _daily_date_filter(period, start_date, end_date)

    extra = ""
    if shed_id:
        extra += " AND d.SHED_ID = :shed_id"
        params["shed_id"] = shed_id
    if line_id:
        extra += " AND d.LINE_ID = :line_id"
        params["line_id"] = int(line_id)

    sql = f"""
        SELECT
            d.SHED_ID,
            d.LINE_ID,
            SUM(d.SHIFT1_TOTAL) AS morning,
            SUM(d.SHIFT2_TOTAL) AS night,
            SUM(d.TOTAL_EGGS)   AS total_eggs
        FROM APPS.DAILY_PRODUCTION_SUMMARY d
        WHERE {date_sql}{extra}
        GROUP BY d.SHED_ID, d.LINE_ID
        ORDER BY d.SHED_ID
    """

    try:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(sql, params)
            data = _rows(cur)

        if not data:
            date_sql2, params2 = _event_date_filter(period, start_date, end_date)
            extra2 = ""
            if shed_id:
                extra2 += " AND e.SHED_ID = :shed_id"
                params2["shed_id"] = shed_id
            if line_id:
                extra2 += " AND e.LINE_ID = :line_id"
                params2["line_id"] = int(line_id)
            sql2 = f"""
                SELECT
                    e.SHED_ID,
                    e.LINE_ID,
                    SUM(CASE WHEN e.SHIFT_ID = 1 THEN 1 ELSE 0 END) AS morning,
                    SUM(CASE WHEN e.SHIFT_ID = 2 THEN 1 ELSE 0 END) AS night,
                    COUNT(*)                                          AS total_eggs
                FROM APPS.EGG_PRODUCTION_EVENT e
                WHERE {date_sql2}{extra2}
                GROUP BY e.SHED_ID, e.LINE_ID
                ORDER BY e.SHED_ID
            """
            with pool.acquire() as conn:
                cur = conn.cursor()
                cur.execute(sql2, params2)
                data = _rows(cur)

        for row in data:
            row.setdefault("shift1_total", row.get("morning", 0))
            row.setdefault("shift2_total", row.get("night",   0))

        return {"success": True, "data": data}
    except Exception as exc:
        logger.error("shift_report error: %s", exc, exc_info=True)
        return _error(str(exc))


# ─────────────────────────────────────────────────────────────
#  6.  DAILY TABLE
#      /api/analytics/daily-table
#      Full detail: one row per (date × shed × line)
#      Size breakdown from EGG_PRODUCTION_EVENT
#      Shift totals from DAILY_PRODUCTION_SUMMARY
# ─────────────────────────────────────────────────────────────

@analytics_router.get("/daily-table")
async def daily_table(
    period:     Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date:   Optional[str] = Query(None),
    shed_id:    Optional[str] = Query(None),
    line_id:    Optional[str] = Query(None),
):
    pool = _get_pool()
    if pool is None:
        return _error("Oracle pool not available", 503)

    date_sql_ev,  params_ev  = _event_date_filter(period, start_date, end_date)
    date_sql_day, params_day = _daily_date_filter(period, start_date, end_date)

    extra_ev = ""
    if shed_id:
        extra_ev += " AND e.SHED_ID = :shed_id"
        params_ev["shed_id"] = shed_id
        params_day["shed_id"] = shed_id
    if line_id:
        extra_ev += " AND e.LINE_ID = :line_id"
        params_ev["line_id"] = int(line_id)
        params_day["line_id"] = int(line_id)

    extra_day = ""
    if shed_id:
        extra_day += " AND d.SHED_ID = :shed_id"
    if line_id:
        extra_day += " AND d.LINE_ID = :line_id"

    sql_ev = f"""
        SELECT
            TO_CHAR(TRUNC(e.EVENT_TIME), 'YYYY-MM-DD')                    AS summary_date,
            e.SHED_ID,
            e.LINE_ID,
            COUNT(*)                                                       AS total_eggs,
            SUM(CASE WHEN e.SIZE_CODE = 'SMALL'         THEN 1 ELSE 0 END) AS small_count,
            SUM(CASE WHEN e.SIZE_CODE = 'MEDIUM'        THEN 1 ELSE 0 END) AS medium_count,
            SUM(CASE WHEN e.SIZE_CODE = 'HIGHER_MEDIUM' THEN 1 ELSE 0 END) AS higher_medium_count,
            SUM(CASE WHEN e.SIZE_CODE = 'BIG'           THEN 1 ELSE 0 END) AS big_count,
            SUM(CASE WHEN e.SIZE_CODE = 'LARGE'         THEN 1 ELSE 0 END) AS large_count,
            SUM(CASE WHEN e.SIZE_CODE = 'STANDARD'      THEN 1 ELSE 0 END) AS standard_count,
            SUM(e.CRACK_FLAG)                                              AS cracked_count,
            SUM(1 - e.CRACK_FLAG)                                         AS good_count,
            ROUND(SUM(e.CRACK_FLAG)*100.0/NULLIF(COUNT(*),0), 2)          AS crack_rate_pct
        FROM APPS.EGG_PRODUCTION_EVENT e
        WHERE {date_sql_ev}{extra_ev}
        GROUP BY TRUNC(e.EVENT_TIME), e.SHED_ID, e.LINE_ID
        ORDER BY TRUNC(e.EVENT_TIME) DESC, e.SHED_ID
    """

    sql_shift = f"""
        SELECT
            TO_CHAR(d.SUMMARY_DATE, 'YYYY-MM-DD') AS summary_date,
            d.SHED_ID,
            d.LINE_ID,
            d.SHIFT1_TOTAL,
            d.SHIFT2_TOTAL
        FROM APPS.DAILY_PRODUCTION_SUMMARY d
        WHERE {date_sql_day}{extra_day}
    """

    try:
        with pool.acquire() as conn:
            cur = conn.cursor()
            cur.execute(sql_ev, params_ev)
            ev_rows = _rows(cur)

            cur2 = conn.cursor()
            cur2.execute(sql_shift, params_day)
            shift_rows = _rows(cur2)

        shift_map = {
            (r["summary_date"], r["shed_id"], str(r["line_id"])): r
            for r in shift_rows
        }

        for row in ev_rows:
            key = (row["summary_date"], row["shed_id"], str(row["line_id"]))
            shift = shift_map.get(key, {})
            row["shift1_total"] = shift.get("shift1_total", 0)
            row["shift2_total"] = shift.get("shift2_total", 0)

        return {"success": True, "data": ev_rows}
    except Exception as exc:
        logger.error("daily_table error: %s", exc, exc_info=True)
        return _error(str(exc))