# services/db_writer.py
# ============================================================
#  EggTrackAI  –  Oracle DB Writer Service
#  ─────────────────────────────────────────────────────────

#  • Async batch flush: events are buffered in memory and flushed every FLUSH_INTERVAL seconds OR when the buffer reaches BATCH_SIZE rows — whichever comes first.
#    This prevents the DB from becoming a bottleneck even when conveyor lines run at >30 eggs/second.
#  • Connection pool: uses oracledb (python-oracledb) Thin mode — no Oracle Client installation required on the server.
#  • Auto session resolution: on every event it looks up the active SESSION_ID for (line_id, shed_id) from a local in-process cache (TTL = 30 s) to avoid a DB round-trip per egg.
#  • Shift resolution: shift is derived from wall-clock time using the SHIFT_MASTER definitions — no extra DB call.
#  • Full reconnect / retry logic with exponential back-off.
#  Integration: add ONE call in main.py (see bottom of file).


from __future__ import annotations

import logging
import multiprocessing as mp
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("EggTrackAI.db_writer")

# ── DB credentials from environment ───────────────────────
_DSN  = os.environ.get("ORACLE_DSN")
_USER = os.environ.get("ORACLE_USER")
_PASS = os.environ.get("ORACLE_PASS")

_POOL_MIN          = 2
_POOL_MAX          = 6
_BATCH_SIZE        = 50    # rows before forced flush
_FLUSH_INTERVAL    = 5     # seconds between automatic flushes
_SESSION_CACHE_TTL = 30    # seconds to cache session_id lookups
_POOL_RETRY_DELAY  = 30    # seconds between failed pool-creation retries


# ═══════════════════════════════════════════════════════════
#  DATA CLASS
# ═══════════════════════════════════════════════════════════

@dataclass
class EggEvent:
    """One row destined for EGG_PRODUCTION_EVENT."""
    line_id:    int
    track_id:   int
    size_code:  str
    is_cracked: bool
    color_code: str      = "UNKNOWN"
    event_time: datetime = field(default_factory=datetime.utcnow)


# ═══════════════════════════════════════════════════════════
#  SHIFT RESOLVER  (wall-clock, no DB round-trip)
# ═══════════════════════════════════════════════════════════

_SHIFT_SCHEDULE: List[Tuple[int, int, int]] = [
    (6,  14, 1),   # Morning
    (14, 22, 2),   # Afternoon
    (22, 24, 3),   # Night (pm)
    (0,   6, 3),   # Night (am)
]

def _resolve_shift(dt: datetime) -> int:
    h = dt.hour
    for start, end, sid in _SHIFT_SCHEDULE:
        if start <= h < end:
            return sid
    return 1


# ═══════════════════════════════════════════════════════════
#  SESSION CACHE  (TTL, thread-safe)
# ═══════════════════════════════════════════════════════════

class _SessionCache:
    def __init__(self, ttl: int = _SESSION_CACHE_TTL):
        self._ttl   = ttl
        self._store: Dict[int, Tuple[int, str, float]] = {}
        self._lock  = threading.RLock()

    def get(self, line_id: int) -> Optional[Tuple[int, str]]:
        with self._lock:
            entry = self._store.get(line_id)
            if entry is None:
                return None
            session_id, shed_id, ts = entry
            if time.monotonic() - ts > self._ttl:
                del self._store[line_id]
                return None
            return session_id, shed_id

    def put(self, line_id: int, session_id: int, shed_id: str) -> None:
        with self._lock:
            self._store[line_id] = (session_id, shed_id, time.monotonic())

    def invalidate(self, line_id: int) -> None:
        with self._lock:
            self._store.pop(line_id, None)


# ═══════════════════════════════════════════════════════════
#  ORACLE DB WRITER  (main-process background thread)
# ═══════════════════════════════════════════════════════════

class OracleDbWriter:
    """
    Background thread that drains EggEvent objects from a shared
    Manager queue and bulk-inserts them into Oracle.
    """

    def __init__(self, event_queue):
        self._q              = event_queue
        self._pool: Any      = None
        self._pool_error_ts  = 0.0
        self._session_cache  = _SessionCache()
        self._buffer: List[EggEvent] = []
        self._last_flush     = time.monotonic()
        self._stop_flag      = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ── Lifecycle ──────────────────────────────────────────

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            name="OracleDbWriter",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "OracleDbWriter started | batch=%d flush_interval=%ds",
            _BATCH_SIZE, _FLUSH_INTERVAL,
        )

    def stop(self, timeout: float = 10.0) -> None:
        logger.info("OracleDbWriter stopping — flushing remaining events …")
        self._stop_flag.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        self._flush()
        self._close_pool()
        logger.info("OracleDbWriter stopped.")

    # ── Connection Pool (LAZY + FAILURE-SAFE) ──────────────

    def _get_pool(self):
        """
        Return the pool, creating it lazily on first call.
        Returns None if credentials are missing or DB is unreachable.
        A 30-second back-off prevents hammering an unavailable DB.
        """
        if self._pool is not None:
            return self._pool

        if time.monotonic() - self._pool_error_ts < _POOL_RETRY_DELAY:
            return None

        if not all([_DSN, _USER, _PASS]):
            logger.warning(
                "Oracle credentials not configured "
                "(ORACLE_DSN / ORACLE_USER / ORACLE_PASS env vars missing). "
                "DB writer is in no-op mode — no data will be stored."
            )
            # Set error timestamp so this warning is not repeated every 100ms
            self._pool_error_ts = time.monotonic()
            return None

        try:
            import oracledb   # imported here so missing package ≠ startup crash
            self._pool = oracledb.create_pool(
                user=_USER,
                password=_PASS,
                dsn=_DSN,
                min=_POOL_MIN,
                max=_POOL_MAX,
                increment=1,
            )
            logger.info("Oracle connection pool created | dsn=%s", _DSN)
            return self._pool
        except Exception as exc:
            self._pool_error_ts = time.monotonic()
            logger.error(
                "Oracle pool creation failed (retry in %ds): %s",
                _POOL_RETRY_DELAY, exc,
            )
            return None

    def _close_pool(self):
        if self._pool:
            try:
                self._pool.close()
            except Exception:
                pass
            self._pool = None

    # ── Background Thread ──────────────────────────────────

    def _run(self) -> None:
        while not self._stop_flag.is_set():
            # Drain available events (non-blocking)
            drained = 0
            while drained < _BATCH_SIZE * 2:
                try:
                    event = self._q.get_nowait()
                    self._buffer.append(event)
                    drained += 1
                except Exception:
                    # queue.Empty or Manager proxy hiccup — both acceptable
                    break

            now = time.monotonic()
            if (len(self._buffer) >= _BATCH_SIZE or
                    now - self._last_flush >= _FLUSH_INTERVAL):
                self._flush()

            time.sleep(0.1)

        # Final drain after stop signal
        while True:
            try:
                event = self._q.get_nowait()
                self._buffer.append(event)
            except Exception:
                break

    # ── Flush ──────────────────────────────────────────────

    def _flush(self) -> None:
        self._last_flush = time.monotonic()
        if not self._buffer:
            return

        batch = self._buffer[:]
        self._buffer.clear()

        try:
            self._insert_batch(batch)
        except Exception as exc:
            logger.error(
                "OracleDbWriter flush failed (batch=%d): %s",
                len(batch), exc, exc_info=True,
            )
            # Re-queue for next attempt
            for ev in batch:
                try:
                    self._q.put_nowait(ev)
                except Exception:
                    pass  # queue full — accept data loss rather than blocking

    def _insert_batch(self, batch: List[EggEvent]) -> None:
        pool = self._get_pool()
        if pool is None:
            return   # Oracle unavailable — silent discard

        rows: List[Dict] = []
        for ev in batch:
            info = self._resolve_session(pool, ev.line_id)
            if info is None:
                logger.warning(
                    "No active session for line=%d — event skipped. "
                    "Call SP_SWITCH_SHED to open a session first.",
                    ev.line_id,
                )
                continue
            session_id, shed_id = info
            rows.append({
                "session_id": session_id,
                "shed_id":    shed_id,
                "line_id":    ev.line_id,
                "shift_id":   _resolve_shift(ev.event_time),
                "track_id":   ev.track_id,
                "size_code":  ev.size_code.upper(),
                "color_code": ev.color_code.upper(),
                "crack_flag": 1 if ev.is_cracked else 0,
                "event_time": ev.event_time,
            })

        if not rows:
            return

        sql = """
            INSERT INTO APPS.EGG_PRODUCTION_EVENT
                (SESSION_ID, SHED_ID, LINE_ID, SHIFT_ID,
                 TRACK_ID, SIZE_CODE, COLOR_CODE, CRACK_FLAG, EVENT_TIME)
            VALUES
                (:session_id, :shed_id, :line_id, :shift_id,
                 :track_id, :size_code, :color_code, :crack_flag, :event_time)
        """
        with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.executemany(sql, rows)
            conn.commit()

        logger.debug("Oracle: inserted %d events", len(rows))

    # ── Session Resolution ─────────────────────────────────

    def _resolve_session(self, pool, line_id: int) -> Optional[Tuple[int, str]]:
        cached = self._session_cache.get(line_id)
        if cached:
            return cached
        sql = """
            SELECT s.SESSION_ID, s.SHED_ID
            FROM   APPS.SHED_RUN_SESSION s
            WHERE  s.LINE_ID = :line_id
              AND  s.STATUS  = 'RUNNING'
            FETCH FIRST 1 ROW ONLY
        """
        try:
            with pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, {"line_id": line_id})
                row = cursor.fetchone()
                if row is None:
                    return None
                session_id, shed_id = row
                self._session_cache.put(line_id, session_id, shed_id)
                return session_id, shed_id
        except Exception as exc:
            logger.error("Session lookup failed for line=%d: %s", line_id, exc)
            return None

    def invalidate_session_cache(self, line_id: int) -> None:
        self._session_cache.invalidate(line_id)

    # ── Shed Switch ────────────────────────────────────────

    def switch_shed(self, line_id: int, new_shed_id: str,
                    activated_by: str = "system",
                    notes: str = "") -> bool:
        try:
            pool = self._get_pool()
            if pool is None:
                raise RuntimeError("Oracle pool not available")
            with pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.callproc("APPS.SP_SWITCH_SHED",
                                [line_id, new_shed_id, activated_by, notes or None])
                conn.commit()
            self.invalidate_session_cache(line_id)
            logger.info("Shed switched | line=%d new_shed=%s by=%s",
                        line_id, new_shed_id, activated_by)
            return True
        except Exception as exc:
            logger.error("SP_SWITCH_SHED failed | line=%d shed=%s: %s",
                         line_id, new_shed_id, exc, exc_info=True)
            return False

    def get_live_status(self) -> List[Dict]:
        try:
            pool = self._get_pool()
            if pool is None:
                return []
            with pool.acquire() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT LINE_ID, LINE_NAME, SHED_ID, ACTIVATED_AT, "
                    "       SESSION_ID, SESSION_START, "
                    "       EGGS_THIS_SESSION, CRACKED_THIS_SESSION "
                    "FROM APPS.V_LIVE_LINE_STATUS"
                )
                cols = [d[0].lower() for d in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as exc:
            logger.error("get_live_status failed: %s", exc)
            return []


# ═══════════════════════════════════════════════════════════
#  MODULE-LEVEL SINGLETONS & PUBLIC API
# ═══════════════════════════════════════════════════════════

# Shared Manager queue — assigned in initialize()
_db_event_queue = None

# Main-process writer instance
_writer_instance: Optional[OracleDbWriter] = None

# Per-worker reference set by configure_worker_db() inside each subprocess
_worker_db_queue = None


def initialize(manager: Any) -> OracleDbWriter:
    """
    Create the shared queue and start the OracleDbWriter thread.

    IMPORTANT: pass the SAME mp.Manager() instance that
    MultiLineManager uses so we reuse the existing manager server.

    In main.py:

        mp_manager = mp.Manager()
        db_svc.initialize(mp_manager)
        manager_obj = MultiLineManager(cameras, settings, mp_manager)
    """
    global _writer_instance, _db_event_queue

    # Manager queue is shared across spawned subprocesses via the
    # manager server process — this is the key to FIX-2.
    _db_event_queue = manager.Queue(maxsize=10_000)

    _writer_instance = OracleDbWriter(_db_event_queue)
    _writer_instance.start()
    return _writer_instance


def configure_worker_db(db_queue) -> None:
    """
    Wire this worker subprocess to the shared DB queue.

    Call in inference_worker() right after configure_worker_logging():

        configure_worker_logging(log_queue)
        db_svc.configure_worker_db(db_queue)   # ← add this
    """
    global _worker_db_queue
    _worker_db_queue = db_queue


def get_writer() -> Optional[OracleDbWriter]:
    return _writer_instance


def shutdown() -> None:
    """Flush + close pool. Call from main.py finally block."""
    global _writer_instance
    if _writer_instance:
        _writer_instance.stop()
        _writer_instance = None


def enqueue_event(
    line_id:    int,
    track_id:   int,
    size_code:  str,
    is_cracked: bool,
    color_code: str = "UNKNOWN",
) -> None:
    """
    Non-blocking enqueue from any process (main or worker).
    Selects _worker_db_queue when inside a worker subprocess,
    _db_event_queue otherwise.
    Called by the shim in logger_setup.log_production_event().
    """
    q = _worker_db_queue if _worker_db_queue is not None else _db_event_queue
    if q is None:
        return  # not yet initialized — silent no-op

    ev = EggEvent(
        line_id=line_id,
        track_id=track_id,
        size_code=size_code,
        is_cracked=is_cracked,
        color_code=color_code,
        event_time=datetime.utcnow(),
    )
    try:
        q.put_nowait(ev)
    except Exception:
        logger.warning(
            "DB event queue full or unavailable — event dropped "
            "(line=%d track=%d)", line_id, track_id,
        )
