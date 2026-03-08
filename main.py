# main.py
# Pipeline behaviour UNCHANGED.
# Enterprise logging: start listener BEFORE workers are spawned.

import uvicorn
import logging
from core.logger_setup import (
    start_log_listener, stop_log_listener,
    log_app_start, log_app_stop, log_audit,
)

# Standard console output (separate from file logging)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EggTrackAI")

APP_VERSION = "1.0.0"

if __name__ == "__main__":
    # ── STEP 1: Start the log listener BEFORE anything else ───
    # This creates the shared queue and starts the single writer thread.
    # The queue is stored in logger_setup module scope so all helpers
    # can reach it via _get_logger() without needing to pass it explicitly.
    log_queue = start_log_listener()

    # ── STEP 2: Store queue on a well-known module so stream_manager
    #    can retrieve it inside the spawned worker processes.
    import core.logger_setup as _ls
    _ls._log_queue = log_queue          # already set by start_log_listener,
                                        # but we expose it for workers too

    # ── STEP 3: Startup audit & system entries ─────────────────
    log_app_start(version=APP_VERSION)
    log_audit("system", "APP_START", ip="localhost", version=APP_VERSION)

    logger.info("Starting Egg Counting & Quality Grading Service")

    try:
        uvicorn.run(
            "api.server:app",
            host="0.0.0.0",
            port=8000,
            workers=1,
            log_level="info",
            reload=False,
        )
    finally:
        log_app_stop()
        log_audit("system", "APP_STOP", ip="localhost")
        stop_log_listener()
