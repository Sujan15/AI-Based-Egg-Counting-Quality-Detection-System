# logger_setup.py

import gzip
import json
import logging
import logging.handlers
import os
import shutil
import traceback

# ── Directory Bootstrap ──────────────────────────────────────
LOG_BASE_DIR = "logs"
_CATEGORIES  = ["system", "ai", "production", "error", "audit"]

for _cat in _CATEGORIES:
    os.makedirs(os.path.join(LOG_BASE_DIR, _cat), exist_ok=True)

class _GzipRotator:
    """
    Passed as rotator= to TimedRotatingFileHandler.
    Compresses the rolled file with gzip then removes the original.
    Safe because it runs only in the listener thread — no contention.
    """
    def __call__(self, source: str, dest: str) -> None:
        gz_dest = dest + ".gz"
        try:
            with open(source, "rb") as f_in, \
                 gzip.open(gz_dest, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            os.remove(source)
        except Exception as exc:
            # Keep uncompressed file rather than losing data
            print(f"[EggTrackAI] Log rotation compression failed: {exc}")

#  SECTION 2 – LOG FORMATS & CHANNEL MAP

_FMT = {
    "system":     ("%(asctime)s.%(msecs)03d %(levelname)-5s EggTrackAI "
                   "%(name)-22s - %(message)s"),
    "ai":         "%(asctime)s.%(msecs)03d INFO AIEngine %(message)s",
    "production": "%(asctime)s.%(msecs)03d %(message)s",
    "error":      "%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
    "audit":      "%(asctime)s.%(msecs)03d %(message)s",
}

# Logger name prefix → channel key
_CHANNEL_PREFIX = {
    "EggTrackAI.system":     "system",
    "EggTrackAI.ai":         "ai",
    "EggTrackAI.production": "production",
    "EggTrackAI.error":      "error",
    "EggTrackAI.audit":      "audit",
}

def _make_file_handler(category: str) -> logging.Handler:
    """Build one TimedRotatingFileHandler. Called only inside the listener."""
    filepath = os.path.join(LOG_BASE_DIR, category, f"{category}.log")
    h = logging.handlers.TimedRotatingFileHandler(
        filepath,
        when="midnight",
        interval=1,
        backupCount=90,
        encoding="utf-8",
        utc=False,
    )
    h.rotator    = _GzipRotator()
    h.setFormatter(
        logging.Formatter(_FMT[category], datefmt="%Y-%m-%d %H:%M:%S")
    )
    h.setLevel(logging.DEBUG)
    return h

#  SECTION 3 – LISTENER  (runs in main process only)

_listener: logging.handlers.QueueListener = None
_log_queue = None


def start_log_listener(log_queue=None):
    """
    Create all file handlers and start the QueueListener thread.

    Must be called ONCE in the main process BEFORE any workers are spawned.
    Returns the log_queue so you can pass it to worker processes.
    """
    global _listener, _log_queue

    if log_queue is None:
        import multiprocessing
        log_queue = multiprocessing.Queue(-1)

    _log_queue = log_queue

    # One file handler per channel — owned exclusively by this thread
    _file_handlers = {cat: _make_file_handler(cat) for cat in _CATEGORIES}

    class _ChannelRouter(logging.Handler):
        """
        Routes each LogRecord to the correct channel file-handler
        based on logger-name prefix.  Unknown names → system log.
        """
        def emit(self, record: logging.LogRecord) -> None:
            for prefix, channel in _CHANNEL_PREFIX.items():
                if record.name.startswith(prefix):
                    try:
                        _file_handlers[channel].emit(record)
                    except Exception:
                        _file_handlers["error"].emit(record)
                    return
            _file_handlers["system"].emit(record)   # fallback

    router = _ChannelRouter()
    router.setLevel(logging.DEBUG)

    _listener = logging.handlers.QueueListener(
        log_queue,
        router,
        respect_handler_level=True,
    )
    _listener.start()

    # Wire the main process root logger to the queue
    _wire_root_to_queue(log_queue)

    _get_logger("system").info(
        'Log listener started | {"pid":"main","channels":5}'
    )
    return log_queue


def stop_log_listener() -> None:
    """Flush the queue and stop the listener. Call on graceful shutdown."""
    global _listener
    if _listener is not None:
        _listener.stop()
        _listener = None


#  SECTION 4 – WORKER BOOTSTRAP

def configure_worker_logging(log_queue) -> None:
    """
    Replace all handlers on the worker's root logger with a QueueHandler.

    Call this as the VERY FIRST statement in every inference_worker()
    function, before any other imports or log calls.
    The worker will never open a log file — it only writes to the queue.
    """
    _wire_root_to_queue(log_queue)


def _wire_root_to_queue(log_queue) -> None:
    root = logging.getLogger()
    # Close and remove every existing handler to prevent file-handle leaks
    for h in root.handlers[:]:
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.DEBUG)
    root.addHandler(qh)
    root.setLevel(logging.DEBUG)

