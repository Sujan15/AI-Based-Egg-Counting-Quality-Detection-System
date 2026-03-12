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

#  SECTION 5 – CHANNEL LOGGER ACCESSORS

def _get_logger(channel: str) -> logging.Logger:
    lg = logging.getLogger(f"EggTrackAI.{channel}")
    lg.setLevel(logging.DEBUG)
    lg.propagate = True   # propagates to root → QueueHandler → listener
    return lg

#  SECTION 6 – PUBLIC HELPER FUNCTIONS

# ── System ────────────────────────────────────────────────────
def log_app_start(version: str = "1.0.0") -> None:
    _get_logger("system").info(
        f'Application started | {{"version":"{version}"}}'
    )


def log_app_stop() -> None:
    _get_logger("system").info(
        'Application stopped | {"reason":"graceful_shutdown"}'
    )


def log_camera_connected(line_id, source: str) -> None:
    _get_logger("system").info(
        f'Camera connected | {{"line":{line_id},"source":"{_mask_rtsp(source)}"}}'
    )


def log_camera_disconnected(line_id, source: str) -> None:
    _get_logger("system").warning(
        f'Camera disconnected, retrying | '
        f'{{"line":{line_id},"source":"{_mask_rtsp(source)}"}}'
    )


def log_camera_reconnected(line_id) -> None:
    _get_logger("system").info(
        f'Camera reconnected successfully | {{"line":{line_id}}}'
    )


def log_worker_started(line_id) -> None:
    _get_logger("system").info(
        f'Inference worker started | {{"line":{line_id}}}'
    )


def log_worker_crashed(line_id, exc: Exception) -> None:
    _get_logger("system").critical(
        f'Inference worker crashed | {{"line":{line_id},"error":"{exc}"}}'
    )


def log_ws_connected(client: str) -> None:
    _get_logger("system").info(
        f'WebSocket stats client connected | {{"client":"{client}"}}'
    )


def log_ws_disconnected(client: str) -> None:
    _get_logger("system").info(
        f'WebSocket stats client disconnected | {{"client":"{client}"}}'
    )


def log_webrtc_offer(line_id) -> None:
    _get_logger("system").info(
        f'WebRTC offer received | {{"line":{line_id}}}'
    )


def log_webrtc_closed(line_id) -> None:
    _get_logger("system").info(
        f'WebRTC peer connection closed | {{"line":{line_id}}}'
    )
# ── AI Performance (sampled 1×/sec — never per-frame) ─────────
def log_ai_perf(line_id, fps: float, det_count: int,
                avg_conf: float, infer_ms: float, track_ids) -> None:
    meta = json.dumps({
        "fps":           round(fps, 1),
        "detections":    det_count,
        "avg_conf":      round(float(avg_conf), 3),
        "inference_ms":  round(infer_ms, 1),
        "active_tracks": len(track_ids),
    }, separators=(",", ":"))
    _get_logger("ai").info(f"line={line_id} | {meta}")


# ── Production Event (once per egg crossing counting line) ────
def log_production_event(line_id, track_id, size: str,
                          is_crack: bool, shift: int = 1) -> None:
    _get_logger("production").info(
        f"LINE={line_id} SHIFT={shift} TRACK={track_id} "
        f"SIZE={size} CRACK={1 if is_crack else 0}"
    )


# ── Error ─────────────────────────────────────────────────────
def log_error(module: str, message: str,
              exc: Exception = None, meta: dict = None) -> None:
    meta_str = (
        f" | {json.dumps(meta, separators=(',', ':'))}" if meta else ""
    )
    _get_logger("error").error(f"{module} {message}{meta_str}")
    if exc:
        _get_logger("error").error(traceback.format_exc())


def log_critical(module: str, message: str,
                 exc: Exception = None) -> None:
    _get_logger("error").critical(f"{module} {message}")
    if exc:
        _get_logger("error").critical(traceback.format_exc())


# ── Audit ─────────────────────────────────────────────────────
def log_audit(user: str, action: str,
              ip: str = "-", **kwargs) -> None:
    extra = "".join(f" {k.upper()}={v}" for k, v in kwargs.items())
    _get_logger("audit").info(
        f"USER={user} ACTION={action} IP={ip}{extra}"
    )


def log_audit_login(user: str, ip: str,
                    success: bool = True) -> None:
    status = "SUCCESS" if success else "FAILED"
    _get_logger("audit").info(
        f"USER={user} ACTION=LOGIN IP={ip} STATUS={status}"
    )


def log_audit_logout(user: str, ip: str) -> None:
    _get_logger("audit").info(
        f"USER={user} ACTION=LOGOUT IP={ip}"
    )


def log_audit_export(user: str, report_type: str = "DAILY",
                     ip: str = "-") -> None:
    _get_logger("audit").info(
        f"USER={user} ACTION=EXPORT_REPORT TYPE={report_type} IP={ip}"
    )


def log_audit_model_update(user: str, version: str,
                            ip: str = "-") -> None:
    _get_logger("audit").info(
        f"USER={user} ACTION=MODEL_UPDATED VERSION={version} IP={ip}"
    )


def log_audit_config_change(user: str, config_file: str,
                             ip: str = "-") -> None:
    _get_logger("audit").info(
        f"USER={user} ACTION=CONFIG_CHANGED FILE={config_file} IP={ip}"
    )


def log_audit_line_toggle(user: str, line_id,
                           state: str, ip: str = "-") -> None:
    _get_logger("audit").info(
        f"USER={user} ACTION=LINE_{state} LINE={line_id} IP={ip}"
    )


# ── Internal Utility ──────────────────────────────────────────
def _mask_rtsp(source: str) -> str:
    """Mask credentials in RTSP URLs before writing to logs."""
    if not isinstance(source, str):
        return str(source)
    if not source.lower().startswith("rtsp://"):
        return source
    try:
        from urllib.parse import urlparse, urlunparse
        p = urlparse(source)
        masked = p._replace(
            netloc=(
                f"***:***@{p.hostname}"
                + (f":{p.port}" if p.port else "")
            )
        )
        return urlunparse(masked)
    except Exception:
        return "rtsp://***"
class _LoggerProxy:
    """
    Thin proxy that forwards every call to the live logger for a channel.
    Using a proxy (rather than a direct reference) ensures the object
    always reflects the current handler configuration — critical because
    configure_worker_logging() replaces handlers on the root logger after
    module import time.
    """
    __slots__ = ("_channel",)

    def __init__(self, channel: str) -> None:
        object.__setattr__(self, "_channel", channel)

    def _logger(self) -> logging.Logger:
        return _get_logger(object.__getattribute__(self, "_channel"))

    def __getattr__(self, name: str):
        return getattr(self._logger(), name)

    def __repr__(self) -> str:
        return repr(self._logger())

