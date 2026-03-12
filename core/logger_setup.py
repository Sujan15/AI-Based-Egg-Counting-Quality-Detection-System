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


