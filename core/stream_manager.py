# core/stream_manager.py

# core/stream_manager.py
# Pipeline behaviour UNCHANGED.
# Key change: configure_worker_logging(log_queue) is the FIRST call
# inside inference_worker() so the spawned process never touches a
# log file directly — it writes only to the shared queue.

import cv2
import multiprocessing as mp
import time
import logging

from core.vision_engine import EggVisionEngine
import core.logger_setup as _ls
from core.logger_setup import (
    configure_worker_logging,
    log_camera_connected, log_camera_disconnected,
    log_camera_reconnected, log_worker_started,
    log_worker_crashed, log_error,
)

logger = logging.getLogger("EggTrackAI")

_RECONNECT_DELAY_S   = 3.0   # seconds between reconnect attempts
_RECONNECT_LOG_EVERY = 5     # warn every N consecutive read failures


def inference_worker(line_config, global_config, result_dict, log_queue):
    """
    Isolated process for one conveyor line.
    log_queue is passed explicitly from MultiLineManager so that
    configure_worker_logging() can wire this process to the listener
    thread that lives in the main process.
    """
    # ── MUST be first: replace any file handlers with QueueHandler ──
    configure_worker_logging(log_queue)

    line_id = line_config['id']
    source  = line_config['source']

    log_worker_started(line_id)

    try:
        engine = EggVisionEngine(global_config, line_config)

        cap = cv2.VideoCapture(source)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

        if cap.isOpened():
            log_camera_connected(line_id, source)
        else:
            log_camera_disconnected(line_id, source)

        consecutive_failures = 0

        while True:
            ret, frame = cap.read()

            if not ret:
                consecutive_failures += 1
                if consecutive_failures == 1 or \
                        consecutive_failures % _RECONNECT_LOG_EVERY == 0:
                    log_camera_disconnected(line_id, source)

                cap.release()
                time.sleep(_RECONNECT_DELAY_S)
                cap = cv2.VideoCapture(source)
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

                if cap.isOpened():
                    log_camera_reconnected(line_id)
                    consecutive_failures = 0
                continue

            if consecutive_failures > 0:
                consecutive_failures = 0

            annotated_frame, stats = engine.process_frame(frame)

            if annotated_frame is not None:
                _, buffer = cv2.imencode(
                    '.jpg', annotated_frame,
                    [cv2.IMWRITE_JPEG_QUALITY, 85]
                )
                result_dict[str(line_id)] = {
                    "frame": buffer.tobytes(),
                    "stats": stats,
                }

            time.sleep(0.01)

    except Exception as exc:
        logger.error(f"Inference Worker {line_id} crashed: {exc}")
        log_worker_crashed(line_id, exc)
        log_error("StreamManager",
                  f"Worker {line_id} unhandled exception",
                  exc=exc, meta={"line": line_id})


class MultiLineManager:
    def __init__(self, camera_config, global_config):
        self.manager     = mp.Manager()
        self.result_dict = self.manager.dict()
        self.processes   = []

        # Retrieve the queue that was created by start_log_listener()
        log_queue = _ls._log_queue

        for line in camera_config['conveyor_lines']:
            if line.get('active', True):
                p = mp.Process(
                    target=inference_worker,
                    # Pass log_queue as an explicit argument so the
                    # spawned process can reach it without re-importing
                    args=(line, global_config, self.result_dict, log_queue),
                    daemon=True,
                )
                p.start()
                self.processes.append(p)
                logger.info(f"Worker {line['id']} started.")

    def get_all_stats(self):
        try:
            return {
                str(id_): {"stats": data['stats']}
                for id_, data in self.result_dict.items()
            }
        except Exception:
            return {}
