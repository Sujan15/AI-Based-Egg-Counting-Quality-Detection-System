# api/server.py
# Pipeline behaviour UNCHANGED. Enterprise logging hooks added.

import yaml
import asyncio
import uuid
from fractions import Fraction

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from aiortc import MediaStreamTrack, RTCPeerConnection, RTCSessionDescription
import av
import cv2
import numpy as np
import logging

from core.stream_manager import MultiLineManager
from core.logger_setup import (
    log_ws_connected, log_ws_disconnected,
    log_webrtc_offer, log_webrtc_closed,
    log_error, sys_logger,
)

# ── Standard console logger ───────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EggTrackAI")

app = FastAPI()
app.mount("/web", StaticFiles(directory="web"), name="web")

with open("config/settings.yaml", "r") as f:
    settings = yaml.safe_load(f)
with open("config/cameras.yaml", "r") as f:
    cameras = yaml.safe_load(f)

sys_logger.info(
    f'Configuration loaded | {{"lines":{len(cameras["conveyor_lines"])},'
    f'"detection_model":"{settings["models"]["detection"]}"}}'
)

manager = MultiLineManager(cameras, settings)
pcs: set = set()


# ── WebRTC Video Track (unchanged) ───────────────────────────
class ProcessedVideoTrack(MediaStreamTrack):
    """
    Industrial WebRTC Track that pulls processed frames from
    Multiprocessing Shared Memory and handles manual PTS timing.
    """
    kind = "video"

    def __init__(self, line_id, manager):
        super().__init__()
        self.line_id    = str(line_id)
        self.manager    = manager
        self._timestamp = 0
        self.fps        = 30
        self.clock_rate = 90000
        self.increment  = int(self.clock_rate / self.fps)

    async def recv(self):
        pts            = self._timestamp
        self._timestamp += self.increment
        time_base      = Fraction(1, self.clock_rate)

        data = self.manager.result_dict.get(self.line_id)

        if data is None:
            img = np.zeros((480, 640, 3), dtype=np.uint8)
            cv2.putText(img, "INITIALIZING AI...", (150, 240),
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
            await asyncio.sleep(1 / self.fps)
        else:
            nparr = np.frombuffer(data['frame'], np.uint8)
            img   = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        frame           = av.VideoFrame.from_ndarray(img, format="bgr24")
        frame.pts       = pts
        frame.time_base = time_base
        return frame


# ── Routes ────────────────────────────────────────────────────
@app.get("/")
async def serve_ui():
    return FileResponse("web/index.html")


@app.post("/offer/{line_id}")
async def offer(line_id: str, request: Request):
    params = await request.json()
    sdp_offer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])

    log_webrtc_offer(line_id)

    pc = RTCPeerConnection()
    pcs.add(pc)

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        if pc.connectionState in ["failed", "closed"]:
            log_webrtc_closed(line_id)
            await pc.close()
            pcs.discard(pc)

    track = ProcessedVideoTrack(line_id, manager)
    pc.addTrack(track)

    await pc.setRemoteDescription(sdp_offer)
    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)

    return {
        "sdp":  pc.localDescription.sdp,
        "type": pc.localDescription.type,
    }


@app.websocket("/ws/stats")
async def websocket_stats(websocket: WebSocket):
    await websocket.accept()
    client = websocket.client.host if websocket.client else "unknown"
    log_ws_connected(client)

    try:
        while True:
            data = manager.get_all_stats()
            await websocket.send_json(data)
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        log_ws_disconnected(client)
    except Exception as exc:
        log_error("WebSocket", "Stats socket error", exc=exc,
                  meta={"client": client})
        log_ws_disconnected(client)


@app.on_event("shutdown")
async def on_shutdown():
    sys_logger.info("FastAPI shutdown event — closing WebRTC peers")
    coros = [pc.close() for pc in pcs]
    await asyncio.gather(*coros)
    pcs.clear()
