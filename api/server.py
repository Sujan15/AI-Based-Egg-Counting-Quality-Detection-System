# api/server.py


import yaml
import asyncio
import uuid
import os
from fractions import Fraction

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, Response
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

# -- Standard console logger -----------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EggTrackAI")

# -- ICE / STUN configuration ----------------------------------
# On a remote server, aiortc must know STUN so it can discover its
# public IP and include reachable ICE candidates in the SDP answer.
# TURN is required when both peers are behind strict NAT/firewall.
# Set TURN_URL / TURN_USER / TURN_PASS env vars to enable TURN.
_ICE_SERVERS = [{"urls": "stun:stun.l.google.com:19302"}]
_TURN_URL  = os.environ.get("TURN_URL")
_TURN_USER = os.environ.get("TURN_USER")
_TURN_PASS = os.environ.get("TURN_PASS")
if _TURN_URL:
    _ICE_SERVERS.append({
        "urls":       _TURN_URL,
        "username":   _TURN_USER or "",
        "credential": _TURN_PASS or "",
    })
    logger.info("TURN server configured: %s", _TURN_URL)
else:
    logger.info(
        "No TURN server configured. If streaming fails on your server, "
        "set TURN_URL / TURN_USER / TURN_PASS environment variables."
    )


# api/server.py – add this block after the existing imports and before `app = FastAPI()`

import multiprocessing as mp
import atexit
import core.logger_setup as _ls
import services.db_writer as db_svc
from core.logger_setup import start_log_listener, stop_log_listener

# ── Shared mp.Manager — injected by main.py BEFORE uvicorn starts ─
# main.py does:  import api.server as _srv; _srv._mp_manager = mp_manager
# This makes _mp_manager available here when MultiLineManager is created.
_mp_manager = None

# ----------------------------------------------------------------------
# Bootstrap initialisation for direct uvicorn runs (without main.py)
# ----------------------------------------------------------------------
def _bootstrap_system():
    """Initialise logging, DB writer and shared manager if not already done."""
    global _mp_manager

    # Already initialised? (log queue exists)
    if _ls._log_queue is not None:
        # Ensure _mp_manager is set from the DB writer (main.py case)
        if _mp_manager is None:
            _mp_manager = db_svc.get_manager()
        return

    # --- First-time initialisation (direct uvicorn run) ---
    log_queue = start_log_listener()
    _ls._log_queue = log_queue

    # Create one shared manager for both result_dict and DB queue
    mp_manager = mp.Manager()

    # Initialise DB writer (creates the shared event queue)
    db_svc.initialize(mp_manager)

    # Use the same manager for MultiLineManager
    _mp_manager = mp_manager

    # Register graceful shutdown handlers
    @atexit.register
    def _shutdown():
        db_svc.shutdown()
        stop_log_listener()
        mp_manager.shutdown()

    # Also register with FastAPI shutdown event (when available)
    try:
        from fastapi import FastAPI
        # The app object will be created later; we add a shutdown hook dynamically.
        # We'll do this after app creation, but atexit already covers it.
    except ImportError:
        pass

# Execute bootstrap immediately
_bootstrap_system()

# ----------------------------------------------------------------------
# Rest of the original server.py continues unchanged
# ----------------------------------------------------------------------


app = FastAPI()

from api.dashboard_routes import dashboard_router
app.include_router(dashboard_router)

from api.analytics_routes import analytics_router
app.include_router(analytics_router)

# # ── Shared mp.Manager — injected by main.py BEFORE uvicorn starts ─
# # main.py does:  import api.server as _srv; _srv._mp_manager = mp_manager
# # This makes _mp_manager available here when MultiLineManager is created.
# _mp_manager = None

app.mount("/web", StaticFiles(directory="web"), name="web")

# -- Shed management routes (Oracle) ----------------------
from api.shed_routes import shed_router
app.include_router(shed_router)

with open("config/settings.yaml", "r") as f:
    settings = yaml.safe_load(f)
with open("config/cameras.yaml", "r") as f:
    cameras = yaml.safe_load(f)

sys_logger.info(
    f'Configuration loaded | {{"lines":{len(cameras["conveyor_lines"])},'
    f'"detection_model":"{settings["models"]["detection"]}"}}'
)

manager = MultiLineManager(cameras, settings, _mp_manager)
pcs: set = set()


# -- WebRTC Video Track (unchanged) ---------------------------
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


# -- Routes ----------------------------------------------------
@app.get("/")
async def serve_ui():
    return RedirectResponse(url="/web/index.html")


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


@app.post("/offer/{line_id}")
async def offer(line_id: str, request: Request):
    params    = await request.json()
    sdp_offer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])

    log_webrtc_offer(line_id)

    # Pass ICE servers so aiortc can gather server-reflexive (srflx)
    # candidates via STUN — essential when running on a remote server.
    from aiortc import RTCConfiguration, RTCIceServer
    ice_servers = [RTCIceServer(urls=s["urls"],
                                username=s.get("username"),
                                credential=s.get("credential"))
                   for s in _ICE_SERVERS]
    pc = RTCPeerConnection(configuration=RTCConfiguration(iceServers=ice_servers))
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

    # -- Wait for ICE gathering to complete --------------------
    # On a remote server, STUN gathering takes up to 3 seconds.
    # Sending the answer before it completes means the SDP contains
    # no reachable candidates — the browser connects but gets no video.
    # We wait up to 4 seconds; if gathering is already complete we
    # return immediately (typical on localhost).
    ice_gathering_complete = asyncio.Event()

    @pc.on("icegatheringstatechange")
    def on_ice_gathering():
        if pc.iceGatheringState == "complete":
            ice_gathering_complete.set()

    if pc.iceGatheringState != "complete":
        try:
            await asyncio.wait_for(ice_gathering_complete.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            logger.warning(
                "ICE gathering timed out for line %s "
                "sending partial candidates. "
                "Consider configuring a TURN server via TURN_URL env var.",
                line_id
            )

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
    sys_logger.info("FastAPI shutdown event - closing WebRTC peers")
    coros = [pc.close() for pc in pcs]
    await asyncio.gather(*coros)
    pcs.clear()
