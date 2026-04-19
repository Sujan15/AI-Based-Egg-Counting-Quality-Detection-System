// (function () {
//   const LINE_IDS = (window.App && window.App.state && window.App.state.lineIds) || [1, 2, 3, 4];
//   const ICE_CONFIG = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }] };
//   const peerConnections = {};
//   let streamsActive = false;

//   async function startWebRTC(lineId, videoElementId) {
//     if (peerConnections[lineId]) return;
//     const videoEl = document.getElementById(videoElementId);
//     if (!videoEl) return;

//     const pc = new RTCPeerConnection(ICE_CONFIG);
//     peerConnections[lineId] = pc;

//     pc.ontrack = function (event) {
//       if (videoEl.srcObject !== event.streams[0]) {
//         videoEl.srcObject = event.streams[0];
//       }
//     };

//     pc.onconnectionstatechange = function () {
//       if (pc.connectionState === "failed" || pc.connectionState === "disconnected") {
//         stopStream(lineId);
//         setTimeout(function () {
//           if (streamsActive) startWebRTC(lineId, videoElementId);
//         }, 5000);
//       }
//     };

//     pc.addTransceiver("video", { direction: "recvonly" });
//     const offer = await pc.createOffer();
//     await pc.setLocalDescription(offer);

//     try {
//       const res = await fetch(`/offer/${lineId}`, {
//         method: "POST",
//         headers: { "Content-Type": "application/json" },
//         body: JSON.stringify({ sdp: pc.localDescription.sdp, type: pc.localDescription.type }),
//       });
//       if (!res.ok) throw new Error(`HTTP ${res.status}`);
//       const answer = await res.json();
//       await pc.setRemoteDescription(new RTCSessionDescription(answer));
//     } catch {
//       stopStream(lineId);
//       setTimeout(function () {
//         if (streamsActive) startWebRTC(lineId, videoElementId);
//       }, 5000);
//     }
//   }

//   function stopStream(lineId) {
//     const pc = peerConnections[lineId];
//     if (pc) {
//       pc.close();
//       delete peerConnections[lineId];
//     }
//     const videoEl = document.getElementById(`video-${lineId}`);
//     if (videoEl) videoEl.srcObject = null;
//   }

//   function startAllStreams() {
//     if (streamsActive) return;
//     streamsActive = true;
//     LINE_IDS.forEach(function (id) {
//       startWebRTC(id, `video-${id}`);
//     });
//   }

//   function stopAllStreams() {
//     streamsActive = false;
//     LINE_IDS.forEach(function (id) {
//       stopStream(id);
//     });
//   }

//   function initOverlay() {
//     const overlay = document.getElementById("single-view-overlay");
//     if (!overlay) return null;

//     const svVideo = document.getElementById("sv-video");
//     const svLineName = document.getElementById("sv-line-name");
//     const svCount = document.getElementById("sv-count");
//     const svQuality = document.getElementById("sv-quality");
//     const svDefects = document.getElementById("sv-defects");
//     const svDotsEl = document.getElementById("sv-dots");
//     let svCurrentIndex = 0;

//     function openOverlay(index) {
//       svCurrentIndex = ((index % LINE_IDS.length) + LINE_IDS.length) % LINE_IDS.length;
//       const lineId = LINE_IDS[svCurrentIndex];

//       const gridVideo = document.getElementById(`video-${lineId}`);
//       if (gridVideo && gridVideo.srcObject && svVideo) {
//         svVideo.srcObject = gridVideo.srcObject;
//       }

//       if (svLineName) svLineName.textContent = `Conveyor Line ${lineId}`;

//       const lastStats = (window.App && window.App.state && window.App.state.lastStats) || {};
//       const st = lastStats[lineId] || { total: 0, broken: 0 };
//       const total = st.total || 0;
//       const broken = st.broken || 0;
//       const defectRate = total > 0 ? ((broken / total) * 100).toFixed(1) : "0.0";

//       if (svCount) svCount.textContent = total.toLocaleString();
//       if (svDefects) svDefects.textContent = defectRate + "%";
//       if (svQuality) svQuality.textContent = (100 - parseFloat(defectRate)).toFixed(1) + "%";

//       if (svDotsEl) {
//         svDotsEl.querySelectorAll(".sv-dot").forEach(function (d, i) {
//           d.classList.toggle("active", i === svCurrentIndex);
//         });
//       }

//       overlay.classList.add("open");
//       document.body.style.overflow = "hidden";
//     }

//     function closeOverlay() {
//       overlay.classList.remove("open");
//       document.body.style.overflow = "";
//       if (svVideo) svVideo.srcObject = null;
//     }

//     function navigateOverlay(delta) {
//       openOverlay(svCurrentIndex + delta);
//     }

//     if (svDotsEl) {
//       svDotsEl.innerHTML = "";
//       LINE_IDS.forEach(function (_id, i) {
//         const dot = document.createElement("span");
//         dot.className = "sv-dot" + (i === 0 ? " active" : "");
//         dot.dataset.index = String(i);
//         dot.addEventListener("click", function () {
//           openOverlay(i);
//         });
//         svDotsEl.appendChild(dot);
//       });
//     }

//     const closeBtn = document.getElementById("sv-close");
//     if (closeBtn) closeBtn.addEventListener("click", closeOverlay);
//     const prevBtn = document.getElementById("sv-prev");
//     if (prevBtn) prevBtn.addEventListener("click", function () { navigateOverlay(-1); });
//     const nextBtn = document.getElementById("sv-next");
//     if (nextBtn) nextBtn.addEventListener("click", function () { navigateOverlay(1); });

//     overlay.addEventListener("click", function (e) {
//       if (e.target === overlay) closeOverlay();
//     });

//     document.addEventListener("keydown", function (e) {
//       if (!overlay.classList.contains("open")) return;
//       if (e.key === "Escape") closeOverlay();
//       if (e.key === "ArrowRight") navigateOverlay(1);
//       if (e.key === "ArrowLeft") navigateOverlay(-1);
//     });

//     LINE_IDS.forEach(function (id, i) {
//       const feed = document.getElementById(`camera-feed-${id}`);
//       if (feed) {
//         feed.addEventListener("click", function () {
//           openOverlay(i);
//         });
//       }
//     });

//     return { overlay, openOverlay, closeOverlay };
//   }

//   function updateCameraView(view, overlayApi) {
//     const cameraGrid = document.getElementById("camera-grid");
//     const cameraCards = document.querySelectorAll(".camera-card");
//     if (!cameraGrid) return;

//     if (view === "single") {
//       if (overlayApi) overlayApi.openOverlay(0);
//     } else {
//       if (overlayApi && overlayApi.overlay.classList.contains("open")) overlayApi.closeOverlay();
//       cameraGrid.classList.remove("full-view");
//       cameraCards.forEach(function (card) {
//         card.style.display = "block";
//       });
//     }
//   }

//   document.addEventListener("DOMContentLoaded", function () {
//     startAllStreams();

//     const overlayApi = initOverlay();

//     document.querySelectorAll(".view-btn").forEach(function (btn) {
//       btn.addEventListener("click", function () {
//         document.querySelectorAll(".view-btn").forEach(function (b) {
//           b.classList.remove("active");
//         });
//         btn.classList.add("active");
//         updateCameraView(btn.getAttribute("data-view"), overlayApi);
//       });
//     });

//     document.addEventListener("visibilitychange", function () {
//       if (document.hidden) stopAllStreams();
//       else startAllStreams();
//     });

//     window.addEventListener("beforeunload", function () {
//       stopAllStreams();
//     });
//   });
// })();


(function () {
  const LINE_IDS = (window.App && window.App.state && window.App.state.lineIds) || [1, 2, 3, 4];
  const ICE_CONFIG = { iceServers: [{ urls: "stun:stun.l.google.com:19302" }] };

  // Per-camera state — entirely local to this browser tab/session
  const peerConnections = {};       // lineId -> RTCPeerConnection
  const streamEnabled   = {};       // lineId -> boolean (user preference)

  // ---------------------------------------------------------------------------
  // Core WebRTC helpers
  // ---------------------------------------------------------------------------

  async function startWebRTC(lineId) {
    if (peerConnections[lineId]) return;           // already connected
    const videoEl = document.getElementById(`video-${lineId}`);
    if (!videoEl) return;

    const pc = new RTCPeerConnection(ICE_CONFIG);
    peerConnections[lineId] = pc;

    pc.ontrack = function (event) {
      if (videoEl.srcObject !== event.streams[0]) {
        videoEl.srcObject = event.streams[0];
      }
    };

    pc.onconnectionstatechange = function () {
      if (pc.connectionState === "failed" || pc.connectionState === "disconnected") {
        _teardown(lineId);
        // Auto-reconnect only if the user still wants this stream ON
        if (streamEnabled[lineId]) {
          setTimeout(function () {
            if (streamEnabled[lineId]) startWebRTC(lineId);
          }, 5000);
        }
      }
    };

    pc.addTransceiver("video", { direction: "recvonly" });
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    // Wait for ICE gathering before sending offer (mirrors original pattern)
    await new Promise(function (resolve) {
      if (pc.iceGatheringState === "complete") { resolve(); return; }
      function check() {
        if (pc.iceGatheringState === "complete") {
          pc.removeEventListener("icegatheringstatechange", check);
          resolve();
        }
      }
      pc.addEventListener("icegatheringstatechange", check);
      setTimeout(resolve, 4000);
    });

    try {
      const res = await fetch(`/offer/${lineId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sdp: pc.localDescription.sdp, type: pc.localDescription.type }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const answer = await res.json();
      await pc.setRemoteDescription(new RTCSessionDescription(answer));
    } catch {
      _teardown(lineId);
      if (streamEnabled[lineId]) {
        setTimeout(function () {
          if (streamEnabled[lineId]) startWebRTC(lineId);
        }, 5000);
      }
    }
  }

  function _teardown(lineId) {
    const pc = peerConnections[lineId];
    if (pc) {
      pc.close();
      delete peerConnections[lineId];
    }
    const videoEl = document.getElementById(`video-${lineId}`);
    if (videoEl) videoEl.srcObject = null;
  }

  function stopStream(lineId) {
    streamEnabled[lineId] = false;
    _teardown(lineId);
  }

  // ---------------------------------------------------------------------------
  // Toggle helpers — update UI + start/stop connection
  // ---------------------------------------------------------------------------

  function _setToggleUI(lineId, isOn) {
    const btn = document.getElementById(`stream-toggle-${lineId}`);
    if (!btn) return;
    if (isOn) {
      btn.innerHTML = '<i class="fas fa-pause-circle"></i> Streaming';
      btn.classList.add("stream-on");
      btn.classList.remove("stream-off");
    } else {
      btn.innerHTML = '<i class="fas fa-play-circle"></i> Paused';
      btn.classList.remove("stream-on");
      btn.classList.add("stream-off");
    }

    // Overlay toggle button (exists only when that line is open in overlay)
    const ovBtn = document.getElementById("sv-stream-toggle");
    if (ovBtn && ovBtn.dataset.lineId === String(lineId)) {
      _syncOverlayToggleUI(isOn, ovBtn);
    }

    // Show/hide the offline placeholder
    const feed = document.getElementById(`camera-feed-${lineId}`);
    if (feed) {
      let placeholder = feed.querySelector(".stream-placeholder");
      if (!isOn) {
        if (!placeholder) {
          placeholder = document.createElement("div");
          placeholder.className = "stream-placeholder";
          placeholder.innerHTML = '<i class="fas fa-video-slash"></i><span>Stream Paused</span>';
          feed.appendChild(placeholder);
        }
      } else {
        if (placeholder) placeholder.remove();
      }
    }
  }

  function _syncOverlayToggleUI(isOn, btn) {
    if (!btn) return;
    if (isOn) {
      btn.innerHTML = '<i class="fas fa-pause-circle"></i> Streaming';
      btn.classList.add("stream-on");
      btn.classList.remove("stream-off");
    } else {
      btn.innerHTML = '<i class="fas fa-play-circle"></i> Paused';
      btn.classList.remove("stream-on");
      btn.classList.add("stream-off");
    }
  }

  function toggleStream(lineId) {
    if (streamEnabled[lineId]) {
      stopStream(lineId);
      _setToggleUI(lineId, false);
    } else {
      streamEnabled[lineId] = true;
      _setToggleUI(lineId, true);
      startWebRTC(lineId);
    }
  }

  // ---------------------------------------------------------------------------
  // Single-view overlay
  // ---------------------------------------------------------------------------

  function initOverlay() {
    const overlay = document.getElementById("single-view-overlay");
    if (!overlay) return null;

    const svVideo   = document.getElementById("sv-video");
    const svLineName= document.getElementById("sv-line-name");
    const svCount   = document.getElementById("sv-count");
    const svQuality = document.getElementById("sv-quality");
    const svDefects = document.getElementById("sv-defects");
    const svDotsEl  = document.getElementById("sv-dots");
    const ovToggle  = document.getElementById("sv-stream-toggle");
    let svCurrentIndex = 0;

    function _refreshOverlayStats(lineId) {
      const lastStats = (window.App && window.App.state && window.App.state.lastStats) || {};
      const st = lastStats[lineId] || { total: 0, broken: 0 };
      const total  = st.total  || 0;
      const broken = st.broken || 0;
      const defectRate = total > 0 ? ((broken / total) * 100).toFixed(1) : "0.0";
      if (svCount)   svCount.textContent   = total.toLocaleString();
      if (svDefects) svDefects.textContent = defectRate + "%";
      if (svQuality) svQuality.textContent = (100 - parseFloat(defectRate)).toFixed(1) + "%";
    }

    function openOverlay(index) {
      svCurrentIndex = ((index % LINE_IDS.length) + LINE_IDS.length) % LINE_IDS.length;
      const lineId   = LINE_IDS[svCurrentIndex];

      // Mirror the grid video's srcObject into the overlay video
      const gridVideo = document.getElementById(`video-${lineId}`);
      if (svVideo) {
        svVideo.srcObject = (gridVideo && gridVideo.srcObject) ? gridVideo.srcObject : null;
      }

      if (svLineName) svLineName.textContent = `Conveyor Line ${lineId}`;
      _refreshOverlayStats(lineId);

      // Sync overlay toggle button
      if (ovToggle) {
        ovToggle.dataset.lineId = String(lineId);
        _syncOverlayToggleUI(!!streamEnabled[lineId], ovToggle);
      }

      if (svDotsEl) {
        svDotsEl.querySelectorAll(".sv-dot").forEach(function (d, i) {
          d.classList.toggle("active", i === svCurrentIndex);
        });
      }

      overlay.classList.add("open");
      document.body.style.overflow = "hidden";
    }

    function closeOverlay() {
      overlay.classList.remove("open");
      document.body.style.overflow = "";
      if (svVideo) svVideo.srcObject = null;
    }

    function navigateOverlay(delta) {
      openOverlay(svCurrentIndex + delta);
    }

    // Overlay stream toggle
    if (ovToggle) {
      ovToggle.addEventListener("click", function (e) {
        e.stopPropagation();
        const lineId = parseInt(ovToggle.dataset.lineId, 10);
        if (!lineId) return;
        toggleStream(lineId);
        // If stream was just enabled, mirror new srcObject when track fires
        if (streamEnabled[lineId]) {
          const gridVideo = document.getElementById(`video-${lineId}`);
          if (gridVideo && gridVideo.srcObject && svVideo) {
            svVideo.srcObject = gridVideo.srcObject;
          }
          // Delay mirror in case track hasn't fired yet
          setTimeout(function () {
            const gv = document.getElementById(`video-${lineId}`);
            if (gv && gv.srcObject && svVideo && !svVideo.srcObject) {
              svVideo.srcObject = gv.srcObject;
            }
          }, 1200);
        } else {
          if (svVideo) svVideo.srcObject = null;
        }
      });
    }

    // Dot navigation
    if (svDotsEl) {
      svDotsEl.innerHTML = "";
      LINE_IDS.forEach(function (_id, i) {
        const dot = document.createElement("span");
        dot.className = "sv-dot" + (i === 0 ? " active" : "");
        dot.dataset.index = String(i);
        dot.addEventListener("click", function () { openOverlay(i); });
        svDotsEl.appendChild(dot);
      });
    }

    const closeBtn = document.getElementById("sv-close");
    if (closeBtn) closeBtn.addEventListener("click", closeOverlay);
    const prevBtn  = document.getElementById("sv-prev");
    if (prevBtn)  prevBtn.addEventListener("click",  function () { navigateOverlay(-1); });
    const nextBtn  = document.getElementById("sv-next");
    if (nextBtn)  nextBtn.addEventListener("click",  function () { navigateOverlay(1);  });

    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) closeOverlay();
    });

    document.addEventListener("keydown", function (e) {
      if (!overlay.classList.contains("open")) return;
      if (e.key === "Escape")       closeOverlay();
      if (e.key === "ArrowRight")   navigateOverlay(1);
      if (e.key === "ArrowLeft")    navigateOverlay(-1);
    });

    // Click on camera feed tile opens overlay (unchanged behaviour)
    LINE_IDS.forEach(function (id, i) {
      const feed = document.getElementById(`camera-feed-${id}`);
      if (feed) {
        feed.addEventListener("click", function (e) {
          // Don't open overlay if user clicked the toggle button inside the header
          if (e.target.closest(".stream-toggle-btn")) return;
          openOverlay(i);
        });
      }
    });

    return { overlay, openOverlay, closeOverlay };
  }

  // ---------------------------------------------------------------------------
  // Grid view toggle
  // ---------------------------------------------------------------------------

  function updateCameraView(view, overlayApi) {
    const cameraGrid = document.getElementById("camera-grid");
    if (!cameraGrid) return;

    if (view === "single") {
      if (overlayApi) overlayApi.openOverlay(0);
    } else {
      if (overlayApi && overlayApi.overlay.classList.contains("open")) overlayApi.closeOverlay();
      cameraGrid.classList.remove("full-view");
      document.querySelectorAll(".camera-card").forEach(function (card) {
        card.style.display = "block";
      });
    }
  }

  // ---------------------------------------------------------------------------
  // Init toggle buttons injected from HTML
  // ---------------------------------------------------------------------------

  function bindToggleButtons() {
    LINE_IDS.forEach(function (id) {
      streamEnabled[id] = false;       // all paused on load
      _setToggleUI(id, false);         // render initial state

      const btn = document.getElementById(`stream-toggle-${id}`);
      if (btn) {
        btn.addEventListener("click", function (e) {
          e.stopPropagation();          // prevent opening overlay
          toggleStream(id);
        });
      }
    });
  }

  // ---------------------------------------------------------------------------
  // Page visibility — stop streams if tab hidden; resume enabled ones on return
  // ---------------------------------------------------------------------------

  function handleVisibilityChange() {
    if (document.hidden) {
      LINE_IDS.forEach(function (id) {
        if (streamEnabled[id]) _teardown(id);
      });
    } else {
      LINE_IDS.forEach(function (id) {
        if (streamEnabled[id]) startWebRTC(id);
      });
    }
  }

  // ---------------------------------------------------------------------------
  // Bootstrap
  // ---------------------------------------------------------------------------

  document.addEventListener("DOMContentLoaded", function () {
    bindToggleButtons();

    const overlayApi = initOverlay();

    document.querySelectorAll(".view-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        document.querySelectorAll(".view-btn").forEach(function (b) { b.classList.remove("active"); });
        btn.classList.add("active");
        updateCameraView(btn.getAttribute("data-view"), overlayApi);
      });
    });

    document.addEventListener("visibilitychange", handleVisibilityChange);

    window.addEventListener("beforeunload", function () {
      LINE_IDS.forEach(function (id) { _teardown(id); });
    });
  });
})();