(function () {
  const lineIds = [1, 2, 3, 4];
  const state = { lineIds, lastStats: {} };
  const statsHandlers = new Set();

  function registerStatsHandler(handler) {
    statsHandlers.add(handler);
    return function unregister() {
      statsHandlers.delete(handler);
    };
  }

  function showNotification(message, type) {
    const n = document.createElement("div");
    n.style.cssText =
      "position:fixed; top:100px; right:20px; padding:15px 20px; border-radius:8px; color:white; font-weight:500; box-shadow:0 4px 12px rgba(0,0,0,0.15); z-index:10000;";
    n.style.backgroundColor =
      type === "success"
        ? "#4caf50"
        : type === "warning"
          ? "#ff9800"
          : type === "error"
            ? "#f44336"
            : "#2196f3";
    n.textContent = message;
    document.body.appendChild(n);
    setTimeout(function () {
      n.remove();
    }, 3000);
  }

  function calculateEggsPerHour(totalEggsToday) {
    const now = new Date();
    const hoursSinceMidnight = now.getHours() + now.getMinutes() / 60;
    if (hoursSinceMidnight <= 0) return 0;
    return Math.round(totalEggsToday / hoursSinceMidnight);
  }

  function setTextById(id, value) {
    const el = document.getElementById(id);
    if (!el) return;
    el.innerText = value;
  }

  function setTextBySelector(selector, value) {
    const el = document.querySelector(selector);
    if (!el) return;
    el.innerText = value;
  }

  function startStatsSocket() {
    const proto = window.location.protocol === "https:" ? "wss" : "ws";
    const socket = new WebSocket(`${proto}://${window.location.host}/ws/stats`);

    socket.onmessage = function (event) {
      let allLines;
      try {
        allLines = JSON.parse(event.data);
      } catch {
        return;
      }

      let totalCount = 0;
      let totalBroken = 0;
      const sizeTotals = { SMALL: 0, MEDIUM: 0, HIGHER_MEDIUM: 0, BIG: 0, LARGE: 0, CRACKED: 0 };
      const lineTotals = {};

      Object.keys(allLines || {}).forEach(function (id) {
        const data = allLines[id];
        if (!data || !data.stats) return;

        const details = data.stats.details || {};
        sizeTotals.SMALL += details.SMALL || 0;
        sizeTotals.MEDIUM += details.MEDIUM || 0;
        sizeTotals.HIGHER_MEDIUM += details.HIGHER_MEDIUM || 0;
        sizeTotals.BIG += details.BIG || 0;
        sizeTotals.LARGE += details.LARGE || 0;
        sizeTotals.CRACKED += details.Cracked || 0;

        const lineTotal = data.stats.total || 0;
        const broken = data.stats.broken || 0;
        lineTotals[id] = lineTotal;
        totalCount += lineTotal;
        totalBroken += broken;

        const countEl = document.getElementById(`line${id}-count`);
        const qualityEl = document.getElementById(`line${id}-quality`);
        const defectsEl = document.getElementById(`line${id}-defects`);

        if (countEl) countEl.innerText = lineTotal.toLocaleString();
        const defectRatePct = lineTotal > 0 ? ((broken / lineTotal) * 100).toFixed(1) : "0.0";
        if (defectsEl) defectsEl.innerText = defectRatePct + "%";
        if (qualityEl) qualityEl.innerText = (100 - parseFloat(defectRatePct)).toFixed(1) + "%";

        state.lastStats[id] = { total: lineTotal, broken };
      });

      setTextById("totalEggs", totalCount.toLocaleString());
      setTextById("brokenEggs", totalBroken.toLocaleString());
      setTextById("qualityEggs", (totalCount - totalBroken).toLocaleString());

      const speed = calculateEggsPerHour(totalCount);
      setTextById("processingSpeed", speed.toLocaleString());
      setTextBySelector("#speedChange span", `${speed.toLocaleString()} eggs/hour`);

      const totalEggsTodayEl = document.getElementById("totalEggsToday");
      if (totalEggsTodayEl) totalEggsTodayEl.innerText = totalCount.toLocaleString();
      const totalCrackedTodayEl = document.getElementById("totalCrackedToday");
      if (totalCrackedTodayEl) totalCrackedTodayEl.innerText = totalBroken.toLocaleString();
      const crackRateEl = document.getElementById("crackRate");
      if (crackRateEl) crackRateEl.innerText = totalCount > 0 ? ((totalBroken / totalCount) * 100).toFixed(1) : "0";

      statsHandlers.forEach(function (handler) {
        try {
          handler({ allLines, totalCount, totalBroken, sizeTotals, lineTotals });
        } catch {
          return;
        }
      });
    };

    socket.onerror = function () {
      return;
    };
  }

  window.App = { registerStatsHandler, showNotification, calculateEggsPerHour, state };

  if (typeof window !== "undefined") {
    try {
      startStatsSocket();
    } catch {
      return;
    }
  }
})();
