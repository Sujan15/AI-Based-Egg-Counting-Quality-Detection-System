(function () {
  function initButtons() {
    const startBtn = document.getElementById("startAllBtn");
    if (startBtn) {
      startBtn.addEventListener("click", function () {
        if (window.App && window.App.showNotification) window.App.showNotification("AI Counting Engines Started", "success");
      });
    }

    const pauseBtn = document.getElementById("pauseAllBtn");
    if (pauseBtn) {
      pauseBtn.addEventListener("click", function () {
        if (window.App && window.App.showNotification) window.App.showNotification("AI Counting Engines Paused", "warning");
      });
    }

    const exportBtn = document.getElementById("exportReportBtn");
    if (exportBtn) {
      exportBtn.addEventListener("click", function () {
        if (window.App && window.App.showNotification) window.App.showNotification("Generating Daily Production Report...", "info");
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    initButtons();
  });
})();
