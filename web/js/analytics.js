(function () {
  const charts = {
    sizeChart: null,
    lineChart: null,
    hourlyChart: null,
    defectChart: null,
    lineViewChart: null,
    shadeChart: null,
  };

  function hasCanvas(id) {
    return Boolean(document.getElementById(id));
  }

  function initSizeChart() {
    const el = document.getElementById("sizeChart");
    if (!el) return;
    const ctx = el.getContext("2d");
    charts.sizeChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: ["Small", "Medium", "Higher Medium", "Big", "Large", "Cracked"],
        datasets: [
          {
            label: "Egg Count",
            data: [0, 0, 0, 0, 0, 0],
            backgroundColor: ["#4caf50", "#00ff00", "#ffff00", "#ff9800", "#ff00ff", "#f44336"],
            borderWidth: 0,
            borderRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { position: "top" },
          tooltip: { callbacks: { label: (c) => `${c.raw.toLocaleString()} eggs` } },
        },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "Count" } },
          x: { title: { display: true, text: "Size Category" } },
        },
      },
    });
  }

  function initLineChart() {
    const el = document.getElementById("lineChart");
    if (!el) return;
    const ctx = el.getContext("2d");
    charts.lineChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: ["Line 1", "Line 2", "Line 3", "Line 4"],
        datasets: [
          {
            label: "Total Eggs",
            data: [0, 0, 0, 0],
            backgroundColor: "#4a6fa5",
            borderWidth: 0,
            borderRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { position: "top" },
          tooltip: { callbacks: { label: (c) => `${c.raw.toLocaleString()} eggs` } },
        },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "Egg Count" } },
          x: { title: { display: true, text: "Conveyor Line" } },
        },
      },
    });
  }

  function initLineViewChart() {
    const el = document.getElementById("lineViewChart");
    if (!el) return;
    const ctx = el.getContext("2d");
    charts.lineViewChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: ["Line 1", "Line 2", "Line 3", "Line 4"],
        datasets: [
          { label: "Small", data: [0, 0, 0, 0], backgroundColor: "#4caf50", borderWidth: 0, borderRadius: 6 },
          { label: "Medium", data: [0, 0, 0, 0], backgroundColor: "#00ff00", borderWidth: 0, borderRadius: 6 },
          { label: "Higher Medium", data: [0, 0, 0, 0], backgroundColor: "#ffff00", borderWidth: 0, borderRadius: 6 },
          { label: "Big", data: [0, 0, 0, 0], backgroundColor: "#ff9800", borderWidth: 0, borderRadius: 6 },
          { label: "Large", data: [0, 0, 0, 0], backgroundColor: "#ff00ff", borderWidth: 0, borderRadius: 6 },
          { label: "Cracked", data: [0, 0, 0, 0], backgroundColor: "#f44336", borderWidth: 0, borderRadius: 6 },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { position: "top" },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.raw.toLocaleString()}` } },
        },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "Egg Count" } },
          x: { title: { display: true, text: "Conveyor Line" } },
        },
      },
    });
  }

  function initShadeChart() {
    const el = document.getElementById("shadeChart");
    if (!el) return;
    const ctx = el.getContext("2d");
    charts.shadeChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: [],
        datasets: [
          {
            label: "Total Eggs",
            data: [],
            backgroundColor: "#4a6fa5",
            borderWidth: 0,
            borderRadius: 6,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: { position: "top" },
          tooltip: { callbacks: { label: (c) => `${c.raw.toLocaleString()} eggs` } },
        },
        scales: {
          y: { beginAtZero: true, title: { display: true, text: "Egg Count" } },
          x: { title: { display: true, text: "Shade ID" } },
        },
      },
    });
  }

  async function loadHourlyChart(hours) {
    const el = document.getElementById("hourlyChart");
    if (!el) return;
    try {
      const res = await fetch(`/api/dashboard/hourly?hours=${hours}`);
      const data = await res.json();
      const labels = data.map((row) => row.hour_slot);
      const totals = data.map((row) => row.total_eggs);
      if (charts.hourlyChart) {
        charts.hourlyChart.data.labels = labels;
        charts.hourlyChart.data.datasets[0].data = totals;
        charts.hourlyChart.update();
      } else {
        const ctx = el.getContext("2d");
        charts.hourlyChart = new Chart(ctx, {
          type: "line",
          data: { labels, datasets: [{ label: "Eggs per hour", data: totals, borderColor: "#2196f3", fill: false }] },
          options: { responsive: true, maintainAspectRatio: true },
        });
      }
    } catch {
      return;
    }
  }

  async function loadDefectChart(hours) {
    const el = document.getElementById("defectChart");
    if (!el) return;
    try {
      const res = await fetch(`/api/dashboard/defect-trend?hours=${hours}`);
      const data = await res.json();
      const labels = data.map((row) => row.hour_slot);
      const crackRates = data.map((row) => row.crack_rate_pct);
      if (charts.defectChart) {
        charts.defectChart.data.labels = labels;
        charts.defectChart.data.datasets[0].data = crackRates;
        charts.defectChart.update();
      } else {
        const ctx = el.getContext("2d");
        charts.defectChart = new Chart(ctx, {
          type: "line",
          data: { labels, datasets: [{ label: "Crack Rate (%)", data: crackRates, borderColor: "#f44336", fill: false }] },
          options: { responsive: true, maintainAspectRatio: true, scales: { y: { max: 100 } } },
        });
      }
    } catch {
      return;
    }
  }

  async function fetchShadePerformance() {
    if (!charts.shadeChart) return;
    try {
      const res = await fetch("/api/dashboard/shed-performance");
      const data = await res.json();
      const labels = data.map((row) => row.shed_id);
      const totals = data.map((row) => row.total_eggs);
      charts.shadeChart.data.labels = labels;
      charts.shadeChart.data.datasets[0].data = totals;
      charts.shadeChart.update();
    } catch {
      return;
    }
  }

  function updateBestLine(allLines, lineTotals) {
    const bestLineEl = document.getElementById("bestLine");
    const bestLineCountEl = document.getElementById("bestLineCount");
    const bestQualityLineEl = document.getElementById("bestQualityLine");
    const bestQualityRateEl = document.getElementById("bestQualityRate");
    if (!bestLineEl && !bestLineCountEl && !bestQualityLineEl && !bestQualityRateEl) return;

    const lineData = [1, 2, 3, 4].map((id) => lineTotals[id] || 0);

    let bestLine = 0;
    let bestCount = 0;
    for (let i = 0; i < lineData.length; i++) {
      if (lineData[i] > bestCount) {
        bestCount = lineData[i];
        bestLine = i + 1;
      }
    }

    if (bestLineEl) bestLineEl.innerText = bestLine ? `Line ${bestLine}` : "-";
    if (bestLineCountEl) bestLineCountEl.innerText = bestCount.toLocaleString();

    let bestQualityLine = 0;
    let bestRate = 100;
    for (const id of [1, 2, 3, 4]) {
      const stats = allLines[id] && allLines[id].stats;
      if (stats && stats.total > 0) {
        const rate = (stats.broken / stats.total) * 100;
        if (rate < bestRate) {
          bestRate = rate;
          bestQualityLine = id;
        }
      }
    }

    if (bestQualityLineEl) bestQualityLineEl.innerText = bestQualityLine ? `Line ${bestQualityLine}` : "-";
    if (bestQualityRateEl) bestQualityRateEl.innerText = bestQualityLine ? (100 - bestRate).toFixed(1) : "0";
  }

  function registerUpdates() {
    if (!window.App || typeof window.App.registerStatsHandler !== "function") return;

    window.App.registerStatsHandler(function ({ allLines, sizeTotals, lineTotals }) {
      if (charts.sizeChart) {
        charts.sizeChart.data.datasets[0].data = [
          sizeTotals.SMALL,
          sizeTotals.MEDIUM,
          sizeTotals.HIGHER_MEDIUM,
          sizeTotals.BIG,
          sizeTotals.LARGE,
          sizeTotals.CRACKED,
        ];
        charts.sizeChart.update();
      }

      if (charts.lineChart) {
        const data = [1, 2, 3, 4].map((id) => lineTotals[id] || 0);
        charts.lineChart.data.datasets[0].data = data;
        charts.lineChart.update();
        updateBestLine(allLines, lineTotals);
      }

      if (charts.lineViewChart) {
        const small = [1, 2, 3, 4].map((id) => (allLines[id] && allLines[id].stats && allLines[id].stats.details && allLines[id].stats.details.SMALL) || 0);
        const med = [1, 2, 3, 4].map((id) => (allLines[id] && allLines[id].stats && allLines[id].stats.details && allLines[id].stats.details.MEDIUM) || 0);
        const higherMed = [1, 2, 3, 4].map(
          (id) => (allLines[id] && allLines[id].stats && allLines[id].stats.details && allLines[id].stats.details.HIGHER_MEDIUM) || 0,
        );
        const big = [1, 2, 3, 4].map((id) => (allLines[id] && allLines[id].stats && allLines[id].stats.details && allLines[id].stats.details.BIG) || 0);
        const large = [1, 2, 3, 4].map((id) => (allLines[id] && allLines[id].stats && allLines[id].stats.details && allLines[id].stats.details.LARGE) || 0);
        const cracked = [1, 2, 3, 4].map((id) => (allLines[id] && allLines[id].stats && allLines[id].stats.details && allLines[id].stats.details.Cracked) || 0);

        charts.lineViewChart.data.datasets[0].data = small;
        charts.lineViewChart.data.datasets[1].data = med;
        charts.lineViewChart.data.datasets[2].data = higherMed;
        charts.lineViewChart.data.datasets[3].data = big;
        charts.lineViewChart.data.datasets[4].data = large;
        charts.lineViewChart.data.datasets[5].data = cracked;
        charts.lineViewChart.update();
      }
    });
  }

  function initRangeListeners() {
    const hourlyRange = document.getElementById("hourlyRange");
    if (hourlyRange) {
      hourlyRange.addEventListener("change", function (e) {
        loadHourlyChart(e.target.value);
      });
    }

    const defectRange = document.getElementById("defectRange");
    if (defectRange) {
      defectRange.addEventListener("change", function (e) {
        loadDefectChart(e.target.value);
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    if (typeof Chart !== "function") return;

    if (hasCanvas("sizeChart")) initSizeChart();
    if (hasCanvas("lineChart")) initLineChart();
    if (hasCanvas("lineViewChart")) initLineViewChart();
    if (hasCanvas("shadeChart")) initShadeChart();

    registerUpdates();
    initRangeListeners();

    const hourlyRange = document.getElementById("hourlyRange");
    if (hourlyRange) loadHourlyChart(hourlyRange.value || 24);
    const defectRange = document.getElementById("defectRange");
    if (defectRange) loadDefectChart(defectRange.value || 24);

    if (charts.shadeChart) {
      fetchShadePerformance();
      setInterval(fetchShadePerformance, 5000);
    }
  });
})();
