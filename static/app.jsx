const { useEffect, useMemo, useRef, useState } = React;

const STATUS_TEXT = {
  normal: "正常",
  pending_review: "待复核",
  mock_fallback: "模拟 / Fallback",
  fetch_failed: "抓取失败",
};

const DATA_KIND_TEXT = {
  real: "真实数据",
  mock: "模拟 / fallback",
};

const RESULT_TEXT = {
  success: "成功",
  partial: "部分完成",
  failure: "失败",
  skipped: "已跳过",
};

const JOB_NAME_TEXT = {
  daily_fetch: "日度抓取任务",
  fetch_primary: "主源抓取",
  fetch_backup: "备源抓取",
  validate_sources: "双源校验",
  mock_backfill: "Fallback 补位",
};

const RANGE_DAYS = {
  "7d": 7,
  "30d": 30,
  "90d": 90,
};

const DEFAULT_SETTINGS_FORM = {
  primary_source: "gas_alberta_public",
  backup_source: "mock_fallback",
  run_time: "08:00",
  diff_threshold_percent: 3,
  retention_policy: "forever",
  enable_validation: false,
  enable_alert: true,
  enable_archive: true,
  available_sources: [],
};

function fetchJSON(url, options = {}) {
  return fetch(url, {
    headers: {
      "Content-Type": "application/json",
    },
    ...options,
  }).then(async (response) => {
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || "请求失败");
    }
    return payload;
  });
}

function formatPrice(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return Number(value).toFixed(3);
}

function formatDateLabel(isoDate) {
  if (!isoDate) return "--";
  const value = new Date(`${isoDate}T00:00:00`);
  return `${String(value.getMonth() + 1).padStart(2, "0")}/${String(value.getDate()).padStart(2, "0")}`;
}

function formatTimestamp(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date);
}

function rangeText(range) {
  if (range === "7d") return "最近 7 天";
  if (range === "30d") return "最近 30 天";
  return "最近 90 天";
}

function retentionText(value) {
  if (value === "forever") return "永久保留";
  if (value === "1095") return "保留 3 年";
  if (value === "365") return "保留 365 天";
  return value || "--";
}

function sourceKindText(item) {
  if (!item) return "--";
  return DATA_KIND_TEXT[item.data_kind] || item.data_kind;
}

function statusText(status) {
  return STATUS_TEXT[status] || status || "--";
}

function resultText(result) {
  return RESULT_TEXT[result] || result || "--";
}

function jobText(jobName) {
  return JOB_NAME_TEXT[jobName] || jobName || "--";
}

function parseNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function buildExportUrl(format, scope, filters) {
  const params = new URLSearchParams();
  params.set("scope", scope);
  if (scope !== "all") {
    if (filters.keyword) params.set("keyword", filters.keyword);
    if (filters.source && filters.source !== "all") params.set("source", filters.source);
    if (filters.status && filters.status !== "all") params.set("status", filters.status);
  }
  return `/api/prices/export.${format}?${params.toString()}`;
}

function Badge({ className = "", children }) {
  return <span className={className}>{children}</span>;
}

function StatusBadge({ status }) {
  return <Badge className={`badge ${status || "normal"}`}>{statusText(status)}</Badge>;
}

function ResultBadge({ result }) {
  return <Badge className={`badge result-badge ${result || "success"}`}>{resultText(result)}</Badge>;
}

function DataKindChip({ kind }) {
  return <Badge className={`chip ${kind || "real"}`}>{DATA_KIND_TEXT[kind] || kind}</Badge>;
}

function MetricCard({ title, value, description, marker }) {
  return (
    <div className="metric-card">
      <div className="metric-card-top">
        <div>
          <div className="metric-title">{title}</div>
          <div className="metric-value">{value}</div>
          <div className="metric-desc">{description}</div>
        </div>
        <div className="metric-badge">{marker}</div>
      </div>
    </div>
  );
}

function LoadingState({ text = "加载中..." }) {
  return <div className="loading-state">{text}</div>;
}

function EmptyState({ text }) {
  return <div className="empty-state">{text}</div>;
}

function ErrorState({ text }) {
  return <div className="error-state">{text}</div>;
}

function ToggleRow({ title, description, checked, onChange }) {
  return (
    <div className="switch-row">
      <div className="switch-copy">
        <strong>{title}</strong>
        <span>{description}</span>
      </div>
      <label className="toggle">
        <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
        <span className="toggle-track"></span>
      </label>
    </div>
  );
}

function TrendChart({ rows, loading, error, range }) {
  const usableRows = rows.filter((row) => row && row.price !== null && row.price !== undefined);
  const [hoverIndex, setHoverIndex] = useState(null);
  const width = 760;
  const height = 330;
  const padding = { top: 20, right: 18, bottom: 38, left: 52 };

  useEffect(() => {
    setHoverIndex(usableRows.length ? usableRows.length - 1 : null);
  }, [usableRows.length]);

  if (loading) {
    return <LoadingState text="趋势数据加载中..." />;
  }

  if (error) {
    return <ErrorState text={`趋势数据加载失败：${error}`} />;
  }

  if (usableRows.length < 2) {
    return <EmptyState text={`${rangeText(range)} 没有足够的数据点。若真实历史不足，系统会明确标识 mock fallback。`} />;
  }

  const prices = usableRows.map((row) => Number(row.price));
  const minPrice = Math.min(...prices);
  const maxPrice = Math.max(...prices);
  const safeRange = maxPrice - minPrice || 1;
  const innerWidth = width - padding.left - padding.right;
  const innerHeight = height - padding.top - padding.bottom;

  const points = usableRows.map((row, index) => {
    const x = padding.left + (innerWidth * index) / Math.max(usableRows.length - 1, 1);
    const y =
      padding.top + innerHeight - ((Number(row.price) - minPrice) / safeRange) * innerHeight;
    return { ...row, x, y };
  });

  const linePath = points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`)
    .join(" ");
  const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${(
    height - padding.bottom
  ).toFixed(2)} L ${points[0].x.toFixed(2)} ${(height - padding.bottom).toFixed(2)} Z`;

  const guideLines = Array.from({ length: 4 }, (_, index) => {
    const ratio = index / 3;
    const y = padding.top + innerHeight * ratio;
    const value = (maxPrice - safeRange * ratio).toFixed(3);
    return { y, value };
  });

  const tickStep = Math.max(1, Math.floor(points.length / 5));
  const xTicks = points.filter((_, index) => index % tickStep === 0 || index === points.length - 1);
  const hoveredPoint = hoverIndex === null ? null : points[hoverIndex];

  function handleMove(event) {
    const rect = event.currentTarget.getBoundingClientRect();
    const ratio = (event.clientX - rect.left) / rect.width;
    const x = padding.left + ratio * innerWidth;
    let nearest = 0;
    let minDistance = Number.POSITIVE_INFINITY;
    points.forEach((point, index) => {
      const distance = Math.abs(point.x - x);
      if (distance < minDistance) {
        minDistance = distance;
        nearest = index;
      }
    });
    setHoverIndex(nearest);
  }

  const realCount = usableRows.filter((row) => row.data_kind === "real").length;
  const mockCount = usableRows.length - realCount;

  return (
    <div>
      <div className="chart-surface">
        <div className="chart-shell">
          <svg
            className="chart-svg"
            viewBox={`0 0 ${width} ${height}`}
            onMouseMove={handleMove}
            onMouseLeave={() => setHoverIndex(points.length - 1)}
          >
            <defs>
              <linearGradient id="trendFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#22c55e" stopOpacity="0.28" />
                <stop offset="100%" stopColor="#22c55e" stopOpacity="0.02" />
              </linearGradient>
            </defs>

            {guideLines.map((line) => (
              <g key={line.value}>
                <line
                  x1={padding.left}
                  x2={width - padding.right}
                  y1={line.y}
                  y2={line.y}
                  stroke="#e2e8f0"
                  strokeDasharray="4 6"
                />
                <text
                  x={padding.left - 12}
                  y={line.y + 4}
                  textAnchor="end"
                  fill="#64748b"
                  fontSize="12"
                >
                  {line.value}
                </text>
              </g>
            ))}

            {xTicks.map((tick) => (
              <text
                key={tick.trade_date}
                x={tick.x}
                y={height - 12}
                textAnchor="middle"
                fill="#64748b"
                fontSize="12"
              >
                {formatDateLabel(tick.trade_date)}
              </text>
            ))}

            <path d={areaPath} fill="url(#trendFill)" />
            <path d={linePath} fill="none" stroke="#16a34a" strokeWidth="2.4" />

            {hoveredPoint ? (
              <g>
                <line
                  x1={hoveredPoint.x}
                  x2={hoveredPoint.x}
                  y1={padding.top}
                  y2={height - padding.bottom}
                  stroke="#86efac"
                  strokeDasharray="3 5"
                />
                <circle cx={hoveredPoint.x} cy={hoveredPoint.y} r="5.2" fill="#16a34a" />
                <circle cx={hoveredPoint.x} cy={hoveredPoint.y} r="10" fill="rgba(34, 197, 94, 0.12)" />
              </g>
            ) : null}
          </svg>

          {hoveredPoint ? (
            <div
              className="chart-tooltip"
              style={{
                left: `${Math.min(92, Math.max(12, (hoveredPoint.x / width) * 100))}%`,
                top: `${Math.max(18, (hoveredPoint.y / height) * 100)}%`,
              }}
            >
              <div className="tooltip-title">{hoveredPoint.trade_date}</div>
              <div className="tooltip-grid">
                <div>价格：{formatPrice(hoveredPoint.price)} {hoveredPoint.unit}</div>
                <div>来源：{hoveredPoint.source}</div>
                <div>状态：{statusText(hoveredPoint.status)}</div>
                <div>类型：{DATA_KIND_TEXT[hoveredPoint.data_kind] || hoveredPoint.data_kind}</div>
              </div>
            </div>
          ) : null}
        </div>
      </div>

      <div className="chart-footer-note">
        <span>{rangeText(range)} 数据点：{usableRows.length}</span>
        <span>真实数据：{realCount}</span>
        <span>模拟 / fallback：{mockCount}</span>
      </div>
    </div>
  );
}

function App() {
  const [range, setRange] = useState("90d");
  const [activeTab, setActiveTab] = useState("table");
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sourceFilter, setSourceFilter] = useState("all");
  const [page, setPage] = useState(1);
  const [notice, setNotice] = useState("");
  const [jobRunning, setJobRunning] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [lastRefreshAt, setLastRefreshAt] = useState("");
  const [pollingPaused, setPollingPaused] = useState(
    typeof document !== "undefined" ? document.hidden : false
  );

  const [latestState, setLatestState] = useState({ loading: true, error: "", item: null, summary: null });
  const [trendState, setTrendState] = useState({ loading: true, error: "", items: [] });
  const [historyState, setHistoryState] = useState({
    loading: true,
    error: "",
    items: [],
    pagination: { page: 1, page_size: 25, total: 0, total_pages: 1 },
    summary: null,
  });
  const [logsState, setLogsState] = useState({ loading: true, error: "", items: [] });
  const [alertsState, setAlertsState] = useState({ loading: true, error: "", items: [] });
  const [settingsForm, setSettingsForm] = useState(DEFAULT_SETTINGS_FORM);
  const [settingsLoading, setSettingsLoading] = useState(true);
  const [settingsError, setSettingsError] = useState("");
  const initialHistoryEffect = useRef(true);
  const refreshInFlightRef = useRef(false);

  useEffect(() => {
    const timer = window.setTimeout(() => setNotice(""), 3600);
    return () => window.clearTimeout(timer);
  }, [notice]);

  useEffect(() => {
    setPage(1);
  }, [search, statusFilter, sourceFilter]);

  async function loadLatest() {
    setLatestState((state) => ({ ...state, loading: true, error: "" }));
    try {
      const payload = await fetchJSON("/api/prices/latest");
      setLatestState({
        loading: false,
        error: "",
        item: payload.item,
        summary: payload.summary,
      });
    } catch (error) {
      setLatestState({ loading: false, error: error.message, item: null, summary: null });
    }
  }

  async function loadTrend() {
    setTrendState((state) => ({ ...state, loading: true, error: "" }));
    const endDate = new Date();
    const startDate = new Date(Date.now() - 119 * 24 * 60 * 60 * 1000);
    const params = new URLSearchParams({
      start_date: startDate.toISOString().slice(0, 10),
      end_date: endDate.toISOString().slice(0, 10),
      page: "1",
      page_size: "200",
    });

    try {
      const payload = await fetchJSON(`/api/prices/history?${params.toString()}`);
      const rows = [...payload.items].sort((left, right) => left.trade_date.localeCompare(right.trade_date));
      setTrendState({ loading: false, error: "", items: rows });
    } catch (error) {
      setTrendState({ loading: false, error: error.message, items: [] });
    }
  }

  async function loadHistory() {
    setHistoryState((state) => ({ ...state, loading: true, error: "" }));
    const params = new URLSearchParams({
      page: String(page),
      page_size: "25",
    });
    if (search.trim()) params.set("keyword", search.trim());
    if (statusFilter !== "all") params.set("status", statusFilter);
    if (sourceFilter !== "all") params.set("source", sourceFilter);

    try {
      const payload = await fetchJSON(`/api/prices/history?${params.toString()}`);
      setHistoryState({
        loading: false,
        error: "",
        items: payload.items,
        pagination: payload.pagination,
        summary: payload.summary,
      });
    } catch (error) {
      setHistoryState((state) => ({ ...state, loading: false, error: error.message }));
    }
  }

  async function loadLogs() {
    setLogsState((state) => ({ ...state, loading: true, error: "" }));
    try {
      const payload = await fetchJSON("/api/jobs/logs?limit=24");
      setLogsState({ loading: false, error: "", items: payload.items });
    } catch (error) {
      setLogsState({ loading: false, error: error.message, items: [] });
    }
  }

  async function loadAlerts() {
    setAlertsState((state) => ({ ...state, loading: true, error: "" }));
    try {
      const payload = await fetchJSON("/api/alerts?limit=24");
      setAlertsState({ loading: false, error: "", items: payload.items });
    } catch (error) {
      setAlertsState({ loading: false, error: error.message, items: [] });
    }
  }

  async function loadSettings() {
    setSettingsLoading(true);
    setSettingsError("");
    try {
      const payload = await fetchJSON("/api/settings");
      setSettingsForm({
        primary_source: payload.primary_source,
        backup_source: payload.backup_source,
        run_time: payload.run_time,
        diff_threshold_percent: payload.diff_threshold_percent,
        retention_policy: payload.retention_policy,
        enable_validation: payload.enable_validation,
        enable_alert: payload.enable_alert,
        enable_archive: payload.enable_archive,
        available_sources: payload.available_sources || [],
      });
      setSettingsLoading(false);
    } catch (error) {
      setSettingsError(error.message);
      setSettingsLoading(false);
    }
  }

  async function refreshDashboard(options = {}) {
    const { includeSettings = false } = options;
    const tasks = [loadLatest(), loadTrend(), loadLogs(), loadAlerts()];
    if (includeSettings) {
      tasks.push(loadSettings());
    }
    await Promise.all(tasks);
  }

  async function refreshPageData(options = {}) {
    const { includeSettings = false, showBusy = false } = options;
    if (refreshInFlightRef.current) {
      return;
    }
    refreshInFlightRef.current = true;
    if (showBusy) {
      setRefreshing(true);
    }
    try {
      await Promise.all([refreshDashboard({ includeSettings }), loadHistory()]);
      setLastRefreshAt(new Date().toISOString());
    } finally {
      refreshInFlightRef.current = false;
      if (showBusy) {
        setRefreshing(false);
      }
    }
  }

  useEffect(() => {
    refreshPageData({ includeSettings: true });
  }, []);

  useEffect(() => {
    if (initialHistoryEffect.current) {
      initialHistoryEffect.current = false;
      return;
    }
    loadHistory().then(() => setLastRefreshAt(new Date().toISOString()));
  }, [page, search, statusFilter, sourceFilter]);

  useEffect(() => {
    function handleVisibilityChange() {
      const hidden = document.hidden;
      setPollingPaused(hidden);
      if (!hidden) {
        refreshPageData();
      }
    }

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => document.removeEventListener("visibilitychange", handleVisibilityChange);
  }, [page, search, statusFilter, sourceFilter]);

  useEffect(() => {
    if (pollingPaused) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      refreshPageData();
    }, 60000);
    return () => window.clearInterval(timer);
  }, [pollingPaused, page, search, statusFilter, sourceFilter]);

  const trendRows = useMemo(() => {
    const required = RANGE_DAYS[range] || 90;
    return trendState.items.slice(Math.max(0, trendState.items.length - required));
  }, [range, trendState.items]);

  const avg30 = useMemo(() => {
    const rows = trendState.items.slice(Math.max(0, trendState.items.length - 30));
    if (!rows.length) return "--";
    const sum = rows.reduce((accumulator, row) => accumulator + parseNumber(row.price), 0);
    return (sum / rows.length).toFixed(3);
  }, [trendState.items]);

  const summary = latestState.summary || historyState.summary;
  const latestItem = latestState.item;
  const sourceOptions = settingsForm.available_sources || [];
  const pendingReviewCount = summary?.status_counts?.pending_review || 0;
  const canonicalTotal = summary?.canonical_total || 0;
  const archiveTotal = summary?.archive_total || 0;
  const realWindowDays = summary?.window_real_days || 0;
  const mockWindowDays = summary?.window_mock_days || 0;
  const filteredTotal = historyState.pagination?.total || 0;

  async function handleRunJob() {
    setJobRunning(true);
    try {
      const payload = await fetchJSON("/api/jobs/run", { method: "POST", body: JSON.stringify({}) });
      setNotice(`抓取任务已完成：${payload.message}`);
      await refreshPageData();
    } catch (error) {
      setNotice(`抓取失败：${error.message}`);
    } finally {
      setJobRunning(false);
    }
  }

  async function handleManualRefresh() {
    try {
      await refreshPageData({ showBusy: true });
      setNotice("页面数据已刷新。");
    } catch (error) {
      setNotice(`刷新失败：${error.message}`);
    }
  }

  async function handleSaveSettings() {
    setSettingsSaving(true);
    try {
      const payload = await fetchJSON("/api/settings", {
        method: "POST",
        body: JSON.stringify({
          primary_source: settingsForm.primary_source,
          backup_source: settingsForm.backup_source,
          run_time: settingsForm.run_time,
          diff_threshold_percent: parseNumber(settingsForm.diff_threshold_percent, 3),
          retention_policy: settingsForm.retention_policy,
          enable_validation: settingsForm.enable_validation,
          enable_alert: settingsForm.enable_alert,
          enable_archive: settingsForm.enable_archive,
        }),
      });
      setSettingsForm((current) => ({ ...current, ...payload, available_sources: payload.available_sources || current.available_sources }));
      setNotice("配置已保存。");
      await loadLatest();
    } catch (error) {
      setNotice(`配置保存失败：${error.message}`);
    } finally {
      setSettingsSaving(false);
    }
  }

  function handleExport(format, scope) {
    const url = buildExportUrl(format, scope, {
      keyword: search,
      source: sourceFilter,
      status: statusFilter,
    });
    window.open(url, "_blank");
  }

  function jumpToTab(nextTab) {
    setActiveTab(nextTab);
    const anchor = document.getElementById("lower-tabs");
    if (anchor) {
      anchor.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  return (
    <div className="app-shell">
      <div className="container">
        <section className="hero">
          <div className="hero-grid">
            <div>
              <div className="eyebrow">
                <span className="dot"></span>
                AECO Daily Price Monitor
              </div>
              <h1>AECO 天然气价格监测工具</h1>
              <p className="lead">
                保留你现有预览页的结构，但把底层切换成真实 API、SQLite 持久化、定时抓取、导出和异常留痕。当前版本主源接入 Gas Alberta 公开 AECO/AB-NIT 日度数据；当公开历史不足时，会明确标记 mock fallback，而不是假装成真实数据。
              </p>
              <div className="actions">
                <button className="button primary" onClick={handleRunJob} disabled={jobRunning}>
                  {jobRunning ? "抓取中..." : "立即抓取"}
                </button>
                <button className="button" onClick={handleManualRefresh} disabled={refreshing || jobRunning}>
                  {refreshing ? "刷新中..." : "刷新页面数据"}
                </button>
                <button className="button" onClick={() => handleExport("csv", "all")}>
                  导出全部历史 CSV
                </button>
                <button className="button" onClick={() => handleExport("json", "all")}>
                  导出全部历史 JSON
                </button>
                <button className="button" onClick={() => jumpToTab("logs")}>
                  查看日志
                </button>
              </div>
              <div className="refresh-strip">
                <span>上次刷新：{lastRefreshAt ? formatTimestamp(lastRefreshAt) : "--"}</span>
                <span>{pollingPaused ? "自动轮询已暂停（页面不在前台）" : "自动轮询：每 60 秒"}</span>
              </div>
            </div>

            <div className="hero-latest">
              {latestState.loading ? (
                <LoadingState text="加载最新价格..." />
              ) : latestState.error ? (
                <ErrorState text={latestState.error} />
              ) : latestItem ? (
                <>
                  <div className="hero-latest-top">
                    <div>
                      <div className="hero-latest-title">最新价格</div>
                      <div className="hero-price">
                        <strong>{formatPrice(latestItem.price)}</strong>
                        <span>{latestItem.unit}</span>
                      </div>
                    </div>
                    <div style={{ display: "grid", gap: "8px", justifyItems: "end" }}>
                      <DataKindChip kind={latestItem.data_kind} />
                      <StatusBadge status={latestItem.status} />
                    </div>
                  </div>
                  <div className="hero-meta-grid">
                    <div className="meta-card">
                      <div className="meta-label">交易日</div>
                      <div className="meta-value">{latestItem.trade_date}</div>
                    </div>
                    <div className="meta-card">
                      <div className="meta-label">数据源</div>
                      <div className="meta-value">{latestItem.source}</div>
                    </div>
                    <div className="meta-card">
                      <div className="meta-label">抓取时间</div>
                      <div className="meta-value">{formatTimestamp(latestItem.fetched_at)}</div>
                    </div>
                    <div className="meta-card">
                      <div className="meta-label">历史保留</div>
                      <div className="meta-value">{retentionText(summary?.retention_policy)}</div>
                    </div>
                    <div className="meta-card">
                      <div className="meta-label">90 天真实覆盖</div>
                      <div className="meta-value">{realWindowDays} 天</div>
                    </div>
                    <div className="meta-card">
                      <div className="meta-label">最后任务状态</div>
                      <div className="meta-value">{summary?.last_job ? resultText(summary.last_job.result) : "--"}</div>
                    </div>
                  </div>
                </>
              ) : (
                <EmptyState text="暂无可展示的价格记录。可先点击“立即抓取”拉起首次数据。"/>
              )}
            </div>
          </div>
        </section>

        <section className="metric-grid">
          <MetricCard
            title="今日价格"
            value={latestItem ? formatPrice(latestItem.price) : "--"}
            description={latestItem ? `最新交易日 ${latestItem.hub}` : "等待首条价格记录"}
            marker="P"
          />
          <MetricCard
            title="30 日均价"
            value={avg30}
            description="按当前 canonical 历史记录计算"
            marker="30"
          />
          <MetricCard
            title="历史归档"
            value={String(archiveTotal)}
            description={`canonical ${canonicalTotal} 条，默认 ${retentionText(summary?.retention_policy)}`}
            marker="DB"
          />
          <MetricCard
            title="待复核记录"
            value={String(pendingReviewCount)}
            description={`90 天窗口真实 ${realWindowDays} 天，fallback ${mockWindowDays} 天`}
            marker="!"
          />
        </section>

        <section className="content-grid">
          <div className="panel">
            <div className="panel-header">
              <div className="panel-title-row">
                <div>
                  <h2 className="panel-title">价格趋势</h2>
                  <p className="panel-description">
                    图表只读取 API 返回的历史记录，不再使用本地生成数组。若真实历史不足，会在图表下方明确提示真实 / fallback 覆盖情况。
                  </p>
                </div>
                <div className="tab-strip">
                  {Object.keys(RANGE_DAYS).map((key) => (
                    <button
                      key={key}
                      className={`tab-button ${range === key ? "active" : ""}`}
                      onClick={() => setRange(key)}
                    >
                      {key === "7d" ? "7 天" : key === "30d" ? "30 天" : "90 天"}
                    </button>
                  ))}
                </div>
              </div>
            </div>
            <div className="panel-body">
              <div className="chart-summary">
                <span>当前范围：{rangeText(range)}</span>
                <span>可视数据点：{trendRows.length}</span>
              </div>
              <TrendChart rows={trendRows} loading={trendState.loading} error={trendState.error} range={range} />
            </div>
          </div>

          <div className="panel">
            <div className="panel-header">
              <h2 className="panel-title">抓取与归档设置</h2>
              <p className="panel-description">先做基础功能，不做权限系统。配置会落库，定时线程会按设置的每日时间自动尝试抓取。</p>
            </div>
            <div className="panel-body">
              {settingsLoading ? (
                <LoadingState text="加载设置..." />
              ) : settingsError ? (
                <ErrorState text={`设置加载失败：${settingsError}`} />
              ) : (
                <div className="form-grid">
                  <div className="field">
                    <label>主数据源</label>
                    <select
                      className="select"
                      value={settingsForm.primary_source}
                      onChange={(event) => setSettingsForm({ ...settingsForm, primary_source: event.target.value })}
                    >
                      {sourceOptions.map((option) => (
                        <option key={option.key} value={option.key}>{option.name}</option>
                      ))}
                    </select>
                  </div>

                  <div className="field">
                    <label>备份数据源</label>
                    <select
                      className="select"
                      value={settingsForm.backup_source}
                      onChange={(event) => setSettingsForm({ ...settingsForm, backup_source: event.target.value })}
                    >
                      {sourceOptions.map((option) => (
                        <option key={option.key} value={option.key}>{option.name}</option>
                      ))}
                    </select>
                  </div>

                  <div className="field">
                    <label>每日抓取时间</label>
                    <input
                      className="input"
                      value={settingsForm.run_time}
                      onChange={(event) => setSettingsForm({ ...settingsForm, run_time: event.target.value })}
                      placeholder="08:00"
                    />
                  </div>

                  <div className="field">
                    <label>差异阈值 (%)</label>
                    <input
                      className="input"
                      type="number"
                      min="0"
                      step="0.1"
                      value={settingsForm.diff_threshold_percent}
                      onChange={(event) =>
                        setSettingsForm({ ...settingsForm, diff_threshold_percent: event.target.value })
                      }
                    />
                  </div>

                  <div className="field">
                    <label>历史数据保留策略</label>
                    <select
                      className="select"
                      value={settingsForm.retention_policy}
                      onChange={(event) => setSettingsForm({ ...settingsForm, retention_policy: event.target.value })}
                    >
                      <option value="365">保留 365 天</option>
                      <option value="1095">保留 3 年</option>
                      <option value="forever">永久保留</option>
                    </select>
                  </div>

                  <div className="switch-group">
                    <ToggleRow
                      title="启用双源校验"
                      description="当前只有在备源具备兼容日度数据时才会执行差异比对；否则会在日志里明确标记为 skipped。"
                      checked={settingsForm.enable_validation}
                      onChange={(checked) => setSettingsForm({ ...settingsForm, enable_validation: checked })}
                    />
                    <ToggleRow
                      title="启用异常提醒"
                      description="抓取失败、单位不一致或差异超阈值时写入 alert_records。"
                      checked={settingsForm.enable_alert}
                      onChange={(checked) => setSettingsForm({ ...settingsForm, enable_alert: checked })}
                    />
                    <ToggleRow
                      title="启用永久归档"
                      description="价格表不做覆盖写入，按 checksum 去重保留抓取痕迹。"
                      checked={settingsForm.enable_archive}
                      onChange={(checked) => setSettingsForm({ ...settingsForm, enable_archive: checked })}
                    />
                  </div>

                  <button className="button dark" onClick={handleSaveSettings} disabled={settingsSaving}>
                    {settingsSaving ? "保存中..." : "保存配置"}
                  </button>
                </div>
              )}
            </div>
          </div>
        </section>

        <section className="lower-section" id="lower-tabs">
          <div className="tabs-head">
            <button className={`tab-button ${activeTab === "table" ? "active" : ""}`} onClick={() => setActiveTab("table")}>
              历史数据
            </button>
            <button className={`tab-button ${activeTab === "logs" ? "active" : ""}`} onClick={() => setActiveTab("logs")}>
              任务日志
            </button>
            <button className={`tab-button ${activeTab === "alerts" ? "active" : ""}`} onClick={() => setActiveTab("alerts")}>
              异常记录
            </button>
          </div>

          {activeTab === "table" ? (
            <div className="panel">
              <div className="panel-header">
                <div className="history-toolbar">
                  <div>
                    <h2 className="panel-title">历史价格记录</h2>
                    <p className="panel-description">
                      支持搜索、分页和导出。表格使用 canonical 历史记录，不一次性渲染全部归档数据。
                    </p>
                  </div>

                  <div className="history-actions">
                    <button className="button small" onClick={() => handleExport("csv", "filtered")}>
                      导出当前筛选 CSV
                    </button>
                    <button className="button small" onClick={() => handleExport("json", "filtered")}>
                      导出当前筛选 JSON
                    </button>
                    <button className="button small" onClick={() => handleExport("csv", "all")}>
                      导出全部 CSV
                    </button>
                    <button className="button small" onClick={() => handleExport("json", "all")}>
                      导出全部 JSON
                    </button>
                  </div>
                </div>
              </div>

              <div className="panel-body">
                <div className="history-toolbar" style={{ marginBottom: "16px" }}>
                  <div className="history-filters">
                    <div className="search-wrap">
                      <span className="search-icon">⌕</span>
                      <input
                        className="input"
                        value={search}
                        onChange={(event) => setSearch(event.target.value)}
                        placeholder="搜索交易日、来源、状态或单位"
                      />
                    </div>

                    <select className="select" value={sourceFilter} onChange={(event) => setSourceFilter(event.target.value)}>
                      <option value="all">全部来源</option>
                      {sourceOptions.map((option) => (
                        <option key={option.key} value={option.key}>{option.name}</option>
                      ))}
                    </select>

                    <select className="select" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                      <option value="all">全部状态</option>
                      <option value="normal">正常</option>
                      <option value="pending_review">待复核</option>
                      <option value="mock_fallback">模拟 / fallback</option>
                      <option value="fetch_failed">抓取失败</option>
                    </select>
                  </div>
                </div>

                <div className="summary-strip">
                  <span>当前结果：{filteredTotal} 条</span>
                  <span>canonical 历史：{canonicalTotal} 条</span>
                  <span>归档总量：{archiveTotal} 条</span>
                  <span>90 天真实覆盖：{realWindowDays} 天</span>
                  <span>90 天 fallback：{mockWindowDays} 天</span>
                </div>

                {historyState.loading ? (
                  <LoadingState text="加载历史表..." />
                ) : historyState.error ? (
                  <ErrorState text={`历史表加载失败：${historyState.error}`} />
                ) : historyState.items.length === 0 ? (
                  <EmptyState text="当前筛选条件下没有历史记录。" />
                ) : (
                  <>
                    <div className="table-wrap">
                      <div className="table-scroll">
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>trade_date</th>
                              <th>hub</th>
                              <th>price</th>
                              <th>unit</th>
                              <th>source</th>
                              <th>fetched_at</th>
                              <th>status</th>
                              <th>数据类型</th>
                            </tr>
                          </thead>
                          <tbody>
                            {historyState.items.map((row) => (
                              <tr key={`${row.trade_date}-${row.source_key}-${row.status}`}>
                                <td>{row.trade_date}</td>
                                <td>{row.hub}</td>
                                <td>{formatPrice(row.price)}</td>
                                <td>{row.unit}</td>
                                <td>{row.source}</td>
                                <td className="table-muted">{formatTimestamp(row.fetched_at)}</td>
                                <td><StatusBadge status={row.status} /></td>
                                <td><DataKindChip kind={row.data_kind} /></td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>

                    <div className="pagination">
                      <div className="pagination-copy">
                        第 {historyState.pagination.page} / {historyState.pagination.total_pages} 页，共 {historyState.pagination.total} 条
                      </div>
                      <div className="pagination-actions">
                        <button
                          className="button small"
                          disabled={historyState.pagination.page <= 1}
                          onClick={() => setPage((value) => Math.max(1, value - 1))}
                        >
                          上一页
                        </button>
                        <button
                          className="button small"
                          disabled={historyState.pagination.page >= historyState.pagination.total_pages}
                          onClick={() =>
                            setPage((value) => Math.min(historyState.pagination.total_pages, value + 1))
                          }
                        >
                          下一页
                        </button>
                      </div>
                    </div>
                  </>
                )}
              </div>
            </div>
          ) : null}

          {activeTab === "logs" ? (
            <div className="panel">
              <div className="panel-header">
                <h2 className="panel-title">任务日志</h2>
                <p className="panel-description">抓取、校验、mock 补位都会写入 fetch_job_log，便于定位每次任务的真实执行结果。</p>
              </div>
              <div className="panel-body">
                {logsState.loading ? (
                  <LoadingState text="加载任务日志..." />
                ) : logsState.error ? (
                  <ErrorState text={`日志加载失败：${logsState.error}`} />
                ) : logsState.items.length === 0 ? (
                  <EmptyState text="暂无日志记录。" />
                ) : (
                  <div className="list-stack">
                    {logsState.items.map((log) => (
                      <div className="log-row" key={log.id}>
                        <div className="log-head">
                          <div className="log-title">
                            <ResultBadge result={log.result} />
                            <span>{jobText(log.job_name)}</span>
                          </div>
                          <div className="log-meta">{formatTimestamp(log.run_at)}</div>
                        </div>
                        <div className="log-copy">
                          <div>来源：{log.source_name || "--"}</div>
                          <div>耗时：{log.duration_ms} ms</div>
                          <div>{log.message || "无额外消息"}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          ) : null}

          {activeTab === "alerts" ? (
            <div className="panel">
              <div className="panel-header">
                <h2 className="panel-title">异常记录</h2>
                <p className="panel-description">这里集中展示抓取失败、真实历史不足、差异待复核和单位异常等告警。</p>
              </div>
              <div className="panel-body">
                {alertsState.loading ? (
                  <LoadingState text="加载异常记录..." />
                ) : alertsState.error ? (
                  <ErrorState text={`异常加载失败：${alertsState.error}`} />
                ) : alertsState.items.length === 0 ? (
                  <EmptyState text="当前没有异常记录。" />
                ) : (
                  <div className="list-stack">
                    {alertsState.items.map((alert) => (
                      <div className="alert-card" key={alert.id}>
                        <div className="alert-head">
                          <div className="alert-title">
                            <StatusBadge status={alert.level === "critical" ? "fetch_failed" : "pending_review"} />
                            <span>{alert.alert_type}</span>
                          </div>
                          <div className="alert-meta">{formatTimestamp(alert.created_at)}</div>
                        </div>
                        <div className="alert-copy">
                          <div>交易日：{alert.trade_date || "--"}</div>
                          <div>来源：{alert.source_name || "--"}</div>
                          <div>{alert.message}</div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          ) : null}
        </section>
      </div>

      {notice ? <div className="toast">{notice}</div> : null}
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
