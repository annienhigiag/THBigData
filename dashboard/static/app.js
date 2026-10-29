// =====================================================================
//  CineData - giao diện web của đồ án Big Data
// =====================================================================
const $ = (s) => document.querySelector(s);
const fmt = (n) => (n == null || isNaN(n) ? "–" : Number(n).toLocaleString("vi-VN"));
const fmt2 = (n) => (n == null || isNaN(n) ? "–" : Number(n).toFixed(2));
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const api = (p) => fetch(p).then((r) => (r.ok ? r.json() : Promise.reject(r.status)));

const PAGES = {
  overview: ["Tổng quan", "Toàn cảnh hệ thống và hoạt động người dùng"],
  realtime: ["Realtime", "Sự kiện người dùng đọc trực tiếp từ Kafka topic movie-events"],
  analytics: ["Phân tích dữ liệu", "Kết quả truy vấn Spark SQL trên dữ liệu HDFS"],
  behavior: ["Hành vi người dùng", "Phân tích dữ liệu streaming đã lưu trên HDFS"],
  recommend: ["Gợi ý phim", "Mô hình Collaborative Filtering (ALS) huấn luyện bằng Spark MLlib"],
  explore: ["Khám phá phim", "Tra cứu danh mục phim MovieLens"],
  system: ["Hệ thống", "Trạng thái các dịch vụ và kiến trúc pipeline"],
};
const EVENT_VI = { view: "Xem trang", rate: "Chấm điểm", click_trailer: "Xem trailer", add_to_watchlist: "Thêm DS xem", watch_complete: "Xem hết" };
const PALETTE = ["#ff3d68", "#7c5cff", "#22c55e", "#f59e0b", "#3b82f6", "#ec4899", "#14b8a6", "#a855f7", "#eab308", "#64748b"];

// ---------------- Chart.js mặc định (theme tối) ----------------
if (window.Chart) {
  Chart.defaults.color = "#8b95ad";
  Chart.defaults.borderColor = "#242c40";
  Chart.defaults.font.family = "Inter, Segoe UI, system-ui, sans-serif";
  Chart.defaults.plugins.legend.labels.usePointStyle = true;
  Chart.defaults.maintainAspectRatio = false;
}
const charts = {};
function chart(id, config) {
  const el = document.getElementById(id);
  if (!el || !window.Chart) return;
  if (charts[id]) {
    charts[id].data = config.data;
    charts[id].update("none");
    return charts[id];
  }
  charts[id] = new Chart(el, config);
  return charts[id];
}
function gradient(id, c1 = "#ff3d68") {
  const ctx = document.getElementById(id)?.getContext("2d");
  if (!ctx) return c1;
  const g = ctx.createLinearGradient(0, 0, 0, 300);
  g.addColorStop(0, c1 + "88");
  g.addColorStop(1, c1 + "00");
  return g;
}
const barOpts = (horizontal = false, extra = {}) => ({
  indexAxis: horizontal ? "y" : "x",
  plugins: { legend: { display: false } },
  scales: { x: { grid: { display: horizontal } }, y: { grid: { display: !horizontal } } },
  borderRadius: 6,
  ...extra,
});

// ---------------- poster giả lập từ tên phim ----------------
function hue(n) { return (Number(n) * 47) % 360; }
function initials(t) {
  return String(t || "?").replace(/^(The|A|An)\s+|,\s*(The|A|An)$/gi, "").split(/\s+/).filter(Boolean).slice(0, 2).map((w) => w[0]).join("").toUpperCase();
}
function posterBg(id) {
  const h = hue(id);
  return `linear-gradient(160deg, hsl(${h} 70% 42%), hsl(${(h + 60) % 360} 60% 18%) 70%, #0a0d14)`;
}
function stars(r) {
  const n = Math.round(Number(r) || 0);
  return "★".repeat(Math.min(5, n)) + "☆".repeat(Math.max(0, 5 - n));
}
function posterCard(m, opts = {}) {
  const title = m.title ?? "";
  const genres = String(m.genres || "").split("|").slice(0, 3).join(" · ");
  return `<div class="poster">
    <div class="art" style="background:${posterBg(m.movieId ?? m.movie_id ?? title.length)}">
      <div class="initials">${esc(initials(title))}</div>
      ${opts.badge ? `<span class="badge">${opts.badge}</span>` : ""}
      ${opts.score != null ? `<span class="badge r">★ ${fmt2(opts.score)}</span>` : ""}
      <div class="ttl">${esc(title)}</div>
    </div>
    <div class="info"><div class="g">${esc(genres || "—")}</div>${opts.foot ? `<div>${opts.foot}</div>` : ""}</div>
  </div>`;
}
function kpi(label, value, hint = "", glow = "#7c5cff22") {
  return `<div class="kpi" style="--glow:${glow}"><div class="label">${label}</div><div class="value">${value}</div><div class="hint">${hint}</div></div>`;
}
function table(cols, rows) {
  if (!rows || !rows.length) return `<div class="empty">Chưa có dữ liệu</div>`;
  return `<table><thead><tr>${cols.map((c) => `<th class="${c.num ? "num" : ""}">${c.h}</th>`).join("")}</tr></thead><tbody>${rows
    .map((r, i) => `<tr>${cols.map((c) => `<td class="${c.num ? "num" : ""}">${c.f ? c.f(r, i) : esc(r[c.k])}</td>`).join("")}</tr>`)
    .join("")}</tbody></table>`;
}
function ago(iso) {
  const s = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (s < 60) return `${s}s trước`;
  if (s < 3600) return `${Math.round(s / 60)} phút trước`;
  return `${Math.round(s / 3600)} giờ trước`;
}

// =====================================================================
//  REALTIME (dùng cho trang Tổng quan + Realtime)
// =====================================================================
let lastRealtime = null;
async function loadRealtime() {
  let d;
  try { d = await api("/api/realtime"); } catch { d = { ok: false, error: "API lỗi" }; }
  lastRealtime = d;
  const dot = $("#liveDot"), txt = $("#liveText"), pill = $("#navLive");
  dot.className = "status-dot " + (d.ok && d.live ? "on" : d.ok ? "idle" : "off");
  txt.textContent = !d.ok ? "Kafka chưa sẵn sàng" : d.empty ? "Chưa có sự kiện" : d.live ? "Đang nhận dữ liệu" : "Generator đang dừng";
  pill.classList.toggle("on", !!(d.ok && d.live));
  renderOverviewRealtime(d);
  renderRealtimePage(d);
}

function renderOverviewRealtime(d) {
  const k = d.kpi || {};
  $("#ovKpis").innerHTML =
    kpi("⚡ Sự kiện / phút", fmt(k.events_last_min), "đọc trực tiếp từ Kafka", "#ff3d6826") +
    kpi("👥 Người dùng hoạt động", fmt(k.active_users_5m), "trong 5 phút", "#7c5cff26") +
    kpi("⭐ Lượt chấm điểm", fmt(k.ratings_5m), `điểm TB ${fmt2(k.avg_rating_5m)}`, "#f59e0b26") +
    kpi("🆕 Người dùng mới", fmt(k.new_users_5m), "đăng ký trong 5 phút", "#22c55e26");
  const tr = d.trending || [];
  $("#ovTrending").innerHTML = tr.length
    ? tr.slice(0, 6).map((m, i) => `<div class="trend">
        <div class="no">${i + 1}</div>
        <div class="mini-poster" style="background:${posterBg(m.movie_id)}">${esc(initials(m.title))}</div>
        <div style="min-width:0"><div class="t-title">${esc(m.title)} <span class="muted small">${m.year ? "(" + m.year + ")" : ""}</span></div>
        <div class="t-meta">${esc(String(m.genres || "").split("|").slice(0, 3).join(" · "))}</div></div>
        <div class="t-num"><b>${fmt(m.events)}</b><div class="muted small">${fmt(m.users)} người xem</div></div></div>`).join("")
    : `<div class="empty">${d.ok ? "Chưa có sự kiện. Hãy chạy DAG 02_streaming_user_events." : "Không kết nối được Kafka."}</div>`;
  if (d.timeline) {
    chart("ovTimeline", { type: "line", data: { labels: d.timeline.labels, datasets: [{ data: d.timeline.values, fill: true, tension: .35, borderColor: "#ff3d68", backgroundColor: gradient("ovTimeline"), pointRadius: 0, borderWidth: 2 }] },
      options: { plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true }, x: { grid: { display: false }, ticks: { maxTicksLimit: 6 } } } } });
  }
}

function renderRealtimePage(d) {
  const k = d.kpi || {};
  $("#rtKpis").innerHTML =
    kpi("Sự kiện / phút", fmt(k.events_last_min), "", "#ff3d6826") +
    kpi("Người dùng hoạt động (5′)", fmt(k.active_users_5m), "", "#7c5cff26") +
    kpi("Lượt chấm điểm (5′)", fmt(k.ratings_5m), "", "#f59e0b26") +
    kpi("Điểm TB (5′)", fmt2(k.avg_rating_5m), "", "#22c55e26") +
    kpi("Sự kiện mới nhất", k.last_event ? new Date(k.last_event).toLocaleTimeString("vi-VN") : "–", k.last_event ? ago(k.last_event) : "", "#3b82f626");
  const nt = $("#rtNotice");
  nt.hidden = !!(d.ok && d.live);
  nt.innerHTML = !d.ok ? `Không kết nối được Kafka: ${esc(d.error || "")}`
    : d.empty ? "Topic <b>movie-events</b> chưa có sự kiện. Hãy trigger DAG <b>02_streaming_user_events</b> hoặc chạy script sinh dữ liệu."
    : "Không có sự kiện mới trong 5 phút qua (script sinh dữ liệu đang dừng). Đang hiển thị các sự kiện gần nhất.";
  if (!d.ok || d.empty) return;

  chart("rtTimeline", { type: "bar", data: { labels: d.timeline.labels, datasets: [{ data: d.timeline.values, backgroundColor: "#ff3d68cc", borderRadius: 4 }] },
    options: barOpts(false, { scales: { x: { grid: { display: false }, ticks: { maxTicksLimit: 8 } } } }) });
  const et = Object.entries(d.event_types);
  chart("rtTypes", { type: "doughnut", data: { labels: et.map(([k]) => EVENT_VI[k] || k), datasets: [{ data: et.map(([, v]) => v), backgroundColor: PALETTE, borderWidth: 0 }] },
    options: { cutout: "62%", plugins: { legend: { position: "bottom" } } } });
  const ct = Object.entries(d.cities);
  chart("rtCities", { type: "bar", data: { labels: ct.map(([k]) => k), datasets: [{ data: ct.map(([, v]) => v), backgroundColor: "#7c5cffcc" }] }, options: barOpts(true) });
  const dv = Object.entries(d.devices);
  chart("rtDevices", { type: "bar", data: { labels: dv.map(([k]) => k), datasets: [{ data: dv.map(([, v]) => v), backgroundColor: PALETTE.slice(2) }] }, options: barOpts() });

  const max = Math.max(...d.trending.map((m) => m.events), 1);
  $("#rtTrending").innerHTML = table([
    { h: "#", f: (r, i) => `<span class="rank ${i < 3 ? "top" : ""}">${i + 1}</span>` },
    { h: "Phim", f: (r) => `<b>${esc(r.title)}</b> <span class="muted small">${r.year || ""}</span>` },
    { h: "Sự kiện", f: (r) => `<div class="bar-cell"><i style="width:${(r.events / max) * 90}px"></i>${r.events}</div>` },
    { h: "Lượt xem", k: "views", num: true },
    { h: "Điểm TB", f: (r) => (r.avg_rating ? `<span class="stars">★</span> ${fmt2(r.avg_rating)}` : "–"), num: true },
  ], d.trending);
  $("#rtFeed").innerHTML = d.feed.map((e) => `<div class="ev">
      <span class="tag t-${e.event_type}">${EVENT_VI[e.event_type] || e.event_type}</span>
      <span class="who">User ${e.user_id}</span>
      <span class="what">${esc(e.title)}${e.rating ? ` <span class="stars">${stars(e.rating)}</span>` : ""}</span>
      <span class="when">${esc(e.city)} · ${esc(e.device)} · ${ago(e.event_time)}</span></div>`).join("");
}

// =====================================================================
//  PHÂN TÍCH + HÀNH VI (từ output/analytics)
// =====================================================================
async function loadAnalytics() {
  let a = {};
  try { a = await api("/api/analytics"); } catch {}
  const has = (k) => a[k] && a[k].length;
  $("#anNotice").hidden = has("genre_stats");

  // KPI
  const ov = a.dataset_overview || [];
  const hist = ov.find((r) => r.source === "historical") || {};
  const strm = ov.find((r) => r.source === "streaming") || {};
  $("#anKpis").innerHTML =
    kpi("🎬 Ratings gốc (MovieLens)", fmt(hist.num_ratings), `${fmt(hist.num_users)} người dùng · ${fmt(hist.num_movies)} phim`, "#ff3d6826") +
    kpi("⚡ Ratings mới từ streaming", fmt(strm.num_ratings), "dữ liệu được làm giàu", "#22c55e26") +
    kpi("⭐ Điểm trung bình", fmt2(hist.avg_rating), "trên thang 5", "#f59e0b26") +
    kpi("🏷️ Số thể loại", fmt((a.genre_stats || []).length), "", "#7c5cff26");

  if (has("genre_stats")) {
    const g = a.genre_stats;
    chart("anGenreCount", { type: "bar", data: { labels: g.map((r) => r.genre), datasets: [{ data: g.map((r) => r.num_ratings), backgroundColor: PALETTE.concat(PALETTE) }] }, options: barOpts(true) });
    const ga = [...g].sort((x, y) => y.avg_rating - x.avg_rating);
    const mn = Math.min(...ga.map((r) => r.avg_rating));
    chart("anGenreAvg", { type: "bar", data: { labels: ga.map((r) => r.genre), datasets: [{ data: ga.map((r) => r.avg_rating), backgroundColor: "#7c5cffcc" }] },
      options: barOpts(true, { scales: { x: { min: Math.floor(mn * 10) / 10 - 0.1 }, y: { grid: { display: false } } } }) });
  }
  if (has("rating_distribution")) {
    const r = a.rating_distribution;
    chart("anDist", { type: "bar", data: { labels: r.map((x) => x.rating), datasets: [{ data: r.map((x) => x.cnt), backgroundColor: r.map((x) => `hsl(${x.rating * 24} 80% 55%)`) }] }, options: barOpts() });
  }
  if (has("ratings_by_decade")) {
    const r = a.ratings_by_decade;
    chart("anDecade", { type: "line", data: { labels: r.map((x) => x.decade + "s"), datasets: [{ data: r.map((x) => x.avg_rating), borderColor: "#f59e0b", backgroundColor: gradient("anDecade", "#f59e0b"), fill: true, tension: .3 }] },
      options: { plugins: { legend: { display: false } } } });
  }
  if (has("ratings_per_year")) {
    const r = a.ratings_per_year;
    chart("anYear", { data: { labels: r.map((x) => x.year), datasets: [
      { type: "bar", label: "Lượt đánh giá", data: r.map((x) => x.num_ratings), backgroundColor: "#ff3d68aa", yAxisID: "y" },
      { type: "line", label: "Người dùng hoạt động", data: r.map((x) => x.active_users), borderColor: "#7c5cff", tension: .3, yAxisID: "y1" }] },
      options: { scales: { y: { position: "left" }, y1: { position: "right", grid: { display: false } }, x: { grid: { display: false } } } } });
  }
  $("#anTopRated").innerHTML = (a.top_rated_movies || []).map((m, i) =>
    posterCard({ ...m, title: m.title.replace(/\s*\(\d{4}\)\s*$/, "") }, { badge: `#${i + 1}`, score: m.avg_rating, foot: `${fmt(m.num_ratings)} lượt đánh giá` })).join("") || `<div class="empty">Chưa có dữ liệu</div>`;
  const pop = a.most_popular_movies || [];
  const pmax = Math.max(...pop.map((m) => m.num_ratings), 1);
  $("#anPopular").innerHTML = table([
    { h: "#", f: (r, i) => `<span class="rank ${i < 3 ? "top" : ""}">${i + 1}</span>` },
    { h: "Phim", f: (r) => `<b>${esc(r.title)}</b>` },
    { h: "Lượt đánh giá", f: (r) => `<div class="bar-cell"><i style="width:${(r.num_ratings / pmax) * 110}px"></i>${fmt(r.num_ratings)}</div>` },
    { h: "Điểm TB", f: (r) => `<span class="stars">★</span> ${fmt2(r.avg_rating)}`, num: true },
  ], pop.slice(0, 12));
  $("#anTags").innerHTML = (a.top_tags || []).map((t) => `<span style="font-size:${12 + Math.min(8, t.cnt / 20)}px">${esc(t.tag)}<b>${t.cnt}</b></span>`).join("") || `<div class="empty">Chưa có dữ liệu</div>`;

  // ----- Hành vi người dùng -----
  $("#bhNotice").hidden = has("stream_event_types");
  const ev = Object.fromEntries((a.stream_event_types || []).map((r) => [r.event_type, r]));
  const total = Object.values(ev).reduce((s, r) => s + r.cnt, 0);
  $("#bhKpis").innerHTML =
    kpi("Tổng sự kiện đã lưu HDFS", fmt(total), "từ Spark Streaming", "#ff3d6826") +
    kpi("Người dùng duy nhất", fmt(Math.max(0, ...Object.values(ev).map((r) => r.users))), "", "#7c5cff26") +
    kpi("Lượt chấm điểm", fmt(ev.rate?.cnt), "bổ sung vào tập huấn luyện", "#f59e0b26") +
    kpi("Tỉ lệ xem hết", ev.view ? ((100 * (ev.watch_complete?.cnt || 0)) / ev.view.cnt).toFixed(1) + "%" : "–", "watch_complete / view", "#22c55e26");
  const order = ["view", "click_trailer", "add_to_watchlist", "watch_complete", "rate"];
  const fmax = Math.max(...order.map((o) => ev[o]?.cnt || 0), 1);
  $("#bhFunnel").innerHTML = total ? order.map((o, i) => `<div class="step"><div class="lbl">${EVENT_VI[o]}</div>
      <div class="bar" style="width:${Math.max(12, (100 * (ev[o]?.cnt || 0)) / fmax)}%; background:linear-gradient(90deg, ${PALETTE[i]}, ${PALETTE[i]}88)">${fmt(ev[o]?.cnt || 0)}</div></div>`).join("")
    : `<div class="empty">Chưa có dữ liệu</div>`;
  if (has("stream_genre_funnel")) {
    const f = [...a.stream_genre_funnel].filter((r) => r.views >= 5).sort((x, y) => y.view_to_complete_rate - x.view_to_complete_rate);
    chart("bhComplete", { type: "bar", data: { labels: f.map((r) => r.genre), datasets: [{ data: f.map((r) => +(100 * r.view_to_complete_rate).toFixed(1)), backgroundColor: "#22c55ecc" }] },
      options: barOpts(true, { plugins: { legend: { display: false }, tooltip: { callbacks: { label: (c) => c.raw + "%" } } } }) });
  }
  if (has("stream_events_per_minute")) {
    const m = a.stream_events_per_minute;
    chart("bhMinute", { type: "line", data: { labels: m.map((r) => new Date(r.minute).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" })), datasets: [
      { label: "Sự kiện", data: m.map((r) => r.events), borderColor: "#ff3d68", backgroundColor: gradient("bhMinute"), fill: true, tension: .3 },
      { label: "Người dùng hoạt động", data: m.map((r) => r.active_users), borderColor: "#7c5cff", tension: .3 }] } });
  }
  if (has("stream_city_device")) {
    const cd = a.stream_city_device;
    const cities = [...new Set(cd.map((r) => r.city))], devs = [...new Set(cd.map((r) => r.device))];
    const mx = Math.max(...cd.map((r) => r.events));
    const val = (c, d) => cd.find((r) => r.city === c && r.device === d)?.events || 0;
    $("#bhHeat").innerHTML = `<table><thead><tr><th>Thành phố</th>${devs.map((d) => `<th class="num">${esc(d)}</th>`).join("")}</tr></thead><tbody>${cities.map((c) =>
      `<tr><td>${esc(c)}</td>${devs.map((d) => { const v = val(c, d); return `<td class="num" style="background:rgba(255,61,104,${(0.08 + 0.75 * v / mx).toFixed(2)})">${v}</td>`; }).join("")}</tr>`).join("")}</tbody></table>`;
  }
  $("#bhTrending").innerHTML = table([
    { h: "#", f: (r, i) => `<span class="rank ${i < 3 ? "top" : ""}">${i + 1}</span>` },
    { h: "Phim", f: (r) => `<b>${esc(r.title)}</b>` },
    { h: "Thể loại", k: "primary_genre" },
    { h: "Sự kiện", k: "events", num: true },
    { h: "Đỉnh / phút", k: "peak_events_per_minute", num: true },
  ], (a.stream_trending_movies || []).slice(0, 10));

  $("#sysTables").innerHTML = Object.keys(a).sort().map((k) => `<span>${k}<b>${a[k].length}</b></span>`).join("") || `<div class="empty">Chưa có</div>`;
}

// =====================================================================
//  GỢI Ý PHIM
// =====================================================================
async function loadModel() {
  let m;
  try { m = await api("/api/model"); } catch { m = { ready: false }; }
  $("#rcNotice").hidden = m.ready;
  if (!m.ready) { $("#rcKpis").innerHTML = ""; return; }
  const b = m.metrics.best, n = m.metrics.num_ratings || {};
  $("#rcKpis").innerHTML =
    kpi("🎯 RMSE", fmt2(b.rmse), "sai số căn trung bình bình phương", "#ff3d6826") +
    kpi("📏 MAE", fmt2(b.mae), "sai số tuyệt đối trung bình", "#7c5cff26") +
    kpi("⚙️ Tham số tốt nhất", `${b.rank} / ${b.regParam}`, "rank / regParam", "#f59e0b26") +
    kpi("📚 Dữ liệu huấn luyện", fmt((n.historical || 0) + (n.streaming || 0)), `gồm ${fmt(n.streaming || 0)} ratings từ streaming`, "#22c55e26");
  const gs = m.metrics.grid_search;
  chart("rcGrid", { type: "bar", data: { labels: gs.map((g) => `rank ${g.rank} · reg ${g.regParam}`), datasets: [
    { label: "RMSE", data: gs.map((g) => g.rmse), backgroundColor: "#ff3d68cc" }, { label: "MAE", data: gs.map((g) => g.mae), backgroundColor: "#7c5cffcc" }] },
    options: { borderRadius: 6, scales: { x: { grid: { display: false } } } } });
  const h = m.history || [];
  chart("rcHist", { type: "line", data: { labels: h.map((r) => String(r.trained_at).replace(/^(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2}).*/, "$3/$2 $4:$5")), datasets: [
    { label: "RMSE", data: h.map((r) => r.rmse), borderColor: "#ff3d68", tension: .3, yAxisID: "y" },
    { label: "Ratings từ streaming", data: h.map((r) => r.streaming_ratings), borderColor: "#22c55e", borderDash: [5, 4], tension: .3, yAxisID: "y1" }] },
    options: { scales: { y1: { position: "right", grid: { display: false } } } } });

  const users = await api("/api/users").catch(() => []);
  const sel = $("#userSelect");
  if (!sel.options.length) {
    sel.innerHTML = users.map((u) => `<option value="${u}">User ${u}</option>`).join("");
    sel.onchange = () => loadRecs(sel.value);
    $("#randomUser").onclick = () => { sel.value = users[Math.floor(Math.random() * users.length)]; loadRecs(sel.value); };
    if (users.length) loadRecs(users[0]);
  }
}
async function loadRecs(uid) {
  const d = await api(`/api/recommendations/${uid}`).catch(() => null);
  if (!d) return;
  $("#userHead").innerHTML = `<div class="avatar">U${uid % 100}</div><div><b>User ${uid}</b>
    <div class="muted small">Đã đánh giá ${fmt(d.num_rated)} phim · Mô hình dự đoán 10 phim người dùng này sẽ thích nhất</div></div>`;
  $("#recList").innerHTML = d.recommendations.map((r) =>
    posterCard({ ...r, title: r.title.replace(/\s*\(\d{4}\)\s*$/, "") }, { badge: `#${r.rank}`, score: r.predicted_rating, foot: "điểm dự đoán" })).join("");
  $("#histList").innerHTML = d.history.length ? d.history.map((r) => posterCard(r, { score: r.rating, foot: r.year || "" })).join("") : `<div class="empty">Người dùng mới, chưa có lịch sử</div>`;
}

// =====================================================================
//  KHÁM PHÁ PHIM
// =====================================================================
let searchTimer;
async function search(q) {
  const res = await api(`/api/movies/search?q=${encodeURIComponent(q)}`).catch(() => []);
  $("#searchResults").innerHTML = res.length
    ? res.map((m) => posterCard(m, { score: m.avg_rating, badge: m.year || null, foot: m.num_ratings ? `${fmt(m.num_ratings)} lượt đánh giá` : "chưa có đánh giá" })).join("")
    : `<div class="empty">Không tìm thấy phim phù hợp</div>`;
}
$("#searchInput").addEventListener("input", (e) => { clearTimeout(searchTimer); searchTimer = setTimeout(() => search(e.target.value.trim()), 250); });

// =====================================================================
//  HỆ THỐNG
// =====================================================================
async function loadStatus() {
  const s = await api("/api/status").catch(() => null);
  if (!s) return;
  $("#svcGrid").innerHTML = s.services.map((x) => `<div class="svc">
      <div class="row"><span class="name">${x.name}</span><span class="badge-st ${x.online ? "on" : "off"}">${x.online ? "● Online" : "● Offline"}</span></div>
      <div class="muted small">${x.host}</div>
      ${x.link ? `<a class="small link" href="${x.link}" target="_blank" rel="noopener">Mở giao diện ↗</a>` : `<span class="muted small">Không có giao diện web</span>`}</div>`).join("") +
    `<div class="svc"><div class="row"><span class="name">Dữ liệu & mô hình</span></div>
      <div class="small">${s.dataset_ready ? "✅" : "⏳"} Bộ dữ liệu MovieLens</div>
      <div class="small">${s.analytics_tables.length ? "✅" : "⏳"} ${s.analytics_tables.length} bảng phân tích</div>
      <div class="small">${s.model_trained_at ? "✅ Mô hình: " + s.model_trained_at : "⏳ Chưa huấn luyện mô hình"}</div></div>`;
  $("#ovServices").innerHTML = s.services.map((x) => `<div><i style="background:${x.online ? "#22c55e" : "#ef4444"}"></i>${x.name}</div>`).join("");
}

// =====================================================================
//  ĐIỀU HƯỚNG
// =====================================================================
function route() {
  const page = (location.hash || "#overview").slice(1);
  const p = PAGES[page] ? page : "overview";
  document.querySelectorAll(".page").forEach((el) => el.classList.toggle("active", el.id === "page-" + p));
  document.querySelectorAll(".nav a").forEach((a) => a.classList.toggle("active", a.dataset.page === p));
  $("#pageTitle").textContent = PAGES[p][0];
  $("#pageSub").textContent = PAGES[p][1];
  $("#sidebar").classList.remove("open");
  Object.values(charts).forEach((c) => c.resize());
  if (p === "explore" && !$("#searchResults").innerHTML) search("");
  if (p === "system") loadStatus();
}
window.addEventListener("hashchange", route);
$("#menuBtn").onclick = () => $("#sidebar").classList.toggle("open");
setInterval(() => ($("#clock").textContent = new Date().toLocaleString("vi-VN")), 1000);

route();
loadRealtime();
loadAnalytics();
loadModel();
loadStatus();
setInterval(() => {
  const p = (location.hash || "#overview").slice(1);
  if (p === "overview" || p === "realtime" || p === "") loadRealtime();
}, 3000);
setInterval(() => { loadAnalytics(); loadStatus(); }, 60000);
