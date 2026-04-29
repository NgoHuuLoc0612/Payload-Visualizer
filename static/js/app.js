/* ═══════════════════════════════════════════════════════════════════════════
   Payload Visualizer — Enterprise Frontend Application
   Modules: API · WebSocket · Editor · TreeRenderer · ChartEngine ·
            DiffEngine · RealtimeEngine · HistoryManager · UI
   ═══════════════════════════════════════════════════════════════════════════ */

'use strict';

// ══════════════════════════════════════════════════════════════════════════════
// CONFIG
// ══════════════════════════════════════════════════════════════════════════════
const CFG = {
  API_BASE: `${location.origin}/api/v1`,
  WS_URL: `ws://${location.host}/ws/stream`,
  FEED_MAX_ROWS: 200,
  SPARK_MAX_POINTS: 60,
  TIMELINE_MAX_POINTS: 120,
  CHART_COLORS: ['#00e5ff','#39ff14','#ff6b35','#bf5af2','#ffb300','#ff3b5c','#4fc3f7','#81c784','#ffb74d','#ce93d8'],
};

// ══════════════════════════════════════════════════════════════════════════════
// UTILITIES
// ══════════════════════════════════════════════════════════════════════════════
const $ = (sel, ctx = document) => ctx.querySelector(sel);
const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];
const el = (tag, cls, html = '') => { const e = document.createElement(tag); if(cls) e.className=cls; if(html) e.innerHTML=html; return e; };
const fmtBytes = b => b < 1024 ? `${b}B` : b < 1048576 ? `${(b/1024).toFixed(1)}KB` : `${(b/1048576).toFixed(2)}MB`;
const fmtNum = n => n >= 1e6 ? `${(n/1e6).toFixed(1)}M` : n >= 1e3 ? `${(n/1e3).toFixed(1)}K` : String(Math.round(n));
const fmtTime = ts => dayjs(ts * 1000).format('HH:mm:ss.SSS');
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const debounce = (fn, ms) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };

// ══════════════════════════════════════════════════════════════════════════════
// TOAST
// ══════════════════════════════════════════════════════════════════════════════
const Toast = {
  show(msg, type = 'info', dur = 2800) {
    const icons = { success:'✓', error:'✕', info:'◈', warn:'⚠' };
    const t = el('div', `toast ${type}`, `<span>${icons[type]||'◈'}</span> ${msg}`);
    $('#toast-container').appendChild(t);
    setTimeout(() => t.remove(), dur + 300);
  }
};

// ══════════════════════════════════════════════════════════════════════════════
// API CLIENT
// ══════════════════════════════════════════════════════════════════════════════
const API = {
  async _req(path, opts = {}, silent = false) {
    try {
      const r = await fetch(`${CFG.API_BASE}${path}`, {
        headers: { 'Content-Type': 'application/json', ...opts.headers },
        ...opts,
      });
      if (!r.ok) {
        const err = await r.json().catch(() => ({ detail: r.statusText }));
        throw new Error(err.detail || r.statusText);
      }
      return r.json();
    } catch (e) {
      if (!silent) Toast.show(e.message, 'error');
      throw e;
    }
  },

  parse: (payload, format = 'auto') =>
    API._req('/parse', { method:'POST', body: JSON.stringify({ payload, format }) }, false),

  parseFile: async (file, format = 'auto') => {
    const fd = new FormData();
    fd.append('file', file);
    const r = await fetch(`${CFG.API_BASE}/parse/file?format=${format}`, { method:'POST', body: fd });
    if (!r.ok) { const e = await r.json().catch(() => ({})); throw new Error(e.detail || r.statusText); }
    return r.json();
  },

  detect: (payload) =>
    API._req('/detect', { method:'POST', body: JSON.stringify({ payload }) }, true),

  diff: (left, right, format) =>
    API._req('/diff', { method:'POST', body: JSON.stringify({ left, right, format }) }),

  transform: (payload, source_format, target_format) =>
    API._req('/transform', { method:'POST', body: JSON.stringify({ payload, source_format, target_format }) }),

  formats: () => API._req('/formats'),
};

// ══════════════════════════════════════════════════════════════════════════════
// CODEMIRROR EDITOR FACTORY
// ══════════════════════════════════════════════════════════════════════════════
const EditorFactory = {
  _modeMap: { json:'javascript', xml:'xml', yaml:'yaml', toml:'toml', graphql:'javascript', jwt:'javascript', csv:'javascript', msgpack:'javascript', har:'javascript', protobuf:'javascript', binary:'javascript' },

  create(container, opts = {}) {
    return CodeMirror(container, {
      value: opts.value || '',
      mode: opts.mode || 'javascript',
      theme: 'material-darker',
      lineNumbers: true,
      lineWrapping: false,
      foldGutter: true,
      gutters: ['CodeMirror-linenumbers', 'CodeMirror-foldgutter'],
      autoCloseBrackets: true,
      matchBrackets: true,
      extraKeys: {
        'Ctrl-Space': 'autocomplete',
        'Ctrl-Q': cm => cm.foldCode(cm.getCursor()),
      },
      readOnly: opts.readOnly || false,
      autoRefresh: true,
    });
  },

  modeForFormat(fmt) {
    return this._modeMap[fmt] || 'javascript';
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// TREE RENDERER
// ══════════════════════════════════════════════════════════════════════════════
const TreeRenderer = {
  render(obj, container) {
    container.innerHTML = '';
    const root = this._renderNode(obj, 'root', 0);
    container.appendChild(root);
  },

  _renderNode(value, key, depth) {
    const wrap = el('div', 'tree-node');
    const isObj = value !== null && typeof value === 'object';
    const isArr = Array.isArray(value);

    if (isObj) {
      const entries = isArr ? value.map((v,i) => [i,v]) : Object.entries(value);
      const count = entries.length;
      const toggle = el('span', 'tree-toggle open', '▶');
      const keySpan = el('span', 'tree-key', key !== 'root' ? (isArr ? `<span style="color:var(--accent3)">[${key}]</span>` : `"${key}"`) : '<span style="color:var(--text-dim)">root</span>');
      const bracket = el('span', '', isArr ? ' [' : ' {');
      const badge = el('span', 'tree-badge', count > 0 ? `${count} ${isArr ? 'items' : 'keys'}` : '');
      const children = el('div', 'tree-children');

      if (depth > 0) {
        wrap.append(toggle, ' ', keySpan, ': ', bracket, badge);
      } else {
        wrap.append(toggle, ' ', keySpan, bracket, badge);
      }
      wrap.appendChild(children);

      // Lazy render for performance
      let rendered = false;
      const renderChildren = () => {
        if (rendered) return;
        rendered = true;
        const limit = 200;
        entries.slice(0, limit).forEach(([k, v]) => {
          children.appendChild(this._renderNode(v, k, depth + 1));
        });
        if (entries.length > limit) {
          const more = el('div', '', `<span style="color:var(--text-muted)">… ${entries.length - limit} more items</span>`);
          children.appendChild(more);
        }
      };

      if (depth < 3) renderChildren();

      toggle.addEventListener('click', () => {
        renderChildren();
        const open = !children.classList.contains('collapsed');
        children.classList.toggle('collapsed', open);
        toggle.classList.toggle('open', !open);
        toggle.textContent = open ? '▶' : '▶';
      });

      const closeBracket = el('div', '', isArr ? ']' : '}');
      wrap.appendChild(closeBracket);
    } else {
      const keySpan = el('span', 'tree-key', `"${key}"`);
      const valSpan = this._valueSpan(value);
      if (key !== 'root') {
        wrap.append(keySpan, ': ', valSpan);
      } else {
        wrap.appendChild(valSpan);
      }
    }
    return wrap;
  },

  _valueSpan(v) {
    if (v === null) return el('span', 'tree-null', 'null');
    if (typeof v === 'string') {
      const display = v.length > 120 ? v.slice(0, 120) + '…' : v;
      return el('span', 'tree-string', `"${_.escape(display)}"`);
    }
    if (typeof v === 'number') return el('span', 'tree-number', String(v));
    if (typeof v === 'boolean') return el('span', 'tree-boolean', String(v));
    return el('span', '', String(v));
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// TABLE RENDERER
// ══════════════════════════════════════════════════════════════════════════════
const TableRenderer = {
  render(fields, container) {
    if (!fields || !fields.length) { container.innerHTML = '<p style="color:var(--text-muted);padding:20px">No fields extracted.</p>'; return; }
    const tbl = el('table', 'data-table');
    tbl.innerHTML = `<thead><tr><th>Path</th><th>Type</th><th>Value</th><th>Depth</th><th>Size</th></tr></thead>`;
    const tbody = el('tbody');
    fields.slice(0, 500).forEach(f => {
      const tr = el('tr');
      const valDisplay = f.value === null ? '<em style="color:var(--text-muted)">null</em>' : _.escape(String(f.value ?? '').slice(0, 80));
      tr.innerHTML = `
        <td style="color:var(--accent);font-size:11px">${_.escape(f.path)}</td>
        <td><span class="type-badge ${f.type}">${f.type}</span></td>
        <td>${valDisplay}</td>
        <td style="text-align:center;color:var(--text-muted)">${f.depth}</td>
        <td style="color:var(--text-muted)">${fmtBytes(f.size_bytes||0)}</td>
      `;
      tbody.appendChild(tr);
    });
    tbl.appendChild(tbody);
    container.innerHTML = '';
    container.appendChild(tbl);
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// SCHEMA RENDERER
// ══════════════════════════════════════════════════════════════════════════════
const SchemaRenderer = {
  render(schema, container) {
    container.innerHTML = '';
    if (!schema) { container.innerHTML = '<p style="color:var(--text-muted);padding:20px">No schema available.</p>'; return; }
    container.appendChild(this._renderNode(schema, 0));
  },

  _renderNode(node, depth) {
    const wrap = el('div', 'schema-node');
    const indent = '  '.repeat(depth);
    const nullable = node.nullable ? '<span class="schema-nullable">?</span>' : '';
    const repeated = node.repeated ? '<span class="schema-repeated">[]</span>' : '';
    wrap.innerHTML = `${indent}<span class="tree-key">${_.escape(node.name)}</span><span class="schema-type">${node.type}</span>${nullable}${repeated}`;
    if (node.children && node.children.length) {
      const children = el('div', 'schema-children');
      node.children.forEach(child => children.appendChild(this._renderNode(child, depth + 1)));
      wrap.appendChild(children);
    }
    return wrap;
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// CHART ENGINE
// ══════════════════════════════════════════════════════════════════════════════
const ChartEngine = {
  _charts: {},

  _baseOpts(dark = true) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: dark ? '#5c7899' : '#4a6080', font: { family: "'Space Grotesk'", size: 11 }, boxWidth: 10 } },
        tooltip: {
          backgroundColor: '#111820', borderColor: '#1e2d3d', borderWidth: 1,
          titleColor: '#c9d5e3', bodyColor: '#5c7899',
        },
      },
      scales: {
        x: { ticks: { color: '#5c7899', font: { size: 10 } }, grid: { color: '#1e2d3d' } },
        y: { ticks: { color: '#5c7899', font: { size: 10 } }, grid: { color: '#1e2d3d' } },
      },
      animation: { duration: 300 },
    };
  },

  _destroy(id) {
    if (this._charts[id]) { this._charts[id].destroy(); delete this._charts[id]; }
  },

  renderAnalysisCharts(result) {
    this._renderTypes(result);
    this._renderDepth(result);
    this._renderNull(result);
    this._renderSize(result);
  },

  _renderTypes(result) {
    this._destroy('types');
    const s = result.stats;
    const labels = ['String','Number','Boolean','Object','Array','Null'];
    const data = [s.string_count, s.number_count, s.boolean_count, s.object_count, s.array_count, s.null_count];
    const ctx = $('#chart-types').getContext('2d');
    this._charts['types'] = new Chart(ctx, {
      type: 'doughnut',
      data: { labels, datasets: [{ data, backgroundColor: CFG.CHART_COLORS, borderWidth: 1, borderColor: '#0d1117' }] },
      options: { ...this._baseOpts(), scales: {}, cutout: '62%', animation: { duration: 400 } },
    });
  },

  _renderDepth(result) {
    this._destroy('depth');
    const fields = result.fields || [];
    const depthCounts = {};
    fields.forEach(f => { depthCounts[f.depth] = (depthCounts[f.depth] || 0) + 1; });
    const labels = Object.keys(depthCounts).map(Number).sort((a,b) => a-b).map(String);
    const data = labels.map(l => depthCounts[Number(l)]);
    const ctx = $('#chart-depth').getContext('2d');
    this._charts['depth'] = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Fields', data, backgroundColor: 'rgba(0,229,255,.4)', borderColor: '#00e5ff', borderWidth: 1 }] },
      options: { ...this._baseOpts(), plugins: { legend: { display: false } } },
    });
  },

  _renderNull(result) {
    this._destroy('null');
    const s = result.stats;
    const nonNull = (s.total_fields || 1) - s.null_count;
    const ctx = $('#chart-null').getContext('2d');
    this._charts['null'] = new Chart(ctx, {
      type: 'pie',
      data: {
        labels: ['Non-null', 'Null'],
        datasets: [{ data: [nonNull, s.null_count], backgroundColor: ['rgba(57,255,20,.5)','rgba(255,59,92,.5)'], borderWidth: 1, borderColor: '#0d1117' }],
      },
      options: { ...this._baseOpts(), scales: {} },
    });
  },

  _renderSize(result) {
    this._destroy('size');
    const fields = result.fields || [];
    if (!fields.length) return;
    const sizes = fields.map(f => f.size_bytes || 0).filter(Boolean).slice(0, 50);
    const labels = sizes.map((_, i) => `f${i}`);
    const ctx = $('#chart-size').getContext('2d');
    this._charts['size'] = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [{ label: 'Bytes', data: sizes, backgroundColor: 'rgba(191,90,242,.4)', borderColor: '#bf5af2', borderWidth: 1 }] },
      options: { ...this._baseOpts(), plugins: { legend: { display: false } }, scales: { x: { display: false }, y: { ticks: { color: '#5c7899', font: { size: 10 } }, grid: { color: '#1e2d3d' } } } },
    });
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// REALTIME ENGINE
// ══════════════════════════════════════════════════════════════════════════════
const RealtimeEngine = {
  _charts: {},
  _sparks: {},
  _sparkData: {},
  _timelineData: { latency: [], throughput: [], labels: [] },
  _formatCounts: {},
  _statusCounts: { '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 },
  _eventCount: 0,
  _paused: false,
  _feedRows: [],

  init() {
    this._initSparks();
    this._initTimelines();
    this._initDonut('#rt-chart-formats', 'formats');
    this._initDonut('#rt-chart-status', 'status');
  },

  _initSparks() {
    ['throughput','latency','errors','total','memory','cpu'].forEach(k => {
      this._sparkData[k] = [];
      const canvas = $(`#spark-${k}`);
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      this._sparks[k] = new Chart(ctx, {
        type: 'line',
        data: { labels: [], datasets: [{ data: [], borderColor: CFG.CHART_COLORS[0], borderWidth: 1.5, fill: true, backgroundColor: 'rgba(0,229,255,.08)', tension: 0.4, pointRadius: 0 }] },
        options: { responsive: false, animation: false, plugins: { legend: { display: false }, tooltip: { enabled: false } }, scales: { x: { display: false }, y: { display: false } } },
      });
    });
  },

  _updateSpark(key, val) {
    const data = this._sparkData[key];
    data.push(val);
    if (data.length > CFG.SPARK_MAX_POINTS) data.shift();
    const chart = this._sparks[key];
    if (!chart) return;
    chart.data.labels = data.map((_, i) => i);
    chart.data.datasets[0].data = [...data];
    chart.update('none');
  },

  _initTimelines() {
    ['latency','throughput'].forEach(k => {
      const canvas = $(`#rt-chart-${k}`);
      if (!canvas) return;
      const color = k === 'latency' ? '#00e5ff' : '#39ff14';
      const ctx = canvas.getContext('2d');
      this._charts[k] = new Chart(ctx, {
        type: 'line',
        data: {
          labels: [],
          datasets: [{
            label: k === 'latency' ? 'ms' : 'rps',
            data: [], borderColor: color, borderWidth: 1.5,
            fill: true, backgroundColor: `${color}12`, tension: 0.3, pointRadius: 0,
          }],
        },
        options: {
          responsive: true, maintainAspectRatio: false, animation: false,
          plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
          scales: {
            x: { ticks: { color: '#5c7899', font: { size: 9 }, maxTicksLimit: 8 }, grid: { color: '#1e2d3d' } },
            y: { ticks: { color: '#5c7899', font: { size: 10 } }, grid: { color: '#1e2d3d' } },
          },
        },
      });
    });
  },

  _initDonut(sel, key) {
    const canvas = $(sel);
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    this._charts[key] = new Chart(ctx, {
      type: 'doughnut',
      data: { labels: [], datasets: [{ data: [], backgroundColor: CFG.CHART_COLORS, borderWidth: 1, borderColor: '#0d1117' }] },
      options: {
        responsive: true, maintainAspectRatio: false, animation: { duration: 500 },
        plugins: {
          legend: { position: 'right', labels: { color: '#5c7899', font: { size: 10 }, boxWidth: 10 } },
          tooltip: { backgroundColor: '#111820', borderColor: '#1e2d3d', borderWidth: 1, titleColor: '#c9d5e3', bodyColor: '#5c7899' },
        },
        cutout: '58%',
      },
    });
  },

  processBatch(batch) {
    if (this._paused) return;
    batch.forEach(item => this._processItem(item));
    this._flushFeed(batch);
  },

  _processItem(item) {
    this._eventCount++;
    // Format counts
    this._formatCounts[item.format] = (this._formatCounts[item.format] || 0) + 1;
    // Status
    const grp = `${Math.floor(item.status / 100)}xx`;
    if (this._statusCounts[grp] !== undefined) this._statusCounts[grp]++;
  },

  applySnapshot(snap) {
    if (this._paused) return;
    // KPIs
    const set = (id, val) => { const e = $(`#kpi-${id} .kpi-value`); if (e) e.textContent = val; };
    set('throughput', fmtNum(snap.throughput_rps));
    set('latency', snap.avg_latency_ms.toFixed(1));
    set('errors', snap.error_rate_pct.toFixed(1) + '%');
    set('total', fmtNum(snap.total_processed));
    set('memory', snap.memory_mb.toFixed(0));
    set('cpu', snap.cpu_pct.toFixed(0) + '%');

    // Sparks
    this._updateSpark('throughput', snap.throughput_rps);
    this._updateSpark('latency', snap.avg_latency_ms);
    this._updateSpark('errors', snap.error_rate_pct);
    this._updateSpark('total', snap.total_processed);
    this._updateSpark('memory', snap.memory_mb);
    this._updateSpark('cpu', snap.cpu_pct);

    // Timelines
    const label = dayjs().format('HH:mm:ss');
    ['latency','throughput'].forEach(k => {
      const chart = this._charts[k];
      if (!chart) return;
      const val = k === 'latency' ? snap.avg_latency_ms : snap.throughput_rps;
      chart.data.labels.push(label);
      chart.data.datasets[0].data.push(val);
      if (chart.data.labels.length > CFG.TIMELINE_MAX_POINTS) {
        chart.data.labels.shift();
        chart.data.datasets[0].data.shift();
      }
      chart.update('none');
    });

    // Format donut
    const fmtLabels = Object.keys(snap.format_distribution);
    const fmtData = fmtLabels.map(k => snap.format_distribution[k]);
    const fc = this._charts['formats'];
    if (fc) {
      fc.data.labels = fmtLabels;
      fc.data.datasets[0].data = fmtData;
      fc.update('none');
    }

    // Status donut
    const sc = this._charts['status'];
    if (sc) {
      sc.data.labels = Object.keys(this._statusCounts);
      sc.data.datasets[0].data = Object.values(this._statusCounts);
      sc.data.datasets[0].backgroundColor = ['#39ff14','#ffb300','#ff6b35','#ff3b5c'];
      sc.update('none');
    }

    $('#rt-event-count').textContent = `${fmtNum(this._eventCount)} events`;
  },

  _flushFeed(batch) {
    const tbody = $('#rt-feed-body');
    if (!tbody) return;
    const frag = document.createDocumentFragment();
    batch.slice(-30).forEach(item => {
      const tr = el('tr', 'new-row');
      const statusClass = item.status < 300 ? 'status-ok' : item.status < 400 ? 'status-redir' : 'status-err';
      tr.innerHTML = `
        <td style="color:var(--text-muted)">${item.id}</td>
        <td style="color:var(--text-muted);font-size:10px">${fmtTime(item.ts)}</td>
        <td><span class="method-badge method-${item.method}">${item.method}</span></td>
        <td style="color:var(--text-dim)">${_.escape(item.path)}</td>
        <td><span class="format-badge" style="font-size:9px">${item.format}</span></td>
        <td>${fmtBytes(item.size_bytes)}</td>
        <td class="${item.latency_ms > 500 ? 'status-err' : item.latency_ms > 150 ? 'status-redir' : 'status-ok'}">${item.latency_ms.toFixed(1)}ms</td>
        <td class="${statusClass}">${item.status}</td>
        <td style="color:${item.anomaly_count > 0 ? 'var(--warn)' : 'var(--text-muted)'}">${item.anomaly_count || '—'}</td>
      `;
      frag.prepend(tr);
      this._feedRows.push(item);
    });
    tbody.prepend(frag);
    // Trim rows
    while (tbody.children.length > CFG.FEED_MAX_ROWS) tbody.lastChild?.remove();
  },

  clear() {
    this._feedRows = [];
    this._eventCount = 0;
    this._formatCounts = {};
    this._statusCounts = { '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 };
    this._timelineData = { latency: [], throughput: [], labels: [] };
    const tbody = $('#rt-feed-body');
    if (tbody) tbody.innerHTML = '';
    ['latency','throughput'].forEach(k => {
      const c = this._charts[k];
      if (c) { c.data.labels = []; c.data.datasets[0].data = []; c.update('none'); }
    });
  },

  pause() { this._paused = true; },
  resume() { this._paused = false; },
  togglePause() { this._paused = !this._paused; return this._paused; },
};

// ══════════════════════════════════════════════════════════════════════════════
// WEBSOCKET MANAGER
// ══════════════════════════════════════════════════════════════════════════════
const WSManager = {
  _ws: null,
  _reconnectDelay: 1500,
  _maxDelay: 30000,
  _handlers: {},

  connect() {
    this._updateStatus('connecting');
    try {
      this._ws = new WebSocket(CFG.WS_URL);
      this._ws.binaryType = 'arraybuffer';
      this._ws.onopen = () => { this._reconnectDelay = 1500; this._updateStatus('connected'); this._subscribe(); };
      this._ws.onclose = () => { this._updateStatus('disconnected'); this._scheduleReconnect(); };
      this._ws.onerror = () => this._updateStatus('disconnected');
      this._ws.onmessage = e => this._onMessage(e);
    } catch (_) { this._scheduleReconnect(); }
  },

  _onMessage(e) {
    let msg;
    try {
      const text = typeof e.data === 'string' ? e.data : new TextDecoder().decode(e.data);
      msg = JSON.parse(text);
    } catch { return; }

    switch (msg.type) {
      case 'metrics_batch': RealtimeEngine.processBatch(msg.batch); break;
      case 'system_snapshot': RealtimeEngine.applySnapshot(msg); break;
      case 'heartbeat': break;
      case 'parse_result': this._emit('parse_result', msg); break;
      default: this._emit(msg.type, msg);
    }
  },

  _subscribe() {
    this.send({ cmd: 'subscribe', interval_ms: parseInt($('#rt-interval')?.value || 250), batch_size: parseInt($('#rt-batch')?.value || 20) });
  },

  send(obj) {
    if (this._ws?.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify(obj));
    }
  },

  _scheduleReconnect() {
    setTimeout(() => this.connect(), this._reconnectDelay);
    this._reconnectDelay = Math.min(this._reconnectDelay * 1.5, this._maxDelay);
  },

  _updateStatus(state) {
    const dot = $('#ws-status-dot');
    const badge = $('#ws-badge');
    dot.className = `ws-status ${state}`;
    dot.querySelector('.label').textContent = state.charAt(0).toUpperCase() + state.slice(1);
    if (badge) badge.style.display = state === 'connected' ? 'inline' : 'none';
  },

  on(event, fn) { this._handlers[event] = fn; },
  _emit(event, data) { this._handlers[event]?.(data); },
};

// ══════════════════════════════════════════════════════════════════════════════
// HISTORY MANAGER
// ══════════════════════════════════════════════════════════════════════════════
const HistoryManager = {
  _items: [],
  _fuse: null,

  add(result, rawText) {
    const item = {
      id: Date.now(),
      ts: Date.now(),
      result,
      raw: rawText.slice(0, 2000),
      label: `${result.format_label} · ${fmtBytes(result.raw_size)}`,
    };
    this._items.unshift(item);
    if (this._items.length > 100) this._items.pop();
    try { localStorage.setItem('pv_history', JSON.stringify(this._items.slice(0, 50))); } catch (_) {}
    this._rebuildFuse();
    this.renderAll();
    return item;
  },

  load() {
    try {
      const saved = JSON.parse(localStorage.getItem('pv_history') || '[]');
      this._items = saved;
    } catch (_) { this._items = []; }
    this._rebuildFuse();
    this.renderAll();
  },

  clear() {
    this._items = [];
    try { localStorage.removeItem('pv_history'); } catch (_) {}
    this.renderAll();
  },

  _rebuildFuse() {
    this._fuse = new Fuse(this._items, { keys: ['label', 'result.format_id', 'result.sha256'], threshold: 0.4 });
  },

  search(query) {
    if (!query.trim()) return this._items;
    return this._fuse?.search(query).map(r => r.item) || this._items;
  },

  renderAll(query = '') {
    const grid = $('#history-grid');
    if (!grid) return;
    const items = this.search(query);
    if (!items.length) {
      grid.innerHTML = '<p style="color:var(--text-muted);padding:40px;text-align:center">No history yet. Parse some payloads!</p>';
      return;
    }
    grid.innerHTML = '';
    items.forEach(item => grid.appendChild(this._renderCard(item)));
  },

  _renderCard(item) {
    const card = el('div', 'history-card');
    const r = item.result;
    const anomalyHtml = (r.anomalies || []).slice(0, 3).map(a =>
      `<span class="anomaly-chip ${a.severity}" style="font-size:10px">${_.escape(a.code)}</span>`
    ).join('');
    card.innerHTML = `
      <div class="history-card-header">
        <span class="history-format">${_.escape(r.format_label)}</span>
        <span class="history-time">${dayjs(item.ts).format('HH:mm:ss')}</span>
      </div>
      <div class="history-sha">${r.sha256}</div>
      <div class="history-stats">
        <div class="history-stat">Size <strong>${fmtBytes(r.raw_size)}</strong></div>
        <div class="history-stat">Fields <strong>${r.stats?.total_fields || 0}</strong></div>
        <div class="history-stat">Depth <strong>${r.stats?.max_depth || 0}</strong></div>
        <div class="history-stat">${r.parse_time_ms}ms</div>
      </div>
      ${anomalyHtml ? `<div class="history-anomalies">${anomalyHtml}</div>` : ''}
    `;
    card.addEventListener('click', () => {
      App.loadResult(r, item.raw);
      App.switchView('analyzer');
    });
    return card;
  },

  export() {
    const blob = new Blob([JSON.stringify(this._items, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `pv-history-${Date.now()}.json`; a.click();
    URL.revokeObjectURL(url);
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// DIFF ENGINE
// ══════════════════════════════════════════════════════════════════════════════
const DiffEngine = {
  _editorLeft: null,
  _editorRight: null,

  init() {
    this._editorLeft = EditorFactory.create($('#diff-editor-left'), { value: '' });
    this._editorRight = EditorFactory.create($('#diff-editor-right'), { value: '' });
  },

  async run() {
    const fmt = $('#diff-format-select').value;
    const left = this._editorLeft.getValue().trim();
    const right = this._editorRight.getValue().trim();
    if (!left || !right) { Toast.show('Both panels need content', 'warn'); return; }
    try {
      const result = await API.diff(left, right, fmt);
      this._renderResult(result);
    } catch (_) {}
  },

  _renderResult(result) {
    const panel = $('#diff-result');
    const body = $('#diff-result-body');
    const count = $('#diff-change-count');
    panel.style.display = '';
    count.textContent = `${result.change_count} change(s)`;
    body.innerHTML = '';
    if (!result.changes.length) { body.innerHTML = '<p style="color:var(--accent2);padding:12px">✓ Payloads are identical</p>'; return; }
    result.changes.forEach(c => {
      const div = el('div', `diff-change ${c.op}`);
      const opLabels = { added: '+ ADDED', removed: '- REMOVED', modified: '~ MODIFIED', type_change: '⇄ TYPE' };
      const fromVal = c.from !== undefined ? `<span class="diff-val"> from: ${_.escape(String(c.from).slice(0,60))}</span>` : '';
      const toVal = c.to !== undefined ? `<span class="diff-val"> → ${_.escape(String(c.to).slice(0,60))}</span>` : '';
      const val = c.value !== undefined ? `<span class="diff-val"> ${_.escape(String(c.value).slice(0,60))}</span>` : '';
      div.innerHTML = `<span class="diff-op">${opLabels[c.op] || c.op}</span><span class="diff-path">${_.escape(c.path)}</span>${fromVal}${toVal}${val}`;
      body.appendChild(div);
    });
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// TRANSFORM ENGINE
// ══════════════════════════════════════════════════════════════════════════════
const TransformEngine = {
  _editorSrc: null,
  _editorOut: null,
  _lastOutput: '',

  init() {
    this._editorSrc = EditorFactory.create($('#tx-editor-src'), { value: '' });
    this._editorOut = EditorFactory.create($('#tx-editor-out'), { readOnly: true, value: '' });
  },

  async run() {
    const from = $('#tx-from').value;
    const to = $('#tx-to').value;
    const src = this._editorSrc.getValue().trim();
    if (!src) { Toast.show('Enter source payload', 'warn'); return; }
    try {
      const result = await API.transform(src, from, to);
      // result is text from the Response
      const text = typeof result === 'string' ? result : JSON.stringify(result, null, 2);
      this._lastOutput = text;
      this._editorOut.setValue(text);
      this._editorOut.setOption('mode', EditorFactory.modeForFormat(to));
      Toast.show(`Transformed ${from.toUpperCase()} → ${to.toUpperCase()}`, 'success');
    } catch (_) {}
  },

  async runText() {
    const from = $('#tx-from').value;
    const to = $('#tx-to').value;
    const src = this._editorSrc.getValue().trim();
    if (!src) { Toast.show('Enter source payload', 'warn'); return; }
    // Fallback: just call the API and show raw response text
    try {
      const r = await fetch(`${CFG.API_BASE}/transform`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ payload: src, source_format: from, target_format: to }),
      });
      const text = await r.text();
      this._lastOutput = text;
      this._editorOut.setValue(text);
      Toast.show(`Transformed ${from.toUpperCase()} → ${to.toUpperCase()}`, 'success');
    } catch (e) { Toast.show(e.message, 'error'); }
  },

  download() {
    if (!this._lastOutput) { Toast.show('Nothing to download', 'warn'); return; }
    const to = $('#tx-to').value;
    const ext = { json: 'json', yaml: 'yaml', toml: 'toml', csv: 'csv', msgpack: 'bin' }[to] || 'txt';
    const blob = new Blob([this._lastOutput], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `transformed.${ext}`; a.click();
    URL.revokeObjectURL(url);
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// SAMPLE PAYLOADS
// ══════════════════════════════════════════════════════════════════════════════
const SAMPLES = {
  json: JSON.stringify({
    id: "evt_01HXYZ789",
    type: "payment.processed",
    version: "2.1.0",
    timestamp: new Date().toISOString(),
    environment: "production",
    actor: { id: "usr_42", email: "alice@example.com", role: "admin", verified: true },
    payload: {
      amount: 4250,
      currency: "USD",
      method: "card",
      card: { brand: "visa", last4: "4242", exp_month: 12, exp_year: 2027 },
      metadata: { order_id: "ord_789", items: 3, coupon: null },
    },
    tags: ["priority:high", "region:us-east-1"],
    retry_count: 0,
    idempotency_key: "idem_abc123",
  }, null, 2),

  jwt: 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImtleS0xIn0.eyJzdWIiOiJ1c3JfNDIiLCJpc3MiOiJodHRwczovL2F1dGguZXhhbXBsZS5jb20iLCJhdWQiOlsiaHR0cHM6Ly9hcGkuZXhhbXBsZS5jb20iXSwiaWF0IjoxNzA5MjM2OTkwLCJleHAiOjE3MDkyNDA1OTAsIm5iZiI6MTcwOTIzNjk5MCwianRpIjoiand0XzAxSFhZWjc4OSIsInJvbGVzIjpbImFkbWluIiwidXNlciJdLCJwZXJtaXNzaW9ucyI6WyJyZWFkOmFsbCIsIndyaXRlOm93biIsImRlbGV0ZTpvd24iXSwic2Vzc2lvbl9pZCI6InNlc3NfYWJjMTIzIn0.signature',

  graphql: `query GetUserProfile($id: ID!, $includeOrders: Boolean = false) {
  user(id: $id) {
    id
    email
    profile {
      displayName
      avatarUrl
      bio
    }
    roles
    createdAt
    orders @include(if: $includeOrders) {
      id
      status
      total
      items {
        productId
        quantity
        price
      }
    }
  }
}`,

  xml: `<?xml version="1.0" encoding="UTF-8"?>
<order xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" id="ORD-789" status="processing">
  <customer id="cust-42">
    <name>Alice Smith</name>
    <email>alice@example.com</email>
    <address>
      <street>123 Main St</street>
      <city>San Francisco</city>
      <state>CA</state>
      <zip>94102</zip>
    </address>
  </customer>
  <items>
    <item sku="SKU-001" qty="2">
      <name>Widget Pro</name>
      <price currency="USD">49.99</price>
    </item>
    <item sku="SKU-002" qty="1">
      <name>Gadget Plus</name>
      <price currency="USD">129.00</price>
    </item>
  </items>
  <total currency="USD">228.98</total>
  <created_at>2024-03-01T12:00:00Z</created_at>
</order>`,

  csv: `id,name,email,role,status,created_at,score,active
1,Alice Smith,alice@example.com,admin,active,2024-01-15,98.5,true
2,Bob Jones,bob@example.com,user,active,2024-01-20,72.0,true
3,Carol White,carol@example.com,moderator,inactive,2024-02-01,,false
4,Dave Brown,dave@example.com,user,active,2024-02-14,85.3,true
5,Eve Davis,eve@example.com,admin,active,2024-03-01,100.0,true`,

  yaml: `# Application Configuration
app:
  name: payload-visualizer
  version: 2.0.0
  environment: production
  debug: false

server:
  host: "0.0.0.0"
  port: 8000
  workers: 4
  timeout: 30

database:
  host: db.example.com
  port: 5432
  name: pv_prod
  pool:
    min: 5
    max: 20
    timeout: 5000

features:
  realtime: true
  diff: true
  transform: true
  export: ["json", "yaml", "csv", "msgpack"]`,
};

// ══════════════════════════════════════════════════════════════════════════════
// MAIN APPLICATION
// ══════════════════════════════════════════════════════════════════════════════
const App = {
  _editor: null,
  _prettyEditor: null,
  _currentResult: null,
  _currentView: 'analyzer',
  _detectTimer: null,

  async init() {
    // CodeMirror main editor
    this._editor = EditorFactory.create($('#editor-container'), {
      value: SAMPLES.json,
      mode: 'javascript',
    });

    // Live stats
    this._editor.on('change', debounce(() => this._updateEditorStats(), 150));
    this._editor.on('change', debounce(() => this._liveDetect(), 600));
    this._updateEditorStats();

    // Pretty editor (read-only)
    this._prettyEditor = EditorFactory.create($('#pretty-container'), { readOnly: true, value: '' });

    // Init sub-engines
    DiffEngine.init();
    TransformEngine.init();
    RealtimeEngine.init();
    HistoryManager.load();

    // Event listeners
    this._bindEvents();

    // WebSocket
    WSManager.connect();

    // Load formats
    try {
      const fmts = await API.formats();
      this._populateFormatSelect(fmts);
    } catch (_) {}
  },

  _bindEvents() {
    // Nav
    $$('.nav-item').forEach(item => {
      item.addEventListener('click', () => this.switchView(item.dataset.view));
    });

    // Sidebar expand on hover
    const sidebar = $('#sidebar');
    const app = $('#app');
    sidebar.addEventListener('mouseenter', () => app.classList.add('sidebar-open'));
    sidebar.addEventListener('mouseleave', () => app.classList.remove('sidebar-open'));

    // Parse
    $('#btn-parse').addEventListener('click', () => this.runParse());
    document.addEventListener('keydown', e => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') this.runParse();
    });

    // File upload
    $('#file-input').addEventListener('change', async e => {
      const file = e.target.files[0];
      if (!file) return;
      this._editor.setValue(await file.text().catch(() => ''));
      const fmt = $('#format-select').value;
      await this.runParseFile(file, fmt);
      e.target.value = '';
    });

    // Sample
    $('#btn-sample').addEventListener('click', () => {
      const fmt = $('#format-select').value;
      const sample = SAMPLES[fmt] || SAMPLES.json;
      this._editor.setValue(sample);
      const mode = EditorFactory.modeForFormat(fmt === 'auto' ? 'json' : fmt);
      this._editor.setOption('mode', mode);
      Toast.show(`Loaded ${fmt === 'auto' ? 'JSON' : fmt.toUpperCase()} sample`, 'info');
    });

    // Format select change
    $('#format-select').addEventListener('change', e => {
      const fmt = e.target.value;
      const mode = EditorFactory.modeForFormat(fmt === 'auto' ? 'json' : fmt);
      this._editor.setOption('mode', mode);
    });

    // Clear
    $('#btn-clear').addEventListener('click', () => { this._editor.setValue(''); this._resetResults(); });

    // Copy input
    $('#btn-copy-input').addEventListener('click', () => {
      navigator.clipboard.writeText(this._editor.getValue()).then(() => Toast.show('Copied!', 'success'));
    });

    // Prettify
    $('#btn-format-pretty').addEventListener('click', () => {
      const val = this._editor.getValue();
      try { this._editor.setValue(JSON.stringify(JSON.parse(val), null, 2)); Toast.show('Prettified', 'success'); }
      catch { Toast.show('Not valid JSON', 'warn'); }
    });

    // Result tabs
    $$('.rtab').forEach(tab => {
      tab.addEventListener('click', () => {
        $$('.rtab').forEach(t => t.classList.remove('active'));
        $$('.rtab-pane').forEach(p => p.classList.remove('active'));
        tab.classList.add('active');
        $(`#rtab-${tab.dataset.rtab}`).classList.add('active');
      });
    });

    // Realtime controls
    $('#rt-interval').addEventListener('input', e => {
      const v = e.target.value;
      $('#rt-interval-val').textContent = `${v}ms`;
      WSManager.send({ cmd: 'subscribe', interval_ms: parseInt(v), batch_size: parseInt($('#rt-batch').value) });
    });
    $('#rt-batch').addEventListener('input', e => {
      const v = e.target.value;
      $('#rt-batch-val').textContent = v;
      WSManager.send({ cmd: 'subscribe', interval_ms: parseInt($('#rt-interval').value), batch_size: parseInt(v) });
    });
    $('#btn-rt-pause').addEventListener('click', () => {
      const paused = RealtimeEngine.togglePause();
      $('#btn-rt-pause').textContent = paused ? '▶ Resume' : '⏸ Pause';
      WSManager.send({ cmd: paused ? 'pause' : 'resume' });
    });
    $('#btn-rt-clear').addEventListener('click', () => RealtimeEngine.clear());

    // Diff
    $('#btn-diff').addEventListener('click', () => DiffEngine.run());

    // Transform
    $('#btn-transform').addEventListener('click', () => TransformEngine.runText());
    $('#btn-tx-download').addEventListener('click', () => TransformEngine.download());

    // History
    $('#history-search').addEventListener('input', debounce(e => HistoryManager.renderAll(e.target.value), 250));
    $('#btn-history-clear').addEventListener('click', () => { HistoryManager.clear(); Toast.show('History cleared', 'info'); });
    $('#btn-history-export').addEventListener('click', () => HistoryManager.export());

    // Theme
    $('#theme-toggle').addEventListener('click', () => {
      const html = document.documentElement;
      const dark = html.dataset.theme !== 'light';
      html.dataset.theme = dark ? 'light' : 'dark';
    });
  },

  switchView(view) {
    this._currentView = view;
    $$('.nav-item').forEach(i => i.classList.toggle('active', i.dataset.view === view));
    $$('.view').forEach(v => v.classList.toggle('active', v.id === `view-${view}`));
    // Refresh editors after becoming visible
    setTimeout(() => {
      if (view === 'diff') { DiffEngine._editorLeft?.refresh(); DiffEngine._editorRight?.refresh(); }
      if (view === 'transform') { TransformEngine._editorSrc?.refresh(); TransformEngine._editorOut?.refresh(); }
    }, 50);
  },

  async runParse() {
    const payload = this._editor.getValue().trim();
    if (!payload) { Toast.show('Enter a payload first', 'warn'); return; }
    const fmt = $('#format-select').value;
    $('#loading-overlay').style.display = 'flex';
    try {
      const result = await API.parse(payload, fmt);
      this.loadResult(result, payload);
      HistoryManager.add(result, payload);
    } catch (e) {
      // Error already toasted by API client
      console.error('Parse error:', e);
    } finally {
      $('#loading-overlay').style.display = 'none';
    }
  },

  async runParseFile(file, format) {
    $('#loading-overlay').style.display = 'flex';
    try {
      const result = await API.parseFile(file, format);
      const raw = await file.text().catch(() => '');
      this.loadResult(result, raw);
      HistoryManager.add(result, raw);
    } catch (e) {
      Toast.show(e.message, 'error');
    } finally {
      $('#loading-overlay').style.display = 'none';
    }
  },

  loadResult(result, rawText = '') {
    this._currentResult = result;
    if (rawText) this._editor.setValue(rawText.slice(0, 50000));

    $('#results-placeholder').style.display = 'none';
    $('#results-content').style.display = '';

    // Stats bar
    $('#r-format').textContent = result.format_label;
    $('#r-size').textContent = fmtBytes(result.raw_size);
    $('#r-fields').textContent = result.stats?.total_fields || 0;
    $('#r-depth').textContent = result.stats?.max_depth || 0;
    $('#r-parse-time').textContent = `${result.parse_time_ms}ms`;
    $('#r-entropy').textContent = result.stats?.entropy?.toFixed(2) || '—';
    $('#detected-format').textContent = result.format_id.toUpperCase();

    // Anomalies
    const abar = $('#anomaly-bar');
    if (result.anomalies?.length) {
      abar.style.display = '';
      abar.innerHTML = result.anomalies.map(a =>
        `<span class="anomaly-chip ${a.severity}" title="${_.escape(a.path || '')}">${a.code}: ${_.escape(a.message.slice(0,60))}</span>`
      ).join('');
    } else {
      abar.style.display = 'none';
    }

    // Tree
    const treeContainer = $('#tree-container');
    if (result.parsed !== null && result.parsed !== undefined) {
      TreeRenderer.render(result.parsed, treeContainer);
    } else {
      treeContainer.innerHTML = '<p style="color:var(--text-muted);padding:20px">No structured data.</p>';
    }

    // Table
    TableRenderer.render(result.fields, $('#table-container'));

    // Schema
    SchemaRenderer.render(result.schema_tree, $('#schema-container'));

    // Pretty
    const pretty = result.pretty || JSON.stringify(result.parsed, null, 2) || '';
    this._prettyEditor.setValue(pretty.slice(0, 50000));
    const mode = EditorFactory.modeForFormat(result.format_id);
    this._prettyEditor.setOption('mode', mode);
    setTimeout(() => this._prettyEditor.refresh(), 100);

    // Hex
    $('#hex-container').textContent = result.pretty && result.format_id === 'binary'
      ? result.pretty
      : Array.from(new TextEncoder().encode(rawText.slice(0, 512)))
          .reduce((lines, b, i) => {
            const row = Math.floor(i/16);
            if (!lines[row]) lines[row] = { hex: [], asc: [] };
            lines[row].hex.push(b.toString(16).padStart(2,'0'));
            lines[row].asc.push(b >= 0x20 && b < 0x7f ? String.fromCharCode(b) : '.');
            return lines;
          }, [])
          .map((r, i) => `${(i*16).toString(16).padStart(8,'0')}  ${r.hex.join(' ').padEnd(47)}  |${r.asc.join('')}|`)
          .join('\n');

    // Meta
    this._renderMeta(result);

    // Charts
    setTimeout(() => ChartEngine.renderAnalysisCharts(result), 50);

    // Activate first tab
    $$('.rtab').forEach(t => t.classList.toggle('active', t.dataset.rtab === 'tree'));
    $$('.rtab-pane').forEach(p => p.classList.toggle('active', p.id === 'rtab-tree'));

    Toast.show(`Parsed ${result.format_label} — ${result.stats?.total_fields || 0} fields`, 'success');
  },

  _renderMeta(result) {
    const container = $('#meta-container');
    container.innerHTML = '';
    const entries = [
      ['Format', result.format_label],
      ['Format ID', result.format_id],
      ['Raw Size', fmtBytes(result.raw_size)],
      ['Parse Time', `${result.parse_time_ms} ms`],
      ['SHA-256', result.sha256],
      ['Total Fields', result.stats?.total_fields],
      ['Max Depth', result.stats?.max_depth],
      ['Null Count', result.stats?.null_count],
      ['Unique Keys', result.stats?.unique_keys],
      ['Entropy', result.stats?.entropy],
      ...Object.entries(result.metadata || {}).map(([k, v]) => [
        k.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
        Array.isArray(v) ? v.join(', ') : String(v ?? '—'),
      ]),
    ];
    entries.forEach(([label, val]) => {
      if (val === undefined || val === null) return;
      const card = el('div', 'meta-card');
      const isSha = label === 'SHA-256';
      card.innerHTML = `<label>${label}</label><div class="meta-val ${isSha ? 'sha-val' : ''}">${_.escape(String(val))}</div>`;
      container.appendChild(card);
    });
  },

  _updateEditorStats() {
    const val = this._editor.getValue();
    const lines = this._editor.lineCount();
    $('#editor-stats').textContent = `${val.length.toLocaleString()} chars · ${lines} lines`;
  },

  async _liveDetect() {
    const val = this._editor.getValue().trim();
    if (!val || val.length < 10) return;
    try {
      const r = await API.detect(val);
      if (r && r.detected) {
        $('#detected-format').textContent = r.detected.toUpperCase();
      }
    } catch (_) {}
  },

  _resetResults() {
    $('#results-placeholder').style.display = '';
    $('#results-content').style.display = 'none';
    $('#detected-format').textContent = '';
    $('#editor-stats').textContent = '0 chars · 0 lines';
  },

  _populateFormatSelect(fmts) {
    // Formats already hardcoded in HTML; just validate
  },
};

// ══════════════════════════════════════════════════════════════════════════════
// BOOTSTRAP
// ══════════════════════════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', () => App.init());
