import React, { useEffect, useReducer, useRef, useState } from 'react';

// ===== Live System Map — the centerpiece =====
// Subscribes to the BFF SSE stream and animates the entire NBA system: every component lights up as
// facts arrive/emit, packets fly along the topic edges, the Temporal state machine tracks workflow
// transitions. Scope to one action/member to watch just its journey, see aggregate analytics build
// up at every stage, and click any node to peek the live payloads flowing through it.

const W = 1260, H = 560;
const NODE_W = 158, NODE_H = 74;
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const GLOW_MS = 1300, PACKET_MS = 900, MAX_PACKETS = 60;

const TOPIC_COLOR = {
  'nba.facts': '#60a5fa', 'nba.member.facts': '#34d399', 'nba.snapshots': '#60a5fa',
  'nba.evaluations': '#c4b5fd', 'nba.activations': '#fbbf24', 'nba.definitions': '#6f6f78',
};
const KIND_COLOR = { source: '#94a3b8', svc: '#818cf8', machine: '#fbbf24', lake: '#34d399', learn: '#a78bfa' };
// per-state colors for the transition boxes that flow out of the state machine (mirror the State Machine view).
const STATE_COLOR = {
  CREATED: '#a78bfa', IN_PROCESS: '#38bdf8', PRESENTED: '#22d3ee', SOFT_COMPLETED: '#34d399',
  HARD_COMPLETED: '#10b981', DECLINED: '#f59e0b', SUPPRESSING: '#9ca3af', FAILED: '#f87171',
  SUPPRESSED: '#6b7280', EXPIRED: '#fb923c', DEBOUNCED: '#7c8190',
};
const STATES = ['CREATED', 'IN_PROCESS', 'PRESENTED', 'SOFT_COMPLETED'];
const TERMINALS = ['FAILED', 'HARD_COMPLETED', 'EXPIRED', 'SUPPRESSED'];   // DEBOUNCED = internal, not surfaced
const actionOf = (label) => (label || '').replace(/^(CREATE|DISPATCH|SUPPRESS|CANCEL)\s+/, '');

function bezierPt(a, c, b, t) {
  const u = 1 - t;
  return { x: u * u * a.x + 2 * u * t * c.x + t * t * b.x, y: u * u * a.y + 2 * u * t * c.y + t * t * b.y };
}
const blankAgg = () => ({ total: 0, topic: {}, op: {}, sent: 0, eligible: 0 });
// latency overlay: green (fast) -> amber -> red (slow). The headline number is the p50 (median) hop — robust to
// the stale fold-back correlation that drags avg into the tens of seconds. Each stage is judged on ITS OWN SLO
// (below), not one global bar: cloud round-trips (Databricks medallion + tunnel) legitimately take seconds and
// must not read as "slow"; local service hops stay tight. Keyed by the DOWNSTREAM node = its smallest inbound hop.
const LAT_SLO = {                                   // [green<, amber<] ms
  source: [45000, 90000], lake: [25000, 45000],     // ingress + full medallion ingest->gold->member.facts over the tunnel
  ml: [15000, 30000],                               // rules->ml = cloud CQL scorer round-trip (~12s is healthy)
  temporal: [3000, 8000],                           // router->temporal incl. the batch-orchestrator settle window
  snapshot: [400, 2000], rules: [250, 1500], router: [250, 1500], action: [600, 2500],
};
const CLOUD_STAGE = { source: 1, lake: 1, ml: 1 };  // round-trips to Databricks — flagged ☁ so seconds read as cloud, not slow
const sloFor = (id) => LAT_SLO[id] || [250, 1500];
const latColor = (id, ms) => { if (ms == null) return '#6b7280'; const [g, a] = sloFor(id); return ms < g ? '#34d399' : ms < a ? '#fbbf24' : '#f87171'; };
const fmtMs = (ms) => ms == null ? '' : ms < 1000 ? Math.round(ms) + 'ms' : (ms / 1000).toFixed(1) + 's';

// tiny inline sparkline over the rolling history (max-normalized). Shows the trend's SHAPE — load ramping up
// (throughput) and latency rising under load then tapering.
function Spark({ vals, color, w = 54, h = 15 }) {
  if (!vals || vals.length < 2) return <svg width={w} height={h} className="spark" />;
  const max = Math.max(1, ...vals);
  const pts = vals.map((v, i) => `${(i / (vals.length - 1)) * w},${h - (v / max) * (h - 2) - 1}`).join(' ');
  return <svg width={w} height={h} className="spark"><polyline points={pts} fill="none" stroke={color} strokeWidth="1.4" opacity="0.9" /></svg>;
}

export default function SystemMap({ live }) {
  const [topo, setTopo] = useState(null);
  const [sel, setSel] = useState(null);
  const [throttle, setThrottle] = useState([]);     // live channel-throttle strip
  const [goldStats, setGoldStats] = useState(null); // the pre-aggregated gold funnel (real reality), polled
  const [stateDist, setStateDist] = useState(null); // { counts:{state:n}, model:[states] } — live state-machine rollup
  const [scope, setScope] = useState('');           // '' = all; else entity name or action name
  const [, force] = useReducer((x) => x + 1, 0);
  const st = useRef({ glow: {}, count: {}, packets: [], events: [], wf: {}, byNode: {}, agg: blankAgg(), actions: new Set(), members: new Set() });
  const queue = useRef([]);
  const metrics = useRef({ throughput: {}, latency: {} });   // live instantaneous: per-node ev/s + per-edge ms
  // HELD (stateful) metrics — the live window empties on a quiet tick, so we KEEP the last-known throughput/latency
  // per node + per edge (with the ms it was last seen) and just dim it when it goes stale, instead of blinking to nothing.
  const held = useRef({ tp: {}, tpAt: {}, lat: {}, latAt: {} });
  const liveRef = useRef(live); liveRef.current = live;
  const scopeRef = useRef(scope); scopeRef.current = scope;
  const [showLat, setShowLat] = useState(true);              // toggle the latency/throughput overlay
  const svgRef = useRef(null);
  const [view, setView] = useState({ tx: 0, ty: 0, k: 1 });  // pan/zoom on top of the fitted viewBox
  const drag = useRef(null);
  const viewRef = useRef(view); viewRef.current = view;

  useEffect(() => { fetch('/topology').then((r) => r.json()).then(setTopo).catch(() => {}); }, []);

  // anchor a line from `from`'s center toward `to` onto `from`'s rectangle border (so edges meet node edges,
  // not centers — no more lines plowing through the boxes).
  const border = (from, to) => {
    const dx = to.x - from.x, dy = to.y - from.y;
    const hw = NODE_W / 2 + 5, hh = NODE_H / 2 + 5;
    const s = Math.min(dx ? hw / Math.abs(dx) : Infinity, dy ? hh / Math.abs(dy) : Infinity);
    return { x: from.x + dx * s, y: from.y + dy * s };
  };
  // pipeline order — drives edge curvature: a "forward" edge (downstream) bows gently UP and rides the spine,
  // a "back" edge (a fold-back to the snapshot / an analytics tail to the lake) bows DOWN into the lower lane,
  // deeper the longer it is so the recirculation arcs nest cleanly instead of plowing through the nodes.
  const ORDER = { source: 0, lake: 1, train: 1.2, snapshot: 2, rules: 3, ml: 3.5, router: 4, temporal: 5, action: 6 };
  const geo = React.useMemo(() => {
    if (!topo) return null;
    const C = {}; topo.nodes.forEach((n) => { C[n.id] = { x: n.x, y: n.y }; });
    const edges = topo.edges.map((e) => {
      const a = border(C[e.from], C[e.to]), b = border(C[e.to], C[e.from]);
      const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2;
      const dx = b.x - a.x, dy = b.y - a.y, len = Math.hypot(dx, dy) || 1;
      const back = (ORDER[e.to] ?? 9) < (ORDER[e.from] ?? 0);
      const analytics = e.to === 'lake' && e.from !== 'source';          // lake observing the pipeline (secondary)
      let nx = -dy / len, ny = dx / len; if (ny < 0) { nx = -nx; ny = -ny; }   // normal pointing DOWN (+y)
      const off = back ? clamp(len * 0.13, 34, 82) : 12;                  // gentle: forward near-straight, fold-backs just bow clear
      const s = back ? 1 : -1;                                            // back bows down, forward bows up
      const c = { x: mx + nx * off * s, y: my + ny * off * s };
      return { ...e, a, b, c, back, analytics, d: `M ${a.x} ${a.y} Q ${c.x} ${c.y} ${b.x} ${b.y}` };
    });
    // fitted viewBox over nodes AND edge control points (so the lower recirculation arcs never clip), + padding.
    const px = [], py = [];
    topo.nodes.forEach((n) => { px.push(n.x - NODE_W / 2, n.x + NODE_W / 2); py.push(n.y - NODE_H / 2, n.y + NODE_H / 2); });
    edges.forEach((e) => { px.push(e.c.x); py.push(e.c.y); });
    const pad = 56, minX = Math.min(...px) - pad, minY = Math.min(...py) - pad;
    const vb = { x: minX, y: minY, w: Math.max(...px) + pad - minX, h: Math.max(...py) + pad - minY };
    return { C, edges, vb };
  }, [topo]);

  // wheel-to-zoom toward the cursor (native, non-passive so we can preventDefault the page scroll).
  useEffect(() => {
    const el = svgRef.current; if (!el || !geo) return;
    const onWheel = (e) => {
      e.preventDefault();
      const r = el.getBoundingClientRect();
      const ux = geo.vb.x + (e.clientX - r.left) * (geo.vb.w / r.width);
      const uy = geo.vb.y + (e.clientY - r.top) * (geo.vb.h / r.height);
      // gentle, delta-scaled zoom (capped per event so a fast mouse wheel or trackpad fling can't lurch).
      const v = viewRef.current, k = clamp(v.k * Math.exp(clamp(-e.deltaY, -80, 80) * 0.0008), 0.5, 4);
      const cx = (ux - v.tx) / v.k, cy = (uy - v.ty) / v.k;
      setView({ k, tx: ux - cx * k, ty: uy - cy * k });
    };
    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, [geo]);

  useEffect(() => {
    if (!topo) return;
    const es = new EventSource('/stream');
    es.onmessage = (e) => { try { const ev = JSON.parse(e.data); if (ev.emitter) queue.current.push(ev); } catch {} };
    // PAGE-LOAD SEED: the BFF's hello carries last-known-good metrics (persisted server-side, broker-clock
    // accurate) WITH ages, so a tab opened during a quiet moment shows a populated map immediately instead of
    // blank. We backdate held.*At by the server-reported age (against OUR clock) so genuinely-stale values
    // still dim correctly, while fresh ones read live.
    es.addEventListener('hello', (e) => {
      try {
        const hl = JSON.parse(e.data); const lk = hl.lastKnown || {}; const nowMs = Date.now(), h = held.current;
        for (const k in (lk.tp || {})) { const x = lk.tp[k]; if (x && x.v > 0) { h.tp[k] = x.v; h.tpAt[k] = nowMs - (x.ageMs || 0); } }
        for (const k in (lk.lat || {})) { const x = lk.lat[k]; if (x && x.v != null) { h.lat[k] = x.v; h.latAt[k] = nowMs - (x.ageMs || 0); } }
        force();
      } catch {}
    });
    es.addEventListener('metrics', (e) => {                  // named SSE event — update live + persist last-known
      try {
        const m = JSON.parse(e.data); metrics.current = m;
        const nowMs = Date.now(), h = held.current;
        for (const k in (m.throughput || {})) if (m.throughput[k] > 0) { h.tp[k] = m.throughput[k]; h.tpAt[k] = nowMs; }
        // p50 (median) + a min-sample gate: a hop colored off 1-2 lucky/stale pairs is noise, not a signal.
        for (const k in (m.latency || {})) { const L = m.latency[k]; if (L && L.n >= 3) { h.lat[k] = L.p50; h.latAt[k] = nowMs; } }
      } catch {}
    });
    return () => es.close();
  }, [topo]);

  // live channel-throttle strip — which outbound channels are filling toward / over their daily cap
  useEffect(() => {
    let alive = true, t;
    const tick = async () => {
      try {
        const r = await fetch('/graphql', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ query: '{ throttleStats { channel sent cap throttled } }' }) });
        const j = await r.json(); if (alive) setThrottle(j.data?.throttleStats || []);
      } catch {}
      if (alive) t = setTimeout(tick, 6000);
    };
    tick();
    return () => { alive = false; clearTimeout(t); };
  }, []);

  // reset aggregates/map when scope changes
  useEffect(() => { const s = st.current; s.agg = blankAgg(); s.count = {}; s.events = []; s.byNode = {}; s.wf = {}; }, [scope]);

  // GOLD-FIRST: the stats strip shows the PRE-AGGREGATED gold funnel (gold_system_stats — the BFF reads the one-row
  // table in ms), POLLED so it stays current as the flywheel writes gold. This is the REAL accumulated reality
  // (distinct members through the funnel), not a from-zero session event counter. The map's per-node rates + the
  // packet animation stay live/event-driven; this is the stateful "what gold says right now" layer.
  useEffect(() => {
    let alive = true, t;
    const tick = async () => {
      try {
        const r = await fetch('/graphql', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ query: '{ funnel { label count } }' }) });
        const j = await r.json(); const f = (j.data && j.data.funnel) || [];
        if (alive && f.length) { const o = {}; f.forEach((x) => { o[x.label] = x.count; }); setGoldStats(o); }
      } catch {}
      if (alive) t = setTimeout(tick, 8000);
    };
    tick();
    return () => { alive = false; clearTimeout(t); };
  }, []);

  // STATE-MACHINE DISTRIBUTION: every (member,action,channel) workflow's CURRENT state, rolled up across the
  // canonical lifecycle. The BFF tracks the latest state per key from nba.actionstate.* facts; we poll the
  // rollup so the box shows a live count for EVERY state, not just the few workflows currently transitioning.
  useEffect(() => {
    let alive = true, t;
    const tick = async () => {
      try {
        const r = await fetch('/graphql', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ query: '{ stateCounts stateModel }' }) });
        const j = await r.json();
        if (alive && j.data) setStateDist({ counts: j.data.stateCounts || {}, model: j.data.stateModel || [] });
      } catch {}
      if (alive) t = setTimeout(tick, 4000);
    };
    tick();
    return () => { alive = false; clearTimeout(t); };
  }, []);

  useEffect(() => {
    if (!geo) return;
    let raf;
    const inScope = (ev) => { const q = scopeRef.current; if (!q) return true; return ev.entity === q || actionOf(ev.label) === q || (ev.label || '').includes(q) || (ev.key || '').includes(q); };
    const apply = (ev, now) => {
      const s = st.current;
      if (ev.entity) s.members.add(ev.entity);
      if (ev.topic === 'nba.activations') s.actions.add(actionOf(ev.label));
      if (!liveRef.current || !inScope(ev)) return;
      // aggregates (scoped)
      const a = s.agg; a.total++; a.topic[ev.topic] = (a.topic[ev.topic] || 0) + 1;
      if (ev.op) a.op[ev.op] = (a.op[ev.op] || 0) + 1;
      if (ev.topic === 'nba.evaluations') { const m = /(\d+) eligible/.exec(ev.label); if (m) a.eligible += +m[1]; }
      if (ev.key && ev.key.startsWith('nba.actionstate') && ev.value === 'IN_PROCESS') a.sent++;
      // map glow + buffers
      s.glow[ev.emitter] = now; s.count[ev.emitter] = (s.count[ev.emitter] || 0) + 1;
      (s.byNode[ev.emitter] ||= []).unshift(ev); if (s.byNode[ev.emitter].length > 30) s.byNode[ev.emitter].length = 30;
      s.events.unshift(ev); if (s.events.length > 80) s.events.length = 80;
      // packets — a state transition (CREATED/IN_PROCESS/…) flows out of the state machine as a LABELED BOX
      // (state-colored); everything else is a plain dot.
      const isState = ev.cat === 'state' && ev.st && ev.st.state;
      const outs = geo.edges.filter((e) => e.from === ev.emitter && e.topic === ev.topic);
      for (const e of (outs.length ? outs : geo.edges.filter((e) => e.from === ev.emitter)).slice(0, 3))
        if (s.packets.length < MAX_PACKETS) s.packets.push({ e, start: now,
          color: isState ? (STATE_COLOR[ev.st.state] || '#818cf8') : (TOPIC_COLOR[ev.topic] || '#818cf8'),
          label: isState ? ev.st.state : null });
      // state machine
      if (ev.topic === 'nba.activations' && ev.op) {
        const key = `${ev.entity}·${actionOf(ev.label)}`;
        const cur = s.wf[key] || { key, entity: ev.entity, name: actionOf(ev.label), state: 'pending', ts: now };
        cur.state = ev.op === 'DISPATCH' ? 'SENT' : ev.op === 'SUPPRESS' ? 'SUPPRESSED' : 'CREATED';
        cur.ts = now; s.wf[key] = cur;
      }
      if (ev.key && ev.key.startsWith('nba.actionstate') && typeof ev.value === 'string') {
        const f = Object.values(s.wf).find((w) => w.entity === ev.entity); if (f) { f.state = ev.value; f.ts = now; }
      }
    };
    const loop = () => {
      const now = performance.now();
      while (queue.current.length) apply(queue.current.shift(), now);
      st.current.packets = st.current.packets.filter((p) => { if (now - p.start >= PACKET_MS) { st.current.glow[p.e.to] = now; return false; } return true; });
      force(); raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, [geo]);

  if (!topo || !geo) return <div className="skeleton" style={{ height: 420 }} />;
  const now = performance.now();
  const glowOf = (id) => Math.max(0, 1 - (now - (st.current.glow[id] || -1e9)) / GLOW_MS);
  const nowMs = Date.now(), STALE_MS = 14000;   // a held metric older than this dims (last-known but going stale)
  const heldTp = (id) => { const h = held.current; return h.tp[id] == null ? null : { v: h.tp[id], stale: nowMs - (h.tpAt[id] || 0) > STALE_MS }; };
  const heldLat = (edge) => { const h = held.current; return h.lat[edge] == null ? null : { v: h.lat[edge], stale: nowMs - (h.latAt[edge] || 0) > STALE_MS }; };
  const wfList = Object.values(st.current.wf).sort((a, b) => b.ts - a.ts).slice(0, 7);
  const peek = sel ? (st.current.byNode[sel] || []) : null;
  const a = st.current.agg;
  // the stats strip = the GOLD funnel (real distinct-member reality from gold_system_stats), not session event counts.
  const gs = goldStats || {};
  const STATS = [
    { k: 'members', label: 'Members', v: gs.Members ?? '—', c: '#60a5fa' },
    { k: 'eval', label: 'Eligible', v: gs.Eligible ?? '—', c: '#c4b5fd' },
    { k: 'scored', label: 'Scored', v: gs.Scored ?? '—', c: '#818cf8' },
    { k: 'create', label: 'Activated', v: gs.Activated ?? '—', c: '#fbbf24' },
    { k: 'sent', label: 'Sent', v: gs.Sent ?? '—', c: '#34d399' },
  ];
  const scopeOpts = [...st.current.actions].filter(Boolean).sort();
  const memberOpts = [...st.current.members].filter(Boolean).sort().slice(0, 40);

  const onPanDown = (e) => { drag.current = { x: e.clientX, y: e.clientY, tx: view.tx, ty: view.ty }; force(); };
  const onPanMove = (e) => {
    if (!drag.current || !svgRef.current || !geo) return;
    const r = svgRef.current.getBoundingClientRect();
    setView((v) => ({ ...v, tx: drag.current.tx + (e.clientX - drag.current.x) * (geo.vb.w / r.width), ty: drag.current.ty + (e.clientY - drag.current.y) * (geo.vb.h / r.height) }));
  };
  const onPanUp = () => { if (drag.current) { drag.current = null; force(); } };
  const zoom = (f) => setView((v) => ({ ...v, k: clamp(v.k * f, 0.5, 4) }));

  return (
    <div className="sysmap-page">
      <div className="sysmap-bar">
        <div className="scope">
          <span className="scope-label">Scope</span>
          <select value={scope} onChange={(e) => { setScope(e.target.value); setSel(null); }}>
            <option value="">All activity</option>
            {scopeOpts.length > 0 && <optgroup label="Action">{scopeOpts.map((x) => <option key={'a' + x} value={x}>{x}</option>)}</optgroup>}
            {memberOpts.length > 0 && <optgroup label="Member">{memberOpts.map((x) => <option key={'m' + x} value={x}>{x}</option>)}</optgroup>}
          </select>
          {scope && <button className="btn ghost sm" onClick={() => setScope('')}>clear</button>}
        </div>
        <div className="stats-strip">
          {goldStats && <span title="distinct-member funnel from the pre-aggregated gold product (gold_system_stats), polled — the real accumulated reality, not session event counts" style={{ fontSize: '.58rem', fontWeight: 700, color: '#34d399', alignSelf: 'center', letterSpacing: '.08em', opacity: 0.85, marginRight: '4px' }}>◆ LAKE</span>}
          {STATS.map((s) => <div className="stat" key={s.k}><div className="stat-v" style={{ color: s.c }}>{s.v}</div><div className="stat-l">{s.label}</div></div>)}
        </div>
        <button className={'btn ghost sm' + (showLat ? ' on' : '')} onClick={() => setShowLat((v) => !v)} title="overlay per-component throughput (ev/s) + per-edge hop latency">⏱ latency {showLat ? 'on' : 'off'}</button>
        <div className="map-zoom" title="drag to pan · scroll to zoom">
          <button className="btn ghost sm" onClick={() => zoom(1.15)}>＋</button>
          <button className="btn ghost sm" onClick={() => zoom(1 / 1.15)}>－</button>
          <button className="btn ghost sm" onClick={() => setView({ tx: 0, ty: 0, k: 1 })} title="reset to fit">⤢ fit</button>
        </div>
      </div>

      {throttle.some((c) => c.cap != null) && (
        <div className="throttle-strip">
          <span className="ts-label">⊟ channel throttle</span>
          {throttle.filter((c) => c.cap != null).map((c) => (
            <span key={c.channel} className={'ts-chip' + (c.throttled ? ' off' : '')} title={`${c.sent} of ${c.cap} daily cap sent today`}>
              {c.channel} <b>{c.sent}/{c.cap}</b>{c.throttled ? ' ⛔ rerouting' : ''}
            </span>
          ))}
        </div>
      )}

      <div className="sysmap-wrap">
        <div className="sysmap">
          <svg ref={svgRef} viewBox={`${geo.vb.x} ${geo.vb.y} ${geo.vb.w} ${geo.vb.h}`} className="sysmap-svg"
            onMouseDown={onPanDown} onMouseMove={onPanMove} onMouseUp={onPanUp} onMouseLeave={onPanUp}
            style={{ cursor: drag.current ? 'grabbing' : 'grab' }}>
            <defs><filter id="glow" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur stdDeviation="4" result="b" /><feMerge><feMergeNode in="b" /><feMergeNode in="SourceGraphic" /></feMerge></filter></defs>
            <g transform={`translate(${view.tx} ${view.ty}) scale(${view.k})`}>
            {geo.edges.map((e) => {
              const hot = Math.max(glowOf(e.from), st.current.packets.some((p) => p.e.id === e.id) ? 1 : 0);
              if (e.lane === 'learn') {                              // the offline LEARNING LOOP — a dashed violet control plane, labeled
                return (
                  <g key={e.id}>
                    <path d={e.d} fill="none" stroke="#a78bfa" strokeWidth="1.4" strokeOpacity="0.5" strokeDasharray="6 5" />
                    {e.elabel && <text x={e.c.x} y={e.c.y - 4} textAnchor="middle" style={{ fill: '#c4b5fd', fontSize: '9px', fontWeight: 600, letterSpacing: '.05em', textTransform: 'uppercase' }}>{e.elabel}</text>}
                  </g>
                );
              }
              const col = TOPIC_COLOR[e.topic] || '#2a2a30';
              if (e.analytics) return <path key={e.id} d={e.d} fill="none" stroke={col} strokeWidth="1" strokeOpacity={0.06 + hot * 0.2} strokeDasharray="3 6" />;
              return <path key={e.id} d={e.d} fill="none" stroke={col} strokeWidth={hot > .2 ? 2.2 : 1.3} strokeOpacity={0.14 + hot * 0.55} />;
            })}
            {st.current.packets.map((p, i) => {
              const t = Math.min(1, (now - p.start) / PACKET_MS); const pt = bezierPt(p.e.a, p.e.c, p.e.b, t);
              if (p.label) {                                  // a state transition — a little labeled box
                const w = p.label.length * 5.6 + 12;
                return (
                  <g key={i} transform={`translate(${pt.x}, ${pt.y})`} opacity={1 - t * 0.25}>
                    <rect x={-w / 2} y="-8" width={w} height="16" rx="4" fill="#0c0c0e" stroke={p.color} strokeWidth="1.2" filter="url(#glow)" />
                    <text x="0" y="3.5" textAnchor="middle" className="pkt-lbl" style={{ fill: p.color }}>{p.label}</text>
                  </g>
                );
              }
              return <circle key={i} cx={pt.x} cy={pt.y} r="4.5" fill={p.color} filter="url(#glow)" opacity={1 - t * 0.3} />;
            })}
            {topo.nodes.map((n) => {
              const g = glowOf(n.id), col = KIND_COLOR[n.kind] || '#818cf8';
              // processing latency WITHIN this component = its forward incoming hop (upstream emit -> this emit).
              // HELD = last-known (persists through quiet windows, dims when stale). ml IS shown now too — its hop is
              // replay-stamped so it reads fast, but it's consistent with the rest rather than a mysterious blank.
              // processing latency = time from this component's MOST-RECENT triggering input = the SMALLEST
              // inbound hop. A fixed forward edge (e.g. lake->snapshot) can correlate the output against a STALE
              // upstream fact for the same entity (when the entity's recent facts are internal fold-backs from
              // ml/temporal/action), wildly overstating it — that was the bogus 46s on the snapshot builder.
              const ins = topo.edges.filter((e) => e.to === n.id && e.lane !== 'learn');
              let latH = null, fwd = null;
              for (const e of ins) { const h = heldLat(e.from + '>' + n.id); if (h && (!latH || h.v < latH.v)) { latH = h; fwd = e; } }
              if (!fwd) fwd = ins.find((e) => (ORDER[e.from] ?? 9) < (ORDER[e.to] ?? 0)) || ins[0];   // structural fallback before any sample
              const tpH = heldTp(n.id);
              return (
                <g key={n.id} transform={`translate(${n.x - NODE_W / 2}, ${n.y - NODE_H / 2})`} onClick={() => setSel(n.id)} style={{ cursor: 'pointer' }}>
                  <rect width={NODE_W} height={NODE_H} rx="11" fill="#121215" stroke={sel === n.id ? col : '#2a2a30'} strokeWidth={sel === n.id ? 2 : 1} />
                  <rect width={NODE_W} height={NODE_H} rx="11" fill={col} opacity={g * 0.16} filter={g > .25 ? 'url(#glow)' : undefined} />
                  <rect x="0" y="0" width="4" height={NODE_H} rx="2" fill={col} opacity={0.5 + g * 0.5} />
                  <text x="15" y="25" className="nm-label" textLength={n.label.length > 15 ? NODE_W - 30 : undefined} lengthAdjust="spacingAndGlyphs">{n.label}</text>
                  <text x="15" y="44" className="nm-sub" textLength={n.sub.length > 24 ? NODE_W - 28 : undefined} lengthAdjust="spacingAndGlyphs">{n.sub}</text>
                  <circle cx={NODE_W - 16} cy="18" r={g > .25 ? 4.5 : 3.5} fill={col} opacity={0.3 + g * 0.7} />
                  {showLat ? (<>
                    {tpH && <text x="15" y="64" className="nm-tp" opacity={tpH.stale ? 0.4 : 1}>{tpH.v}/s</text>}
                    {latH && <text x={NODE_W - 13} y="64" textAnchor="end" className="nm-lat" opacity={latH.stale ? 0.4 : 1} style={{ fill: latColor(n.id, latH.v) }}>{CLOUD_STAGE[n.id] ? '☁' : '⏱'} {fmtMs(latH.v)}</text>}
                  </>) : (
                    <text x="15" y="64" className="nm-count">Σ {st.current.count[n.id] || 0}</text>
                  )}
                </g>
              );
            })}
            </g>
          </svg>
        </div>

        <aside className="sysmap-side">
          {showLat && (
            <div className="panel">
              <div className="panel-h">Load &amp; latency · live {(metrics.current.history?.length || 0) * 5}s window</div>
              {topo.nodes.map((n) => {
                const hist = metrics.current.history || [];
                const tpS = hist.map((p) => (p.tp && p.tp[n.id]) || 0);
                // same as the map node: the SMALLEST inbound hop = the real processing latency (avoids the
                // stale-upstream correlation that inflated the snapshot builder).
                const inEdges = topo.edges.filter((e) => e.to === n.id && e.lane !== 'learn');
                let latH2 = null, latKey = null;
                for (const e of inEdges) { const k = e.from + '>' + n.id; const h = heldLat(k); if (h && (!latH2 || h.v < latH2.v)) { latH2 = h; latKey = k; } }
                const latS = latKey ? hist.map((p) => (p.lat && p.lat[latKey]) || 0) : [];
                const tpH2 = heldTp(n.id);
                return (
                  <div className="trow" key={n.id}>
                    <span className="tname">{n.label}</span>
                    <Spark vals={tpS} color="#38bdf8" /><span className="tval" style={{ opacity: tpH2 && tpH2.stale ? 0.4 : 1 }}>{tpH2 ? tpH2.v + '/s' : '—'}</span>
                    <Spark vals={latS} color={latColor(n.id, latH2 ? latH2.v : null)} /><span className="tlat" style={{ color: latColor(n.id, latH2 ? latH2.v : null), opacity: latH2 && latH2.stale ? 0.4 : 1 }}>{CLOUD_STAGE[n.id] && latH2 ? '☁ ' : ''}{latH2 ? fmtMs(latH2.v) : '—'}</span>
                  </div>
                );
              })}
            </div>
          )}
          <div className="panel grow statemachine">
            <div className="panel-h">Temporal · state machine
              {stateDist && <span className="sm-total">{Object.values(stateDist.counts).reduce((a, b) => a + b, 0).toLocaleString()} active</span>}
            </div>
            {!stateDist ? <div className="muted sm">loading state counts…</div> : (() => {
              const { counts, model } = stateDist;
              const order = (model && model.length) ? model : Object.keys(STATE_COLOR);
              const max = Math.max(1, ...order.map((s) => counts[s] || 0));
              return (
                <div className="state-dist">
                  {order.map((s) => {
                    const n = counts[s] || 0;
                    return (
                      <div className={'state-row' + (n ? '' : ' zero')} key={s}>
                        <span className="state-name" style={{ color: STATE_COLOR[s] || '#818cf8' }}>{s.replace(/_/g, ' ')}</span>
                        <span className="state-bar-wrap"><span className="state-bar" style={{ width: (n / max * 100) + '%', background: STATE_COLOR[s] || '#818cf8' }} /></span>
                        <span className="state-count">{n.toLocaleString()}</span>
                      </div>
                    );
                  })}
                </div>
              );
            })()}
            {wfList.length > 0 && <div className="sm-live-h">live transitions</div>}
            {wfList.slice(0, 2).map((w) => (
              <div className="wf" key={w.key}>
                <div className="wf-top"><span className="wf-entity">{w.entity}</span><span className="wf-name">{w.name}</span></div>
                <div className="wf-track">
                  {STATES.map((s) => <span key={s} className={'wf-state' + (w.state === s ? ' on' : '') + (STATES.indexOf(w.state) > STATES.indexOf(s) ? ' done' : '')}>{s}</span>)}
                  {TERMINALS.includes(w.state) && <span className="wf-state term">{w.state}</span>}
                </div>
              </div>
            ))}
          </div>
          {sel && (
            <div className="panel grow">
              <div className="panel-h">Peek · {sel} <button className="btn icon" onClick={() => setSel(null)}>✕</button></div>
              {!peek?.length && <div className="muted sm">no events captured yet</div>}
              {peek?.slice(0, 16).map((e, i) => (
                <div className="peek" key={i}><span className="peek-topic" style={{ color: TOPIC_COLOR[e.topic] }}>{e.topic.replace('nba.', '')}</span><span className="peek-entity">{e.entity}</span><span className="peek-label">{e.label}</span></div>
              ))}
            </div>
          )}
        </aside>

        {/* Live event stream — drops into the LEFT column UNDER the map (grid row 2), filling the gap
            the taller right aside leaves. Nothing extends below the aside, so the page stops scrolling.
            Events wrap left-to-right newest-first; overflow rows are clipped (no internal scroll). */}
        <div className="panel event-stream-bar">
          <div className="panel-h">Live event stream{scope && <span className="scoped"> · {scope}</span>}
            {st.current.events.length > 0 && <span className="es-count">{st.current.events.length} live</span>}
          </div>
          <div className="ticker-bar">
            {st.current.events.slice(0, 80).map((e, i) => (
              <span className="tick-chip" key={i}><span className="tick-dot" style={{ background: TOPIC_COLOR[e.topic] }} /><span className="tick-emit">{e.emitter}</span><span className="tick-topic">{e.topic.replace('nba.', '')}</span><span className="tick-label">{e.label}</span></span>
            ))}
            {!st.current.events.length && <div className="muted sm">turn on continuous mode &amp; produce facts to watch the system come alive…</div>}
          </div>
        </div>
      </div>
    </div>
  );
}
