import React, { useEffect, useReducer, useRef, useState } from 'react';

// ===== Live State Machine =====
// Watches every ChannelAction walk the disposition-driven state machine in real time. Subscribes to the
// BFF SSE stream: nba.actionstate.* transitions light up the state cells + step the per-action tracks;
// router decisions (CREATE/SUPPRESS/SOFT_COMPLETE/HARD_COMPLETE) + activation-layer dispositions scroll by
// in a per-layer feed; counts aggregate per state. The states/flow mirror nba-temporal exactly.

const FLOW = [   // the happy path, left to right
  { s: 'CREATED', c: '#a78bfa', sub: 'debounce + dedup' },
  { s: 'IN_PROCESS', c: '#38bdf8', sub: 'dispatched' },
  { s: 'PRESENTED', c: '#22d3ee', sub: 'delivered' },
  { s: 'SOFT_COMPLETED', c: '#34d399', sub: 'engaged' },
  { s: 'HARD_COMPLETED', c: '#10b981', sub: 'goal met ★', term: true },
];
const BRANCH = [   // off-ramps
  { s: 'DECLINED', c: '#f59e0b', sub: 'opted out' },
  { s: 'SUPPRESSING', c: '#9ca3af', sub: 'cancel in flight' },
  { s: 'EXPIRED', c: '#fb923c', sub: 'TTL, no convert ✗', term: true },
  { s: 'FAILED', c: '#f87171', sub: 'bounce / error', term: true },
  { s: 'SUPPRESSED', c: '#6b7280', sub: 'pulled', term: true },
  // DEBOUNCED is an internal terminal (lost the debounce race, nothing sent) — tracked in the data layer
  // for ML, not surfaced as a UI cell.
];
const COLOR = Object.fromEntries([...FLOW, ...BRANCH].map((x) => [x.s, x.c]));
const ALL_STATES = [...FLOW, ...BRANCH].map((x) => x.s);
const CAT_COLOR = { router: '#fbbf24', disposition: '#34d399', state: '#818cf8', completion: '#10b981' };
const CAT_LABEL = { router: 'ROUTER', disposition: 'CHANNEL', state: 'WORKFLOW', completion: 'GOAL' };
const GLOW_MS = 1500;

export default function StateMachine({ live }) {
  const [, force] = useReducer((x) => x + 1, 0);
  const liveRef = useRef(live); liveRef.current = live;
  // slug ("member·action·channel") -> { member, action, channel, state, raw, ts }
  const wf = useRef(new Map());
  const counts = useRef(Object.fromEntries(ALL_STATES.map((s) => [s, 0])));
  const glow = useRef({});            // state -> last-transition timestamp
  const feed = useRef([]);            // rolling categorized events
  const totals = useRef({ created: 0, inProcess: 0, soft: 0, hard: 0, expired: 0, failed: 0, suppressed: 0 });
  const queue = useRef([]);

  // Counts come from GOLD (latest-fact-per-key) via the BFF resolver — seed on mount AND re-anchor every 15s so
  // the panel can't drift stale (live SSE transitions only animate the glow/tracks between re-anchors).
  useEffect(() => {
    const seed = () => fetch('/graphql', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query: '{ stateCounts }' }) })
      .then((r) => r.json()).then((j) => { if (j.data?.stateCounts) { counts.current = { ...counts.current, ...j.data.stateCounts }; force(); } })
      .catch(() => {});
    seed();
    const t = setInterval(seed, 15000);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    const es = new EventSource('/stream');
    es.onmessage = (e) => { try { const ev = JSON.parse(e.data); if (ev.cat) queue.current.push(ev); } catch {} };
    return () => es.close();
  }, []);

  useEffect(() => {
    let raf;
    const apply = (ev, now) => {
      if (!liveRef.current) return;
      // rolling feed (newest first)
      feed.current.unshift(ev); if (feed.current.length > 60) feed.current.length = 60;
      if (ev.cat === 'router' && ev.op) {
        if (ev.op === 'SOFT_COMPLETE') totals.current.soft++;
        else if (ev.op === 'HARD_COMPLETE') totals.current.hard++;
      }
      // a workflow STATE transition: re-bucket the counts and step the track
      if (ev.cat === 'state' && ev.st && ev.st.state) {
        const slug = `${ev.entity}·${ev.st.action}·${ev.st.channel}`;
        const prev = wf.current.get(slug);
        if (prev && counts.current[prev.state] != null) counts.current[prev.state] = Math.max(0, counts.current[prev.state] - 1);
        if (counts.current[ev.st.state] != null) counts.current[ev.st.state]++;
        wf.current.set(slug, { member: ev.entity, action: ev.st.action, channel: ev.st.channel, state: ev.st.state, raw: prev?.raw, ts: now });
        glow.current[ev.st.state] = now;
        if (ev.st.state === 'CREATED') totals.current.created++;
        else if (ev.st.state === 'IN_PROCESS') totals.current.inProcess++;
        else if (ev.st.state === 'EXPIRED') totals.current.expired++;
        else if (ev.st.state === 'FAILED') totals.current.failed++;
        else if (ev.st.state === 'SUPPRESSED') totals.current.suppressed++;
      }
      if (ev.cat === 'disposition' && ev.st) {   // attach the raw provider status to the live track
        // entity:key -> we don't have the slug split here; tag the most recent matching member track
        for (const [k, w] of wf.current) if (k.startsWith(ev.entity + '·')) { w.raw = ev.st.raw; break; }
      }
    };
    const loop = () => {
      const now = performance.now();
      while (queue.current.length) apply(queue.current.shift(), now);
      // expire stale tracks (terminal + old) so the stepping panel stays live
      for (const [k, w] of wf.current) if ((now - w.ts) > 90000) wf.current.delete(k);
      force(); raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(raf);
  }, []);

  const now = performance.now();
  const glowOf = (s) => Math.max(0, 1 - (now - (glow.current[s] || -1e9)) / GLOW_MS);
  const tracks = [...wf.current.values()].sort((a, b) => b.ts - a.ts).slice(0, 20);
  const T = totals.current;
  // CONVERSION HEALTH (from the gold-anchored state counts) — the headline monitoring metric: of everything that
  // RESOLVED to a terminal state, what share hard-completed (the goal) vs expired/failed/declined/suppressed.
  const C = counts.current;
  const n = (s) => C[s] || 0;
  const hard = n('HARD_COMPLETED'), expired = n('EXPIRED');
  const otherTerm = n('FAILED') + n('DECLINED') + n('SUPPRESSED');
  const resolved = hard + expired + otherTerm;
  const inFlight = n('CREATED') + n('IN_PROCESS') + n('PRESENTED') + n('SOFT_COMPLETED') + n('SUPPRESSING');
  const pct = (x) => (resolved ? (x / resolved) * 100 : 0);
  const compRate = pct(hard);
  const rateColor = compRate >= 60 ? '#10b981' : compRate >= 40 ? '#fbbf24' : '#f87171';

  const cell = (x) => {
    const g = glowOf(x.s), n = counts.current[x.s] || 0;
    return (
      <div key={x.s} className={'sm-cell' + (x.term ? ' term' : '')} style={{ borderColor: g > .15 ? x.c : '#26262c', boxShadow: g > .15 ? `0 0 ${8 + g * 16}px ${x.c}55` : 'none' }}>
        <div className="sm-cell-n" style={{ color: x.c }}>{n}</div>
        <div className="sm-cell-s">{x.s}</div>
        <div className="sm-cell-sub">{x.sub}</div>
        <div className="sm-cell-bar" style={{ background: x.c, opacity: 0.25 + g * 0.75 }} />
      </div>
    );
  };

  return (
    <div className="sm-wrap">
      <div className="sm-health">
        <div className="sm-h-main">
          <div className="sm-h-rate" style={{ color: rateColor }}>{resolved ? compRate.toFixed(0) + '%' : '—'}</div>
          <div className="sm-h-lbl">completion rate<span className="sm-h-sub">hard-completed ÷ resolved</span></div>
        </div>
        <div className="sm-h-bar" title={`hard ${hard} · expired ${expired} · failed/declined/suppressed ${otherTerm}`}>
          <span className="sm-h-seg" style={{ width: pct(hard) + '%', background: '#10b981' }} />
          <span className="sm-h-seg" style={{ width: pct(expired) + '%', background: '#fb923c' }} />
          <span className="sm-h-seg" style={{ width: pct(otherTerm) + '%', background: '#f87171' }} />
        </div>
        <div className="sm-h-metrics">
          <div className="sm-h-m"><b style={{ color: '#38bdf8' }}>{inFlight.toLocaleString()}</b><span>in flight</span></div>
          <div className="sm-h-m"><b>{resolved.toLocaleString()}</b><span>resolved</span></div>
          <div className="sm-h-m"><b style={{ color: '#10b981' }}>{hard.toLocaleString()}</b><span>hard ★</span></div>
          <div className="sm-h-m"><b style={{ color: '#fb923c' }}>{expired.toLocaleString()}</b><span>expired</span></div>
        </div>
      </div>

      <div className="sm-flow">
        <div className="sm-row">{FLOW.map((x, i) => <React.Fragment key={x.s}>{i > 0 && <span className="sm-arrow">→</span>}{cell(x)}</React.Fragment>)}</div>
        <div className="sm-row branch">{BRANCH.map(cell)}</div>
      </div>

      <div className="sm-cols">
        <div className="panel grow">
          <div className="panel-h">Live tracks · actions stepping the machine</div>
          <div className="sm-tracks">
          {!tracks.length && <div className="muted sm">{live ? 'waiting for activations…' : 'turn on continuous mode + produce facts'}</div>}
          {tracks.map((w) => (
            <div className="sm-track" key={w.member + w.action + w.channel}>
              <div className="sm-track-top">
                <span className="wf-entity">{w.member}</span>
                <span className="wf-name">{w.action}<span className="sm-chan">/{w.channel}</span></span>
                {w.raw && <span className="sm-raw">{w.raw}</span>}
              </div>
              <div className="sm-track-states">
                {FLOW.map((x) => {
                  const idx = FLOW.findIndex((f) => f.s === w.state);
                  const here = w.state === x.s, done = idx >= 0 && FLOW.indexOf(x) < idx;
                  return <span key={x.s} className={'sm-pip' + (here ? ' on' : '') + (done ? ' done' : '')}
                    style={{ background: here ? x.c : done ? x.c + '66' : '#222', borderColor: here ? x.c : 'transparent' }} title={x.s} />;
                })}
                {BRANCH.some((b) => b.s === w.state) && <span className="sm-term" style={{ color: COLOR[w.state] }}>{w.state}</span>}
                <span className="sm-cur" style={{ color: COLOR[w.state] || '#aaa' }}>{w.state}</span>
              </div>
            </div>
          ))}
          </div>
        </div>

        <div className="panel grow">
          <div className="panel-h">Event feed · by layer</div>
          <div className="sm-feed">
            {feed.current.slice(0, 60).map((e, i) => (
              <div className="sm-fe" key={i}>
                <span className="sm-fe-cat" style={{ color: CAT_COLOR[e.cat], borderColor: (CAT_COLOR[e.cat] || '#555') + '55' }}>{CAT_LABEL[e.cat] || e.cat}</span>
                <span className="sm-fe-ent">{e.entity}</span>
                <span className="sm-fe-lbl">{feLabel(e)}</span>
              </div>
            ))}
            {!feed.current.length && <div className="muted sm">no state-machine events yet…</div>}
          </div>
        </div>
      </div>
    </div>
  );
}

// compact, layer-aware label for a feed row
function feLabel(e) {
  if (e.cat === 'router') return `${e.op}${e.payload?.actionId ? ' ' + e.payload.actionId : ''}${e.payload?.channel ? '/' + e.payload.channel : ''}`;
  if (e.cat === 'state' && e.st) return `${e.st.action}/${e.st.channel} → ${e.st.state}`;
  if (e.cat === 'disposition' && e.st) return `${(e.key || '').replace('nba.disposition.', '')} = ${e.st.raw}${e.st.state ? ' (' + e.st.state + ')' : ''}`;
  if (e.cat === 'completion') return (e.key || '').replace('nba.', '') + ' = ' + e.value;
  return e.label;
}
