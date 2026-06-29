import React, { useEffect, useRef, useState } from 'react';
import { gql, coerce } from './gql.js';
import { Funnel, BarList, Donut, Kpi } from './charts.jsx';
import { Studio, Groups, Experiences } from './authoring.jsx';
import SystemMap from './SystemMap.jsx';
import StateMachine from './StateMachine.jsx';
import TraceView from './TraceView.jsx';

// ---- real-time polling hook ----
function usePoll(fn, intervalMs, on = true, deps = []) {
  const [data, setData] = useState(null);
  const [err, setErr] = useState(null);
  const ref = useRef(fn); ref.current = fn;
  useEffect(() => {
    let alive = true, t;
    const tick = async () => {
      try { const d = await ref.current(); if (alive) { setData(d); setErr(null); } }
      catch (e) { if (alive) setErr(e.message); }
      if (alive && on) t = setTimeout(tick, intervalMs);
    };
    tick();
    return () => { alive = false; clearTimeout(t); };
  }, [intervalMs, on, ...deps]);
  return [data, err];
}

const ago = (ts) => {
  if (!ts) return '';
  const s = Math.max(0, Math.floor((Date.now() - ts) / 1000));
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  return Math.floor(s / 3600) + 'h ago';
};

const NAV = [
  { group: 'Live', items: [
    { id: 'system', label: 'System Map', icon: '◉' },
    { id: 'states', label: 'State machine', icon: '⊞' },
    { id: 'trace', label: 'Trace replay', icon: '⟲' },
    { id: 'member', label: 'Member snapshot', icon: '◍' },
  ] },
  { group: 'Analytics', items: [
    { id: 'overview', label: 'Overview', icon: '◧' },
    { id: 'model', label: 'ML model', icon: '◆' },
    { id: 'rlpolicy', label: 'RL policy', icon: '⊛' },
    { id: 'performance', label: 'Action performance', icon: '▤' },
    { id: 'throttle', label: 'Channel throttle', icon: '⊟' },
    { id: 'dispositions', label: 'Dispositions', icon: '◔' },
    { id: 'variants', label: 'Variants A/B', icon: '⎇' },
    { id: 'funnel', label: 'Rule funnel', icon: '⌗' },
    { id: 'audit', label: 'Live audit', icon: '◷' },
    { id: 'routing', label: 'Fact routing', icon: '⌥' },
  ] },
  { group: 'Operations', items: [
    { id: 'validation', label: 'Validation', icon: '✓' },
    { id: 'health', label: 'Throughput', icon: '∿' },
    { id: 'dlq', label: 'Dead-letter queues', icon: '☢' },
  ] },
  { group: 'Studio', items: [
    { id: 'st-actions', label: 'Actions', icon: '✦' },
    { id: 'st-groups', label: 'Action groups', icon: '☰' },
    { id: 'st-experiences', label: 'Experiences', icon: '❂' },
    { id: 'st-milestones', label: 'Milestones', icon: '✓' },
    { id: 'st-global', label: 'Global rules', icon: '⊞' },
    { id: 'st-channel', label: 'Channel rules', icon: '⋔' },
  ] },
];

export default function App() {
  const [view, setView] = useState('system');
  const [live, setLive] = useState(true);
  const [traceTarget, setTraceTarget] = useState(null);   // deep-link a member→decision into Trace replay
  const openTrace = (member, cid) => { setTraceTarget({ member, cid }); setView('trace'); };
  const [status] = usePoll(() => gql(`{ lakeStatus { configured tables members } }`).then((d) => d.lakeStatus), 6000, live);
  const current = NAV.flatMap((g) => g.items).find((i) => i.id === view);

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand"><span className="logo">◢◤</span><div><b>Command Center</b><span className="brand-sub">NBA · next best action</span></div></div>
        <nav className="nav">
          {NAV.map((g) => (
            <div className="nav-group" key={g.group}>
              <div className="nav-group-label">{g.group}</div>
              {g.items.map((it) => (
                <button key={it.id} className={'nav-item' + (view === it.id ? ' on' : '')} onClick={() => setView(it.id)}>
                  <span className="nav-icon">{it.icon}</span>{it.label}
                </button>
              ))}
            </div>
          ))}
        </nav>
        <div className="sidebar-foot">
          {status && (status.configured
            ? <span className="lake-ok"><span className="dot live" /> {Intl.NumberFormat().format(status.members)} members</span>
            : <span className="lake-off"><span className="dot off" /> lake offline</span>)}
        </div>
      </aside>

      <div className="main">
        <header className="pagebar">
          <h2>{current?.label}</h2>
          <div className="pagebar-right">
            <button className={'pill' + (live ? ' on' : '')} onClick={() => setLive((v) => !v)}>{live ? '● live' : '❚❚ paused'}</button>
          </div>
        </header>
        <main className="content">
          {view === 'system' && <SystemMap live={live} />}
          {view === 'states' && <StateMachine live={live} />}
          {view === 'trace' && <TraceView target={traceTarget} />}
          {view === 'member' && <MemberSnapshot onOpenTrace={openTrace} />}
          {view === 'overview' && <Overview live={live} />}
          {view === 'model' && <ModelView live={live} onNav={setView} />}
          {view === 'rlpolicy' && <RLView live={live} />}
          {view === 'performance' && <Actions live={live} />}
          {view === 'throttle' && <Throttle live={live} />}
          {view === 'dispositions' && <DispositionFunnels live={live} />}
          {view === 'variants' && <VariantsView live={live} />}
          {view === 'funnel' && <RuleFunnel />}
          {view === 'audit' && <LiveAudit live={live} />}
          {view === 'routing' && <Routing />}
          {view === 'validation' && <Validation live={live} />}
          {view === 'health' && <Health live={live} />}
          {view === 'dlq' && <Dlq live={live} />}
          {view === 'st-actions' && <Studio kind="actions" />}
          {view === 'st-groups' && <Groups />}
          {view === 'st-experiences' && <Experiences />}
          {view === 'st-milestones' && <Studio kind="milestone" />}
          {view === 'st-global' && <Studio kind="global" />}
          {view === 'st-channel' && <Studio kind="channel" />}
        </main>
      </div>
    </div>
  );
}

function Card({ title, sub, children, wide, right }) {
  return (
    <section className={'card' + (wide ? ' wide' : '')}>
      <div className="card-head"><div><h3>{title}</h3>{sub && <span className="card-sub">{sub}</span>}</div>{right}</div>
      {children}
    </section>
  );
}

// ---- Channel throttle: token-bucket RATE (trickle) + DAILY ceiling (reroute), per channel ----
const winLabel = (s) => (s >= 3600 ? s / 3600 + 'h' : s >= 60 ? s / 60 + 'm' : s + 's');
const gateState = (c) => (c.throttled ? 'suppress' : c.rateThrottled ? 'wait' : (c.cap != null || c.rateCap != null) ? 'send' : 'none');
const STATE_META = {
  suppress: { col: '#f87171', label: '⛔ SUPPRESS → reroute' },
  wait: { col: '#fbbf24', label: '⏳ WAIT → trickling' },
  send: { col: '#4ade80', label: '● SEND → flowing' },
  none: { col: '#7d8590', label: 'unthrottled' },
};
// ── ML MODEL — champion/challenger + the per-archetype "watch it learn" view (gold_model_card). ──
const MDL_HUE = { email: '#38bdf8', sms: '#4ade80', push: '#a78bfa', voice: '#fbbf24' };
const mpct = (x) => (x == null ? '—' : Math.round(x * 100) + '%');
const mauc = (x) => (x == null ? '—' : (Math.round(x * 1e4) / 1e4).toFixed(4));

function ArmBar({ ch, model, fstar, isPick, isTruth }) {
  const hue = MDL_HUE[ch] || '#888';
  return (
    <div className="mdl-arm">
      <span className="mdl-arm-l" style={{ color: hue, fontWeight: isTruth ? 700 : 400 }}>{ch}</span>
      <span className="mdl-bar">
        <i style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: Math.max(0, Math.min(100, model * 100)) + '%', background: hue, opacity: 0.55 }} />
        <b title={'ground-truth f* ' + mpct(fstar)} style={{ position: 'absolute', top: -2, bottom: -2, left: Math.max(0, Math.min(100, fstar * 100)) + '%', width: 2, background: '#fff', boxShadow: '0 0 4px #fff' }} />
      </span>
      <span className="mdl-arm-v">{mpct(model)} <em className="muted">f*{mpct(fstar)}</em></span>
      <span className="mdl-pick" style={{ color: hue }}>{isPick ? '◄ picks' : ''}</span>
    </div>
  );
}

function ArcheCard({ a }) {
  const arms = ['email', 'sms', 'push', 'voice'];
  const beats = a.challenger_pick === a.fstar_pick && a.champion_pick !== a.fstar_pick;
  return (
    <section className={'card' + (beats ? ' mdl-win' : '')}>
      <div className="card-head">
        <div>
          <h3>{a.name}</h3>
          <span className="card-sub">
            {beats
              ? <>model picks <b style={{ color: '#fbbf24' }}>{a.challenger_pick}</b> · heuristic picked <s className="muted">{a.champion_pick}</s> · truth {a.fstar_pick}</>
              : <>model picks <b>{a.challenger_pick || '—'}</b> · matches truth {a.fstar_pick}</>}
          </span>
        </div>
      </div>
      {arms.map((ch) => (
        <ArmBar key={ch} ch={ch} model={(a.challenger || {})[ch] || 0} fstar={(a.fstar || {})[ch] || 0}
          isPick={a.challenger_pick === ch} isTruth={a.fstar_pick === ch} />
      ))}
    </section>
  );
}

function ModelView({ live, onNav }) {
  const [card] = usePoll(() => gql(`{ modelCard }`).then((d) => d.modelCard), 15000, live);
  if (!card) return <div className="muted" style={{ padding: 20 }}>no model card yet — run the Databricks <code>nba_ml_model_card</code> job (emits to Kafka <code>nba.model.card</code> → the lake medallion lands <code>gold_model_card</code>).</div>;
  const cp = card.champion || {}, ch = card.challenger || {};
  const arche = card.archetypes || [];
  const N = arche.length || 1;
  const matchTruth = arche.filter((a) => a.challenger_pick === a.fstar_pick).length;
  const champMatch = arche.filter((a) => a.champion_pick === a.fstar_pick).length;
  const beatsOn = arche.filter((a) => a.challenger_pick === a.fstar_pick && a.champion_pick !== a.fstar_pick);
  const hist = (card.history || []).filter((h) => h.train_auc != null);
  const ready = ch.pct_achievable != null && ch.pct_achievable >= 98 && matchTruth >= champMatch && matchTruth === arche.length;
  const band = (v) => Math.max(2, Math.min(100, ((v - 0.5) / 0.25) * 100));   // map AUC 0.50–0.75 onto the bar
  return (
    <div className="mdl-page">
      {/* role: this is the propensity model — NOT the live sequencer (that's the CQL policy) */}
      <div className="mdl-role">
        <span><b>Propensity model</b> · predicts P(convert) per channel — seeds the cold-start channel prior and a baseline. The live action <i>sequencing</i> is the CQL RL policy.</span>
        {onNav && <button className="btn ghost sm" onClick={() => onNav('rlpolicy')}>RL policy →</button>}
      </div>

      {/* champion (heuristic, serving) vs challenger (learned, candidate) — the promotion decision */}
      <div className="mdl-vs">
        <div className="mdl-vs-side">
          <div className="mdl-vs-tag champ">CHAMPION · serving</div>
          <div className="mdl-vs-ver">v{cp.version ?? '—'}</div>
          <div className="mdl-vs-kind">heuristic baseline</div>
          <div className="mdl-vs-row">AUC <b>—</b></div>
          <div className="mdl-vs-row">truth-optimal channel <b>{champMatch}/{N}</b></div>
        </div>
        <div className="mdl-vs-mid">vs</div>
        <div className={'mdl-vs-side' + (ready ? ' win' : '')}>
          <div className="mdl-vs-tag chall">CHALLENGER · candidate</div>
          <div className="mdl-vs-ver">v{ch.version ?? '—'}</div>
          <div className="mdl-vs-kind">learned model</div>
          <div className="mdl-vs-row">AUC <b style={{ color: '#4ade80' }}>{mauc(ch.train_auc)}</b> <em className="muted">/ oracle {mauc(ch.oracle_auc)}</em></div>
          <div className="mdl-vs-row">% of achievable <b style={{ color: '#a78bfa' }}>{ch.pct_achievable != null ? ch.pct_achievable + '%' : '—'}</b> · truth-optimal <b style={{ color: '#4ade80' }}>{matchTruth}/{N}</b></div>
        </div>
        <div className={'mdl-verdict' + (ready ? ' ok' : '')}>
          {ready ? (
            <><span className="mdl-verdict-badge">✓ promote</span>Challenger <b>v{ch.version}</b> sits at the Bayes-optimal ceiling (<b>{ch.pct_achievable}%</b> of achievable) and picks the truth-optimal channel on <b>{matchTruth}/{N}</b> profiles{beatsOn.length ? <> — beating the heuristic on <b style={{ color: '#fbbf24' }}>{beatsOn.map((a) => a.name).join(', ')}</b></> : null}.</>
          ) : (
            <><span className="mdl-verdict-badge hold">evaluating</span>Challenger <b>v{ch.version}</b> at {ch.pct_achievable != null ? ch.pct_achievable + '%' : '—'} of achievable — not yet clear of the champion.</>
          )}
        </div>
      </div>

      <div className="mdl-sec">watch it learn — predicted P(convert) per channel vs ground-truth f* <span className="muted">(filled bar = model · white tick = truth · model matches truth {matchTruth}/{N} profiles)</span></div>
      <div className="thr-grid mdl-arche-grid">
        {arche.map((a) => <ArcheCard key={a.name} a={a} />)}
        {!arche.length && <div className="muted">no archetype data</div>}
      </div>

      {hist.length > 0 && (
        <>
          <div className="mdl-sec">model vs achievable ceiling · AUC per registered version <span className="muted">(filled bar = model train AUC · tick = oracle ceiling — it tracks the ceiling, so absolute AUC moves only with the data)</span></div>
          <div className="mdl-auc">
            {hist.map((h) => (
              <div className="mdl-arm" key={h.version}>
                <span className="mdl-arm-l">v{h.version}</span>
                <span className="mdl-bar">
                  <i style={{ position: 'absolute', left: 0, top: 0, bottom: 0, width: band(h.train_auc) + '%', background: '#34d399', opacity: 0.55 }} />
                  {h.oracle_auc != null && <b title={'oracle ceiling ' + mauc(h.oracle_auc)} style={{ position: 'absolute', top: -2, bottom: -2, left: band(h.oracle_auc) + '%', width: 2, background: '#fbbf24', boxShadow: '0 0 4px #fbbf24' }} />}
                </span>
                <span className="mdl-arm-v">{mauc(h.train_auc)}{h.oracle_auc != null && <em className="muted"> / {mauc(h.oracle_auc)}</em>}</span>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

// ── RL POLICY — offline-RL (CQL) next-best-action: long-term milestone value vs the myopic model. ──
const RL_ORDER = ['random', 'myopic (today)', 'BC', 'CQL', 'sequencer (oracle)'];
const RL_LABEL = { 'random': 'Random', 'myopic (today)': 'Myopic (today)', 'BC': 'Behavior clone', 'CQL': 'CQL · RL policy', 'sequencer (oracle)': 'Oracle ceiling' };
const RL_HUE = { 'random': '#6f6f78', 'myopic (today)': '#f87171', 'BC': '#fbbf24', 'CQL': '#34d399', 'sequencer (oracle)': '#60a5fa' };
const MS_HUE = ['#60a5fa', '#34d399', '#fbbf24', '#f472b6'];

function RLView({ live }) {
  const [card] = usePoll(() => gql(`{ modelCard }`).then((d) => d.modelCard), 15000, live);
  const pol = card && card.policy;
  if (!pol) return <div className="muted" style={{ padding: 20 }}>no RL policy card yet — the offline-RL training (CQL) hasn't emitted its <code>policy</code> section to the model card.</div>;
  const P = pol.policies || {};
  const cql = P['CQL'] || { return: 0, reach: {} }, my = P['myopic (today)'] || { return: 0, reach: {} }, orc = P['sequencer (oracle)'] || { return: 0, reach: {} };
  const present = RL_ORDER.filter((k) => P[k]);
  const lo = Math.min(0, ...present.map((k) => P[k].return)), hi = Math.max(1, ...present.map((k) => P[k].return));
  const span = hi - lo || 1;
  const dyn = pol.dynamics;
  return (
    <div>
      <div className="thr-summary">
        <div className="kpi"><div className="kpi-val" style={{ color: '#34d399' }}>{(cql.return).toFixed(1)}</div><div className="kpi-label">CQL journey value</div><div className="kpi-sub">vs myopic {(my.return).toFixed(1)}</div></div>
        <div className="kpi"><div className="kpi-val" style={{ color: '#34d399' }}>{Math.round((cql.reach.upgraded || 0) * 100)}%</div><div className="kpi-label">reaches Upgraded</div><div className="kpi-sub">myopic {Math.round((my.reach.upgraded || 0) * 100)}%</div></div>
        <div className="kpi"><div className="kpi-val" style={{ color: '#60a5fa' }}>{pol.n_episodes || '—'}</div><div className="kpi-label">offline episodes</div><div className="kpi-sub">trained, no live experiments</div></div>
        <div className="kpi"><div className="kpi-val" style={{ color: dyn && dyn.ordering_preserved ? '#34d399' : '#f87171' }}>{dyn ? dyn.state_delta_mse : '—'}</div><div className="kpi-label">world-model error</div><div className="kpi-sub">{dyn && dyn.ordering_preserved ? 'ordering preserved' : 'Δstate MSE'}</div></div>
      </div>
      <div className="mdl-sec">policy value — discounted milestone reward per journey (offline-evaluated in the world model) <span className="muted">· myopic is what we serve today</span></div>
      <div className="rl-bars">
        {present.map((k) => {
          const r = P[k].return, w = Math.max(2, ((r - lo) / span) * 100), hue = RL_HUE[k];
          return (
            <div className="rl-row" key={k}>
              <span className="rl-name" style={{ color: hue }}>{RL_LABEL[k]}</span>
              <span className="rl-track"><i style={{ width: w + '%', background: hue }} /></span>
              <span className="rl-val">{r.toFixed(2)}</span>
              <span className="rl-up" style={{ color: hue }}>{Math.round((P[k].reach.upgraded || 0) * 100)}% upg</span>
            </div>
          );
        })}
      </div>
      <div className="mdl-sec">milestone ladder — the policy optimizes long-term VALUE, sequencing toward the deep milestones</div>
      <div className="rl-ladder">
        {(pol.milestones || []).map((m, i) => (
          <div className="rl-ms" key={m.id} style={{ borderColor: MS_HUE[i % 4] }}>
            <div className="rl-ms-v" style={{ color: MS_HUE[i % 4] }}>+{m.value}</div>
            <div className="rl-ms-n">{m.name}</div>
            <div className="rl-ms-r"><b style={{ color: '#34d399' }}>{Math.round((cql.reach[m.id] || 0) * 100)}%</b> <s className="muted">{Math.round((my.reach[m.id] || 0) * 100)}%</s></div>
            {i < (pol.milestones.length - 1) && <span className="rl-ms-arrow">→</span>}
          </div>
        ))}
      </div>
      <div className="rl-note">CQL reaches the valuable <b style={{ color: '#f472b6' }}>Upgraded</b> milestone <b style={{ color: '#34d399' }}>{Math.round((cql.reach.upgraded || 0) * 100)}%</b> of journeys — the myopic model we serve today reaches it <b style={{ color: '#f87171' }}>{Math.round((my.reach.upgraded || 0) * 100)}%</b>. Learned offline from logged data, no live experiments. <span className="muted">(green = CQL · strike = myopic)</span></div>
    </div>
  );
}

function Throttle({ live }) {
  const [stats] = usePoll(() => gql(`{ throttleStats { channel sent cap utilization throttled rate rateCap windowSeconds rateThrottled series { label count } } }`).then((d) => d.throttleStats), 5000, live);
  const [cfg] = usePoll(() => gql(`{ channelConfig }`).then((d) => d.channelConfig || {}), 6000, live);
  const batchOf = (ch) => Number((cfg || {})[ch]) || 1;
  const s = stats || [];
  const governed = s.filter((c) => c.cap != null || c.rateCap != null);
  return (
    <div>
      <div className="thr-summary">
        <Kpi label="rerouting (daily full)" value={governed.filter((c) => c.throttled).length} accent="#f87171" />
        <Kpi label="trickling (rate cap)" value={governed.filter((c) => c.rateThrottled && !c.throttled).length} accent="#fbbf24" />
        <Kpi label="sends today (all channels)" value={s.reduce((a, c) => a + c.sent, 0)} accent="#4ade80" />
      </div>
      <div className="thr-grid">
        {s.map((c) => <ThrottleCard key={c.channel} c={c} maxBatch={batchOf(c.channel)} />)}
        {!s.length && <div className="muted">no channel data yet</div>}
      </div>
    </div>
  );
}
function ThrottleCard({ c, maxBatch }) {
  const setBatch = async (v) => { try { await gql(`mutation($c:String!,$m:Int!){ setChannelMaxBatch(channel:$c, maxBatch:$m) }`, { c: c.channel, m: Math.max(1, v) }); } catch (e) { /* ignore */ } };
  const dPct = c.cap ? Math.min(100, Math.round((c.sent / c.cap) * 100)) : null;
  const max = Math.max(1, ...c.series.map((x) => x.count));
  const win = winLabel(c.windowSeconds);
  const st = gateState(c), meta = STATE_META[st];
  const sub = [c.rateCap != null && `${c.rateCap}/${win} rate`, c.cap != null && `${c.cap}/day ceiling`].filter(Boolean).join(' · ') || 'no cap — unthrottled';
  return (
    <section className={'card thr-card' + (st === 'suppress' ? ' is-throttled' : st === 'wait' ? ' is-waiting' : '')}>
      <div className="card-head">
        <div><h3>{c.channel}</h3><span className="card-sub">{sub}</span></div>
        <span className="thr-badge" style={{ color: meta.col, borderColor: meta.col }}>{meta.label}</span>
      </div>

      {c.rateCap != null && (
        <div className="thr-block">
          <div className="thr-blabel">rate · token bucket <span className="muted">{c.rate}/{c.rateCap} this {win}{c.rateThrottled ? ' — bucket full, trickling' : ''}</span></div>
          <div className="thr-bucket">
            {Array.from({ length: Math.min(c.rateCap, 30) }, (_, i) => <span key={i} className={'tok' + (i < c.rate ? ' on' : '')} style={i < c.rate ? { background: meta.col } : null} />)}
          </div>
        </div>
      )}

      <div className="thr-block">
        <div className="thr-blabel">daily <span className="muted">{c.cap != null ? `${c.sent}/${c.cap}${c.throttled ? ' — ceiling hit, rerouting' : ''}` : `${c.sent} sent · no ceiling`}</span></div>
        {c.cap != null && <div className="thr-bar"><div className="thr-fill" style={{ width: dPct + '%', background: c.throttled ? '#f87171' : '#4ade80' }} /></div>}
      </div>

      <div className="thr-spark">
        {c.series.map((h, i) => <div key={i} className="thr-hbar" title={`${h.label}:00 — ${h.count} sent`} style={{ height: Math.max(2, (h.count / max) * 34) + 'px', background: meta.col, opacity: h.count ? 0.85 : 0.16 }} />)}
      </div>
      <div className="thr-batch">max batch / send
        <input type="number" min="1" defaultValue={maxBatch} key={maxBatch} onBlur={(e) => setBatch(parseInt(e.target.value) || 1)} />
        <span className="muted">{maxBatch > 1 ? `top ${maxBatch} actions batched into one ${c.channel}` : 'one action per send'}</span>
      </div>
      <div className="thr-foot muted">
        {st === 'wait' ? `rate bucket full — new sends WAIT and trickle as the ${win} window refills`
          : st === 'suppress' ? 'daily ceiling hit — new sends SUPPRESS and reroute to another channel'
          : 'sends today by hour (UTC)'}
      </div>
    </section>
  );
}

// ---- Disposition funnels: per-channel delivery/engagement (outbound) + Presented/Accepted/Completed (inbound) ----
function DispositionFunnels({ live }) {
  const [funnels] = usePoll(() => gql(`{ dispositionFunnels { channel kind stages { status n } } }`).then((d) => d.dispositionFunnels), 5000, live);
  const f = funnels || [];
  return (
    <div className="thr-grid">
      {f.map((c) => {
        const max = Math.max(1, ...c.stages.map((s) => s.n));
        const col = c.kind === 'inbound' ? '#a78bfa' : '#34d399';
        return (
          <section className="card" key={c.channel}>
            <div className="card-head"><div><h3>{c.channel}</h3><span className="card-sub">{c.kind} funnel</span></div></div>
            <div className="disp-funnel">
              {c.stages.map((s, i) => (
                <div className="disp-row" key={i}>
                  <div className="disp-label">{s.status}</div>
                  <div className="disp-track"><div className="disp-fill" style={{ width: Math.max(3, (s.n / max) * 100) + '%', background: col }} /></div>
                  <div className="disp-n">{s.n}</div>
                </div>
              ))}
            </div>
          </section>
        );
      })}
      {!f.length && <div className="muted">no dispositions captured yet</div>}
    </div>
  );
}

// ---- Variants A/B: per action-channel, each content key's send + engagement funnel, winner crowned ----
function VariantsView({ live }) {
  const [data] = usePoll(() => gql(`{ variantPerformance { actionId name channel base variants { contentKey isBase sent stages { status n } deepest conversion winner } } }`).then((d) => d.variantPerformance), 5000, live);
  const d = data || [];
  return (
    <div>
      <div className="vp-intro">Content-key <b>variants</b> per action-channel — which template each member got, and how each performed. The <span className="vp-tag win">🏆 winner</span> is the variant with the best conversion to its deepest engagement stage. Authored in <b>Studio → Actions</b> (the ＋ next to a content key).</div>
      <div className="vp-grid">
        {d.map((a) => (
          <section className="card vp-card" key={a.actionId + a.channel}>
            <div className="card-head"><div><h3>{a.name}</h3><span className="card-sub">{a.channel} · {a.variants.length} variants competing</span></div></div>
            <div className="vp-variants">
              {a.variants.map((v) => {
                const maxN = Math.max(1, ...v.stages.map((s) => s.n));
                return (
                  <div className={'vp-variant' + (v.winner ? ' win' : '')} key={v.contentKey}>
                    <div className="vp-vhead">
                      <span className="vp-key" title={v.contentKey}>{v.contentKey}</span>
                      {v.isBase && <span className="vp-tag base">base</span>}
                      {v.winner && <span className="vp-tag win">🏆</span>}
                    </div>
                    <div className="vp-conv"><b>{Math.round(v.conversion * 100)}%</b><span className="muted">{v.deepest ? '→ ' + v.deepest : 'delivered'}</span></div>
                    <div className="vp-funnel">
                      {v.stages.map((s, i) => (
                        <div className="vp-stage" key={i} title={`${s.status}: ${s.n}`}>
                          <span className="vp-slab">{s.status}</span>
                          <div className="vp-bar"><div style={{ width: Math.max(4, (s.n / maxN) * 100) + '%', background: v.winner ? '#34d399' : '#6ea8fe' }} /></div>
                          <span className="vp-sn">{s.n}</span>
                        </div>
                      ))}
                      {!v.stages.length && <div className="muted sm">no sends yet</div>}
                    </div>
                  </div>
                );
              })}
            </div>
          </section>
        ))}
        {!d.length && <div className="muted">No content-key variants with A/B data yet — add a variant in <b>Studio → Actions</b> and let sends accrue (each disposition carries the variant the member got).</div>}
      </div>
    </div>
  );
}

// ---- Validation: read-only checks against the live system -> "is the platform operational?" ----
function Validation({ live }) {
  const [h] = usePoll(() => gql(`{ systemChecks { operational passed total ts checks { name ok group detail validates } } }`).then((d) => d.systemChecks), 20000, live);
  if (!h) return <div className="skeleton" style={{ height: 220 }} />;
  const groups = [...new Set(h.checks.map((c) => c.group))];
  return (
    <div>
      <section className={'card op-banner ' + (h.operational ? 'ok' : 'bad')}>
        <div className="op-status">{h.operational ? '● SYSTEM OPERATIONAL' : '▲ ATTENTION NEEDED'}</div>
        <div className="op-sub">{h.passed}/{h.total} read-only checks passing · last validated {ago(h.ts)} · auto-revalidates every 20s</div>
      </section>
      {groups.map((g) => (
        <Card key={g} title={g} wide>
          <div className="chk-list">
            {h.checks.filter((c) => c.group === g).map((c, i) => (
              <div className={'chk-row ' + (c.ok ? 'ok' : 'bad')} key={i}>
                <span className="chk-mark">{c.ok ? '✓' : '✕'}</span>
                <div className="chk-body"><div className="chk-name">{c.name}</div><div className="chk-validates">{c.validates}</div></div>
                <div className="chk-detail">{c.detail}</div>
              </div>
            ))}
          </div>
        </Card>
      ))}
    </div>
  );
}

// ---- Throughput: per-layer event rate vs the time-of-day baseline; flags degraded/quiet layers ----
function Health({ live }) {
  const [layers] = usePoll(() => gql(`{ layerHealth { layer source lastEventAgeSec lastHour prevHour baseline status } }`).then((d) => d.layerHealth), 12000, live);
  const L = layers || [];
  const age = (s) => (s == null ? '—' : s < 60 ? s + 's' : s < 3600 ? Math.floor(s / 60) + 'm' : Math.floor(s / 3600) + 'h');
  const COL = { ok: '#4ade80', degraded: '#f87171', error: '#f87171', quiet: '#fbbf24' };
  return (
    <div className="health-grid">
      {L.map((l) => {
        const col = COL[l.status] || '#6ea8fe';
        const vsBase = l.baseline ? Math.round((l.lastHour / l.baseline) * 100) : null;
        const trend = l.lastHour - l.prevHour;
        return (
          <section className="card hl-card" key={l.layer}>
            <div className="card-head"><div><h3>{l.layer}</h3><span className="card-sub">{l.source}</span></div><span className="hl-badge" style={{ color: col, borderColor: col }}>{l.status}</span></div>
            <div className="hl-metrics">
              <div className="hl-metric"><div className="hl-num" style={{ color: col }}>{l.lastHour}</div><div className="hl-lab">last hour {trend ? <span className={trend > 0 ? 'up' : 'dn'}>{trend > 0 ? '▲' : '▼'}{Math.abs(trend)}</span> : ''}</div></div>
              <div className="hl-metric"><div className="hl-num">{l.baseline ?? '—'}</div><div className="hl-lab">baseline · this hr</div></div>
              <div className="hl-metric"><div className="hl-num">{age(l.lastEventAgeSec)}</div><div className="hl-lab">since last event</div></div>
            </div>
            {vsBase != null && <div className="hl-bar"><div className="hl-fill" style={{ width: Math.min(100, vsBase) + '%', background: col }} /></div>}
            <div className="hl-foot muted">{vsBase != null ? `${vsBase}% of normal for this hour` : 'no 7-day baseline yet'}</div>
          </section>
        );
      })}
      {!L.length && <div className="skeleton" style={{ height: 160 }} />}
    </div>
  );
}

function Overview({ live }) {
  const [funnel] = usePoll(() => gql(`{ funnel { label count } }`).then((d) => d.funnel), 5000, live);
  const [disp] = usePoll(() => gql(`{ dispositions { bucket n } }`).then((d) => d.dispositions), 5000, live);
  const [scores] = usePoll(() => gql(`{ scoreDistribution { bucket n } }`).then((d) => d.scoreDistribution), 8000, live);
  const f = funnel || [];
  const get = (l) => (f.find((x) => x.label === l) || {}).count || 0;
  const conv = get('Members') ? Math.round((get('Sent') / get('Members')) * 100) : 0;
  return (
    <div className="overview-page">
      <div className="kpis">
        <Kpi label="Members" value={get('Members')} accent="#6ea8fe" />
        <Kpi label="Eligible" value={get('Eligible')} accent="#a78bfa" />
        <Kpi label="Activated" value={get('Activated')} accent="#fbbf24" />
        <Kpi label="Sent" value={get('Sent')} accent="#4ade80" />
        <Kpi label="Conversion" value={conv} sub="members → sent" accent="#4ade80" />
      </div>
      <div className="grid">
        <Card title="Action funnel" sub="member drop-off through the NBA pipeline">
          {funnel ? <Funnel stages={funnel} /> : <Skeleton />}
        </Card>
        <Card title="Dispositions" sub="final action outcomes">
          {disp ? <Donut data={disp} /> : <Skeleton />}
        </Card>
        <Card title="Score distribution" sub="ML propensity, all scored actions">
          {scores ? <BarList items={scores} valueKey="n" labelKey="bucket" accent="#a78bfa" /> : <Skeleton />}
        </Card>
      </div>
    </div>
  );
}

function Actions({ live }) {
  const [perf] = usePoll(() => gql(`{ actionPerformance { id name eligible activated suppressed sent avgScore } }`).then((d) => d.actionPerformance), 5000, live);
  const [sup] = usePoll(() => gql(`{ suppressedActions }`).then((d) => new Set(d.suppressedActions)), 4000, live);
  const suppressedSet = sup || new Set();
  const [busy, setBusy] = useState(null);
  const [note, setNote] = useState({});
  const suppress = async (id, on) => {
    setBusy(id);
    try { await gql(`mutation($a:ID!,$s:Boolean!){ suppressAction(actionId:$a, suppressed:$s) }`, { a: id, s: on }); setNote((n) => ({ ...n, [id]: on ? 'suppressed' : 'restored' })); }
    catch (e) { setNote((n) => ({ ...n, [id]: 'error' })); }
    setBusy(null);
  };
  return (
    <Card title="Action performance" sub="eligibility → activation → send, per action · suppress pulls an action out of rotation everywhere" wide>
      {!perf ? <Skeleton /> : (
        <table className="grid-table">
          <thead><tr><th>Action</th><th>Eligible</th><th>Activated</th><th>Suppressed</th><th>Sent</th><th>Avg score</th><th>Conversion</th><th>Operator</th></tr></thead>
          <tbody>
            {perf.map((a) => {
              const conv = a.eligible ? Math.round((a.sent / a.eligible) * 100) : 0;
              return (
                <tr key={a.id} className={suppressedSet.has(a.id) ? 'row-suppressed' : ''}>
                  <td className="name">{a.name || a.id}{suppressedSet.has(a.id) && <span className="sup-badge">⊘ suppressed</span>}</td>
                  <td>{a.eligible}</td><td>{a.activated}</td><td className="muted">{a.suppressed}</td>
                  <td className="good">{a.sent}</td>
                  <td>{a.avgScore != null ? a.avgScore.toFixed(2) : '—'}</td>
                  <td><div className="minibar"><div style={{ width: conv + '%' }} /></div><span className="conv">{conv}%</span></td>
                  <td className="op-cell">
                    {suppressedSet.has(a.id)
                      ? <button className="btn sm primary" disabled={busy === a.id} onClick={() => suppress(a.id, false)}>Restore</button>
                      : <button className="btn ghost sm" disabled={busy === a.id} onClick={() => suppress(a.id, true)}>Suppress</button>}
                    {note[a.id] && <span className={'op-note ' + note[a.id]}>{note[a.id]}</span>}
                  </td>
                </tr>
              );
            })}
            {!perf.length && <tr><td colSpan="8" className="muted">no actions yet</td></tr>}
          </tbody>
        </table>
      )}
    </Card>
  );
}

function LiveAudit({ live }) {
  const [feed] = usePoll(() => gql(`{ liveFeed(limit: 60) { ts kind entity label detail source } }`).then((d) => d.liveFeed), 3000, live);
  const kindColor = (k) => k?.startsWith('activation') ? '#fbbf24' : k === 'eligible' ? '#a78bfa' : k === 'score' ? '#6ea8fe' : k === 'disposition' ? '#4ade80' : '#7d8590';
  return (
    <Card title="Live audit feed" sub="every NBA event, newest first — facts · evaluations · activations" wide right={<span className="live-tag"><span className="dot live" /> streaming</span>}>
      {!feed ? <Skeleton /> : (
        <div className="feed">
          {feed.map((e, i) => (
            <div className="feed-row" key={i}>
              <span className="feed-time">{ago(e.ts)}</span>
              <span className="feed-kind" style={{ color: kindColor(e.kind), borderColor: kindColor(e.kind) + '55' }}>{e.kind}</span>
              <span className="feed-entity">{e.entity}</span>
              <span className="feed-label">{e.label}</span>
              <span className="feed-detail">{e.detail}</span>
              <span className="feed-src">{e.source}</span>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}

function Routing() {
  const [edges] = usePoll(() => gql(`{ actionFactMap { id name fact } }`).then((d) => d.actionFactMap), 30000, false);
  const byAction = {};
  (edges || []).forEach((e) => { (byAction[e.name || e.id] ||= []).push(e.fact); });
  return (
    <Card title="Action → fact routing" sub="which facts each action depends on (factsUsed) — drives fact routing" wide>
      {!edges ? <Skeleton /> : (
        <div className="routing">
          {Object.entries(byAction).map(([name, facts]) => (
            <div className="route" key={name}>
              <div className="route-action">{name}</div>
              <div className="route-facts">{facts.map((f) => <span className="chip" key={f}>{f}</span>)}</div>
            </div>
          ))}
          {!edges.length && <div className="muted">no definitions ingested</div>}
        </div>
      )}
    </Card>
  );
}

// Interactive what-if funnel: load an action's rules, edit thresholds, see member drop-off live over gold.
function RuleFunnel() {
  const [actions, setActions] = useState([]);
  const [globals, setGlobals] = useState([]);
  const [channels, setChannels] = useState([]);
  const [sel, setSel] = useState(null);
  const [model, setModel] = useState(null);
  const [stages, setStages] = useState(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    gql(`{ actions { id name channels { channel } inclusion exclusion } globalRules { id name logic } channelRules { id name channel logic } }`)
      .then((d) => { setActions(d.actions); setGlobals(d.globalRules); setChannels(d.channelRules); if (d.actions[0]) load(d.actions[0], d.globalRules, d.channelRules); });
  }, []);

  function load(a, gs = globals, cs = channels) {
    setSel(a.id);
    const chans = (a.channels || []).map((c) => c.channel);
    setModel({
      inclusion: a.inclusion || { op: 'all', conditions: [] },
      exclusion: a.exclusion || { op: 'any', conditions: [] },
      globalRules: gs,
      channelRules: cs.filter((c) => chans.includes(c.channel)),
    });
  }

  async function run(m) {
    if (!m) return;
    setBusy(true);
    try {
      const input = {
        inclusion: m.inclusion, exclusion: m.exclusion,
        globalRules: m.globalRules.map((g) => ({ name: g.name, logic: g.logic })),
        channelRules: m.channelRules.map((c) => ({ name: c.name, logic: c.logic })),
      };
      const d = await gql(`query($i: JSON!){ ruleFunnel(input: $i) { label count } }`, { i: input });
      setStages(d.ruleFunnel);
    } catch (e) { setStages([{ label: 'error: ' + e.message, count: 0 }]); }
    setBusy(false);
  }
  useEffect(() => { if (model) run(model); /* eslint-disable-next-line */ }, [model]);

  const editCond = (treeKey, idx, val) => {
    setModel((m) => { const n = JSON.parse(JSON.stringify(m)); n[treeKey].conditions[idx].value = coerce(val); return n; });
  };
  const editRuleCond = (which, ri, ci, val) => {
    setModel((m) => { const n = JSON.parse(JSON.stringify(m)); n[which][ri].logic.conditions[ci].value = coerce(val); return n; });
  };

  return (
    <div className="grid">
      <Card title="Rule funnel — what-if" sub="re-evaluated directly against gold (not Drools). Edit thresholds → live member drop-off." wide
        right={<select value={sel || ''} onChange={(e) => load(actions.find((a) => a.id === e.target.value))}>
          {actions.map((a) => <option key={a.id} value={a.id}>{a.name}</option>)}</select>}>
        {!model ? <Skeleton /> : (
          <div className="rf-editor">
            <RuleGroup title="Inclusion (must match)" tree={model.inclusion} onEdit={(i, v) => editCond('inclusion', i, v)} />
            {model.exclusion.conditions?.length > 0 && <RuleGroup title="Exclusion (blocks)" tree={model.exclusion} onEdit={(i, v) => editCond('exclusion', i, v)} danger />}
            {model.globalRules.map((g, ri) => <RuleGroup key={g.id} title={'Global · ' + (g.name || g.id)} tree={g.logic} onEdit={(ci, v) => editRuleCond('globalRules', ri, ci, v)} muted />)}
            {model.channelRules.map((c, ri) => <RuleGroup key={c.id} title={'Channel · ' + (c.name || c.id)} tree={c.logic} onEdit={(ci, v) => editRuleCond('channelRules', ri, ci, v)} muted />)}
          </div>
        )}
      </Card>
      <Card title="Member drop-off" sub={busy ? 'computing…' : 'live over gold_member_facts'}>
        {stages ? <Funnel stages={stages} accent="#a78bfa" /> : <Skeleton />}
      </Card>
    </div>
  );
}

function RuleGroup({ title, tree, onEdit, danger, muted }) {
  const conds = (tree && tree.conditions) || [];
  return (
    <div className={'rule-group' + (danger ? ' danger' : '') + (muted ? ' muted-grp' : '')}>
      <div className="rg-title">{title} <span className="rg-op">{tree?.op}</span></div>
      {conds.map((c, i) => c.conditions ? <div key={i} className="muted">(nested group)</div> : (
        <div className="cond" key={i}>
          <span className="cond-fact">{c.fact}</span>
          <span className="cond-cmp">{c.cmp}</span>
          <input className="cond-val" defaultValue={String(c.value)} onChange={(e) => onEdit(i, e.target.value)} />
        </div>
      ))}
      {!conds.length && <div className="muted">no conditions</div>}
    </div>
  );
}

// ---- Dead-letter queues: per consistency-consumer DLQ depth + last error, with replay/flush ----
function Dlq({ live }) {
  const [stats] = usePoll(() => gql(`{ dlqStats { consumer topic depth lastError lastTs sourceTopic } }`).then((d) => d.dlqStats), 5000, live);
  const [busy, setBusy] = useState(null);
  const [note, setNote] = useState({});
  const act = async (consumer, op) => {
    setBusy(consumer + op);
    try {
      const m = op === 'replay' ? `mutation($c:String!){ replayDlq(consumer:$c) }` : `mutation($c:String!){ flushDlq(consumer:$c) }`;
      const d = await gql(m, { c: consumer });
      const r = (d.replayDlq || d.flushDlq || {});
      setNote((n) => ({ ...n, [consumer]: op === 'replay' ? `replayed ${r.replayed ?? '?'} → source` : `flushed ${r.flushed ?? '?'}` }));
    } catch (e) { setNote((n) => ({ ...n, [consumer]: 'error: ' + e.message })); }
    setBusy(null);
  };
  const s = stats || [];
  const totalDepth = s.reduce((a, c) => a + c.depth, 0);
  return (
    <div>
      <div className="thr-summary">
        <Kpi label="consumers with DLQ" value={s.filter((c) => c.depth > 0).length} accent="#f87171" />
        <Kpi label="total dead-lettered" value={totalDepth} accent="#fbbf24" />
        <Kpi label="consumers monitored" value={s.length} accent="#6ea8fe" />
      </div>
      <div className="thr-grid">
        {s.map((c) => (
          <section className={'card dlq-card' + (c.depth > 0 ? ' has-dlq' : '')} key={c.consumer}>
            <div className="card-head">
              <div><h3>{c.consumer}</h3><span className="card-sub">{c.sourceTopic ? 'replays → ' + c.sourceTopic : c.topic}</span></div>
              <span className="dlq-depth" style={{ color: c.depth > 0 ? '#f87171' : '#4ade80' }}>{c.depth}</span>
            </div>
            {c.depth > 0 && c.lastError && <div className="dlq-err" title={c.lastError}>⚠ {c.lastError}</div>}
            {c.depth > 0 && c.lastTs ? <div className="muted sm">last poison {ago(c.lastTs)}</div> : <div className="muted sm">clean</div>}
            <div className="dlq-actions">
              <button className="btn primary sm" disabled={!c.depth || busy} onClick={() => act(c.consumer, 'replay')}>{busy === c.consumer + 'replay' ? '…' : 'Replay'}</button>
              <button className="btn ghost sm" disabled={!c.depth || busy} onClick={() => act(c.consumer, 'flush')}>{busy === c.consumer + 'flush' ? '…' : 'Flush'}</button>
              {note[c.consumer] && <span className="op-note">{note[c.consumer]}</span>}
            </div>
          </section>
        ))}
        {!s.length && <div className="muted">no DLQ topics yet — no consistency consumer has dead-lettered a message</div>}
      </div>
    </div>
  );
}

const Skeleton = () => <div className="skeleton" />;

// ---- Member snapshot: current facts (gold) grouped into a dot-path tree + the member's NBA history ----
const EVKIND = { router: '#fbbf24', activation: '#f0883e', state: '#6ea8fe', disposition: '#4ade80', milestone: '#a78bfa' };
const CHCOL = { email: '#60a5fa', sms: '#34d399', push: '#a78bfa', voice: '#fbbf24', mail: '#f59e0b' };
const OUTCOME_COLOR = { Opened: '#34d399', LinkClicked: '#10b981', Answered: '#10b981', Completed: '#10b981', Delivered: '#60a5fa',
  Declined: '#f59e0b', Dismissed: '#f59e0b', Unsubscribe: '#f87171', STOP: '#f87171', EXPIRED: '#fb923c',
  HARD_COMPLETED: '#10b981', SOFT_COMPLETED: '#34d399', PRESENTED: '#22d3ee', IN_PROCESS: '#38bdf8', CREATED: '#a78bfa', dispatched: '#f0883e' };

// The member's STORY as one chronological timeline: milestones reached (the wins) interleaved with every action
// the system SENT (each CREATE decision). Click any action to replay exactly why it was decided (the trace).
function MemberTimeline({ snap, onOpenTrace }) {
  const items = [];
  (snap.milestones || []).forEach((m) => items.push({ type: 'milestone', ts: m.completedAt, name: m.name, logic: m.logic }));
  (snap.actions || []).forEach((a) => {
    const evs = (a.events || []).slice().sort((x, y) => x.ts - y.ts);
    evs.forEach((e, idx) => {
      if (e.kind !== 'router' || e.op !== 'CREATE') return;     // one timeline entry per SENT decision
      const after = evs.slice(idx + 1);
      const nextDec = after.findIndex((x) => x.kind === 'router' && (x.op === 'CREATE' || x.op === 'SUPPRESS'));
      const win = nextDec >= 0 ? after.slice(0, nextDec) : after;   // events belonging to THIS send
      const disp = win.find((x) => x.kind === 'disposition');
      const states = win.filter((x) => x.kind === 'state').map((x) => x.value);
      const dispatched = win.some((x) => x.kind === 'activation' && x.op === 'DISPATCH');
      items.push({ type: 'action', ts: e.ts, action: a.name, channel: a.channel, cid: e.correlationId, contentKey: e.contentKey,
        outcome: disp?.value || states[states.length - 1] || (dispatched ? 'dispatched' : 'created') });
    });
  });
  items.sort((x, y) => x.ts - y.ts);
  if (!items.length) return <div className="muted sm">no journey yet — no milestones or sent actions for this member</div>;
  return (
    <div className="mtl">
      {items.map((it, i) => it.type === 'milestone' ? (
        <div className="mtl-row" key={i}>
          <span className="mtl-rail"><span className="mtl-dot ms">★</span></span>
          <div className="mtl-card ms" title={'completion criteria — ' + critText(it.logic)}>
            <span className="mtl-ms-name">{it.name}</span>
            <span className="mtl-meta">milestone reached · {ago(it.ts)}</span>
          </div>
        </div>
      ) : (
        <div className="mtl-row" key={i}>
          <span className="mtl-rail"><span className="mtl-dot" style={{ background: CHCOL[it.channel] || '#818cf8' }} /></span>
          <button className="mtl-card action" disabled={!it.cid} onClick={() => it.cid && onOpenTrace && onOpenTrace(snap.entityId, it.cid)}>
            <div className="mtl-line1">
              <span className="mtl-action">{it.action}</span>
              <span className="mtl-chan" style={{ color: CHCOL[it.channel] }}>{it.channel}</span>
              <span className="mtl-arrow">→</span>
              <span className="mtl-outcome" style={{ color: OUTCOME_COLOR[it.outcome] || '#9ca3af' }}>{it.outcome}</span>
              <span className="mtl-meta">{ago(it.ts)}</span>
            </div>
            {it.cid && <span className="mtl-trace">replay decision →</span>}
          </button>
        </div>
      ))}
    </div>
  );
}
// Render a milestone's completion criteria (condition tree) as readable text — for the timeline hover.
function critText(logic) {
  if (!logic) return 'no criteria';
  const one = (c) => (c && c.conditions) ? '(' + critText(c) + ')' : `${c.fact} ${c.cmp} ${c.value ?? ''}`.trim();
  const conds = (logic.conditions || []).map(one);
  if (!conds.length) return 'always';
  return conds.join(logic.op === 'any' ? '  OR  ' : '  AND  ');
}
// The member JOURNEY: milestones + activations (DISPATCH/CANCEL only) + dispositions, chronological. The
// events before each milestone are what led up to it; hover a milestone to see its completion criteria.
function Journey({ snap }) {
  const events = [];
  (snap.milestones || []).forEach((m) => events.push({ ts: m.completedAt, kind: 'milestone', name: m.name, logic: m.logic }));
  (snap.actions || []).forEach((a) => (a.events || []).forEach((e) => {
    if (e.kind === 'activation') events.push({ ts: e.ts, kind: 'activation', action: a.name, channel: a.channel, op: e.op });
    else if (e.kind === 'disposition') events.push({ ts: e.ts, kind: 'disposition', action: a.name, channel: a.channel, value: e.value });
  }));
  events.sort((x, y) => x.ts - y.ts);
  if (!events.length) return <div className="muted sm">no journey yet — no milestones, activations, or dispositions for this member</div>;
  return (
    <div className="jrn">
      {events.map((e, i) => e.kind === 'milestone' ? (
        <div className="jrn-ms" key={i} title={'completion criteria — ' + critText(e.logic)}>
          <span className="jrn-ms-dot">✓</span>
          <div className="jrn-ms-body"><span className="jrn-ms-name">{e.name}</span>
            <span className="jrn-ms-meta">milestone reached · {ago(e.ts)} · <i>hover for criteria</i></span></div>
        </div>
      ) : (
        <div className={'jrn-evt ' + e.kind} key={i}>
          <span className="jrn-dot" style={{ background: EVKIND[e.kind] }} />
          <span className="jrn-time">{ago(e.ts)}</span>
          <span className="jrn-label">{e.kind === 'activation' ? `${e.op} → activation layer` : `disposition · ${e.value}`}</span>
          <span className="jrn-action">{e.action} · {e.channel}</span>
        </div>
      ))}
    </div>
  );
}
const fmtVal = (v) => { if (v == null) return '—'; const s = String(v); return s.length > 90 ? s.slice(0, 90) + '…' : s; };
function countLeaves(node) {
  const ck = Object.keys(node.children || {});
  if (node.fact && !ck.length) return 1;
  return ck.reduce((n, k) => n + countLeaves(node.children[k]), 0);
}
function buildFactTree(facts) {
  const root = { name: '', children: {} };
  for (const f of facts || []) {
    const parts = (f.key || '').split('.');
    let node = root;
    parts.forEach((p, i) => { node.children[p] ||= { name: p, children: {} }; node = node.children[p]; if (i === parts.length - 1) node.fact = f; });
  }
  return root;
}
function FactNode({ node, depth }) {
  const childKeys = Object.keys(node.children || {}).sort();
  const [open, setOpen] = useState(depth < 1);
  if (node.fact && !childKeys.length) {
    const f = node.fact;
    return (
      <div className="ft-leaf" style={{ paddingLeft: depth * 16 + 8 }}>
        <span className="ft-seg">{node.name}</span>
        <span className="ft-val mono">{fmtVal(f.value)}</span>
        <span className="ft-meta">{[f.valueType, f.source, f.eventTs ? ago(f.eventTs) : ''].filter(Boolean).join(' · ')}</span>
      </div>
    );
  }
  return (
    <div className="ft-branch">
      <div className="ft-brow" style={{ paddingLeft: depth * 16 + 8 }} onClick={() => setOpen((o) => !o)}>
        <span className="ft-tw">{childKeys.length ? (open ? '▾' : '▸') : '·'}</span>
        <span className="ft-name">{node.name}</span>
        <span className="ft-count">{countLeaves(node)}</span>
      </div>
      {open && childKeys.map((k) => <FactNode key={k} node={node.children[k]} depth={depth + 1} />)}
    </div>
  );
}
function MemberSnapshot({ onOpenTrace }) {
  const [q, setQ] = useState('');
  const [snap, setSnap] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [samples, setSamples] = useState([]);
  useEffect(() => { gql(`{ sampleMembers(limit: 8) }`).then((d) => setSamples(d.sampleMembers || [])).catch(() => {}); }, []);
  const search = async (who) => {
    const m = (who ?? q).trim(); if (!m) return;
    if (who != null) setQ(who);
    setBusy(true); setErr(null);
    try {
      const d = await gql(`query($m:String!){ memberSnapshot(memberId:$m){ found entityId nbaId entityType updatedTs
        facts { key value valueType eventTs source }
        milestones { id name completedAt logic }
        actions { actionId name channel lastTs events { ts kind op value score contentKey correlationId source } } } }`, { m });
      setSnap(d.memberSnapshot);
    } catch (e) { setErr(e.message); setSnap(null); }
    setBusy(false);
  };
  const tree = snap && snap.found ? buildFactTree(snap.facts) : null;
  const msReached = snap?.milestones?.length || 0;
  return (
    <div>
      <Card title="Member snapshot" sub="the member's whole story — milestones reached + every action sent; click an action to replay its decision" wide
        right={<span className="ms-search">
          <input list="ms-samples" placeholder={`member id (e.g. ${samples[0] || 'hcm-00171'})`} value={q} onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => { if (e.key === 'Enter') search(); }} />
          <datalist id="ms-samples">{samples.map((s) => <option key={s} value={s} />)}</datalist>
          <button className="btn primary sm" disabled={busy} onClick={() => search()}>{busy ? '…' : 'Search'}</button>
        </span>}>
        {err && <div className="err">{err}</div>}
        {!snap && !err && (samples.length > 0
          ? <div className="trace-samples"><span className="ts-hint">no member id? try</span>{samples.map((s) => <button key={s} className="trace-sample-chip" onClick={() => search(s)}>{s}</button>)}</div>
          : <div className="muted">search a member to see their story</div>)}
        {snap && !snap.found && <div className="muted">no member found for “{q}” — try an entityId (e.g. {samples[0] || 'hcm-00171'}) or an nba_… id</div>}
        {snap && snap.found && (
          <div className="ms-head">
            <b>{snap.entityId}</b>
            <span className="tag">{snap.entityType}</span>
            <span className="mono muted">{snap.nbaId}</span>
            <span className="muted sm">{msReached} milestones · {snap.actions.length} action-channels · {snap.facts.length} facts · updated {ago(snap.updatedTs)}</span>
          </div>
        )}
      </Card>
      {snap && snap.found && (
        <div className="grid ms-grid">
          <Card title="Journey" sub="milestones + every action sent, oldest first — click an action to replay exactly why it was decided" wide>
            <MemberTimeline snap={snap} onOpenTrace={onOpenTrace} />
          </Card>
          <Card title="Facts" sub="current value of every fact, grouped by key" wide>
            <div className="ft-tree">
              {tree && Object.keys(tree.children).sort().map((k) => <FactNode key={k} node={tree.children[k]} depth={0} />)}
              {!snap.facts.length && <div className="muted sm">no facts for this member</div>}
            </div>
          </Card>
        </div>
      )}
    </div>
  );
}
