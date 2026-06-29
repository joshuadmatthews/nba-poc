import React, { useEffect, useMemo, useState } from 'react';
import { gql } from './gql.js';

// ===== Trace Replay — a decision DOSSIER ===================================================
// One member decision told as a CAUSAL story, top-to-bottom and in plain language:
//   WHO + the outcome → the PIPELINE it flowed through → which FACTS made which actions
//   eligible vs blocked (and why) → which eligible action SCORED highest and went out.
// The deciding facts are highlighted, blocked actions are grouped by their shared reason.

const CH_COLOR = { email: '#60a5fa', sms: '#34d399', push: '#a78bfa', voice: '#fbbf24', mail: '#f59e0b' };
const STAGE_ICO = { snapshot: '▣', rules: '⚖', ml: '◆', router: '⤳', action: '✉' };
const ago = (ts) => { if (!ts) return ''; const s = Math.floor((Date.now() - ts) / 1000); return s < 60 ? s + 's ago' : s < 3600 ? Math.floor(s / 60) + 'm ago' : s < 86400 ? Math.floor(s / 3600) + 'h ago' : Math.floor(s / 86400) + 'd ago'; };
const fmtScore = (s) => { if (s == null) return null; const n = Number(s); if (!isFinite(n)) return null; return Math.abs(n) >= 1000 ? n.toLocaleString(undefined, { maximumFractionDigits: 0 }) : (Math.abs(n) >= 10 ? n.toFixed(1) : n.toFixed(2)); };
const shortKey = (k) => { const p = String(k).split('.'); return p[p.length - 1]; };
const domainOf = (k) => { const p = String(k).split('.'); return p.length >= 2 ? p[1] : 'other'; };
const isBoolish = (v) => /^(true|false)$/i.test(String(v).trim());
const cleanVal = (v) => String(v).replace(/\s*\(default\)/i, '').trim();
const wasDefault = (v) => /\(default\)/i.test(String(v));

// the eligibility checks encode the fact->rule link as "<factKey> = <actual> <op> <required>"
function parseDetail(detail) {
  const mm = /^(\S+)\s*=\s*(.+?)\s*(<=|>=|!=|=|<|>)\s*(.+)$/.exec(String(detail || ''));
  if (!mm) return { raw: String(detail || '') };
  return { key: mm[1], actual: mm[2].trim(), op: mm[3], required: mm[4].trim() };
}
function friendly(p) {
  if (!p) return 'rule did not match';
  if (!p.key) return p.raw || 'rule did not match';
  const sk = shortKey(p.key), act = cleanVal(p.actual) + (wasDefault(p.actual) ? ' (not set)' : '');
  if (p.op === '=' ) return `needs ${sk} = ${p.required}, but it's ${act}`;
  if (p.op === '!=') return `needs ${sk} ≠ ${p.required}`;
  return `${sk} is ${act} — must be ${p.op} ${p.required}`;
}
// the decisive failing check for a blocked channel (prefer the Inclusion "does this apply" failure)
function decisive(ch) {
  let first = null;
  for (const st of ch.stages || []) {
    if (st.passed) continue;
    const failed = (st.checks || []).find((c) => !c.passed);
    const d = failed ? { stage: st.label, ...parseDetail(failed.detail) } : { stage: st.label, raw: 'rule did not match' };
    if (/inclusion/i.test(st.label || '')) return d;
    if (!first) first = d;
  }
  return first;
}
// the FULL "why this went out" for an eligible action: the targeting facts that made it apply (Inclusion),
// the guardrails it cleared (Global/Channel gates), on which channel.
const gateShort = (label) => String(label).replace(/^(Global|Channel)\s*·\s*/, '').replace(/\s*\(.*\)\s*$/, '');
const passPhrase = (detail) => { const p = parseDetail(detail); if (!p.key) return p.raw; return shortKey(p.key) + ' = ' + cleanVal(p.actual) + (wasDefault(p.actual) ? ' (not set)' : ''); };
function explainOf(c) {
  const ch = (c.channels || []).find((x) => x.won) || (c.channels || []).find((x) => x.eligible);
  if (!ch || !ch.stages) return null;
  const isIncl = (st) => /inclusion|exclusion/i.test(st.label || '');
  const targets = ch.stages.filter(isIncl).flatMap((st) => (st.checks || []).map((k) => passPhrase(k.detail))).filter(Boolean);
  const gates = ch.stages.filter((st) => !isIncl(st)).map((st) => ({ name: gateShort(st.label), passed: st.passed }));
  return { channel: ch.channel, targets, gates };
}

export default function TraceView({ target }) {
  const [entity, setEntity] = useState('');
  const [traces, setTraces] = useState(null);
  const [trace, setTrace] = useState(null);
  const [err, setErr] = useState(null);
  const [samples, setSamples] = useState([]);
  const [open, setOpen] = useState(() => new Set());   // expanded blocked groups (reason text)
  const [factFilter, setFactFilter] = useState('');

  useEffect(() => { gql(`{ sampleMembers(limit: 7) }`).then((d) => setSamples(d.sampleMembers || [])).catch(() => {}); }, []);
  // deep-link from another page (e.g. Member snapshot → "replay decision"): load that member's decisions and
  // select the specific correlationId.
  useEffect(() => { if (target && target.cid) loadTraces(target.member, target.cid); /* eslint-disable-next-line */ }, [target]);

  async function loadTraces(e, preferCid) {
    setErr(null); setTraces(null); setTrace(null);
    const who = (e ?? entity).trim(); if (!who) return;
    if (e != null) setEntity(who);
    try {
      const d = await gql(`query($e:String!){ recentTraces(entity:$e, limit:20){ correlationId winner suppressedCount ts } }`, { e: who });
      setTraces(d.recentTraces);
      if (preferCid) { loadTrace(preferCid); return; }   // deep-link: jump straight to the requested decision
      const first = (d.recentTraces || []).find((t) => t.winner) || d.recentTraces?.[0];
      if (first) loadTrace(first.correlationId); else setErr('no decisions found for that member');
    } catch (ex) { setErr(ex.message); }
  }
  async function loadTrace(cid) {
    setErr(null); setOpen(new Set());
    try { const d = await gql(`query($c:ID!){ trace(correlationId:$c) }`, { c: cid }); setTrace(d.trace?.found ? d.trace : null); if (!d.trace?.found) setErr('no events for that correlationId'); }
    catch (ex) { setErr(ex.message); }
  }

  // ---- derive the decision model from the trace steps ----
  const m = useMemo(() => {
    if (!trace) return null;
    const byNode = {}; (trace.steps || []).forEach((s) => { byNode[s.node] = s; });
    const facts = byNode.snapshot?.facts || {};
    const explanations = byNode.rules?.explanations || [];
    const scored = byNode.ml?.scored || [];
    const router = byNode.router || {};
    const winner = router.winner || null, batch = router.batch || null, suppressed = router.suppressed || [];
    const dispositions = byNode.action?.dispositions || [], workflows = byNode.temporal?.workflows || [];

    const scoreMap = {}; scored.forEach((s) => { scoreMap[s.name + '|' + s.channel] = s.score; });
    if (winner) scoreMap[winner.name + '|' + winner.channel] = winner.score;
    (batch?.actions || []).forEach((a) => { if (a.score != null) scoreMap[a.name + '|' + a.channel] = a.score; });
    const winSet = new Set();
    if (batch) (batch.actions || []).forEach((a) => winSet.add(a.name + '|' + a.channel));
    else if (winner) winSet.add(winner.name + '|' + winner.channel);
    const suppMap = {}; suppressed.forEach((s) => { suppMap[s.name + '|' + s.channel] = s.reason; });
    const dispoMap = {}; [...dispositions, ...workflows].forEach((d) => { if (d && d.disposition) dispoMap[d.name + '|' + d.channel] = d.disposition; });

    const cands = explanations.map((ex) => {
      const channels = (ex.channels || []).map((c) => {
        const key = ex.name + '|' + c.channel;
        return { ...c, key, won: winSet.has(key), score: scoreMap[key] ?? null, dispo: dispoMap[key] || null, lostReason: suppMap[key] || null };
      });
      const eligN = channels.filter((c) => c.eligible).length;
      const cand = { actionId: ex.actionId, name: ex.name, channels, eligN, wonHere: channels.some((c) => c.won) };
      if (eligN === 0) { const d = decisive(channels[0] || {}); cand.block = d; cand.blockText = friendly(d); cand.blockKey = d?.key || null; }
      return cand;
    });
    cands.sort((a, b) => (b.wonHere - a.wonHere) || (b.eligN - a.eligN) || a.name.localeCompare(b.name));

    const eligible = cands.filter((c) => c.eligN > 0);
    const blocked = cands.filter((c) => c.eligN === 0);
    const groups = {}; blocked.forEach((c) => { (groups[c.blockText] ||= { reason: c.blockText, key: c.blockKey, actions: [] }).actions.push(c); });
    const blockedGroups = Object.values(groups).sort((a, b) => b.actions.length - a.actions.length);
    const decidingKeys = new Set(blockedGroups.map((g) => g.key).filter(Boolean));

    const eligCombos = cands.reduce((n, c) => n + c.eligN, 0);
    const blockedCombos = cands.reduce((n, c) => n + (c.channels.length - c.eligN), 0);
    const winScore = winner ? winner.score : (batch?.actions?.[0]?.score ?? null);
    const winDispo = winner ? dispoMap[winner.name + '|' + winner.channel] : null;

    return { facts, factsAt: byNode.snapshot?.factsAt, eligible, blockedGroups, decidingKeys,
      winner, batch, suppressed, scored, eligCombos, blockedCombos, winScore, winDispo, factCount: Object.keys(facts).length };
  }, [trace]);

  const toggle = (k) => setOpen((s) => { const n = new Set(s); n.has(k) ? n.delete(k) : n.add(k); return n; });

  const factGroups = useMemo(() => {
    if (!m) return [];
    const q = factFilter.trim().toLowerCase(), g = {};
    Object.entries(m.facts).forEach(([k, v]) => {
      // member ATTRIBUTES drive decisions — show those; hide internal pipeline state (nba.score JSON blobs,
      // nba.actionstate/disposition/throttle) that just clutters the "why".
      if (!k.startsWith('operator.')) return;
      if (q && !(k.toLowerCase().includes(q) || String(v).toLowerCase().includes(q))) return;
      (g[domainOf(k)] ||= []).push({ k, short: shortKey(k), v: String(v), deciding: m.decidingKeys.has(k) });
    });
    return Object.entries(g).map(([dom, rows]) => [dom, rows.sort((a, b) => (b.deciding - a.deciding) || a.short.localeCompare(b.short))])
      .sort((a, b) => b[1].length - a[1].length);
  }, [m, factFilter]);

  const cid = String(trace?.correlationId || '');

  return (
    <div className="trace-page">
      <div className="trace-bar">
        <input className="trace-input" list="trace-samples" placeholder={`member id (e.g. ${samples[0] || 'hcm-00171'}) or nbaId`} value={entity}
          onChange={(e) => setEntity(e.target.value)} onKeyDown={(e) => e.key === 'Enter' && loadTraces()} />
        <datalist id="trace-samples">{samples.map((s) => <option key={s} value={s} />)}</datalist>
        <button className="btn primary sm" onClick={() => loadTraces()}>Find decisions</button>
        {traces && <select className="trace-select" value={cid} onChange={(e) => e.target.value && loadTrace(e.target.value)}>
          <option value="">{traces.length} decisions — pick one…</option>
          {traces.map((t) => <option key={t.correlationId} value={t.correlationId}>{t.winner || 'no activation'} · {t.suppressedCount} suppressed · {ago(t.ts)}</option>)}
        </select>}
        {err && <span className="err sm">{err}</span>}
      </div>

      {samples.length > 0 && !trace && (
        <div className="trace-samples">
          <span className="ts-hint">no member id? try</span>
          {samples.map((s) => <button key={s} className="trace-sample-chip" onClick={() => { setEntity(s); loadTraces(s); }}>{s}</button>)}
        </div>
      )}

      {!trace || !m ? (
        <div className="td-empty">
          <div className="td-empty-ico">⚖</div>
          <div className="td-empty-t">Replay any decision</div>
          <div className="td-empty-d">See which member facts made which actions eligible, how the eligible ones scored, and which one was sent — the whole decision, in plain language.</div>
        </div>
      ) : (
        <div className="trace-dossier">
          {/* ---- header: who + outcome ---- */}
          <div className="td-header">
            <div className="td-who">
              <div className="td-member">{trace.entityId}</div>
              <div className="td-meta">decided {ago(trace.ts)} · corr <span className="mono">{cid.slice(0, 8)}</span></div>
            </div>
            <div className={'td-outcome ' + (m.batch ? 'batch' : m.winner ? 'won' : 'none')}>
              {m.batch ? (<>
                <div className="td-oc-label"><span className="td-oc-star">⛁</span> Batched · {m.batch.count} actions on <b style={{ color: CH_COLOR[m.batch.channel] }}>{m.batch.channel}</b></div>
                <div className="td-oc-list">{(m.batch.actions || []).map((a) => <span key={a.name} className="td-oc-chip">{a.name}{a.disposition && a.disposition !== 'pending' ? ` · ${a.disposition}` : ''}</span>)}</div>
              </>) : m.winner ? (<>
                <div className="td-oc-top"><span className="td-oc-star">★</span><span className="td-oc-win">{m.winner.name}</span><span className="td-oc-arrow">sent on</span><span className="td-oc-ch" style={{ color: CH_COLOR[m.winner.channel] }}>{m.winner.channel}</span></div>
                <div className="td-oc-sub">{fmtScore(m.winScore) != null && <span className="td-oc-score">top score {fmtScore(m.winScore)}</span>}{m.winner.contentKey && <span className="td-oc-variant" title="content variant served">{m.winner.contentKey}</span>}{m.winDispo && <span className="td-oc-dispo">{m.winDispo}</span>}</div>
              </>) : (<>
                <div className="td-oc-label"><span className="td-oc-star">∅</span> No action sent</div>
                <div className="td-oc-sub">every candidate was blocked or suppressed</div>
              </>)}
            </div>
          </div>

          {/* ---- pipeline ribbon ---- */}
          <div className="td-pipeline">
            {[
              ['snapshot', 'Facts', `${m.factCount} gathered`],
              ['rules', 'Eligibility', `${m.eligCombos} pass · ${m.blockedCombos} blocked`],
              ['ml', 'Scoring', `${m.scored.length} scored`],
              ['router', 'Routing', m.batch ? `${m.batch.count} batched` : m.winner ? `1 won · ${m.suppressed.length} lost` : 'none'],
              ['action', 'Delivery', m.winDispo || (m.winner || m.batch ? 'dispatched' : '—')],
            ].map(([node, label, val], i) => (
              <React.Fragment key={node}>
                {i > 0 && <span className="td-arrow">→</span>}
                <div className="td-stage">
                  <div className="td-stage-ico">{STAGE_ICO[node]}</div>
                  <div className="td-stage-txt"><div className="td-stage-l">{label}</div><div className="td-stage-v">{val}</div></div>
                </div>
              </React.Fragment>
            ))}
          </div>

          {/* ---- body: the decision + the facts that drove it ---- */}
          <div className="td-body">
            <div className="td-candidates">
              {/* what competed (eligible) */}
              <div className="td-col-h">What competed <span className="td-col-sub">eligible actions — the highest propensity score is sent</span></div>
              {m.eligible.length === 0 && <div className="muted sm" style={{ padding: '4px 2px 12px' }}>nothing was eligible for this member at decision time</div>}
              {m.eligible.map((c) => (
                <div className={'td-cand' + (c.wonHere ? ' won' : '')} key={c.name}>
                  <div className="td-cand-head">
                    <span className="td-cand-name">{c.name}</span>
                    {c.wonHere ? <span className="td-won-badge">★ SENT</span> : <span className="td-elig-badge">eligible</span>}
                    {c.wonHere && fmtScore(m.winScore) != null && <span className="td-cand-score">{fmtScore(m.winScore)}</span>}
                  </div>
                  <div className="td-cand-why">{c.wonHere
                    ? `Highest score among ${m.eligible.length} eligible — sent on ${m.winner?.channel || m.batch?.channel}.`
                    : (c.channels.find((ch) => ch.lostReason)?.lostReason || 'Eligible, but another action scored higher.')}</div>
                  <div className="td-chans">
                    {c.channels.map((ch) => (
                      <span key={ch.channel} className={'td-chan' + (ch.eligible ? ' ok' : ' no') + (ch.won ? ' won' : '')}
                        style={ch.eligible ? { borderColor: CH_COLOR[ch.channel] + (ch.won ? '' : '55') } : undefined} title={ch.eligible ? (ch.won ? 'winning channel' : 'eligible channel') : (c.blockText || 'blocked')}>
                        <span className="td-chan-mark">{ch.won ? '★' : ch.eligible ? '✓' : '✕'}</span>
                        <span className="td-chan-name" style={ch.eligible ? { color: CH_COLOR[ch.channel] } : undefined}>{ch.channel}</span>
                        {ch.won && ch.dispo && ch.dispo !== 'pending' && <span className="td-chan-dispo">{ch.dispo}</span>}
                      </span>
                    ))}
                  </div>
                  {(() => {
                    const ex = explainOf(c); if (!ex) return null;
                    const show = c.wonHere || open.has('cand:' + c.name);
                    return (<>
                      {!c.wonHere && <button className="td-ex-toggle" onClick={() => toggle('cand:' + c.name)}>{show ? '▾' : '▸'} why it was eligible</button>}
                      {show && (
                        <div className="td-explain">
                          <div className="td-ex-row"><span className="td-ex-h">Targets</span><span className="td-ex-body">{ex.targets.length ? ex.targets.join('  ·  ') : 'all members (no targeting rule)'}</span></div>
                          <div className="td-ex-row"><span className="td-ex-h">Cleared</span><span className="td-ex-body td-gates">{ex.gates.length ? ex.gates.map((g, i) => <span key={i} className={'td-gate' + (g.passed ? '' : ' no')}>{g.passed ? '✓' : '✕'} {g.name}</span>) : <span className="td-gate">✓ no guardrails tripped</span>}</span></div>
                          {c.wonHere && <div className="td-ex-row"><span className="td-ex-h">Selected</span><span className="td-ex-body">top propensity score <b>{fmtScore(m.winScore)}</b> on <b style={{ color: CH_COLOR[ex.channel] }}>{ex.channel}</b>{m.eligible.length > 1 ? ` — beat ${m.eligible.length - 1} other eligible action${m.eligible.length - 1 !== 1 ? 's' : ''}` : ''}</span></div>}
                        </div>
                      )}
                    </>);
                  })()}
                </div>
              ))}

              {/* not eligible — grouped by the shared fact-based reason */}
              {m.blockedGroups.length > 0 && (
                <div className="td-col-h blocked">Not eligible <span className="td-col-sub">grouped by the fact that ruled them out</span></div>
              )}
              {m.blockedGroups.map((g) => (
                <div className="td-bgroup" key={g.reason}>
                  <button className={'td-bg-head' + (open.has(g.reason) ? ' open' : '')} onClick={() => toggle(g.reason)}>
                    <span className="td-bg-x">✕</span>
                    <span className="td-bg-reason">{g.reason}{g.key && <span className="td-bg-fact">{shortKey(g.key)}</span>}</span>
                    <span className="td-bg-count">{g.actions.length} action{g.actions.length > 1 ? 's' : ''}</span>
                    <span className="td-bg-caret">{open.has(g.reason) ? '▾' : '▸'}</span>
                  </button>
                  <div className="td-bg-actions">
                    {g.actions.map((c) => <span key={c.name} className="td-bg-chip">{c.name}</span>)}
                  </div>
                  {open.has(g.reason) && (
                    <div className="td-bg-detail">
                      {(g.actions[0]?.block && g.actions[0].block.key) ? (
                        <div className="td-block-stage">at rule <b>{g.actions[0].block.stage}</b> · check: <span className="mono">{shortKey(g.actions[0].block.key)} = {cleanVal(g.actions[0].block.actual)}{wasDefault(g.actions[0].block.actual) ? ' (not set)' : ''}</span>, needs <span className="mono">{g.actions[0].block.op} {g.actions[0].block.required}</span></div>
                      ) : <div className="td-block-stage">{g.actions[0]?.block?.raw || 'rule did not match'}</div>}
                    </div>
                  )}
                </div>
              ))}
            </div>

            <aside className="td-side">
              <div className="td-side-h">Facts at decision time <span className="td-col-sub">{m.factsAt} · ◆ drove this decision</span></div>
              <input className="td-fact-filter" placeholder="filter facts…" value={factFilter} onChange={(e) => setFactFilter(e.target.value)} />
              <div className="td-facts">
                {factGroups.map(([dom, rows]) => (
                  <div className="td-fact-group" key={dom}>
                    <div className="td-fact-group-h">{dom} <span>{rows.length}</span></div>
                    {rows.map((f) => (
                      <div className={'td-fact' + (f.deciding ? ' deciding' : '')} key={f.k} title={f.k}>
                        <span className="td-fact-k">{f.deciding && <span className="td-fact-mark">◆</span>}{f.short}</span>
                        <span className={'td-fact-v' + (isBoolish(f.v) ? (/^true$/i.test(f.v) ? ' t' : ' f') : '')}>{f.v}</span>
                      </div>
                    ))}
                  </div>
                ))}
                {!factGroups.length && <div className="muted sm">no facts match</div>}
              </div>
            </aside>
          </div>
        </div>
      )}
    </div>
  );
}
