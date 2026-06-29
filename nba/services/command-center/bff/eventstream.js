// Live event stream for the System Map — the Command Center centerpiece.
// Tails EVERY NBA topic over NATIVE Kafka (kafkajs) — the BFF runs on the NBA network, so it reaches
// Redpanda's INTERNAL anonymous listener directly at nba-redpanda:9092 (no pandaproxy, no SASL). Maps
// each message to the component that EMITTED it and broadcasts to the browser over SSE so the live system
// map lights nodes/edges as facts flow. Ring buffers per node for payload "peek". fromBeginning=false ->
// live flow, not backlog. Native Kafka also exposes record HEADERS (e.g. kind=router) for free.
import kafkajs from 'kafkajs';          // CJS module — destructure from the default (named ESM exports are partial)
import snappy from 'snappyjs';
import { sql, NS, lakeConfigured } from './lake.js';   // gold is the source of truth for the state rollup
const { Kafka, logLevel, CompressionCodecs, CompressionTypes } = kafkajs;
// Redpanda producers compress with Snappy; kafkajs ships no Snappy codec, so register a pure-JS one
// (snappyjs — no native build). Kafka v2 record batches use raw block Snappy, which snappyjs handles.
CompressionCodecs[CompressionTypes.Snappy] = () => ({
  compress: async (encoder) => snappy.compress(encoder.buffer),
  decompress: async (buffer) => snappy.uncompress(buffer),
});

const BROKER = process.env.CC_KAFKA_BROKER || 'nba-redpanda:9092';
// nba.facts (the 21M-row "all facts, nobody subscribes yet" feature stream) is NOT an edge topic and just
// starved the observer — dropped from the tail so the BFF keeps up on the topics the map actually draws.
const TOPICS = ['datalake.streaming-inbound', 'nba.member.facts', 'nba.snapshots', 'nba.evaluations', 'nba.activations', 'nba.definitions'];
const GROUP = 'cc-live-systemmap';

// ---- system topology (nodes = components, edges = topic flows) ----
// x,y are layout coords (0..1000 / 0..560) the UI renders directly.
export const TOPOLOGY = {
  nodes: [
    // External source systems stream to datalake.streaming-inbound (NOT NBA kafka directly). Databricks is the
    // front door: it medallions the inbound stream and EMITS two topics — nba.facts (ALL facts, for a future ML
    // layer; nobody subscribes yet) and nba.member.facts (only facts that carry an action mapping). The
    // snapshot-builder listens ONLY to member.facts; internal NBA facts (scores/states/dispositions) also ride it.
    // Laid out as a recirculating loop: ingest on the left, the forward decision→activation path arcs across the
    // TOP (snapshot → rules → router → state machine → activation), and the fold-backs recirculate along the
    // BOTTOM back into the snapshot. ml is a branch off rules; mlfuture sits above the lake.
    { id: 'source', label: 'Source Systems', sub: 'stream → datalake-inbound', x: 70, y: 460, kind: 'source' },
    { id: 'lake', label: 'Databricks Lake', sub: 'medallion · ingest + emit facts', x: 70, y: 300, kind: 'lake' },
    { id: 'train', label: 'Model Training', sub: 'Databricks · reconstruct → CQL → promote', x: 250, y: 560, kind: 'learn' },
    { id: 'snapshot', label: 'Snapshot Builder', sub: 'member.facts · LWW snapshot', x: 440, y: 300, kind: 'svc' },
    { id: 'rules', label: 'Rules Engine', sub: 'Drools eligibility', x: 620, y: 160, kind: 'svc' },
    { id: 'ml', label: 'ML Policy', sub: 'RL · CQL · eval + features', x: 620, y: 450, kind: 'learn' },
    { id: 'router', label: 'Action Router', sub: 'winning channel · top-N', x: 850, y: 150, kind: 'svc' },
    { id: 'temporal', label: 'State Machine', sub: 'per-action workflows', x: 1080, y: 250, kind: 'machine' },
    { id: 'action', label: 'Unified Activation Layer', sub: 'one comm · dispositions', x: 1010, y: 455, kind: 'svc' },
  ],
  edges: [
    // --- ingress: external stream -> lake -> emitted facts ---
    { id: 'e1', from: 'source', to: 'lake', topic: 'datalake.streaming-inbound' },   // source systems stream IN
    { id: 'e2', from: 'lake', to: 'snapshot', topic: 'nba.member.facts' },           // lake emits action-mapped facts
    // --- the LEARNING LOOP (offline, on Databricks): the lake's accrued journeys train the policy the live scorer serves ---
    { id: 'e3', from: 'lake', to: 'train', lane: 'learn', elabel: 'journeys' },       // gold journeys → reconstruct + train (CQL)
    { id: 'e3b', from: 'train', to: 'ml', lane: 'learn', elabel: 'model' },           // promoted CQL Q-net → the live policy reloads it
    { id: 'e3c', from: 'lake', to: 'ml', lane: 'learn', elabel: 'features' },         // the policy reads each member's rich features from gold
    // --- NBA pipeline (internal facts ride member.facts) ---
    { id: 'e4', from: 'snapshot', to: 'rules', topic: 'nba.snapshots' },
    { id: 'e5', from: 'rules', to: 'ml', topic: 'nba.evaluations' },
    { id: 'e6', from: 'rules', to: 'router', topic: 'nba.evaluations' },
    { id: 'e7', from: 'ml', to: 'snapshot', topic: 'nba.member.facts' },   // scores ride member.facts into the snapshot, then up through rules in the eval
    { id: 'e8', from: 'router', to: 'temporal', topic: 'nba.member.facts' },
    { id: 'e9', from: 'temporal', to: 'action', topic: 'nba.activations' },
    { id: 'e10', from: 'temporal', to: 'snapshot', topic: 'nba.member.facts' },       // states fold back
    { id: 'e11', from: 'action', to: 'snapshot', topic: 'nba.member.facts' },         // dispositions fold back
    // (The lake also tails the NBA internal topics for silver/gold analytics, but we don't draw those return
    //  trips — the lake is shown purely as the SOURCE of facts into the pipeline, not a sink.)
  ],
};

// which component EMITTED an event, from (topic, source)
export function emitterFor(topic, source, kind) {
  if (kind === 'router') return 'router';   // router decisions ride member.facts with a kind=router header
  if (topic === 'datalake.streaming-inbound') return 'source';   // external source systems stream IN to the lake
  if (topic === 'nba.facts') return 'lake';                      // the lake emits the ALL-facts feature stream (future ML)
  if (topic === 'nba.snapshots') return 'snapshot';
  if (topic === 'nba.evaluations') return 'rules';
  if (topic === 'nba.activations') return 'temporal';   // state machine's DISPATCH/CANCEL (router moved to member.facts)
  if (topic === 'nba.definitions') return 'source';
  // member.facts: disambiguate by the internal source (router decisions ride member.facts, source=action-router)
  const s = (source || '').toLowerCase();
  if (s === 'ml') return 'ml';
  if (s === 'temporal') return 'temporal';
  if (s === 'action-layer') return 'action';
  if (s === 'action-router') return 'router';
  if (s === 'rules-engine' || s === 'rules') return 'rules';
  // lake-emitted member.facts: materialized external facts + comms/throttle counts + gold reconcile.
  if (s === 'comms-lake' || s === 'throttle-lake' || s === 'databricks-gold' || s === 'datalake' || s === 'datalake-inbound') return 'lake';
  // any other member.facts with no internal source = an external fact the lake materialized.
  if (topic === 'nba.member.facts') return 'lake';
  return 'source';
}

const subscribers = new Set();
const recentGlobal = [];                 // last N events (all)
const recentByNode = new Map();          // nodeId -> last events
const stats = new Map();                 // nodeId -> count
let lastEvent = null;

// ---- state-machine rollup, sourced from GOLD (the latest-fact-per-key product) ----
// gold_member_snapshot holds the CURRENT nba.actionstate.{action}.{channel} per member — the authoritative
// "where every workflow is now". We poll it on a short cycle (TTL-cached in databricks.js) rather than
// accumulating in memory: the old in-memory rollup never cleared, so it drifted stale across resets and showed
// phantom counts (the whole reason it looked wrong after a wipe).
export const CANONICAL_STATES = ['CREATED', 'IN_PROCESS', 'PRESENTED', 'SOFT_COMPLETED',
  'DECLINED', 'SUPPRESSING', 'FAILED', 'HARD_COMPLETED', 'EXPIRED', 'SUPPRESSED', 'DEBOUNCED'];
let _goldStates = Object.fromEntries(CANONICAL_STATES.map((s) => [s, 0]));
async function refreshGoldStates() {
  try {
    const rows = await sql(`SELECT value AS state, count(*) AS n FROM ${NS}.gold_member_snapshot WHERE key LIKE 'nba.actionstate.%' GROUP BY value`, 8000);
    const next = Object.fromEntries(CANONICAL_STATES.map((s) => [s, 0]));
    for (const r of rows) if (r.state in next) next[r.state] = Number(r.n);
    _goldStates = next;
  } catch { /* keep last-good on a transient lake hiccup */ }
}
if (lakeConfigured) { refreshGoldStates(); setInterval(refreshGoldStates, 10000).unref?.(); }
// The UI seeds + periodically re-anchors its counts from this; live SSE transitions just animate between polls.
export function stateCounts() { return { ..._goldStates }; }
/** Classify a streamed member-fact / decision into a state-machine CATEGORY + parsed state transition.
 *  cat: 'state' (a workflow transition) | 'disposition' (delivery) | 'router' (CREATE/SUPPRESS/Soft/Hard). */
function smEnrich(topic, d) {
  if (d && d.op) return { cat: 'router', st: null };                       // router decision (op rides member.facts)
  if (topic === 'nba.member.facts' && d && typeof d.key === 'string') {
    if (d.key.startsWith('nba.actionstate.')) {
      const rem = d.key.slice('nba.actionstate.'.length); const dot = rem.lastIndexOf('.');
      const st = { action: rem.slice(0, dot), channel: dot < 0 ? '' : rem.slice(dot + 1), state: String(d.value) };
      return { cat: 'state', st };   // live transition (feed/glow only) — counts come from gold, not accumulated here
    }
    if (d.key.startsWith('nba.disposition.')) return { cat: 'disposition', st: { raw: String(d.value), state: d.state || null } };
    if (d.key.startsWith('nba.completion.')) return { cat: 'completion', st: null };
  }
  return { cat: null, st: null };
}

function pushBuf(arr, item, max) { arr.unshift(item); if (arr.length > max) arr.length = max; }

// ---- live INSTANTANEOUS metrics (sliding window) — throughput per component + per-edge hop latency ----
// Component B's processing time = (B's outbound EMIT time) - (the inbound event B consumed = A's outbound
// EMIT time), correlated on the SAME entity. EMIT time = the Kafka record timestamp (ev.emitTs), and these
// topics are set to message.timestamp.type=LogAppendTime, so that timestamp is stamped by the BROKER at the
// instant the record lands in the log — ONE shared clock for every producer (they all share Kafka). That
// makes the subtraction immune to the two things that wrecked it before:
//   • Producer clock skew — the Databricks CLOUD sim and the LOCAL containers have ~28s skew under CreateTime;
//     LogAppendTime ignores the producer clock entirely (the local broker stamps both).
//   • Observer lag — the BFF can fall tens of thousands of msgs behind on the member.facts firehose; with the
//     append time ON the record, WHEN we read it no longer matters. (We also dropped the unused nba.facts
//     21M-row stream from the tail so the observer keeps up and the map reacts in real time.)
// The old approach (payload eventTs, or producer CreateTime) is what fabricated the bogus 192s (lake→snapshot)
// and 392s (source→lake) — data-age + skew, not processing latency. A correlation window (CORRELATION_WIN_MS)
// drops stale cross-burst pairs so a quiet stage can't fabricate a giant hop. The 4-hour trend/averages are a
// separate LAKE gold product; this is the live "what the system just did" view.
const METRIC_WIN_MS = 30000;                 // keep ~30s of samples for the rolling avg/p95
const CORRELATION_WIN_MS = 60000;            // max plausible hop; a wider gap = stale/cross-burst pair, dropped
const UPSTREAM_OF = {};                       // node -> [upstream nodes], from the topology edges
for (const e of TOPOLOGY.edges) (UPSTREAM_OF[e.to] = UPSTREAM_OF[e.to] || []).push(e.from);
const lastByEntity = new Map();               // entity -> Map(node -> last EMIT ts) for hop correlation
const lastEntityAt = new Map();               // entity -> recvTs (LRU trim)
const nbaToEntity = new Map();                // nbaId -> entityId (snapshots/evals/acts carry both)
let tpSamples = [];                           // {node, recvTs}
let hopSamples = [];                          // {edge:'a>b', ms, recvTs}

// Canonical correlation key = the original entityId (most events carry it; nbaId-only ones resolve via the
// snapshot's nbaId<->entityId pairing). Without this, the eval (keyed nbaId) wouldn't join the snapshot
// (keyed entityId) and only the first hop would light up.
function canonEntity(ev) {
  const d = ev.payload || {};
  if (d.entityId) { if (d.nbaId) nbaToEntity.set(d.nbaId, d.entityId); return d.entityId; }
  if (d.nbaId) return nbaToEntity.get(d.nbaId) || d.nbaId;
  return ev.entity;
}

function recordMetric(ev) {
  const now = Date.now();                                           // wall-clock for the rolling-window trims only
  tpSamples.push({ node: ev.emitter, recvTs: now });
  const entity = canonEntity(ev);
  const emit = ev.emitTs || now;                                    // BROKER append time (LogAppendTime) — the shared clock
  if (entity) {
    const m = lastByEntity.get(entity);
    for (const a of (UPSTREAM_OF[ev.emitter] || [])) {              // processing time vs the inbound event from each upstream
      const at = m && m.get(a);                                    // the upstream's broker-append time for THIS entity
      if (at != null) { const ms = emit - at; if (ms >= 0 && ms < CORRELATION_WIN_MS) hopSamples.push({ edge: a + '>' + ev.emitter, ms, recvTs: now }); }
    }
    let mm = m; if (!mm) { mm = new Map(); lastByEntity.set(entity, mm); }
    mm.set(ev.emitter, emit);                                       // store the shared broker clock, not our read time
    lastEntityAt.set(entity, now);
  }
  const cut = now - METRIC_WIN_MS;
  if (tpSamples.length > 4000 || (tpSamples[0] && tpSamples[0].recvTs < cut)) tpSamples = tpSamples.filter((s) => s.recvTs >= cut);
  if (hopSamples.length > 4000 || (hopSamples[0] && hopSamples[0].recvTs < cut)) hopSamples = hopSamples.filter((s) => s.recvTs >= cut);
  if (lastByEntity.size > 5000) for (const [k, t] of lastEntityAt) if (t < cut) { lastByEntity.delete(k); lastEntityAt.delete(k); }
}

/** Instantaneous rollup for the UI: per-node throughput (events/sec, last 10s) + per-edge latency (avg/p95/n
 *  over the window). Edge key is "from>to" matching the topology. */
export function metrics() {
  const now = Date.now(), w10 = now - 10000;
  const tp = {};
  for (const s of tpSamples) if (s.recvTs >= w10) tp[s.node] = (tp[s.node] || 0) + 1;
  const throughput = {}; for (const k in tp) throughput[k] = +(tp[k] / 10).toFixed(2);
  const byEdge = {};
  for (const s of hopSamples) (byEdge[s.edge] = byEdge[s.edge] || []).push(s.ms);
  const latency = {};
  for (const e in byEdge) {
    const a = byEdge[e].sort((x, y) => x - y);
    // p50 (median) is the headline number — robust to the stale-fold-back correlation outliers that drag avg
    // wildly (an entity's recirculated fact can pair the output against a 40s-old upstream emit). avg/p95 kept
    // for the tooltip + trend so the tail is still visible.
    latency[e] = {
      p50: a[Math.floor(a.length / 2)],
      avg: Math.round(a.reduce((s, x) => s + x, 0) / a.length),
      p95: a[Math.min(a.length - 1, Math.floor(a.length * 0.95))],
      n: a.length,
    };
  }
  return { throughput, latency, ts: now };
}

function broadcast(ev) {
  lastEvent = ev;
  recordMetric(ev);
  pushBuf(recentGlobal, ev, 120);
  const arr = recentByNode.get(ev.emitter) || []; pushBuf(arr, ev, 25); recentByNode.set(ev.emitter, arr);
  stats.set(ev.emitter, (stats.get(ev.emitter) || 0) + 1);
  const data = `data: ${JSON.stringify(ev)}\n\n`;
  for (const res of subscribers) { try { res.write(data); } catch { /* dropped */ } }
}

// Rolling trend history (DEMO: a real ~5-min in-memory window, one point per 5s — stands in for the 4-hour
// lake gold product, which would correlate stages over silver tables. Same shape, shorter window, no lake.)
const HISTORY_PTS = 60, HISTORY_EVERY_MS = 5000;
let metricHistory = [];          // [{ ts, tp:{node:evps}, lat:{edge:avgMs} }]
let lastHistAt = 0;
function snapshotHistory(m) {
  const lat = {}; for (const e in m.latency) if (m.latency[e].n >= MIN_SAMPLES) lat[e] = m.latency[e].p50;
  metricHistory.push({ ts: m.ts, tp: m.throughput, lat });
  if (metricHistory.length > HISTORY_PTS) metricHistory.shift();
}
export const metricsHistory = () => metricHistory;

// LAST-KNOWN-GOOD per node/edge — PERSISTS beyond the 30s sample window (it only updates, never clears) so a
// browser that loads during a quiet moment still gets a populated map immediately. The live window empties on
// a quiet tick; this is the server-side stateful baseline (the BFF's accurate broker-clock measurements kept
// around), seeded into a fresh tab via the hello event. `at` = wall-clock it was last seen, so the client can
// dim values that have gone stale rather than show them as live.
const lastKnown = { tp: {}, tpAt: {}, lat: {}, latAt: {} };
const MIN_SAMPLES = 3;   // don't publish a hop colored off 1-2 lucky pairs — needs a quorum to be trustworthy
function updateLastKnown(m, now) {
  for (const k in m.throughput) if (m.throughput[k] > 0) { lastKnown.tp[k] = m.throughput[k]; lastKnown.tpAt[k] = now; }
  // headline = p50 (robust); gate on a min sample count so a single stale pair can't paint a stage red.
  for (const k in m.latency) { const L = m.latency[k]; if (L && L.n >= MIN_SAMPLES) { lastKnown.lat[k] = L.p50; lastKnown.latAt[k] = now; } }
}
// snapshot the last-known WITH AGES (ms since seen) so the client seeds held.*At against ITS OWN clock — no
// cross-machine clock skew. Returns { tp, lat } where each value rides with how stale it is.
export function lastKnownSeed() {
  const now = Date.now();
  const tp = {}, lat = {};
  for (const k in lastKnown.tp) tp[k] = { v: lastKnown.tp[k], ageMs: now - lastKnown.tpAt[k] };
  for (const k in lastKnown.lat) lat[k] = { v: lastKnown.lat[k], ageMs: now - lastKnown.latAt[k] };
  return { tp, lat };
}

// push the instantaneous rollup (+ the rolling history) to subscribers as a named SSE event, ~every 2s;
// snapshot the history every 5s regardless so the trend always builds.
setInterval(() => {
  const m = metrics();
  const now = m.ts;
  updateLastKnown(m, now);
  if (now - lastHistAt >= HISTORY_EVERY_MS) { lastHistAt = now; snapshotHistory(m); }
  if (!subscribers.size) return;
  const data = `event: metrics\ndata: ${JSON.stringify({ ...m, history: metricHistory })}\n\n`;
  for (const res of subscribers) { try { res.write(data); } catch { /* dropped */ } }
}, 2000).unref?.();

export function addSubscriber(res) {
  subscribers.add(res);
  // backfill the last ~40 events so a new tab isn't blank
  res.write(`event: hello\ndata: ${JSON.stringify({ topology: TOPOLOGY, stats: Object.fromEntries(stats), stateCounts: stateCounts(), metrics: metrics(), lastKnown: lastKnownSeed() })}\n\n`);
  for (const ev of recentGlobal.slice(0, 40).reverse()) res.write(`data: ${JSON.stringify(ev)}\n\n`);
  res.on('close', () => subscribers.delete(res));
}
export const recentForNode = (id) => recentByNode.get(id) || [];
export const liveStats = () => ({ stats: Object.fromEntries(stats), subscribers: subscribers.size, last: lastEvent });

export function summarize(topic, d) {
  // a compact, human-friendly one-liner + the parsed payload for peek
  if (topic === 'nba.evaluations') { const cas = d.channelActions || []; return { entity: d.nbaId, label: `${cas.filter((c) => c.eligible).length} eligible`, source: 'rules-engine' }; }
  if (topic === 'nba.activations') return { entity: d.entityId || d.nbaId, label: `${d.op} ${d.name || (Array.isArray(d.actions) ? 'batch x' + d.actions.length : '')}`.trim(), source: d.source || 'state-machine' };
  if (topic === 'nba.snapshots') return { entity: d.entityId || d.nbaId, label: `snapshot · ${Object.keys(d.facts || {}).length} facts`, source: 'snapshot-builder' };
  if (topic === 'nba.definitions') return { entity: d.id, label: d.name || 'definition', source: 'action-library' };
  // a router DECISION rides member.facts now (op + no fact key) — summarize it like an activation, not a fact
  if (d.op) return { entity: d.entityId || d.nbaId, label: `${d.op} ${d.name || (Array.isArray(d.actions) ? 'batch x' + d.actions.length : '')}`.trim(), source: d.source || 'action-router' };
  return { entity: d.entityId, label: `${d.key} = ${typeof d.value === 'object' ? '{…}' : d.value}`, source: d.source };
}

let started = false;
export async function startConsumer(log = console.log) {
  if (started) return; started = true;
  const kafka = new Kafka({ clientId: 'cc-bff', brokers: [BROKER], logLevel: logLevel.ERROR, retry: { retries: 20 } });
  // a fresh group each boot + fromBeginning:false -> we tail the LIVE edge, never replay the backlog
  const consumer = kafka.consumer({ groupId: `${GROUP}-${Date.now()}`, allowAutoTopicCreation: false });
  try {
    await consumer.connect();
    await consumer.subscribe({ topics: TOPICS, fromBeginning: false });
    log('[stream] tailing ' + TOPICS.length + ' NBA topics via native Kafka ' + BROKER + ' (live edge)');
    // eachBatch (not eachMessage): the fact channels are busy, so we drain a whole partition batch per
    // call instead of one message at a time — heartbeat + resolveOffset keep the consumer healthy under load.
    await consumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        const { topic, partition } = batch;
        for (const message of batch.messages) {
          if (!isRunning() || isStale()) break;
          try {
            const d = JSON.parse(message.value.toString('utf8'));
            if (d && typeof d === 'object') {
              const kind = message.headers && message.headers.kind ? message.headers.kind.toString() : null;
              const s = summarize(topic, d);
              const emitter = emitterFor(topic, s.source || d.source, kind);
              const sm = smEnrich(topic, d);   // state-machine category + parsed transition (updates STATE_OF)
              broadcast({
                id: `${topic}:${partition}:${message.offset}`, topic, emitter,
                entity: s.entity, label: s.label, source: s.source || d.source || null,
                ts: Number(d.eventTs || d.evaluatedAt || d.updatedTs || Date.now()),   // business time (display only — NOT used for latency)
                emitTs: Number(message.timestamp) || Date.now(),                       // broker append time (LogAppendTime) = shared clock, used for latency
                key: d.key || null, op: d.op || null, value: d.value !== undefined ? d.value : null,
                cat: sm.cat, st: sm.st,           // 'state'|'disposition'|'router'|'completion' (+ parsed transition)
                payload: d,
              });
            }
          } catch { /* skip unparseable */ }
          resolveOffset(message.offset);
        }
        await heartbeat();
      },
    });
  } catch (e) { log('[stream] kafka consumer error: ' + e.message); started = false; }
}
