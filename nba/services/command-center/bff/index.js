// NBA Command Center BFF — GraphQL over (a) the Action Library REST API for authoring, and
// (b) the Databricks datalake for analytics + the rule funnel. Analytics queries run against the
// gold/silver lake ("can be slower"), never the hot path. See databricks.js + rulesql.js.
import { ApolloServer } from '@apollo/server';
import { expressMiddleware } from '@apollo/server/express4';
import express from 'express';
import cors from 'cors';
import { GraphQLJSON } from 'graphql-scalars';
import { sql, NS, lakeConfigured, num, backend } from './lake.js';
import { buildFunnel } from './rulesql.js';
import { startConsumer, addSubscriber, recentForNode, liveStats, TOPOLOGY, stateCounts, CANONICAL_STATES } from './eventstream.js';
import { dlqStats, replayDlq, flushDlq, startDlqTracker } from './dlq.js';
import { explainEligibility } from './explain.js';
import * as library from './library.js';
const esc = (s) => String(s).replace(/'/g, "''");

const API = (process.env.NBA_ACTIONLIB_URL || 'http://nba-action-library:7001').replace(/\/$/, '');


// AUTHORING lives HERE now (the command center owns the library — definitions/taxonomy/suppress/config in
// Postgres + the outbox; see library.js). api() stays the single funnel the resolvers call: authoring paths
// dispatch to the local library; everything else (runtime reads: /next-action, /snapshot, ...) still proxies
// to the ACTION API, which is now a pure serve/disposition surface.
async function api(path, opts = {}) {
  const local = await localLibrary(path, opts);
  if (local !== undefined) return local;
  const r = await fetch(API + path, { headers: { 'Content-Type': 'application/json' }, ...opts });
  if (r.status === 404) return null;
  const t = await r.text();
  if (!r.ok) throw new Error(t ? t.slice(0, 200) : `actionlib ${path}: ${r.status}`);   // surface the body (e.g. "group not empty")
  return t ? JSON.parse(t) : null;
}

// Local dispatch for the authoring surface (method + path -> library call). Returns undefined for
// non-authoring paths (falls through to the action-API proxy above). 404 -> null, matching api()'s contract.
const DEF_TABLES = { 'actions': ['action', 'ACTION'], 'global-rules': ['global_rule', 'GLOBAL_RULE'],
                     'channel-rules': ['channel_rule', 'CHANNEL_RULE'], 'milestones': ['milestone', 'MILESTONE'] };
async function localLibrary(path, opts = {}) {
  const method = (opts.method || 'GET').toUpperCase();
  const body = opts.body ? JSON.parse(opts.body) : {};
  const p = decodeURIComponent(path.split('?')[0]).replace(/\/$/, '');
  const seg = p.split('/').filter(Boolean);
  const err404 = () => { const e = new Error('not_found'); e.status = 404; return e; };
  try {
    // /actions/{id}/group | /actions/{id}/experience (assignments re-emit the def via the outbox)
    if (seg[0] === 'actions' && seg.length === 3 && method === 'POST') {
      if (seg[2] === 'group') return await library.assignField(seg[1], 'groupId', body.groupId || null, 'action_group');
      if (seg[2] === 'experience') return await library.assignField(seg[1], 'experienceId', body.experienceId || null, 'experience');
    }
    if (DEF_TABLES[seg[0]]) {
      const [table, aggType] = DEF_TABLES[seg[0]];
      if (seg.length === 1 && method === 'GET') return await library.listDefs(table);
      if (seg.length === 1 && method === 'POST') return await library.upsertDef(table, aggType, body, null);
      if (seg.length === 2 && method === 'PUT') return await library.upsertDef(table, aggType, body, seg[1]);
      if (seg.length === 2 && method === 'GET') return await library.getDef(table, seg[1]);   // null on missing == api()'s 404
      if (seg.length === 2 && method === 'DELETE') return await library.deleteDef(table, aggType, seg[1]);
    }
    if (p === '/facts' && method === 'GET') return await library.facts();
    if (p === '/suppress' && method === 'POST') return await library.suppress(body.actionId, body.channel || '', body.suppressed !== false);
    if (p === '/suppressed' && method === 'GET') return await library.suppressed();
    if (p === '/channel-config' && method === 'GET') return await library.channelConfig();
    if (p === '/channel-config' && method === 'POST') return await library.setChannelConfig(body.channel, body.maxBatch);
    if (p === '/groups' && method === 'GET') return await library.listGroups();
    if (p === '/groups' && method === 'POST') return await library.createGroup(body.name, body.parentId);
    if (seg[0] === 'groups' && seg.length === 2 && method === 'DELETE') return await library.deleteGroup(seg[1]);
    if (p === '/experiences' && method === 'GET') return await library.listExperiences();
    if (p === '/experiences' && method === 'POST') return await library.createExperience(body.name, body.description);
    if (seg[0] === 'experiences' && seg.length === 2 && method === 'DELETE') return await library.deleteExperience(seg[1]);
  } catch (e) {
    if (e.status === 404) return null;
    throw e;
  }
  return undefined;   // not an authoring path -> proxy to the action API
}

const typeDefs = `#graphql
  scalar JSON
  type Channel { channel: String!, contentKey: String, variants: JSON, softCompletion: String }
  # completion = the hard-completion GOAL (condition tree over member facts); hardTtlSeconds = how long we
  # wait for it (attribution window); autoExcludeOnCompletion (default true) = auto-retire once completed.
  # ttlSeconds is the per-channel soft re-send cooldown. softCompletion (per channel) overrides the funnel bar.
  type Action { id: ID!, name: String!, ttlSeconds: Int, channels: [Channel!]!, inclusion: JSON, exclusion: JSON, completion: JSON, hardTtlSeconds: Int, autoExcludeOnCompletion: Boolean, factsUsed: [String!]!, groupId: ID, experienceId: ID }
  type Rule { id: ID!, name: String, channel: String, logic: JSON, factsUsed: [String!]! }
  # Action GROUPS — a taxonomy tree (parentId = NULL at the root). Actions carry a groupId.
  type ActionGroup { id: ID!, name: String!, parentId: ID }
  # EXPERIENCES — a second, business-facing taxonomy (enrollment, onboarding…). Actions carry an experienceId.
  type Experience { id: ID!, name: String!, description: String }
  # MILESTONE — a def (name + structured logic) the rules engine evaluates; completion latches permanently.
  type Milestone { id: ID!, name: String, logic: JSON, factsUsed: [String!]! }
  # A milestone a member has COMPLETED (permanent), with the criteria (logic) for the viewer's hover.
  type MemberMilestone { id: ID!, name: String, completedAt: Float, logic: JSON }

  type Stage { label: String!, count: Int! }
  type ActionPerf { id: ID!, name: String, eligible: Int!, activated: Int!, suppressed: Int!, sent: Int!, avgScore: Float }
  type Bucket { bucket: String!, n: Int! }
  type FeedItem { ts: Float, kind: String, entity: String, label: String, detail: String, source: String }
  type FactEdge { id: ID!, name: String, fact: String! }
  type LakeStatus { configured: Boolean!, tables: JSON, members: Int }
  # Channel-level throttle: today's sends vs the daily ceiling + the rolling-window RATE (token bucket).
  type ChannelThrottle { channel: String!, sent: Int!, cap: Int, utilization: Float, throttled: Boolean!,
    rate: Int!, rateCap: Int, windowSeconds: Int!, rateThrottled: Boolean!, series: [Stage!]! }
  # Read-only validation: invariant checks run against the LIVE system (no writes) -> "operational".
  type Check { name: String!, ok: Boolean!, group: String!, detail: String, validates: String }
  type SystemHealth { operational: Boolean!, passed: Int!, total: Int!, ts: Float!, checks: [Check!]! }
  # Per-layer throughput vs the time-of-day baseline (avg of this hour over the last 7 days).
  type LayerHealth { layer: String!, source: String!, lastEventAgeSec: Int, lastHour: Int!, prevHour: Int!, baseline: Float, status: String! }
  # Disposition FUNNEL per channel: outbound (Sent -> Delivered -> Read -> LinkClicked ...) and
  # inbound/pull (Presented -> Accepted -> Completed). Stages are ordered; n is the count at each.
  type FunnelStage { status: String!, n: Int! }
  type ChannelDispositions { channel: String!, kind: String!, stages: [FunnelStage!]! }
  # Content-VARIANT performance (A/B): per action-channel, each content key's send + engagement funnel,
  # so you can see which variant converts best. isBase = the channel's default key; winner = top conversion.
  type VariantPerf { contentKey: String!, isBase: Boolean!, sent: Int!, stages: [FunnelStage!]!, deepest: String, conversion: Float!, winner: Boolean! }
  type ActionVariants { actionId: ID!, name: String!, channel: String!, base: String, variants: [VariantPerf!]! }
  # Fact library — every member fact known in gold, for the rule builders' fact autocomplete.
  type FactDef { key: String!, valueType: String, distinctValues: Int, count: Int, samples: [String!]! }
  # Dead-letter queue per consistency consumer: poison records that failed processing, replayable to source.
  type DlqStat { consumer: String!, topic: String!, depth: Int!, lastError: String, lastTs: Float, sourceTopic: String }
  # Member snapshot — the current value of EVERY fact for a member (gold), plus their NBA history
  # (activations, workflow-state transitions, dispositions) grouped per action-channel.
  type MemberFact { key: String!, value: String, valueType: String, eventTs: Float, source: String }
  type MemberActionEvent { ts: Float, kind: String!, op: String, value: String, score: Float, contentKey: String, correlationId: String, source: String }
  type MemberAction { actionId: ID!, name: String, channel: String, lastTs: Float, events: [MemberActionEvent!]! }
  type MemberSnapshot { found: Boolean!, entityId: String, nbaId: String, entityType: String, updatedTs: Float, facts: [MemberFact!]!, actions: [MemberAction!]!, milestones: [MemberMilestone!]! }

  type Query {
    actions: [Action!]!
    action(id: ID!): Action
    globalRules: [Rule!]!
    channelRules: [Rule!]!
    milestones: [Milestone!]!
    actionGroups: [ActionGroup!]!
    experiences: [Experience!]!
    # analytics (Databricks lake)
    lakeStatus: LakeStatus!
    funnel: [Stage!]!
    actionPerformance: [ActionPerf!]!
    throttleStats: [ChannelThrottle!]!
    systemChecks: SystemHealth!
    layerHealth: [LayerHealth!]!
    dispositionFunnels: [ChannelDispositions!]!
    variantPerformance: [ActionVariants!]!
    factLibrary: [FactDef!]!
    suppressedActions: [String!]!
    channelConfig: JSON
    dispositions: [Bucket!]!
    scoreDistribution: [Bucket!]!
    # the ML model card (champion/challenger versions + AUC, per-archetype model_p vs f*, learning curve,
    # adaptation status) — gold_model_card, fed by the ML job via Kafka nba.model.card through the lake medallion.
    modelCard: JSON
    liveFeed(limit: Int): [FeedItem!]!
    actionFactMap: [FactEdge!]!
    ruleFunnel(input: JSON!): [Stage!]!
    # trace audit — follow one decision (correlationId) or a member through the system
    recentTraces(entity: String!, limit: Int): [TraceSummary!]!
    # a few recent members that have a decision to replay — seeds the trace picker's autocomplete
    sampleMembers(limit: Int): [String!]!
    trace(correlationId: ID!): JSON
    # member snapshot — every current fact (gold) + the member's full NBA history
    memberSnapshot(memberId: String!): MemberSnapshot
    # dead-letter queues — per consistency consumer
    dlqStats: [DlqStat!]!
    # live state-machine rollup: { state: count } across all active (member,action,channel) workflows
    stateCounts: JSON!
    stateModel: [String!]!
  }
  type TraceSummary { correlationId: ID!, entityId: String, nbaId: String, ts: Float, winner: String, suppressedCount: Int }

  input ChannelInput { channel: String!, contentKey: String, variants: JSON, softCompletion: String }
  input ActionInput { id: ID, name: String!, ttlSeconds: Int, channels: [ChannelInput!]!, inclusion: JSON, exclusion: JSON, completion: JSON, hardTtlSeconds: Int, autoExcludeOnCompletion: Boolean, groupId: ID }
  input RuleInput { id: ID, name: String, channel: String, logic: JSON }
  type Mutation {
    upsertAction(input: ActionInput!): Action!
    deleteAction(id: ID!): Boolean!
    upsertGlobalRule(input: RuleInput!): Rule!
    deleteGlobalRule(id: ID!): Boolean!
    upsertChannelRule(input: RuleInput!): Rule!
    deleteChannelRule(id: ID!): Boolean!
    # operator suppress: pull an action (or one action-channel) out of rotation, or restore it
    suppressAction(actionId: ID!, channel: String, suppressed: Boolean!): JSON
    # max actions batched into one send per channel (the router collects up to this many on the winning channel)
    setChannelMaxBatch(channel: String!, maxBatch: Int!): JSON
    # action GROUPS (taxonomy): create a group (optionally under a parent), delete an EMPTY group, assign an action
    createActionGroup(name: String!, parentId: ID): ActionGroup!
    deleteActionGroup(id: ID!): Boolean!
    assignActionGroup(actionId: ID!, groupId: ID): JSON
    # EXPERIENCES (business-journey taxonomy)
    createExperience(name: String!, description: String): Experience!
    deleteExperience(id: ID!): Boolean!
    assignExperience(actionId: ID!, experienceId: ID): JSON
    # MILESTONES (rule-evaluated, permanently latched on completion)
    upsertMilestone(input: RuleInput!): Milestone!
    deleteMilestone(id: ID!): Boolean!
    # DLQ ops: replay re-produces the poison records to their source topic (then truncates); flush discards them
    replayDlq(consumer: String!): JSON
    flushDlq(consumer: String!): JSON
  }
`;

const needLake = () => { if (!lakeConfigured) throw new Error('Databricks lake not configured (DATABRICKS_HOST/CLIENT_ID/CLIENT_SECRET)'); };
const J = (s) => { try { return s ? JSON.parse(s) : null; } catch { return null; } };

// Read definitions from the lake (dim_definitions) — fallback when the Action Library REST API is
// not reachable (e.g. BFF running outside the container network). Also keeps the rule funnel
// self-contained against gold.
async function lakeDefs(defType) {
  const rows = await sql(`SELECT id, name, channel, ttlSeconds, channelsJson, inclusionJson, exclusionJson, logicJson, factsUsedJson FROM ${NS}.dim_definitions WHERE defType='${defType}' ORDER BY name`, 10000);
  return rows;
}
async function apiOrLake(path, defType, shape) {
  try { const r = await api(path); if (r) return r; } catch { /* fall through to lake */ }
  if (!lakeConfigured) return [];
  return (await lakeDefs(defType)).map(shape);
}
const shapeAction = (r) => ({ id: r.id, name: r.name, ttlSeconds: r.ttlSeconds ? Number(r.ttlSeconds) : null, channels: J(r.channelsJson) || [], inclusion: J(r.inclusionJson), exclusion: J(r.exclusionJson), completion: J(r.completionJson) || null, hardTtlSeconds: r.hardTtlSeconds ? Number(r.hardTtlSeconds) : null, autoExcludeOnCompletion: r.autoExcludeOnCompletion == null ? null : !!r.autoExcludeOnCompletion, factsUsed: J(r.factsUsedJson) || [], groupId: r.groupId || null, experienceId: r.experienceId || null });
const shapeRule = (r) => ({ id: r.id, name: r.name, channel: r.channel, logic: J(r.logicJson), factsUsed: J(r.factsUsedJson) || [] });

// The cap a channel rule sets for a metric: nba.throttle.{ch}.{metric} < N -> N (<= N -> N+1).
// Mirrors the rules-engine DRL + the Temporal ThrottleGate so the UI shows the SAME caps enforced.
// metric = 'daily' (the hard ceiling, eligibility) or 'rate' (the gate's per-window trickle cap).
function throttleLimit(logic, metric = 'daily') {
  const conds = logic?.conditions;
  if (!Array.isArray(conds)) return null;
  for (const c of conds) {
    const f = c.fact || '';
    if (f.startsWith('nba.throttle.') && f.endsWith('.' + metric)) {
      if (c.cmp === 'lt') return Number(c.value);
      if (c.cmp === 'lte') return Number(c.value) + 1;
    }
  }
  return null;
}

const resolvers = {
  JSON: GraphQLJSON,
  Query: {
    actions: () => apiOrLake('/actions', 'ACTION', shapeAction),
    action: (_p, { id }) => api('/actions/' + encodeURIComponent(id)),
    globalRules: () => apiOrLake('/global-rules', 'GLOBAL_RULE', shapeRule),
    channelRules: () => apiOrLake('/channel-rules', 'CHANNEL_RULE', shapeRule),
    actionGroups: async () => { try { return (await api('/groups')) || []; } catch { return []; } },
    experiences: async () => { try { return (await api('/experiences')) || []; } catch { return []; } },
    milestones: () => apiOrLake('/milestones', 'MILESTONE', shapeRule),
    dlqStats: () => dlqStats(),
    stateCounts: () => stateCounts(),
    stateModel: () => CANONICAL_STATES,

    // the ML model card — latest gold_model_card row (one JSON blob). Page-load analytics, gold→BFF like the rest.
    modelCard: async () => {
      if (!lakeConfigured) return null;
      try {
        const r = await sql(`SELECT card FROM ${NS}.gold_model_card ORDER BY ts DESC LIMIT 1`, 10000);
        return r[0]?.card ? JSON.parse(r[0].card) : null;
      } catch { return null; }
    },

    lakeStatus: async () => {
      if (!lakeConfigured) return { configured: false, tables: {}, members: 0 };
      const tbls = ['silver_fact_history', 'silver_eval_eligible', 'silver_activations', 'silver_snapshots', 'dim_definitions', 'gold_member_snapshot'];
      const counts = {};
      await Promise.all(tbls.map(async (t) => {
        const r = await sql(`SELECT count(*) c FROM ${NS}.${t}`, 8000); counts[t] = num(r[0]?.c);
      }));
      const m = await sql(`SELECT count(distinct entityId) c FROM ${NS}.gold_member_snapshot`, 8000);
      return { configured: true, tables: counts, members: num(m[0]?.c) };
    },

    funnel: async () => {
      needLake();
      // PRE-AGGREGATED gold product: the expensive count(distinct) over the 12M-row silver tables runs ONCE on a
      // schedule into gold_system_stats (a one-row table), NOT on every page load. The BFF just reads that table (ms).
      // Fall back to the live on-the-fly counts only if the gold product hasn't been materialized yet.
      let rows = await sql(`SELECT members, eligible, scored, activated, sent FROM ${NS}.gold_system_stats LIMIT 1`, 8000).catch(() => []);
      if (!rows || !rows.length) rows = await sql(`
        SELECT
          (SELECT count(distinct entityId) FROM ${NS}.gold_member_snapshot) AS members,
          (SELECT count(distinct nbaId) FROM ${NS}.silver_eval_eligible) AS eligible,
          (SELECT count(distinct entityId) FROM ${NS}.gold_member_snapshot WHERE key LIKE 'nba.score.%') AS scored,
          (SELECT count(distinct nbaId) FROM ${NS}.silver_activations WHERE op='CREATE') AS activated,
          (SELECT count(distinct entityId) FROM ${NS}.gold_member_snapshot WHERE key LIKE 'nba.disposition.%' AND value='sent') AS sent
      `);
      const r = rows[0] || {};
      return [
        { label: 'Members', count: num(r.members) },
        { label: 'Eligible', count: num(r.eligible) },
        { label: 'Scored', count: num(r.scored) },
        { label: 'Activated', count: num(r.activated) },
        { label: 'Sent', count: num(r.sent) },
      ];
    },

    actionPerformance: async () => {
      needLake();
      const rows = await sql(`
        WITH el AS (SELECT actionId, count(distinct nbaId) eligible FROM ${NS}.silver_eval_eligible GROUP BY actionId),
             ac AS (SELECT actionId,
                      count(distinct CASE WHEN op='CREATE' THEN nbaId END) activated,
                      count(distinct CASE WHEN op='SUPPRESS' THEN nbaId END) suppressed,
                      round(avg(score),3) avgScore FROM ${NS}.silver_activations GROUP BY actionId),
             se AS (SELECT actionId, count(distinct entityId) sent FROM ${NS}.silver_fact_history
                    WHERE factClass='disposition' AND value='sent' GROUP BY actionId)
        SELECT d.id, d.name,
               coalesce(el.eligible,0) eligible, coalesce(ac.activated,0) activated,
               coalesce(ac.suppressed,0) suppressed, coalesce(se.sent,0) sent, ac.avgScore
        FROM ${NS}.dim_definitions d
        LEFT JOIN el ON el.actionId=d.id LEFT JOIN ac ON ac.actionId=d.id LEFT JOIN se ON se.actionId=d.id
        WHERE d.defType='ACTION' ORDER BY eligible DESC`);
      return rows.map((r) => ({ id: r.id, name: r.name, eligible: num(r.eligible), activated: num(r.activated), suppressed: num(r.suppressed), sent: num(r.sent), avgScore: r.avgScore == null ? null : Number(r.avgScore) }));
    },

    suppressedActions: async () => { try { return (await api('/suppressed')) || []; } catch { return []; } },
    channelConfig: async () => { try { return (await api('/channel-config')) || {}; } catch { return {}; } },

    // Fact library: every MEMBER fact present in gold (internal nba.* pipeline facts excluded), with its
    // type, cardinality and a few sample values — feeds the rule builders' fact autocomplete everywhere.
    factLibrary: async () => {
      needLake();
      const rows = await sql(`
        SELECT key, max(valueType) AS valueType, count(DISTINCT value) AS distinctValues, count(*) AS n,
               slice(array_sort(collect_set(value)), 1, 8) AS samples
        FROM ${NS}.gold_member_snapshot
        WHERE key NOT LIKE 'nba.%'
        GROUP BY key ORDER BY key`, 30000);
      return rows.map((r) => ({ key: r.key, valueType: r.valueType, distinctValues: num(r.distinctValues),
        count: num(r.n), samples: J(r.samples) || [] }));
    },

    dispositions: async () => {
      needLake();
      const rows = await sql(`SELECT value bucket, count(*) n FROM ${NS}.gold_member_snapshot WHERE key LIKE 'nba.disposition.%' GROUP BY value ORDER BY n DESC`);
      return rows.map((r) => ({ bucket: r.bucket, n: num(r.n) }));
    },

    // Per-channel disposition funnel — mirrors the action-library channel funnels so each channel's
    // delivery/engagement (or inbound Presented->Accepted->Completed) is visible end to end.
    dispositionFunnels: async () => {
      needLake();
      const OUT = { email: ['Delivered', 'Read', 'LinkClicked'], sms: ['Delivered', 'LinkClicked'], push: ['Delivered', 'Opened'], voice: ['Answered', 'Completed'], mail: ['Delivered'] };
      const INBOUND = ['Presented', 'Accepted', 'Completed'];
      const rows = await sql(`
        SELECT channel, value status, count(distinct entityId, actionId, channel, eventTs) n
        FROM ${NS}.silver_fact_history
        WHERE factClass='disposition' AND channel IS NOT NULL GROUP BY channel, value`, 4000);
      const by = {}; rows.forEach((r) => { (by[r.channel] ||= {})[r.status] = num(r.n); });
      return Object.keys(by).sort().map((ch) => {
        const c = by[ch], outbound = !!OUT[ch];
        const order = OUT[ch] || INBOUND;
        const stages = [];
        if (c['sent']) stages.push({ status: 'Sent', n: c['sent'] });        // the outbound send itself
        order.forEach((s) => { if (c[s] != null || outbound) stages.push({ status: s, n: c[s] || 0 }); });
        Object.keys(c).forEach((s) => { if (s !== 'sent' && !order.includes(s)) stages.push({ status: s, n: c[s] }); });
        return { channel: ch, kind: outbound ? 'outbound' : 'inbound', stages };
      });
    },

    // Content-VARIANT A/B performance: per action-channel, each content key's send + engagement funnel.
    // contentKey rides every disposition (the variant the member actually got), so we can attribute
    // delivery/engagement back to the variant and crown a winner (best conversion to the deepest stage).
    variantPerformance: async () => {
      needLake();
      const OUT = { email: ['Delivered', 'Read', 'LinkClicked'], sms: ['Delivered', 'LinkClicked'], push: ['Delivered', 'Opened'], voice: ['Answered', 'Completed'], mail: ['Delivered'] };
      const INBOUND = ['Presented', 'Accepted', 'Completed'];
      const rows = await sql(`
        SELECT actionId, channel, contentKey, value status, count(*) n
        FROM ${NS}.silver_fact_history
        WHERE factClass='disposition' AND contentKey IS NOT NULL AND contentKey <> '' AND channel IS NOT NULL
        GROUP BY actionId, channel, contentKey, value`, 8000);
      const groups = {};   // "actionId|channel" -> { contentKey -> { status -> n } }
      rows.forEach((r) => { const g = (groups[r.actionId + '|' + r.channel] ||= {}); (g[r.contentKey] ||= {})[r.status] = num(r.n); });
      // action defs give the human name + which key is the base vs a declared variant
      const actions = await apiOrLake('/actions', 'ACTION', shapeAction).catch(() => []);
      const def = {};
      actions.forEach((a) => (a.channels || []).forEach((c) => { def[a.id + '|' + c.channel] = { name: a.name, base: c.contentKey || '' }; }));
      const out = [];
      for (const k of Object.keys(groups)) {
        const [actionId, channel] = k.split('|');
        const d = def[k] || { name: actionId, base: '' };
        const keys = Object.keys(groups[k]);
        if (keys.length <= 1) continue;                       // not an A/B — single content key, skip
        const order = OUT[channel] || INBOUND;
        const variants = keys.map((ck) => {
          const c = groups[k][ck], sent = c['sent'] || 0;
          const stages = [];
          if (sent) stages.push({ status: 'Sent', n: sent });
          order.forEach((s) => { if (c[s] != null) stages.push({ status: s, n: c[s] }); });
          let deepest = null, deepN = 0;
          order.forEach((s) => { if (c[s]) { deepest = s; deepN = c[s]; } });
          return { contentKey: ck, isBase: ck === d.base, sent, stages, deepest, conversion: sent ? deepN / sent : 0, winner: false };
        }).sort((a, b) => b.conversion - a.conversion || b.sent - a.sent);
        if (variants.length && (variants[0].conversion > 0 || variants[0].sent > 0)) variants[0].winner = true;
        out.push({ actionId, name: d.name, channel, base: d.base, variants });
      }
      return out.sort((a, b) => a.name.localeCompare(b.name));
    },

    // Channel-level throttle: today's send LEVEL (net IN_PROCESS sends minus FAILED/SUPPRESSED — the SAME
    // count the lake's emit_throttle broadcasts to the engine on nba.definitions) vs the Studio cap, plus
    // the hourly curve so you can watch a channel fill up.
    throttleStats: async () => {
      needLake();
      const WINDOW = Number(process.env.NBA_THROTTLE_WINDOW_SECONDS || 300);   // rate window (matches the lake default)
      const channelRules = await apiOrLake('/channel-rules', 'CHANNEL_RULE', shapeRule);
      const caps = {}, rateCaps = {};
      for (const r of channelRules) {
        const d = throttleLimit(r.logic, 'daily'); if (d != null) caps[r.channel] = d;
        const rt = throttleLimit(r.logic, 'rate'); if (rt != null) rateCaps[r.channel] = rt;
      }
      // A SEND = the IN_PROCESS actionstate the workflow emits on DISPATCH; FAILED/SUPPRESSED give the token
      // back. Matches emit_throttle EXACTLY — NOT a downstream disposition='sent' (which is never emitted, so
      // the old query always returned 0). Dedup the actionstate rows, then net the counts per channel.
      const netSends = (extra) => `
        SELECT channel, greatest(0, count_if(value='IN_PROCESS') - count_if(value IN ('FAILED','SUPPRESSED'))) sent
        FROM (SELECT DISTINCT entityId, actionId, channel, eventTs, value FROM ${NS}.silver_fact_history
              WHERE factClass='actionstate' AND value IN ('IN_PROCESS','FAILED','SUPPRESSED') AND channel IS NOT NULL ${extra})
        GROUP BY channel`;
      const sent = await sql(netSends('AND to_date(from_unixtime(eventTs/1000)) = current_date()'), 4000);
      const rate = await sql(netSends(`AND eventTs >= (unix_timestamp()-${WINDOW})*1000`), 4000);
      const hourly = await sql(`
        SELECT channel, hour(from_unixtime(eventTs/1000)) hr, count(*) n
        FROM ${NS}.silver_fact_history
        WHERE factClass='actionstate' AND value='IN_PROCESS' AND channel IS NOT NULL
          AND to_date(from_unixtime(eventTs/1000)) = current_date() GROUP BY channel, hr`, 4000);
      const sentBy = {}; sent.forEach((r) => { sentBy[r.channel] = num(r.sent); });
      const rateBy = {}; rate.forEach((r) => { rateBy[r.channel] = num(r.sent); });
      const series = {};
      hourly.forEach((r) => { (series[r.channel] ||= {})[num(r.hr)] = num(r.n); });
      const chans = [...new Set([...Object.keys(caps), ...Object.keys(rateCaps), ...Object.keys(sentBy), 'email', 'sms', 'push', 'mail', 'voice'])];
      return chans.map((ch) => {
        const s = sentBy[ch] || 0, cap = caps[ch] ?? null, rt = rateBy[ch] || 0, rCap = rateCaps[ch] ?? null;
        const ser = series[ch] || {};
        return {
          channel: ch, sent: s, cap,
          utilization: cap ? Math.round((s / cap) * 1000) / 1000 : null,
          throttled: cap != null && s >= cap,
          rate: rt, rateCap: rCap, windowSeconds: WINDOW, rateThrottled: rCap != null && rt >= rCap,
          series: Array.from({ length: 24 }, (_, h) => ({ label: String(h), count: ser[h] || 0 })),
        };
      }).sort((a, b) => ((b.cap != null || b.rateCap != null) - (a.cap != null || a.rateCap != null)) || b.sent - a.sent);
    },

    // Read-only validation — invariant checks against the LIVE system (never writes). The dashboard
    // shows "what we validate + the result", so a glance says whether the platform is operational.
    systemChecks: async () => {
      needLake();
      const checks = [];
      const add = (name, group, ok, detail, validates) => checks.push({ name, group, ok: !!ok, detail: detail || null, validates: validates || null });
      const q1 = async (s) => { try { return (await sql(s, 9000))[0] || {}; } catch { return null; } };

      const m = await q1(`SELECT count(distinct entityId) c FROM ${NS}.gold_member_snapshot`);
      add('Lake reachable · members present', 'Lake', m && num(m.c) > 0, m ? `${num(m.c)} members in gold` : 'lake query failed', 'all analytics + reporting read from the governed gold lake');

      const cr = await apiOrLake('/channel-rules', 'CHANNEL_RULE', shapeRule).catch(() => []);
      const capped = cr.filter((r) => throttleLimit(r.logic) != null);
      add('Throttle caps authored', 'Throttle', capped.length > 0, capped.length ? `${capped.length} capped: ${capped.map((r) => `${r.channel}<${throttleLimit(r.logic)}`).join(', ')}` : 'no channel cap rules', 'a Studio channel rule sets each channel daily cap');

      for (const [label, tbl, ts] of [['Eligibility', 'silver_eval_eligible', 'evaluatedAt'], ['Activations', 'silver_activations', 'eventTs'], ['Snapshots', 'silver_snapshots', 'updatedTs']]) {
        const r = await q1(`SELECT max(${ts}) t FROM ${NS}.${tbl}`);
        const ageMin = r && num(r.t) ? Math.round((Date.now() - num(r.t)) / 60000) : null;
        add(`${label} producing`, 'Pipeline', ageMin != null, ageMin != null ? `last event ${ageMin}m ago` : 'no events found', `the ${label.toLowerCase()} layer is emitting`);
      }

      // Orphans: activated (member,action,channel) tuples that were NEVER eligible. Compares DISTINCT
      // tuples (an action re-fires after TTL, so raw activation counts exceed eligibility — distinct is
      // the right grain). Should be 0.
      const cons = await q1(`
        SELECT count(*) orphans FROM (SELECT DISTINCT nbaId, actionId, channel FROM ${NS}.silver_activations WHERE op='CREATE') a
        WHERE NOT EXISTS (SELECT 1 FROM ${NS}.silver_eval_eligible e
          WHERE e.nbaId=a.nbaId AND e.actionId=a.actionId AND e.channel=a.channel)`);
      add('Activations within eligibility', 'Consistency', cons && num(cons.orphans) === 0, cons ? (num(cons.orphans) === 0 ? 'every activation traces to an eligibility' : `${num(cons.orphans)} orphan activation tuple(s)`) : 'query failed', 'no action is ever activated that was not first eligible');

      const d = await q1(`SELECT count(*) c FROM ${NS}.silver_fact_history WHERE factClass='disposition' AND value='sent'`);
      add('Sends captured for the throttle count', 'Throttle', d != null && num(d.c) >= 0, d ? `${num(d.c)} sent dispositions in the lake` : 'query failed', 'every send is recorded so the channel throttle counts it (NBA + external)');

      const passed = checks.filter((c) => c.ok).length;
      return { operational: passed === checks.length, passed, total: checks.length, ts: Date.now(), checks };
    },

    // Per-layer throughput vs the time-of-day baseline — surfaces a layer that's gone quiet or is
    // running outside its normal range. Baseline = avg count for THIS hour-of-day over the last 7 days.
    layerHealth: async () => {
      needLake();
      const LAYERS = [
        { layer: 'Facts (firehose)', tbl: 'silver_fact_history', ts: 'eventTs', filt: '' },
        { layer: 'Snapshots', tbl: 'silver_snapshots', ts: 'updatedTs', filt: '' },
        { layer: 'Eligibility', tbl: 'silver_eval_eligible', ts: 'evaluatedAt', filt: '' },
        { layer: 'Activations', tbl: 'silver_activations', ts: 'eventTs', filt: '' },
        { layer: 'Dispositions', tbl: 'silver_fact_history', ts: 'eventTs', filt: "AND factClass='disposition'" },
      ];
      return Promise.all(LAYERS.map(async (L) => {
        try {
          const r = (await sql(`
            SELECT
              (SELECT count(*) FROM ${NS}.${L.tbl} WHERE ${L.ts} >= (unix_timestamp()-3600)*1000 ${L.filt}) lastHour,
              (SELECT count(*) FROM ${NS}.${L.tbl} WHERE ${L.ts} >= (unix_timestamp()-7200)*1000 AND ${L.ts} < (unix_timestamp()-3600)*1000 ${L.filt}) prevHour,
              (SELECT max(${L.ts}) FROM ${NS}.${L.tbl} WHERE 1=1 ${L.filt}) lastTs,
              (SELECT count(*)/7.0 FROM ${NS}.${L.tbl}
                 WHERE hour(from_unixtime(${L.ts}/1000)) = hour(from_unixtime(unix_timestamp()))
                   AND to_date(from_unixtime(${L.ts}/1000)) BETWEEN date_sub(current_date(),7) AND date_sub(current_date(),1) ${L.filt}) baseline
          `, 9000))[0] || {};
          const lastHour = num(r.lastHour), prevHour = num(r.prevHour);
          const baseline = r.baseline != null ? Number(r.baseline) : 0;
          const lastTs = num(r.lastTs);
          const ageSec = lastTs ? Math.max(0, Math.round((Date.now() - lastTs) / 1000)) : null;
          let status = 'ok';
          if (baseline >= 3 && lastHour < baseline * 0.25) status = 'degraded';
          else if (baseline < 1 && lastHour === 0 && (ageSec == null || ageSec > 3600)) status = 'quiet';
          return { layer: L.layer, source: L.tbl, lastEventAgeSec: ageSec, lastHour, prevHour, baseline: Math.round(baseline * 10) / 10, status };
        } catch {
          return { layer: L.layer, source: L.tbl, lastEventAgeSec: null, lastHour: 0, prevHour: 0, baseline: null, status: 'error' };
        }
      }));
    },

    scoreDistribution: async () => {
      needLake();
      const rows = await sql(`
        SELECT CASE WHEN scoreVal<0.2 THEN '0.0–0.2' WHEN scoreVal<0.4 THEN '0.2–0.4'
                    WHEN scoreVal<0.6 THEN '0.4–0.6' WHEN scoreVal<0.8 THEN '0.6–0.8' ELSE '0.8–1.0' END bucket,
               count(*) n FROM ${NS}.silver_fact_history WHERE factClass='score' AND scoreVal IS NOT NULL GROUP BY 1 ORDER BY 1`);
      return rows.map((r) => ({ bucket: r.bucket, n: num(r.n) }));
    },

    liveFeed: async (_p, { limit }) => {
      needLake();
      const n = Math.min(Math.max(limit || 50, 1), 200);
      const rows = await sql(`
        SELECT * FROM (
          SELECT eventTs ts, factClass kind, entityId entity, key label, value detail, source FROM ${NS}.silver_fact_history
          UNION ALL SELECT eventTs, concat('activation:',op), entityId, name, channel, source FROM ${NS}.silver_activations
          UNION ALL SELECT evaluatedAt, 'eligible', nbaId, name, channel, 'rules-engine' FROM ${NS}.silver_eval_eligible
        ) ORDER BY ts DESC LIMIT ${n}`, 2500);
      return rows.map((r) => ({ ts: num(r.ts), kind: r.kind, entity: r.entity, label: r.label, detail: r.detail, source: r.source }));
    },

    actionFactMap: async () => {
      needLake();
      const rows = await sql(`SELECT id, name, fact FROM ${NS}.action_fact_map ORDER BY name, fact`);
      return rows.map((r) => ({ id: r.id, name: r.name, fact: r.fact }));
    },

    ruleFunnel: async (_p, { input }) => {
      needLake();
      const stages = buildFunnel(input || {});
      const cols = stages.map((s, i) => `sum(CASE WHEN ${s.sql} THEN 1 ELSE 0 END) AS s${i}`).join(', ');
      const rows = await sql(`SELECT ${cols} FROM ${NS}.gold_member_facts`, 1500);
      const r = rows[0] || {};
      return stages.map((s, i) => ({ label: s.label, count: num(r[`s${i}`]) }));
    },

    recentTraces: async (_p, { entity, limit }) => {
      needLake();
      const n = Math.min(Math.max(limit || 12, 1), 50);
      const e = esc(entity);
      const idr = await sql(`SELECT nbaId, entityId FROM ${NS}.gold_member_idmap WHERE entityId='${e}' OR nbaId='${e}' LIMIT 1`, 8000);
      const nbaId = idr[0]?.nbaId || entity, entityId = idr[0]?.entityId || entity;
      const rows = await sql(`
        SELECT correlationId, max(eventTs) ts, max(CASE WHEN op='CREATE' THEN name END) winner,
               count(DISTINCT CASE WHEN op='SUPPRESS' THEN concat(actionId,':',channel) END) suppressed
        FROM ${NS}.silver_activations WHERE nbaId='${esc(nbaId)}' AND correlationId IS NOT NULL
        GROUP BY correlationId ORDER BY ts DESC LIMIT ${n}`, 3000);
      return rows.map((r) => ({ correlationId: r.correlationId, nbaId, entityId, ts: num(r.ts), winner: r.winner, suppressedCount: num(r.suppressed) }));
    },

    // a handful of GOOD demo members for the snapshot/trace pickers — ranked by journey DEPTH (milestones
    // completed, then distinct actions taken), so the autocomplete showcases members who actually progressed
    // through the journey rather than ones stuck re-firing the welcome action. Ordering by milestones desc
    // naturally buries the stuck-at-step-1 members (0 milestones, 1 action). Each still has a CREATE to replay.
    sampleMembers: async (_p, { limit }) => {
      needLake();
      const n = Math.min(Math.max(limit || 7, 1), 20);
      const rows = await sql(`
        SELECT m.entityId AS entityId
        FROM ${NS}.gold_member_idmap m
        JOIN ${NS}.silver_activations a ON a.nbaId = m.nbaId AND a.op='CREATE'
        LEFT JOIN (SELECT nbaId, count(*) ms FROM ${NS}.silver_milestones GROUP BY nbaId) sm ON sm.nbaId = m.nbaId
        WHERE m.entityId IS NOT NULL
        GROUP BY m.entityId
        ORDER BY max(coalesce(sm.ms, 0)) DESC, count(distinct a.actionId) DESC, max(a.eventTs) DESC
        LIMIT ${n}`, 4000).catch(() => []);
      return rows.map((r) => r.entityId).filter(Boolean);
    },

    trace: async (_p, { correlationId }) => {
      needLake();
      const cid = esc(correlationId);
      const acts = await sql(`SELECT entityId, nbaId, op, actionId, channel, name, score, contentKey, eventTs FROM ${NS}.silver_activations WHERE correlationId='${cid}' ORDER BY eventTs`, 2000);
      const evals = await sql(`SELECT actionId, channel, name FROM ${NS}.silver_eval_eligible WHERE correlationId='${cid}'`, 2000);
      const snap = await sql(`SELECT entityId, nbaId, factCount, updatedTs FROM ${NS}.silver_snapshots WHERE correlationId='${cid}' LIMIT 1`, 2000);
      const entityId = acts[0]?.entityId || snap[0]?.entityId;
      const nbaId = acts[0]?.nbaId || snap[0]?.nbaId;
      if (!entityId && !evals.length && !acts.length) return { correlationId, found: false };
      // per-action dispositions (sent|suppressed|failed) — silver_fact_history has no correlationId, so key by
      // member + action-channel (the latest disposition per action-channel) to colour the trace's outcome steps.
      const dmap = {};
      if (entityId) {
        const disps = await sql(`SELECT actionId, channel, value, eventTs FROM ${NS}.silver_fact_history WHERE entityId='${esc(entityId)}' AND factClass='disposition' ORDER BY eventTs`, 2000);
        disps.forEach((d) => { dmap[d.actionId + ':' + d.channel] = d.value; });
      }
      // DECISION-TIME facts from the captured snapshot (accurate "why"); fall back to current gold.
      const facts = {};
      let factsAt = 'decision-time';
      try {
        const fj = snap[0]?.factsJson ? JSON.parse(snap[0].factsJson) : null;
        if (fj && Object.keys(fj).length) { for (const k of Object.keys(fj)) facts[k] = String(fj[k]?.value ?? fj[k]); }
      } catch { /* fall through */ }
      if (!Object.keys(facts).length && entityId) {
        factsAt = 'current';
        const factRows = await sql(`SELECT key, value FROM ${NS}.gold_member_snapshot WHERE entityId='${esc(entityId)}'`, 2000);
        factRows.forEach((r) => { facts[r.key] = r.value; });
      }
      const [actions, globals, channels] = await Promise.all([
        apiOrLake('/actions', 'ACTION', shapeAction), apiOrLake('/global-rules', 'GLOBAL_RULE', shapeRule), apiOrLake('/channel-rules', 'CHANNEL_RULE', shapeRule)]);
      const eligSet = new Set(evals.map((e) => e.actionId + ':' + e.channel));
      const explanations = actions.map((a) => explainEligibility(a, globals, channels, facts));
      const creates = acts.filter((a) => a.op === 'CREATE');
      const winner = creates[0] || null;
      const dispatched = acts.find((a) => a.op === 'DISPATCH') || null;
      const suppressed = acts.filter((a) => a.op === 'SUPPRESS');
      const wscore = winner && winner.score != null ? Number(winner.score) : null;
      const scored = acts.filter((a) => a.score != null).map((a) => ({ name: a.name, channel: a.channel, score: Number(a.score) }));
      // a BATCH = >1 CREATE on the winning channel (each action keeps its own workflow + disposition)
      const isBatch = creates.length > 1;
      const batchChannel = winner ? winner.channel : null;
      const dispOf = (a) => (a ? dmap[a.actionId + ':' + a.channel] || 'pending' : 'pending');
      const batchActions = creates.map((c) => ({ actionId: c.actionId, name: c.name, channel: c.channel,
        score: c.score != null ? Number(c.score) : null, contentKey: c.contentKey || null, disposition: dispOf(c) }));
      const steps = [
        { node: 'snapshot', title: 'Snapshot assembled', detail: `${Object.keys(facts).length} facts gathered for member ${entityId} (${factsAt})`, facts, factsAt },
        { node: 'rules', title: 'Eligibility evaluated', detail: `${actions.length} actions checked → ${evals.length} eligible action-channels`, explanations, eligible: [...eligSet] },
        { node: 'ml', title: 'Propensity scored', detail: scored.length ? scored.map((s) => `${s.name}/${s.channel} = ${s.score}`).join('   ') : 'no scores in this trace', scored },
        { node: 'router', title: isBatch ? 'Next-best-actions batched' : 'Next-best-action chosen',
          detail: isBatch
            ? `Batched top-${creates.length} on ${batchChannel} (winning channel); suppressed ${suppressed.length} other-channel action(s)`
            : (winner ? `Picked ${winner.name} / ${winner.channel}${winner.contentKey ? ' · ' + winner.contentKey : ''} (top score ${wscore}); suppressed ${suppressed.length} other(s)` : 'no action activated'),
          batch: isBatch ? { channel: batchChannel, count: creates.length, actions: batchActions } : null,
          winner: winner ? { name: winner.name, channel: winner.channel, score: wscore, contentKey: winner.contentKey || null } : null,
          suppressed: suppressed.map((s) => ({ name: s.name, channel: s.channel, score: s.score != null ? Number(s.score) : null,
            reason: winner ? `suppressed — ${winner.name} scored higher (${wscore} ≥ ${s.score})` : 'suppressed' })) },
        { node: 'temporal', title: 'Workflow lifecycle',
          detail: isBatch
            ? `batch orchestrator → ${creates.length} per-action workflows on ${batchChannel} (each tracked individually)`
            : (dispatched ? `dispatched ${dispatched.name} / ${dispatched.channel}` : (winner ? 'debounced → pending dispatch' : 'no workflow')),
          workflows: isBatch ? batchActions
            : (dispatched ? [{ actionId: dispatched.actionId, name: dispatched.name, channel: dispatched.channel, contentKey: dispatched.contentKey || null, disposition: dispOf(dispatched) }] : []) },
        { node: 'action', title: 'Disposition',
          detail: isBatch
            ? `one ${batchChannel} send → ${creates.length} dispositions: ${batchActions.map((a) => `${a.name}=${a.disposition}`).join(', ')}`
            : (dispatched ? `${dispOf(dispatched)} — ${dispatched.name} via ${dispatched.channel}` : (winner ? 'queued' : '—')),
          dispositions: isBatch ? batchActions
            : (dispatched ? [{ name: dispatched.name, channel: dispatched.channel, contentKey: dispatched.contentKey || null, disposition: dispOf(dispatched) }] : []) },
        { node: 'lake', title: 'Audited to the lake', detail: 'snapshot · evaluation · activations captured to silver + gold' },
      ];
      return { correlationId, found: true, entityId, nbaId, ts: num(acts[0]?.eventTs || snap[0]?.updatedTs), winner: winner ? winner.name : null, steps };
    },

    // Member snapshot: the current value of every fact (gold_member_snapshot) for a member, plus their NBA
    // history — activations (silver_activations) + workflow-state transitions + dispositions
    // (silver_fact_history) — grouped per action-channel into a merged timeline. Search by entityId or nbaId.
    memberSnapshot: async (_p, { memberId }) => {
      needLake();
      const e = esc((memberId || '').trim());
      let id = (await sql(`SELECT nbaId, entityId, entityType FROM ${NS}.gold_member_idmap WHERE entityId='${e}' OR nbaId='${e}' LIMIT 1`, 8000))[0];
      if (!id) id = (await sql(`SELECT nbaId, entityId, max(entityType) entityType FROM ${NS}.gold_member_snapshot WHERE entityId='${e}' OR nbaId='${e}' GROUP BY nbaId, entityId LIMIT 1`, 8000))[0];
      if (!id) return { found: false, facts: [], actions: [] };
      const ei = esc(id.entityId), ni = esc(id.nbaId || id.entityId);
      const [facts, acts, states, disps, miles] = await Promise.all([
        sql(`SELECT key, value, valueType, eventTs, source FROM ${NS}.gold_member_snapshot WHERE entityId='${ei}' ORDER BY key`, 8000),
        sql(`SELECT op, actionId, channel, name, score, contentKey, eventTs, correlationId, source FROM ${NS}.silver_activations WHERE nbaId='${ni}' OR entityId='${ei}' ORDER BY eventTs`, 8000),
        sql(`SELECT actionId, channel, value, eventTs, source FROM ${NS}.silver_fact_history WHERE entityId='${ei}' AND factClass='actionstate' ORDER BY eventTs`, 8000),
        sql(`SELECT actionId, channel, value, contentKey, eventTs, source FROM ${NS}.silver_fact_history WHERE entityId='${ei}' AND factClass='disposition' ORDER BY eventTs`, 8000),
        sql(`SELECT m.milestoneId id, m.name, m.completedAt, d.logicJson FROM ${NS}.silver_milestones m
             LEFT JOIN ${NS}.dim_definitions d ON d.id = m.milestoneId AND d.defType='MILESTONE'
             WHERE m.nbaId='${ni}' ORDER BY m.completedAt`, 8000).catch(() => []),
      ]);
      const byAction = {};
      const grp = (actionId, channel) => (byAction[actionId + '|' + channel] ||= { actionId, channel, name: null, events: [] });
      // silver_activations holds two op-families: the ROUTER's CREATE/SUPPRESS decisions (router facts) and
      // the state machine's DISPATCH/CANCEL — the ACTIVATIONS actually sent to the unified activation layer.
      acts.forEach((a) => { const x = grp(a.actionId, a.channel); if (a.name) x.name = a.name;
        const op = (a.op || '').toUpperCase();
        const kind = (op === 'DISPATCH' || op === 'CANCEL') ? 'activation' : 'router';
        x.events.push({ ts: num(a.eventTs), kind, op: a.op, score: a.score != null ? Number(a.score) : null, contentKey: a.contentKey || null, correlationId: a.correlationId || null, source: a.source }); });
      states.forEach((s) => grp(s.actionId, s.channel).events.push({ ts: num(s.eventTs), kind: 'state', value: s.value, source: s.source }));
      disps.forEach((d) => grp(d.actionId, d.channel).events.push({ ts: num(d.eventTs), kind: 'disposition', value: d.value, contentKey: d.contentKey || null, source: d.source }));
      const actions = Object.values(byAction).map((x) => {
        const events = x.events.sort((a, b) => a.ts - b.ts);
        return { actionId: x.actionId, name: x.name || x.actionId, channel: x.channel, lastTs: events.length ? events[events.length - 1].ts : 0, events };
      }).sort((a, b) => b.lastTs - a.lastTs);
      const updatedTs = facts.reduce((m, f) => Math.max(m, num(f.eventTs)), 0);
      const milestones = (miles || []).map((m) => ({ id: m.id, name: m.name, completedAt: num(m.completedAt), logic: J(m.logicJson) }));
      return { found: true, entityId: id.entityId, nbaId: id.nbaId, entityType: id.entityType, updatedTs,
        facts: facts.map((f) => ({ key: f.key, value: f.value, valueType: f.valueType, eventTs: num(f.eventTs), source: f.source })), actions, milestones };
    },
  },
  Mutation: {
    upsertAction: (_p, { input }) => api('/actions', { method: 'POST', body: JSON.stringify(input) }),
    deleteAction: async (_p, { id }) => { await api('/actions/' + encodeURIComponent(id), { method: 'DELETE' }); return true; },
    upsertGlobalRule: (_p, { input }) => api('/global-rules', { method: 'POST', body: JSON.stringify(input) }),
    deleteGlobalRule: async (_p, { id }) => { await api('/global-rules/' + encodeURIComponent(id), { method: 'DELETE' }); return true; },
    upsertChannelRule: (_p, { input }) => api('/channel-rules', { method: 'POST', body: JSON.stringify(input) }),
    deleteChannelRule: async (_p, { id }) => { await api('/channel-rules/' + encodeURIComponent(id), { method: 'DELETE' }); return true; },
    suppressAction: (_p, { actionId, channel, suppressed }) => api('/suppress', { method: 'POST', body: JSON.stringify({ actionId, channel: channel || '', suppressed }) }),
    setChannelMaxBatch: (_p, { channel, maxBatch }) => api('/channel-config', { method: 'POST', body: JSON.stringify({ channel, maxBatch }) }),
    createActionGroup: (_p, { name, parentId }) => api('/groups', { method: 'POST', body: JSON.stringify({ name, parentId: parentId || null }) }),
    deleteActionGroup: async (_p, { id }) => { await api('/groups/' + encodeURIComponent(id), { method: 'DELETE' }); return true; },
    assignActionGroup: (_p, { actionId, groupId }) => api('/actions/' + encodeURIComponent(actionId) + '/group', { method: 'POST', body: JSON.stringify({ groupId: groupId || null }) }),
    createExperience: (_p, { name, description }) => api('/experiences', { method: 'POST', body: JSON.stringify({ name, description: description || null }) }),
    deleteExperience: async (_p, { id }) => { await api('/experiences/' + encodeURIComponent(id), { method: 'DELETE' }); return true; },
    assignExperience: (_p, { actionId, experienceId }) => api('/actions/' + encodeURIComponent(actionId) + '/experience', { method: 'POST', body: JSON.stringify({ experienceId: experienceId || null }) }),
    upsertMilestone: (_p, { input }) => api('/milestones', { method: 'POST', body: JSON.stringify(input) }),
    deleteMilestone: async (_p, { id }) => { await api('/milestones/' + encodeURIComponent(id), { method: 'DELETE' }); return true; },
    replayDlq: (_p, { consumer }) => replayDlq(consumer),
    flushDlq: (_p, { consumer }) => flushDlq(consumer),
  },
};

const server = new ApolloServer({ typeDefs, resolvers });
await server.start();

const app = express();
app.use(cors());
app.use('/graphql', express.json({ limit: '2mb' }), expressMiddleware(server));

// System Map: live SSE event stream + topology + per-node payload peek
app.get('/topology', (_req, res) => res.json(TOPOLOGY));
app.get('/recent/:node', (req, res) => res.json(recentForNode(req.params.node)));
app.get('/livestats', (_req, res) => res.json(liveStats()));
app.get('/stream', (req, res) => {
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.flushHeaders?.();
  res.write(': connected\n\n');
  const ka = setInterval(() => { try { res.write(': ka\n\n'); } catch {} }, 20000);
  req.on('close', () => clearInterval(ka));
  addSubscriber(res);
});

// The authoring REST surface (the routes the action API used to expose) — now served here, on the command
// center, straight from the library module. Scripts/partners that authored against :7001 point here instead.
app.use(library.router(express));

const PORT = Number(process.env.PORT || 4000);
library.init()
  .then(() => app.listen(PORT, '0.0.0.0', () => console.log('[command-center-bff] up :' + PORT
      + ' | library: LOCAL (authoring owned here) | runtime -> ' + API
      + ' | lake ' + (lakeConfigured ? `${backend}:${NS}` : 'NOT CONFIGURED'))))
  .catch((e) => { console.error('[command-center-bff] library init failed:', e); process.exit(1); });
startConsumer();
startDlqTracker();
