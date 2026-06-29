// BFF unit tests — pure logic, no infra. Run: node --test  (from bff/)
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { treeSql, buildFunnel } from '../rulesql.js';
import { emitterFor, summarize, TOPOLOGY } from '../eventstream.js';
import { sumOffsets, replayRecord } from '../dlq.js';
import { stateCounts, CANONICAL_STATES } from '../eventstream.js';

// ---- rule-to-SQL compiler ----
test('treeSql: empty/absent tree matches all', () => {
  assert.equal(treeSql(null), 'true');
  assert.equal(treeSql({ op: 'all', conditions: [] }), 'true');
});

test('treeSql: numeric comparators cast + default to type-zero', () => {
  const s = treeSql({ op: 'all', conditions: [{ fact: 'operator.activity.daysSinceLogin', cmp: 'gte', value: 14 }] });
  assert.match(s, /try_cast\(element_at\(facts, 'operator\.activity\.daysSinceLogin'\) AS DOUBLE\)/);
  assert.match(s, /COALESCE\(.*, 0\) >= 14/);
});

test('treeSql: boolean eq', () => {
  const s = treeSql({ op: 'all', conditions: [{ fact: 'operator.profile.isDNC', cmp: 'eq', value: true }] });
  assert.match(s, /lower\(element_at\(facts, 'operator\.profile\.isDNC'\)\) = 'true'/);
  assert.match(s, /= true/);
});

test('treeSql: string ne quotes + escapes', () => {
  const s = treeSql({ op: 'any', conditions: [{ fact: 'operator.tier', cmp: 'ne', value: "go'ld" }] });
  assert.match(s, /<> 'go''ld'/);
});

test('treeSql: exists + in', () => {
  assert.match(treeSql({ op: 'all', conditions: [{ fact: 'x', cmp: 'exists' }] }), /element_at\(facts, 'x'\) IS NOT NULL/);
  assert.match(treeSql({ op: 'all', conditions: [{ fact: 'x', cmp: 'in', value: [1, 2, 3] }] }), /IN \(1, 2, 3\)/);
});

test('treeSql: all uses AND, any uses OR, nested groups compose', () => {
  const s = treeSql({ op: 'all', conditions: [
    { fact: 'a', cmp: 'gt', value: 1 },
    { op: 'any', conditions: [{ fact: 'b', cmp: 'eq', value: true }, { fact: 'c', cmp: 'lt', value: 5 }] },
  ] });
  assert.match(s, / AND /); assert.match(s, / OR /);
});

test('buildFunnel: cumulative stages from inclusion/exclusion/global/channel', () => {
  const stages = buildFunnel({
    inclusion: { op: 'all', conditions: [{ fact: 'a', cmp: 'gte', value: 1 }] },
    exclusion: { op: 'any', conditions: [{ fact: 'b', cmp: 'eq', value: true }] },
    globalRules: [{ name: 'cap', logic: { op: 'all', conditions: [{ fact: 'c', cmp: 'lt', value: 3 }] } }],
    channelRules: [{ name: 'email cap', logic: { op: 'all', conditions: [{ fact: 'd', cmp: 'lt', value: 2 }] } }],
  });
  assert.equal(stages[0].label, 'All members');
  assert.deepEqual(stages.map((s) => s.label), ['All members', 'Inclusion', 'Exclusion', 'cap', 'email cap']);
  // cumulative: each stage's sql contains the prior predicate
  assert.ok(stages[4].sql.length > stages[1].sql.length);
  assert.match(stages[2].sql, /NOT/); // exclusion negated
});

// ---- event-stream emitter mapping ----
test('emitterFor: topic-derived emitters', () => {
  assert.equal(emitterFor('nba.snapshots', null), 'snapshot');
  assert.equal(emitterFor('nba.evaluations', null), 'rules');
  assert.equal(emitterFor('nba.activations', 'state-machine'), 'temporal');   // activations = the state machine's DISPATCH/CANCEL
  assert.equal(emitterFor('nba.definitions', null), 'source');
});

test('emitterFor: facts disambiguated by source + header', () => {
  assert.equal(emitterFor('nba.member.facts', 'ml'), 'ml');
  assert.equal(emitterFor('nba.member.facts', 'temporal'), 'temporal');
  assert.equal(emitterFor('nba.member.facts', 'action-layer'), 'action');
  // retired in-pod datalake -> comms/throttle counting moved into the Databricks lake node.
  assert.equal(emitterFor('nba.member.facts', 'comms-lake'), 'lake');
  assert.equal(emitterFor('nba.member.facts', 'throttle-lake'), 'lake');
  assert.equal(emitterFor('nba.member.facts', 'action-router'), 'router');     // router decisions ride member.facts
  assert.equal(emitterFor('nba.member.facts', null, 'router'), 'router');      // ...identified by the kind=router header
  assert.equal(emitterFor('nba.facts', 'databricks-gold'), 'lake');
  assert.equal(emitterFor('nba.facts', 'keycloak'), 'source');
});

test('summarize: per-topic compact labels', () => {
  assert.match(summarize('nba.evaluations', { nbaId: 'n1', channelActions: [{ eligible: true }, { eligible: true }, { eligible: false }] }).label, /2 eligible/);
  assert.equal(summarize('nba.activations', { op: 'CREATE', name: 'Reengage', entityId: 'e1' }).label, 'CREATE Reengage');
  assert.match(summarize('nba.snapshots', { entityId: 'e1', facts: { a: 1, b: 2 } }).label, /2 facts/);
});

test('TOPOLOGY: every edge references real nodes', () => {
  const ids = new Set(TOPOLOGY.nodes.map((n) => n.id));
  for (const e of TOPOLOGY.edges) {
    assert.ok(ids.has(e.from), `edge from ${e.from}`);
    assert.ok(ids.has(e.to), `edge to ${e.to}`);
  }
  assert.ok(TOPOLOGY.nodes.length >= 8);
});

// ---- DLQ admin (replay/flush) ----
test('sumOffsets: depth = high watermark - low watermark across partitions', () => {
  assert.deepEqual(sumOffsets([{ partition: 0, offset: '100', low: '40' }]), { high: 100, low: 40, depth: 60 });
  // multi-partition sums both; an empty topic (high==low) is depth 0
  assert.equal(sumOffsets([{ partition: 0, offset: '10', low: '0' }, { partition: 1, offset: '5', low: '5' }]).depth, 10);
  assert.equal(sumOffsets([]).depth, 0);
});

test('replayRecord: re-produces the EXACT original to its source topic (key+value+headers preserved)', () => {
  // an envelope as the Java consumers write it (consumer/topic(source)/partition/offset/key/value/headers/error/dlqTs)
  const env = { consumer: 'ml-scorer-scorer', topic: 'nba.evaluations', partition: 0, offset: 14595,
    key: 'Lead:1', value: '{"nbaId":"nba_x","eligible":[{"actionId":"a","channel":"email"}]}',
    headers: { kind: 'score' }, error: 'boom', dlqTs: 1781467000000 };
  const msg = replayRecord(env);
  assert.equal(msg.topic, 'nba.evaluations');                 // back to the SOURCE topic, not the DLQ
  assert.equal(msg.messages[0].key, 'Lead:1');                 // key preserved -> same partition
  assert.equal(msg.messages[0].value, env.value);              // verbatim value -> idempotency relies on this
  assert.deepEqual(msg.messages[0].headers, { kind: 'score' });
});

// ---- state-machine rollup ----
test('CANONICAL_STATES: the 11 disposition-driven states (no SENT; IN_PROCESS=dispatched, PRESENTED=delivered)', () => {
  assert.equal(CANONICAL_STATES.length, 11);
  for (const s of ['CREATED', 'IN_PROCESS', 'PRESENTED', 'SOFT_COMPLETED', 'DECLINED', 'SUPPRESSING', 'FAILED', 'HARD_COMPLETED', 'EXPIRED', 'SUPPRESSED', 'DEBOUNCED'])
    assert.ok(CANONICAL_STATES.includes(s), `missing ${s}`);
  assert.ok(!CANONICAL_STATES.includes('SENT'), 'SENT was removed');
});

test('stateCounts: keyed by every canonical state, defaulting to 0', () => {
  const c = stateCounts();
  for (const s of CANONICAL_STATES) assert.equal(typeof c[s], 'number', `${s} is a number`);
  // a fresh process (no facts streamed) reports all zero
  assert.ok(Object.values(c).every((n) => n >= 0));
});

test('replayRecord: null key tolerated; missing source topic is skipped', () => {
  assert.equal(replayRecord({ topic: 'nba.facts', value: 'v' }).messages[0].key, null);
  assert.equal(replayRecord({ value: 'v' }), null);            // no source topic -> nothing to replay
  assert.equal(replayRecord(null), null);
});
