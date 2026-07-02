// ACTION LIBRARY — authoring, owned by the COMMAND CENTER (moved out of the action API, which is now a pure
// runtime surface: getActions + postDispositions). This module is the control plane: definitions CRUD
// (actions / global rules / channel rules / milestones), taxonomy (groups + experiences), operator suppress,
// channel config, and the fact-type catalog. Faithful port of the Java ActionLibrary authoring internals:
//   - definitions live in Postgres (actionlib) and publish to nba.definitions via the TRANSACTIONAL OUTBOX
//     (outbox_defs; Debezium EventRouter relays — same connector, same tables, nothing on the pipeline moved)
//   - upserts run the TYPE-CORRECTNESS GATE against nba:facttype and stamp factsUsed (collectFacts)
//   - every def mutation recomputes nba:rulefacts from Postgres (the snapshot lean-filter's source of truth)
//   - suppress writes the ACTION_SUPPRESS flag to the definitions broadcast (single write; the temporal
//     suppress-feed / Flink state machine / action API's defs cache all react to the topic)
// The action API keeps READ-ONLY access to the same tables (its serve-time catalog); this module owns writes.
import pg from 'pg';
import { createClient } from 'redis';
import crypto from 'node:crypto';

const { Pool } = pg;

const PG_HOST = process.env.NBA_PG_HOST || 'nba-postgres';
const PG_DB = process.env.NBA_PG_DB || 'actionlib';
const PG_USER = process.env.NBA_PG_USER || 'nba';
const PG_PASS = process.env.NBA_PG_PASSWORD || 'nba';
const REDIS_HOST = process.env.NBA_REDIS_HOST || 'nba-redis';
const DEFS_TOPIC = process.env.NBA_DEFINITIONS_TOPIC || 'nba.definitions';

const pool = new Pool({ host: PG_HOST, database: PG_DB, user: PG_USER, password: PG_PASS, max: 5 });
const redis = createClient({ url: `redis://${REDIS_HOST}:6379` });
redis.on('error', (e) => console.warn('[library] redis:', e.message));

const DDL = [
  `create table if not exists action       (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())`,
  `create table if not exists global_rule  (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())`,
  `create table if not exists channel_rule (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())`,
  // transactional outbox (Debezium Outbox Event Router convention; the shared nba-outbox connector tails it).
  // payload NULL = tombstone for the compacted definitions topic (def delete).
  `create table if not exists outbox_defs (id uuid primary key default gen_random_uuid(), aggregatetype text not null, aggregateid text not null, kind text, payload text, created_at timestamptz not null default now())`,
  `create table if not exists action_group (id text primary key, name text not null, parent_id text references action_group(id), updated_at timestamptz not null default now())`,
  `create table if not exists experience (id text primary key, name text not null, description text, updated_at timestamptz not null default now())`,
  `create table if not exists milestone (id text primary key, doc jsonb not null, updated_at timestamptz not null default now())`,
];

async function init() {
  await redis.connect();
  for (let i = 0; i < 30; i++) {
    try { await pool.query('select 1'); break; }
    catch (e) { console.log('[library] waiting for db...', e.message); await new Promise(r => setTimeout(r, 2000)); }
  }
  for (const d of DDL) await pool.query(d);
  await recomputeRuleFacts();   // authoritative boot-time load (mirrors the Java startDefinitionsCache boot)
  console.log(`[library] up: db=${PG_HOST}/${PG_DB} redis=${REDIS_HOST} outbox->${DEFS_TOPIC}`);
}

// ── outbox + rulefacts ───────────────────────────────────────────────────────────────────────────
async function outbox(client, aggregatetype, aggregateid, kind, payload) {
  await client.query('insert into outbox_defs(aggregatetype, aggregateid, kind, payload) values ($1,$2,$3,$4)',
    [aggregatetype, aggregateid, kind, payload]);
}

/** nba:rulefacts = union of factsUsed across all current defs, from POSTGRES (authoritative). */
async function recomputeRuleFacts() {
  const q = `select distinct f from (
      select jsonb_array_elements_text(doc->'factsUsed') f from action
      union all select jsonb_array_elements_text(doc->'factsUsed') f from global_rule
      union all select jsonb_array_elements_text(doc->'factsUsed') f from channel_rule
      union all select jsonb_array_elements_text(doc->'factsUsed') f from milestone) x`;
  try {
    // BOOTSTRAP GUARD: if the library DB holds NO defs at all (fresh stack — the demo seeds the TOPIC +
    // redis-defs.sh directly, not Postgres), leave nba:rulefacts alone; an empty library must not clobber
    // a topic-seeded environment. The recompute becomes authoritative once anything is authored here.
    const n = await pool.query('select (select count(*) from action) + (select count(*) from global_rule) + (select count(*) from channel_rule) + (select count(*) from milestone) as n');
    if (Number(n.rows[0].n) === 0) { console.log('[library] defs DB empty — leaving nba:rulefacts to the seed'); return; }
    const { rows } = await pool.query(q);
    const facts = rows.map(r => r.f).filter(f => f && f.trim());
    await redis.del('nba:rulefacts');
    if (facts.length) await redis.sAdd('nba:rulefacts', facts);
    console.log('[library] nba:rulefacts ->', facts.length, 'facts');
  } catch (e) { console.warn('[library] rulefacts recompute failed:', e.message); }
}

// ── condition-tree helpers (verbatim semantics from the Java) ────────────────────────────────────
function collectFacts(node, out) {
  if (!node) return;
  if (node.fact) out.add(node.fact);
  if (Array.isArray(node.conditions)) for (const c of node.conditions) collectFacts(c, out);
}

/** TYPE-CORRECTNESS GATE: reject operators that don't fit the fact's declared type (nba:facttype).
 *  Unknown facts pass; 'exists' fits any type. Throws Error on mismatch. */
async function validateTree(node) {
  if (!node) return;
  if (node.fact && (node.cmp || 'eq') !== 'exists') {
    const type = await redis.hGet('nba:facttype', node.fact);
    if (type) {
      const t = type.toUpperCase();
      let ok;
      if (t === 'BOOL' || t === 'BOOLEAN') ok = ['eq', 'ne', 'exists'];
      else if (t === 'STRING') ok = ['eq', 'ne', 'in', 'exists'];
      else ok = ['eq', 'ne', 'gt', 'gte', 'lt', 'lte', 'exists'];
      if (!ok.includes(node.cmp || 'eq'))
        throw Object.assign(new Error(`fact '${node.fact}' is ${type}; operator '${node.cmp || 'eq'}' is invalid (allowed: [${ok.join(', ')}])`), { status: 400 });
    }
  }
  if (Array.isArray(node.conditions)) for (const c of node.conditions) await validateTree(c);
}

// ── definitions CRUD (actions / global_rule / channel_rule / milestone) ─────────────────────────
async function upsertDef(table, aggType, doc, pathId) {
  doc = { ...doc };
  let id = pathId || doc.id;
  if (!id || !String(id).trim()) id = `${aggType.toLowerCase()}_${crypto.randomUUID().slice(0, 8)}`;
  doc.id = id;
  await validateTree(doc.inclusion); await validateTree(doc.exclusion);
  await validateTree(doc.logic); await validateTree(doc.completion);
  if (Array.isArray(doc.channels)) for (const ch of doc.channels)
    if (Array.isArray(ch.variants)) for (const v of ch.variants) await validateTree(v.conditions);
  const facts = new Set();
  collectFacts(doc.inclusion, facts); collectFacts(doc.exclusion, facts); collectFacts(doc.logic, facts);
  if (Array.isArray(doc.channels)) for (const ch of doc.channels)
    if (Array.isArray(ch.variants)) for (const v of ch.variants) collectFacts(v.conditions, facts);
  if (aggType === 'ACTION') collectFacts(doc.completion, facts);   // hard-completion facts ride the lean snapshot
  doc.factsUsed = [...facts].sort();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(
      `insert into ${table}(id, doc, updated_at) values ($1, $2::jsonb, now())
       on conflict (id) do update set doc = excluded.doc, updated_at = now()`, [id, JSON.stringify(doc)]);
    await outbox(client, DEFS_TOPIC, `${aggType}:${id}`, null, JSON.stringify(doc));   // same tx -> Debezium
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
  await recomputeRuleFacts();
  console.log(`[library] upsert ${table} id=${id} factsUsed=[${doc.factsUsed}]`);
  return doc;
}

async function listDefs(table) {
  const { rows } = await pool.query(`select doc from ${table} order by updated_at desc`);
  return rows.map(r => r.doc);
}

async function getDef(table, id) {
  const { rows } = await pool.query(`select doc from ${table} where id = $1`, [id]);
  return rows.length ? rows[0].doc : null;
}

async function deleteDef(table, aggType, id) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`delete from ${table} where id = $1`, [id]);
    await outbox(client, DEFS_TOPIC, `${aggType}:${id}`, null, null);   // null payload = compaction tombstone
    await client.query('COMMIT');
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
  await recomputeRuleFacts();
  return { deleted: id };
}

// ── groups (taxonomy tree) ───────────────────────────────────────────────────────────────────────
async function listGroups() {
  const { rows } = await pool.query('select id, name, parent_id from action_group order by name');
  return rows.map(r => ({ id: r.id, name: r.name, parentId: r.parent_id }));
}

async function createGroup(name, parentId) {
  name = (name || '').trim(); parentId = parentId || null;
  if (!name) throw Object.assign(new Error('name required'), { status: 400 });
  if (parentId) {
    const { rows } = await pool.query('select 1 from action_group where id = $1', [parentId]);
    if (!rows.length) throw Object.assign(new Error('parent group not found: ' + parentId), { status: 400 });
  }
  const id = 'grp_' + crypto.randomUUID().slice(0, 8);
  await pool.query('insert into action_group(id, name, parent_id) values ($1,$2,$3)', [id, name, parentId]);
  return { id, name, parentId };
}

async function deleteGroup(id) {
  const kids = await pool.query('select count(*)::int n from action_group where parent_id = $1', [id]);
  const acts = await pool.query(`select count(*)::int n from action where doc->>'groupId' = $1`, [id]);
  if (kids.rows[0].n > 0 || acts.rows[0].n > 0)
    throw Object.assign(new Error('group not empty'), { status: 409, childGroups: kids.rows[0].n, actions: acts.rows[0].n });
  const del = await pool.query('delete from action_group where id = $1', [id]);
  if (!del.rowCount) throw Object.assign(new Error('group not found'), { status: 404 });
  return { deleted: id };
}

/** Assign/clear an action's group or experience — updates the doc + re-emits the def in the SAME outbox tx. */
async function assignField(actionId, field, value, checkTable) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query('select doc from action where id = $1', [actionId]);
    if (!rows.length) throw Object.assign(new Error('action not found: ' + actionId), { status: 404 });
    const doc = rows[0].doc;
    if (value) {
      const ck = await client.query(`select 1 from ${checkTable} where id = $1`, [value]);
      if (!ck.rows.length) throw Object.assign(new Error(`${checkTable} not found: ` + value), { status: 400 });
      doc[field] = value;
    } else delete doc[field];
    await client.query('update action set doc = $1::jsonb, updated_at = now() where id = $2', [JSON.stringify(doc), actionId]);
    await outbox(client, DEFS_TOPIC, 'ACTION:' + actionId, null, JSON.stringify(doc));
    await client.query('COMMIT');
    return { id: actionId, [field]: value || '' };
  } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
}

// ── experiences (flat business taxonomy) ─────────────────────────────────────────────────────────
async function listExperiences() {
  const { rows } = await pool.query('select id, name, description from experience order by name');
  return rows.map(r => ({ id: r.id, name: r.name, description: r.description }));
}

async function createExperience(name, description) {
  name = (name || '').trim();
  if (!name) throw Object.assign(new Error('name required'), { status: 400 });
  const id = 'exp_' + crypto.randomUUID().slice(0, 8);
  await pool.query('insert into experience(id, name, description) values ($1,$2,$3)', [id, name, description || null]);
  return { id, name, description: description || null };
}

async function deleteExperience(id) {
  const acts = await pool.query(`select count(*)::int n from action where doc->>'experienceId' = $1`, [id]);
  if (acts.rows[0].n > 0) throw Object.assign(new Error('experience not empty'), { status: 409, actions: acts.rows[0].n });
  const del = await pool.query('delete from experience where id = $1', [id]);
  if (!del.rowCount) throw Object.assign(new Error('experience not found'), { status: 404 });
  return { deleted: id };
}

// ── operator suppress + channel config + facts ───────────────────────────────────────────────────
/** Single write: the ACTION_SUPPRESS flag on the definitions broadcast (the trigger for the temporal
 *  suppress-feed / Flink fleet-cancel / the action API's serve-time strip). Same semantics as the Java. */
async function suppress(actionId, channel, suppressed) {
  if (!actionId) throw Object.assign(new Error('actionId required'), { status: 400 });
  const target = channel ? `${actionId}.${channel}` : actionId;
  const fact = {
    entityType: 'SYSTEM', entityId: '__action', key: 'nba.actionsuppress.' + target,
    value: !!suppressed, valueType: 'BOOL', actionId, channel: channel || '',
    eventTs: Date.now(), source: 'operator',
  };
  const client = await pool.connect();
  try { await outbox(client, DEFS_TOPIC, 'ACTION_SUPPRESS:' + target, 'action-suppress', JSON.stringify(fact)); }
  finally { client.release(); }
  console.log(`[library] operator ${suppressed ? 'SUPPRESS' : 'UNSUPPRESS'} ${target} (outbox)`);
  return fact;
}

const suppressed = () => redis.sMembers('nba:suppressed');
const facts = () => redis.hGetAll('nba:facttype');
const channelConfig = () => redis.hGetAll('nba:channel:maxbatch');
async function setChannelConfig(channel, maxBatch) {
  if (!channel) throw Object.assign(new Error('channel required'), { status: 400 });
  const mb = Math.max(1, Number(maxBatch) || 1);
  await redis.hSet('nba:channel:maxbatch', channel, String(mb));
  return { channel, maxBatch: mb };
}

// ── REST router: the SAME authoring surface the action API used to expose, now on the command center ──
function router(express) {
  const r = express.Router();
  r.use(express.json({ limit: '2mb' }));
  const wrap = (fn) => async (req, res) => {
    try { res.json(await fn(req)); }
    catch (e) {
      const body = { error: e.message };
      if (e.childGroups !== undefined) body.childGroups = e.childGroups;
      if (e.actions !== undefined) body.actions = e.actions;
      res.status(e.status || 500).json(body);
    }
  };
  const defRoutes = (path, table, aggType) => {
    r.post(`/${path}`, wrap(rq => upsertDef(table, aggType, rq.body, null)));
    r.put(`/${path}/:id`, wrap(rq => upsertDef(table, aggType, rq.body, rq.params.id)));
    r.get(`/${path}`, wrap(() => listDefs(table)));
    r.get(`/${path}/:id`, wrap(async rq => {
      const d = await getDef(table, rq.params.id);
      if (!d) throw Object.assign(new Error('not_found'), { status: 404 });
      return d;
    }));
    r.delete(`/${path}/:id`, wrap(rq => deleteDef(table, aggType, rq.params.id)));
  };
  defRoutes('actions', 'action', 'ACTION');
  defRoutes('global-rules', 'global_rule', 'GLOBAL_RULE');
  defRoutes('channel-rules', 'channel_rule', 'CHANNEL_RULE');
  defRoutes('milestones', 'milestone', 'MILESTONE');
  r.get('/facts', wrap(() => facts()));
  r.post('/suppress', wrap(rq => suppress(rq.body.actionId, rq.body.channel || '', rq.body.suppressed !== false)));
  r.get('/suppressed', wrap(() => suppressed()));
  r.post('/channel-config', wrap(rq => setChannelConfig(rq.body.channel, rq.body.maxBatch)));
  r.get('/channel-config', wrap(() => channelConfig()));
  r.get('/groups', wrap(() => listGroups()));
  r.post('/groups', wrap(rq => createGroup(rq.body.name, rq.body.parentId)));
  r.delete('/groups/:id', wrap(rq => deleteGroup(rq.params.id)));
  r.post('/actions/:id/group', wrap(rq => assignField(rq.params.id, 'groupId', rq.body.groupId || null, 'action_group')));
  r.get('/experiences', wrap(() => listExperiences()));
  r.post('/experiences', wrap(rq => createExperience(rq.body.name, rq.body.description)));
  r.delete('/experiences/:id', wrap(rq => deleteExperience(rq.params.id)));
  r.post('/actions/:id/experience', wrap(rq => assignField(rq.params.id, 'experienceId', rq.body.experienceId || null, 'experience')));
  return r;
}

export {
  init, router,
  upsertDef, listDefs, getDef, deleteDef,
  listGroups, createGroup, deleteGroup, assignField,
  listExperiences, createExperience, deleteExperience,
  suppress, suppressed, facts, channelConfig, setChannelConfig,
};
