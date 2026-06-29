// Lakebase (Databricks-managed Postgres) client for the Command Center BFF — the SAME interface as
// databricks.js (sql/sql1/num/NS/lakeConfigured) so the resolvers don't change. Selected by lake.js when
// NBA_LAKEBASE_HOST is set. This lets the WHOLE UI run off scale-to-zero Postgres with the SQL warehouse
// fully stopped (cheap, warehouse-free debugging); the warehouse is only needed to REFRESH the data (loader).
//
// The resolvers' SQL is Databricks/Spark dialect. We bridge the gap WITHOUT rewriting every query:
//   - identifiers: the loader stores columns LOWERCASE, so Postgres' unquoted-fold matches Spark's CamelCase.
//   - namespace:  NS='public' turns `${NS}.t` into `public.t`.
//   - functions:  pgize() rewrites the handful of Spark-isms (from_unixtime, unix_timestamp, to_date, hour,
//                 date_sub, count_if, multi-arg count(distinct), round(double,n), element_at, try_cast, slice/
//                 array_sort/collect_set) into Postgres equivalents.
//   - result keys: recase() maps lowercase columns back to camelCase via the loader's _colcase table.
import pg from 'pg';
const { Pool } = pg;

const HOST = process.env.NBA_LAKEBASE_HOST || '';
const LB_USER = process.env.NBA_LAKEBASE_USER || '';
const LB_INSTANCE = process.env.NBA_LAKEBASE_INSTANCE || 'nba-lakebase';
const LB_DB = process.env.NBA_LAKEBASE_DB || 'nba';
const DBX = (process.env.DATABRICKS_HOST || '').replace(/\/$/, '');
const CID = process.env.DATABRICKS_CLIENT_ID;
const SECRET = process.env.DATABRICKS_CLIENT_SECRET;

export const NS = 'public';
export const lakeConfigured = Boolean(HOST && LB_USER && DBX && CID && SECRET);
export const num = (v) => (v == null ? 0 : Number(v));

// ---- Databricks OAuth (M2M) -> short-lived Lakebase Postgres credential -------------------------------
let _dtok = null, _dExp = 0;
async function dbxToken() {
  if (_dtok && Date.now() < _dExp) return _dtok;
  const r = await fetch(DBX + '/oidc/v1/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'client_credentials', client_id: CID, client_secret: SECRET, scope: 'all-apis' }),
  });
  if (!r.ok) throw new Error('databricks token: ' + r.status);
  const j = await r.json();
  _dtok = j.access_token; _dExp = Date.now() + (j.expires_in - 60) * 1000;
  return _dtok;
}
let _lb = null, _lbExp = 0;
async function lbCred() {
  if (_lb && Date.now() < _lbExp) return _lb;
  const r = await fetch(DBX + '/api/2.0/database/credentials', {
    method: 'POST', headers: { Authorization: 'Bearer ' + (await dbxToken()), 'Content-Type': 'application/json' },
    body: JSON.stringify({ request_id: globalThis.crypto.randomUUID(), instance_names: [LB_INSTANCE] }),   // request_id REQUIRED
  });
  if (!r.ok) throw new Error('lakebase cred: ' + r.status);
  const j = await r.json();
  _lb = j.token; _lbExp = Date.now() + 50 * 60 * 1000;   // credential lives ~1h; refresh at 50m
  return _lb;
}

// ---- connection pool (password is a function -> pg fetches a fresh credential per new connection) -------
let _pool = null;
function pool() {
  if (_pool) return _pool;
  _pool = new Pool({
    host: HOST, port: 5432, database: LB_DB, user: LB_USER, password: lbCred,
    ssl: { rejectUnauthorized: false }, max: 6, idleTimeoutMillis: 30000, connectionTimeoutMillis: 10000,
  });
  _pool.on('error', () => {});   // a scaled-to-zero idle conn dropping is normal; pg replaces it
  return _pool;
}

// ---- SQL dialect translation (Spark -> Postgres) -------------------------------------------------------
// Replace every `name(...)` call, extracting its balanced (quote-aware) argument string, via fn(arg).
function replaceFn(s, name, fn) {
  const ln = name.toLowerCase();
  let out = '', i = 0;
  while (i < s.length) {
    const idx = s.toLowerCase().indexOf(ln + '(', i);
    if (idx < 0) { out += s.slice(i); break; }
    const prev = idx > 0 ? s[idx - 1] : ' ';
    if (/[A-Za-z0-9_]/.test(prev)) { out += s.slice(i, idx + ln.length + 1); i = idx + ln.length + 1; continue; }
    let depth = 0, q = false, j = idx + ln.length;   // j at '('
    const start = j + 1;
    for (; j < s.length; j++) {
      const ch = s[j];
      if (ch === "'" && s[j - 1] !== '\\') q = !q;
      else if (!q && ch === '(') depth++;
      else if (!q && ch === ')') { depth--; if (depth === 0) break; }
    }
    out += s.slice(i, idx) + fn(s.slice(start, j));
    i = j + 1;
  }
  return out;
}
function splitArgs(arg) {
  const out = []; let d = 0, q = false, cur = '';
  for (let k = 0; k < arg.length; k++) {
    const ch = arg[k];
    if (ch === "'" && arg[k - 1] !== '\\') q = !q;
    if (!q) { if (ch === '(') d++; else if (ch === ')') d--; else if (ch === ',' && d === 0) { out.push(cur); cur = ''; continue; } }
    cur += ch;
  }
  out.push(cur);
  return out.map((x) => x.trim());
}

export function pgize(sql) {
  let s = sql;
  // no-arg / keyword forms first (so nested forms translate cleanly)
  s = s.replace(/\bunix_timestamp\(\)/gi, '(extract(epoch from now()))');
  s = s.replace(/\bcurrent_date\(\)/gi, 'current_date');
  // map/array lookups: element_at(m, 'k') -> (m->>'k')  (facts is JSONB)
  s = replaceFn(s, 'element_at', (a) => { const [m, k] = splitArgs(a); return `(${m}->>${k})`; });
  // try_cast(x AS DOUBLE) -> safe numeric cast (NULL on non-numeric), no helper function/privilege needed
  s = replaceFn(s, 'try_cast', (a) => {
    const x = a.replace(/\s+AS\s+\w+\s*$/i, '').trim();
    return `(CASE WHEN (${x}) ~ '^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$' THEN (${x})::double precision ELSE NULL END)`;
  });
  // count_if(c) -> count(*) FILTER (WHERE c)
  s = replaceFn(s, 'count_if', (a) => `count(*) FILTER (WHERE ${a})`);
  // time: from_unixtime(x) -> to_timestamp(x); to_date(x) -> (x)::date; hour(x) -> extract(hour from x)
  s = replaceFn(s, 'from_unixtime', (a) => `to_timestamp(${a})`);
  s = replaceFn(s, 'to_date', (a) => `(${a})::date`);
  s = replaceFn(s, 'hour', (a) => `extract(hour from ${a})`);
  // date_sub(d, n) -> (d - n)
  s = replaceFn(s, 'date_sub', (a) => { const [d, n] = splitArgs(a); return `(${d} - (${n}))`; });
  // round(double, n) -> round((double)::numeric, n)   (PG round/2 needs numeric)
  s = replaceFn(s, 'round', (a) => { const p = splitArgs(a); return p.length === 2 ? `round((${p[0]})::numeric, ${p[1]})` : `round(${a})`; });
  // multi-arg count(distinct a,b,c) -> count(distinct (a,b,c))  (PG row form)
  s = replaceFn(s, 'count', (a) => {
    const t = a.trim();
    const m = /^distinct\s+/i.exec(t);
    if (m) { const inner = t.slice(m[0].length); if (splitArgs(inner).length > 1) return `count(distinct (${inner}))`; }
    return `count(${a})`;
  });
  // slice(array_sort(collect_set(x)), start, len) -> to_json((array_agg(distinct x order by x))[start:end])::text
  s = s.replace(/slice\(\s*array_sort\(\s*collect_set\(\s*([^()]+?)\s*\)\s*\)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)/gi,
    (_m, col, start, len) => `to_json((array_agg(distinct ${col} order by ${col}))[${start}:${Number(start) + Number(len) - 1}])::text`);
  return s;
}

// ---- result key re-casing (lowercase -> original camelCase) --------------------------------------------
let _colmap = new Map();
function recase(rows) {
  if (!rows.length || !_colmap.size) return rows;
  return rows.map((r) => { const o = {}; for (const k in r) o[_colmap.get(k) || k] = r[k]; return o; });
}

let _inited = null;
function init() {
  if (_inited) return _inited;
  _inited = (async () => {
    const m = new Map();
    try {
      const r = await pool().query('SELECT lc, orig FROM public."_colcase"');
      r.rows.forEach((x) => m.set(x.lc, x.orig));
    } catch { /* loader not run yet — recase becomes a no-op */ }
    // camelCase aliases that live ONLY in BFF SQL (never a stored column)
    for (const a of ['avgScore', 'distinctValues', 'lastHour', 'prevHour', 'lastTs']) m.set(a.toLowerCase(), a);
    _colmap = m;
  })();
  return _inited;
}

const _cache = new Map();
// Run a statement against Lakebase; returns array of row objects {camelCaseCol: value}. Cached for ttlMs
// (same statement-keyed TTL cache as databricks.js, so resolvers are unchanged).
export async function sql(statement, ttlMs = 4000) {
  await init();
  const hit = _cache.get(statement);
  if (hit && Date.now() < hit.exp) return hit.val;
  const translated = pgize(statement);
  let rows;
  try {
    const r = await pool().query(translated);
    rows = recase(r.rows);
  } catch (e) {
    throw new Error('lakebase sql: ' + (e.message || e) + ' :: ' + translated.slice(0, 240));
  }
  _cache.set(statement, { val: rows, exp: Date.now() + ttlMs });
  return rows;
}
export async function sql1(statement, ttlMs) { return (await sql(statement, ttlMs))[0] || {}; }
