// Databricks SQL client for the Command Center BFF.
// SP OAuth M2M -> SQL Statement Execution API, with a small TTL cache so real-time polling from the
// UI doesn't hammer the warehouse. This is the analytics path ("can be slower") — never the hot path.
const HOST = (process.env.DATABRICKS_HOST || '').replace(/\/$/, '');
const CID = process.env.DATABRICKS_CLIENT_ID;
const SECRET = process.env.DATABRICKS_CLIENT_SECRET;
const WAREHOUSE_ENV = process.env.DATABRICKS_WAREHOUSE_ID;

export const NS = process.env.NBA_LAKE_NS || 'workspace.nba_poc';
export const lakeConfigured = Boolean(HOST && CID && SECRET);

let _tok = null, _tokExp = 0, _wh = null;

async function token() {
  if (_tok && Date.now() < _tokExp) return _tok;
  const r = await fetch(HOST + '/oidc/v1/token', {
    method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ grant_type: 'client_credentials', client_id: CID, client_secret: SECRET, scope: 'all-apis' }),
  });
  if (!r.ok) throw new Error('databricks token: ' + r.status);
  const j = await r.json();
  _tok = j.access_token; _tokExp = Date.now() + (j.expires_in - 60) * 1000;
  return _tok;
}

async function warehouse() {
  if (WAREHOUSE_ENV) return WAREHOUSE_ENV;
  if (_wh) return _wh;
  const r = await fetch(HOST + '/api/2.0/sql/warehouses', { headers: { Authorization: 'Bearer ' + await token() } });
  const j = await r.json();
  _wh = (j.warehouses || [])[0]?.id;
  if (!_wh) throw new Error('no SQL warehouse found');
  return _wh;
}

const _cache = new Map();

// Run a SQL statement; returns array of row objects {col: value}. Cached for ttlMs.
export async function sql(statement, ttlMs = 4000) {
  const hit = _cache.get(statement);
  if (hit && Date.now() < hit.exp) return hit.val;
  const wh = await warehouse();
  const auth = { Authorization: 'Bearer ' + await token() };
  let r = await fetch(HOST + '/api/2.0/sql/statements', {
    method: 'POST', headers: { ...auth, 'Content-Type': 'application/json' },
    body: JSON.stringify({ warehouse_id: wh, statement, wait_timeout: '50s', format: 'JSON_ARRAY' }),
  });
  let j = await r.json();
  while (j.status && ['PENDING', 'RUNNING'].includes(j.status.state)) {
    await new Promise((s) => setTimeout(s, 800));
    r = await fetch(HOST + '/api/2.0/sql/statements/' + j.statement_id, { headers: auth });
    j = await r.json();
  }
  if (j.status && j.status.state !== 'SUCCEEDED') throw new Error('databricks sql: ' + (j.status.error?.message || j.status.state));
  const cols = (j.manifest?.schema?.columns || []).map((c) => c.name);
  const rows = (j.result?.data_array || []).map((row) => Object.fromEntries(row.map((v, i) => [cols[i], v])));
  const val = rows;
  _cache.set(statement, { val, exp: Date.now() + ttlMs });
  return val;
}

// Convenience: first row, or {} .
export async function sql1(statement, ttlMs) { return (await sql(statement, ttlMs))[0] || {}; }
export const num = (v) => (v == null ? 0 : Number(v));
