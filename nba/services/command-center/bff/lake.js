// Analytics-backend selector. The BFF reads gold/silver either from the Databricks SQL warehouse
// (databricks.js) or from Lakebase Postgres (lakebase.js) — chosen ONCE at startup by env. Both expose
// the identical { sql, sql1, num, NS, lakeConfigured } surface, so the resolvers never change.
//   NBA_LAKEBASE_HOST set -> Lakebase (scale-to-zero; debug the whole app with the warehouse stopped).
//   otherwise            -> the SQL warehouse (original behaviour).
const useLakebase = Boolean(process.env.NBA_LAKEBASE_HOST);
const mod = await import(useLakebase ? './lakebase.js' : './databricks.js');

export const sql = mod.sql;
export const sql1 = mod.sql1;
export const num = mod.num;
export const NS = mod.NS;
export const lakeConfigured = mod.lakeConfigured;
export const backend = useLakebase ? 'lakebase' : 'warehouse';
