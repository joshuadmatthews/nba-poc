#!/usr/bin/env python3
"""Load the gold/silver tables the Command Center BFF reads from the Databricks lake into the Lakebase Postgres
('nba' db). A direct mirror (read via the SQL warehouse, INSERT into PG) — used instead of UC synced tables because
this POC account's metastore is rootless (no storage_root), so the synced-table DLT pipeline can't initialize. In a
properly-provisioned account this is a managed synced table with TRIGGERED refresh; here we run this loader (one-time
while gold is frozen, or on a schedule when the loop is live). Env: DBX_HOST, DBX_TOKEN, WH, PG_HOST, PG_USER, PG_PASS.

Columns are created LOWERCASE so the BFF's Databricks-dialect SQL (unquoted CamelCase identifiers, which Postgres
folds to lowercase) matches without rewriting every query. A `_colcase` table records lowercase->original casing so
the BFF can re-case result keys back to camelCase for the UI. Map-typed columns (gold_member_facts.facts) land as
JSONB so the rule funnel's element_at/key lookups work.
"""
import os, json
from decimal import Decimal
from databricks import sql as dbsql
import psycopg2
from psycopg2.extras import execute_values

NS = "workspace.nba_poc"
TABLES = ["gold_member_snapshot", "gold_member_idmap", "gold_member_facts", "gold_system_stats", "gold_model_card",
          "silver_activations", "silver_eval_eligible", "silver_snapshots", "silver_fact_history",
          "silver_milestones", "dim_definitions", "action_fact_map"]
# columns that arrive as a Spark MAP (dict) — store as JSONB so the rule funnel can do key lookups (facts->>'k')
JSONB_COLS = {"facts"}

def is_map(v):
    return isinstance(v, dict)

def pgtype(name, vals):
    if name.lower() in JSONB_COLS:
        return "jsonb"
    for v in vals:
        if v is None: continue
        if isinstance(v, bool): return "boolean"
        if isinstance(v, int): return "bigint"
        if isinstance(v, (float, Decimal)): return "double precision"
        if is_map(v): return "jsonb"
        return "text"
    return "text"

def coerce(v, pt):
    if pt == "jsonb":
        if v is None: return None
        if isinstance(v, dict): return json.dumps(v)
        # the Databricks connector returns a Spark map<k,v> as a LIST of [k, v] pairs — rebuild an OBJECT so the
        # rule funnel's key lookups (facts->>'k') work (an array would make every fact read as missing -> default 0).
        if isinstance(v, list):
            try: return json.dumps({kv[0]: kv[1] for kv in v})
            except Exception: return json.dumps(v)
        return v if isinstance(v, str) else json.dumps(v)
    if v is None or isinstance(v, (bool, int, str)): return v
    if isinstance(v, (float, Decimal)): return float(v)
    return str(v)

dbx = dbsql.connect(server_hostname=os.environ["DBX_HOST"].replace("https://", "").rstrip("/"),
                    http_path=f"/sql/1.0/warehouses/{os.environ['WH']}", access_token=os.environ["DBX_TOKEN"])
pg = psycopg2.connect(host=os.environ["PG_HOST"], port=5432, dbname="nba", user=os.environ["PG_USER"],
                      password=os.environ["PG_PASS"], sslmode="require")
pg.autocommit = True
pgc = pg.cursor()

# casing metadata: lowercase column name -> original (camelCase) name, for the BFF result re-caser
pgc.execute('CREATE TABLE IF NOT EXISTS public."_colcase" (lc text PRIMARY KEY, orig text)')
casing = {}

for t in TABLES:
    try:
        c = dbx.cursor(); c.execute(f"SELECT * FROM {NS}.{t}")
        cols = [d[0] for d in c.description]
        rows = c.fetchall(); c.close()
        sample = rows[:300]
        types = [pgtype(cols[i], [r[i] for r in sample]) for i in range(len(cols))]
        lcols = [c.lower() for c in cols]
        for orig, lc in zip(cols, lcols):
            casing[lc] = orig
        pgc.execute(f'DROP TABLE IF EXISTS public."{t}"')
        pgc.execute(f'CREATE TABLE public."{t}" (' + ", ".join(f'"{lcols[i]}" {types[i]}' for i in range(len(cols))) + ")")
        if rows:
            data = [[coerce(r[i], types[i]) for i in range(len(cols))] for r in rows]
            collist = ", ".join(f'"{ci}"' for ci in lcols)
            execute_values(pgc, f'INSERT INTO public."{t}" ({collist}) VALUES %s', data, page_size=2000)
        print(f"  {t:24} {len(rows):>7} rows  ({len(cols)} cols)", flush=True)
    except Exception as e:
        print(f"  {t:24} FAILED  {type(e).__name__}: {str(e)[:160]}", flush=True)

# persist the casing map (camelCase aliases that exist only in BFF SQL are added by the BFF itself)
if casing:
    execute_values(pgc, 'INSERT INTO public."_colcase" (lc, orig) VALUES %s ON CONFLICT (lc) DO UPDATE SET orig=EXCLUDED.orig',
                   list(casing.items()), page_size=500)

# helpful indexes for the UI's point lookups (columns are lowercase now)
for ix in ['CREATE INDEX IF NOT EXISTS ix_gms_ent ON public.gold_member_snapshot(entityid)',
           'CREATE INDEX IF NOT EXISTS ix_gms_nba ON public.gold_member_snapshot(nbaid)',
           'CREATE INDEX IF NOT EXISTS ix_act_corr ON public.silver_activations(correlationid)',
           'CREATE INDEX IF NOT EXISTS ix_act_nba ON public.silver_activations(nbaid)',
           'CREATE INDEX IF NOT EXISTS ix_fh_ent ON public.silver_fact_history(entityid)',
           'CREATE INDEX IF NOT EXISTS ix_idmap_ent ON public.gold_member_idmap(entityid)']:
    try: pgc.execute(ix)
    except Exception as e: print("  idx skip:", str(e)[:80])
print(f"  indexes created · {len(casing)} column-casing entries", flush=True)
dbx.close(); pg.close()
