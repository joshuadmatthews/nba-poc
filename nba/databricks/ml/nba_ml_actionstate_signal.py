# Databricks notebook source
# Is the ACTION-STATE -> OUTCOME signal actually in REAL journey history? Test the hypothesis directly: does a channel
# that SOFT_COMPLETED (member engaged but didn't convert) have a HIGHER subsequent HARD_COMPLETE rate than one that
# didn't? If yes, the warm/follow-up effect is REAL + learnable -> we replace the hand-coded WARM_BOOST with the learned
# effect. If no, it isn't in the data and the hand-coded effect was unfounded. Sourced entirely from the facts.
import json
from pyspark.sql import functions as F
dbutils.widgets.text("src_catalog", "workspace"); dbutils.widgets.text("src_schema", "nba_poc")
SRC = f"{dbutils.widgets.get('src_catalog')}.{dbutils.widgets.get('src_schema')}"

ast = (spark.table(f"{SRC}.silver_fact_history").where("factClass = 'actionstate'")
       .select("entityId", "channel", F.upper(F.col("value")).alias("state")))
print("actionstate fact value distribution:")
ast.groupBy("state").count().orderBy(F.desc("count")).show(20, False)

# per (member, channel): did it ever soft-complete? ever hard-complete?
g = ast.groupBy("entityId", "channel").agg(
    F.max(F.when(F.col("state") == "SOFT_COMPLETED", 1).otherwise(0)).alias("had_soft"),
    F.max(F.when(F.col("state") == "HARD_COMPLETED", 1).otherwise(0)).alias("had_hard"))
p = g.toPandas()
warm = p[p.had_soft == 1]; cold = p[p.had_soft == 0]
r_warm = float(warm.had_hard.mean()) if len(warm) else 0.0
r_cold = float(cold.had_hard.mean()) if len(cold) else 0.0
out = {"n_member_channel_pairs": int(len(p)), "n_soft_completed": int(len(warm)), "n_no_soft": int(len(cold)),
       "hard_rate_AFTER_soft": round(r_warm, 3), "hard_rate_NO_soft": round(r_cold, 3),
       "warm_lift": round(r_warm - r_cold, 3), "rel_lift": round((r_warm - r_cold) / r_cold, 2) if r_cold else None}
print("\nSIGNAL:", json.dumps(out))
print("=> warm_lift > 0 means soft-completed channels convert MORE -> the action-state effect is REAL in the data.")
dbutils.notebook.exit(json.dumps(out))
