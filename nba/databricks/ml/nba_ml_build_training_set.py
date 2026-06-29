# Databricks notebook source
# BUILD TRAINING SET — point-in-time supervised examples from real history, STATE-TRANSITION anchored.
#   anchor    = nba.actionstate.{action}.{channel} == 'IN_PROCESS'  (one row per REAL send)
#   label     = the episode's terminal actionstate (HARD_COMPLETED=1 ; EXPIRED/FAILED/DECLINED=0 ; else excluded)
#   features  = the member's snapshot AS OF the send (latest updatedTs <= decisionTs) -> no label leakage
#   propensity= logged action-selection prob if present, else 0.5 (the policy is argmax; ML owns exploration)
# Output: {ml_ns}.training_examples
#
# We anchor on the IN_PROCESS STATE TRANSITION, NOT the router's silver_activations DISPATCH row. The workflow
# emits IN_PROCESS itself on DISPATCH (after debounce + throttle), so it's the same "real send" filter — but the
# DISPATCH activation row carries a NULL entityId, which collapsed the (entityId, actionId, channel) label join
# to ~0 and starved training. The actionstate facts carry the member entityId, and anchor + outcome now come from
# the SAME silver_fact_history, so the join key is consistent by construction. (No router change needed.)
#
# NOTE the propensity caveat: unbiased off-policy learning wants the TRUE p(chosen action | policy). Today the
# router is argmax (no logged propensity); predictions_log fills it in once the ML layer logs exploration probs.

# COMMAND ----------

# MAGIC %run ./nba_ml_common

# COMMAND ----------

import json, time as _time
ML_NS, SRC_NS = ml_widgets()
dbutils.widgets.text("train_window_hours", "")           # RECENCY: only train on sends within the last N hours.
_TWH = dbutils.widgets.get("train_window_hours").strip()  # Empty = all history. The adaptation demo passes a window
_CUTOFF = int(_time.time() * 1000) - int(float(_TWH) * 3600000) if _TWH else None   # matching the CURRENT world so a
W = __import__("pyspark.sql.window", fromlist=["Window"]).Window                    # fresh shift actually moves it.

# all per-channel workflow states for every member (the source of truth for "what happened on each channel")
states_all = (spark.table(f"{SRC_NS}.silver_fact_history").where("factClass = 'actionstate'")
              .select("entityId", "actionId", "channel", F.col("value").alias("state"), F.col("eventTs").alias("stTs"))
              .where("entityId IS NOT NULL AND channel IS NOT NULL"))

# ANCHOR: each IN_PROCESS = a real send went out on this channel (emitted on DISPATCH, post debounce+throttle).
# Carries the member entityId -> no dependency on the router's (null-entityId) DISPATCH row.
sends = (states_all.where("state = 'IN_PROCESS'")
         .select("entityId", "actionId", "channel", F.col("stTs").alias("decisionTs")).dropDuplicates())
if _CUTOFF is not None:
    sends = sends.where(F.col("decisionTs") >= _CUTOFF)
    print(f"recency window: {_TWH}h -> sends since {_CUTOFF}")

# bound each send-episode by the NEXT send for the same (entity, action, channel) so a later episode's states
# don't bleed into this one.
sends = sends.withColumn("nextTs", F.lead("decisionTs").over(
    W.partitionBy("entityId", "actionId", "channel").orderBy("decisionTs")))

# CONTEXTUAL fatigue state AS OF each send (point-in-time, no leakage): prior sends in the trailing 14d on THIS channel
# / THIS action, and recency since the last contact. Range frame on decisionTs (ms), excluding the current send.
_DAY = 86400 * 1000
sends = (sends
    .withColumn("thisChannelRecentN", F.count(F.lit(1)).over(
        W.partitionBy("entityId", "channel").orderBy("decisionTs").rangeBetween(-14 * _DAY, -1)).cast("double"))
    .withColumn("thisActionRecentN", F.count(F.lit(1)).over(
        W.partitionBy("entityId", "actionId").orderBy("decisionTs").rangeBetween(-14 * _DAY, -1)).cast("double"))
    .withColumn("_lastTs", F.max("decisionTs").over(
        W.partitionBy("entityId").orderBy("decisionTs").rangeBetween(-14 * _DAY, -1)))
    .withColumn("daysSinceLastContact", F.when(F.col("_lastTs").isNull(), F.lit(14.0))
                .otherwise((F.col("decisionTs") - F.col("_lastTs")) / _DAY).cast("double")).drop("_lastTs"))

# the states reached in THIS episode's window. label_from_states requires it reached PRESENTED (the member got the
# offer) for a valid 0/1 — sends that never delivered (IN_PROCESS->EXPIRED/FAILED) are excluded, not counted 0.
ep = (sends.join(states_all, ["entityId", "actionId", "channel"])
      .where("stTs >= decisionTs AND (nextTs IS NULL OR stTs < nextTs)"))
agg = ep.groupBy("entityId", "actionId", "channel", "decisionTs").agg(F.collect_set("state").alias("states"))

@F.udf(T.IntegerType())
def label_udf(states):
    return label_from_states(states)

labeled = (agg.withColumn("label", label_udf("states"))
           .where("label IS NOT NULL")
           .withColumn("source", F.lit("real")))
# re-attach the point-in-time contextual fatigue features (dropped by the groupBy) on the send key
labeled = labeled.join(sends.select("entityId", "actionId", "channel", "decisionTs",
                                    "thisChannelRecentN", "thisActionRecentN", "daysSinceLastContact"),
                       ["entityId", "actionId", "channel", "decisionTs"])

# POINT-IN-TIME features: the member's snapshot AS OF the send (latest updatedTs <= decisionTs). Inner join on
# entityId drops sends with no prior snapshot (cold-start, no features to learn from).
snaps = (spark.table(f"{SRC_NS}.silver_snapshots")
         .select("entityId", "nbaId", F.col("updatedTs").alias("snapTs"), parse_features_udf("factsJson").alias("f")))
asof = labeled.join(snaps, "entityId").where("snapTs <= decisionTs")
pit = W.partitionBy("entityId", "actionId", "channel", "decisionTs").orderBy(F.col("snapTs").desc())
feat = (asof.withColumn("_rn", F.row_number().over(pit)).where("_rn = 1").drop("_rn")
        .select("entityId", "nbaId", "actionId", "channel", "decisionTs", "label", "source",
                "thisChannelRecentN", "thisActionRecentN", "daysSinceLastContact",
                *[F.col(f"f.{c}").alias(c) for c in GOLD_FEATS]))
examples = feat.where("daysSinceLogin IS NOT NULL")   # require a real point-in-time snapshot

# REAL propensity from predictions_log (the scorer logs p(chosen | policy) at score time) if present, else 0.5.
# Deduped on the join key so a multi-row log can't fan out the example set.
try:
    plog = (spark.table(f"{ML_NS}.predictions_log")
            .select("nbaId", "actionId", "channel", F.col("propensity").alias("logged_prop"))
            .dropDuplicates(["nbaId", "actionId", "channel"]))
    examples = (examples.join(plog, ["nbaId", "actionId", "channel"], "left")
                .withColumn("propensity", F.coalesce(F.col("logged_prop"), F.lit(0.5))).drop("logged_prop"))
except Exception:
    examples = examples.withColumn("propensity", F.lit(0.5))

cols = ["nbaId", "entityId", "actionId", "channel", "decisionTs"] + FEATURE_COLS + ["label", "propensity", "source"]
out = examples.select(*cols).withColumn("ingestTs", F.current_timestamp())
out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{ML_NS}.training_examples")
n = out.count(); pos = out.where("label = 1").count()
print(f"training_examples: {n} rows  positives={pos}  base_rate={pos/max(n,1):.3f}")
dbutils.notebook.exit(json.dumps({"rows": n, "positives": pos}))
