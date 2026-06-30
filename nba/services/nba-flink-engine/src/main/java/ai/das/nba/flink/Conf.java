package ai.das.nba.flink;

import java.io.Serializable;

/**
 * Resolved runtime config. Like the KStreams engine, the Flink job is additive + mode-gated:
 *   NBA_FLINK_MODE = shadow | authoritative   (default shadow)
 *     shadow        — sinks write ".shadow" topics, NO Redis write-through; drives nothing (diff/measure safely).
 *     authoritative — writes the real topics + the Redis mirrors so it can be the writer for the live system.
 */
public class Conf implements Serializable {
    public String bootstrap;
    public String group;
    public boolean authoritative;
    public boolean scoreEnabled;
    public int parallelism;
    public long checkpointMs;
    public String redisHost;
    public int redisPort;
    public boolean redisWriteThrough;
    public long debounceSeconds;
    public long throttleRecheckSeconds;
    public int debounceMaxRechecks;
    public long dispositionStepMs;
    public double hardFraction;
    public boolean entryEarliest;

    public String memberFacts, snapshots, evaluations, definitions, facts, dlq, activations;

    public static Conf fromEnv() {
        Conf c = new Conf();
        c.bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        c.group = env("NBA_FLINK_GROUP", "nba-flink-engine");
        c.authoritative = "authoritative".equalsIgnoreCase(env("NBA_FLINK_MODE", "shadow"));
        // Internal score stage: ON by default (local is self-contained). Turn OFF in prod so the Databricks RL
        // scorer (nba_ml_score_rl, already subscribed to nba.evaluations) is the sole writer of nba.score.* —
        // its scores fold back through the snapshot via nba.member.facts (avoids double-scoring).
        c.scoreEnabled = !"off".equalsIgnoreCase(env("NBA_FLINK_SCORE", "on"));
        c.parallelism = Integer.parseInt(env("NBA_FLINK_PARALLELISM", "1"));
        c.checkpointMs = Long.parseLong(env("NBA_FLINK_CHECKPOINT_MS", "10000"));
        c.redisHost = env("NBA_REDIS_HOST", "nba-redis");
        c.redisPort = Integer.parseInt(env("NBA_REDIS_PORT", "6379"));
        c.redisWriteThrough = c.authoritative && !"false".equalsIgnoreCase(env("NBA_FLINK_REDIS_WRITE", "true"));
        c.debounceSeconds = Long.parseLong(env("NBA_DEBOUNCE_SECONDS", "60"));
        c.throttleRecheckSeconds = Long.parseLong(env("NBA_THROTTLE_RECHECK_SECONDS", "5"));
        c.debounceMaxRechecks = Integer.parseInt(env("NBA_DEBOUNCE_MAX_RECHECKS", "4"));
        c.dispositionStepMs = Long.parseLong(env("NBA_DISPOSITION_STEP_MS", "1500"));
        c.hardFraction = Double.parseDouble(env("NBA_HARD_FRACTION", "0.4"));
        // The member.facts ENTRY source normally starts at the live edge (latest) — additive, no history replay
        // (see NbaFlinkApp.sourceStream). For a frozen-input REPLAY (the equivalence proof seeds members, THEN
        // starts the engine), set NBA_OFFSET_RESET=earliest so the entry source folds the pre-seeded history and
        // produces shadow output to diff. Inter-stage .shadow sources stay latest — those topics are freshly
        // recreated (empty), so latest==earliest==offset 0 for them.
        c.entryEarliest = "earliest".equalsIgnoreCase(env("NBA_OFFSET_RESET", "latest"));
        c.memberFacts = env("NBA_MEMBER_FACTS", "nba.member.facts");
        c.snapshots = env("NBA_TOPIC_OUT", "nba.snapshots");
        c.evaluations = env("NBA_EVALUATIONS_TOPIC", "nba.evaluations");
        c.definitions = env("NBA_DEFINITIONS_TOPIC", "nba.definitions");
        c.facts = env("NBA_FACTS_TOPIC", "nba.facts");
        c.dlq = env("NBA_DLQ", "nba.dlq.flink-engine");
        c.activations = env("NBA_ACT_TOPIC", "nba.activations");
        return c;
    }

    /** Real topic when authoritative, a ".shadow" sibling otherwise. */
    public String sink(String topic) { return authoritative ? topic : topic + ".shadow"; }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
