package ai.das.nba.rules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;
import org.kie.internal.utils.KieHelper;
import org.kie.api.io.ResourceType;
import redis.clients.jedis.JedisPooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NBA Rules Engine (Drools).
 *
 * Step 1: consumes nba.definitions, STORES the actions + global/channel rules, and
 * DYNAMICALLY builds a Drools pack — it compiles the simple structured logic (condition
 * trees the Command Center authored) into DRL and a KieBase, rebuilt whenever a
 * definition changes. (Step 2 evaluates snapshots against it and emits nba.evaluations,
 * attaching the stored action to each eligible action-channel.)
 */
public class RulesEngine {
    private static final Logger log = LoggerFactory.getLogger(RulesEngine.class);
    static final ObjectMapper M = new ObjectMapper();

    // SCORE TTL. A score is a PREDICTION with a shelf life; the snapshot's event-time LWW only guards against
    // out-of-order (older) scores, not against a score that is simply OLD in wall-clock terms (a quiet member, a
    // missed bulk-score run, or a score from a now-superseded model). Past NBA_SCORE_TTL_SECONDS we treat an
    // nba.score.* fact as ABSENT in the evaluation, so the router never acts on a stale score and the resulting
    // eligible-but-unscored eval re-triggers the scorer. Applies ONLY to nba.score.* — completions / lifecycle
    // states / dispositions / milestones are durable facts and never expire. 0 (default) disables = carry-forever
    // (prior behavior). Refresh path: the daily bulk re-scores everyone, so set TTL ~1.5-2x the bulk cadence and
    // the gate is a pure safety net for scores the bulk missed.
    static final long SCORE_TTL_MS = Long.parseLong(System.getenv().getOrDefault("NBA_SCORE_TTL_SECONDS", "0")) * 1000L;

    // Stored definitions (so we can attach actions to results + rebuild the pack).
    static final Map<String, JsonNode> actions = new ConcurrentHashMap<>();
    static final Map<String, JsonNode> globalRules = new ConcurrentHashMap<>();
    static final Map<String, JsonNode> channelRules = new ConcurrentHashMap<>();
    // Milestone defs (name + structured logic). Evaluated per member in Java (treePass), NOT Drools.
    // Completion latches as a durable nba.milestone.{id} FACT on the snapshot and rides every eval thereafter.
    static final Map<String, JsonNode> milestones = new ConcurrentHashMap<>();

    // GLOBAL throttle level. The lake emits nba.throttle.{channel}.daily and it rides in on
    // whichever member snapshot the snapshot-builder happened to be batching. We retain the latest
    // per key (event-time LWW) here and apply it to EVERY member's evaluation — so a population-wide
    // channel cap throttles everyone, not just the member whose snapshot carried it in.
    // value = long[]{level, eventTs}.
    static final Map<String, long[]> GLOBAL_THROTTLE = new ConcurrentHashMap<>();

    // Channels SATURATED for the day. The Temporal gate predicts "this channel can't clear its backlog
    // before midnight" and suppresses an action with reason=throttle; that rides the state fact to the
    // definitions topic (THROTTLE_HOT:{channel}). We mark the channel ineligible until midnight (the
    // daily budget resets) — so ML re-scores the population onto other channels. value = hotUntil (ms).
    static final Map<String, Long> CHANNEL_HOT_UNTIL = new ConcurrentHashMap<>();

    // Operator suppression IS applied here now. A suppressed action ({actionId}) or action-channel
    // ({actionId}.{channel}) rides the definitions stream as ACTION_SUPPRESS:{target}, retained in this
    // in-memory set, and marks the matching action-channel eligible=false on the NEXT eval — so the
    // action-router's in-flight SUPPRESS (fires on !eligible && cancellable) naturally cancels it. The
    // read-edge filters (action-router won't activate it; inbound serve strips it) remain as defense-in-
    // depth; IMMEDIATE in-flight cancellation is handled by the Temporal SuppressionWorkflow. Mirrors the
    // action-router's own SUPPRESSED set.
    static final java.util.Set<String> SUPPRESSED = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Operator-suppressed if the WHOLE action is suppressed ({actionId}) or just this action-channel ({actionId}.{channel}). */
    static boolean isOperatorSuppressed(String actionId, String channel) {
        return SUPPRESSED.contains(actionId) || SUPPRESSED.contains(actionId + "." + channel);
    }

    static volatile KieBase kieBase;

    // Coarse projection of the disposition-driven workflow state: is a workflow in-flight (occupying the slot)?
    // The ACTION ROUTER consumes this `active` flag, NOT the individual states — it stays decoupled from the
    // state machine's transitions. Terminals (FAILED/HARD_COMPLETED/SUPPRESSED) + null are NOT active (free).
    static final java.util.Set<String> ACTIVE_STATES = java.util.Set.of(
            "CREATED", "IN_PROCESS", "SUPPRESSING", "PRESENTED", "SOFT_COMPLETED", "DECLINED");

    // The single eligibility object lives at nba:eligibility:{nbaId} — WRITTEN by the action-router (kafka ->
    // router -> redis). The rules engine only READS it, to detect change (skip the emit when nothing moved).
    static final String ELIG_KEY = "nba:eligibility:";
    // CHANGE-DETECT source for the "skip duplicate eval" guard: "redis" reads the prior eligibility object from
    // nba:eligibility (the router-written copy — the ONLY thing this service reads from Redis) | "memory" keeps
    // each member's last-emitted [fullHash, eligHash] in-process, no Redis. Memory is safe because nba.snapshots
    // is member-partitioned, so this instance always owns the same members. Default redis (flag = the only change).
    // Trade-off: a rebalance/restart starts with an empty cache -> one harmless re-emit per member.
    static final String CHANGE_DETECT = env("NBA_ELIG_CHANGE_DETECT", "redis");
    static final java.util.Map<String, String[]> LAST_SIG = new java.util.concurrent.ConcurrentHashMap<>();
    // Completion + milestone are DETECTED here (goal rules over the snapshot) and ride the eval as flags. The durable
    // nba.completion.* / nba.milestone.* FACTS are PUBLISHED by the action-router (a member-facts producer), so the
    // engine stays a pure evaluator with no side effects. Opt-out derives off the durable STOP/Unsubscribe
    // disposition facts. So everything the engine needs is re-derivable from the snapshot — no side-latch keys.

    public static void main(String[] args) {
        String bootstrap = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String defsTopic = env("NBA_DEFINITIONS_TOPIC", "nba.definitions");
        String snapTopic = env("NBA_SNAPSHOTS_TOPIC", "nba.snapshots");
        String evalTopic = env("NBA_EVALUATIONS_TOPIC", "nba.evaluations");

        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        KafkaProducer<String, String> producer = new KafkaProducer<>(pp);
        JedisPooled redis = new JedisPooled(env("NBA_REDIS_HOST", "nba-redis"), 6379);

        // Prometheus /metrics on a side port (no HTTP surface otherwise).
        Metrics.serve(Integer.parseInt(env("NBA_METRICS_PORT", "9404")));

        // Thread A: keep the Drools pack current from definitions.
        new Thread(() -> runDefsConsumer(bootstrap, defsTopic), "defs-consumer").start();
        // Thread (main): evaluate snapshots against the current pack.
        runSnapshotsConsumer(bootstrap, snapTopic, evalTopic, producer, redis);
    }

    static void runDefsConsumer(String bootstrap, String defsTopic) {
        Properties cp = consumerProps(bootstrap, "rules-engine-defs-" + UUID.randomUUID().toString().substring(0, 8));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
        consumer.subscribe(List.of(defsTopic));
        log.info("definitions consumer up: " + defsTopic);
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            boolean changed = false;
            for (ConsumerRecord<String, String> r : recs) {
                try { if (applyDef(r.key(), r.value())) changed = true; }
                catch (Exception e) { log.warn("bad def " + r.key() + ": " + e); }
            }
            if (changed) rebuild();   // structural changes only — throttle-level updates don't rebuild
        }
    }

    static void runSnapshotsConsumer(String bootstrap, String snapTopic, String evalTopic,
                                     KafkaProducer<String, String> producer, JedisPooled redis) {
        // Wait for the initial pack so we never evaluate against an empty KieBase.
        while (kieBase == null) {
            try { Thread.sleep(400); } catch (InterruptedException ie) { return; }
        }
        Properties cp = consumerProps(bootstrap, "rules-engine-snapshots");
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cp);
        consumer.subscribe(List.of(snapTopic));
        log.info("snapshots consumer up: " + snapTopic + " -> " + evalTopic);
        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                long t0 = System.nanoTime();
                try { evaluate(r.value(), producer, evalTopic, redis); }
                catch (Exception e) { log.warn("eval error: " + e + " :: " + r.value()); Metrics.counter("nba_rules_eval_errors_total").increment(); }
                finally { Metrics.timer("nba_rules_eval_seconds").record(System.nanoTime() - t0, java.util.concurrent.TimeUnit.NANOSECONDS); }
            }
        }
    }

    static Properties consumerProps(String bootstrap, String group) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return cp;
    }

    /** Evaluate one snapshot against the current Drools pack; emit the eligible
     *  action-channels (with the stored action ATTACHED) to nba.evaluations + Redis. */
    static void evaluate(String snapJson, KafkaProducer<String, String> producer, String evalTopic, JedisPooled redis) throws Exception {
        JsonNode snap = M.readTree(snapJson);
        String nbaId = snap.path("nbaId").asText();
        Map<String, Object> f = new HashMap<>();
        JsonNode facts = snap.get("facts");
        if (facts != null) {
            long nowMs = System.currentTimeMillis();
            for (Iterator<Map.Entry<String, JsonNode>> it = facts.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String fk = e.getKey();
                // SCORE TTL: drop a stale score so it can't drive an action. The score's eventTs rides on the
                // snapshot fact envelope ({value,valueType,eventTs,source}); past the TTL we skip it entirely ->
                // f.get("nba.score...") is null below -> the channelAction emits score=null -> the router won't
                // pick it AND the scorer re-scores the now-unscored-but-eligible action. nba.score.* only.
                if (SCORE_TTL_MS > 0 && fk.startsWith("nba.score.")) {
                    long ets = e.getValue().path("eventTs").asLong(0L);
                    if (ets > 0L && (nowMs - ets) > SCORE_TTL_MS) {
                        Metrics.counter("nba_rules_scores_expired_total").increment();
                        continue;   // stale score -> absent from this evaluation
                    }
                }
                f.put(fk, typedValue(e.getValue()));
            }
        }
        // Apply the GLOBAL throttle level to THIS evaluation. It arrives on the broadcast definitions
        // stream (every instance gets it, current + freshly-initialized pods), retained in
        // GLOBAL_THROTTLE — authoritative for the channel cap for EVERY member, regardless of which
        // member's traffic last advanced it. This is the "make it global" step.
        for (Map.Entry<String, long[]> e : GLOBAL_THROTTLE.entrySet()) f.put(e.getKey(), e.getValue()[0]);

        // MILESTONES are durable as nba.milestone.{id} FACTS on the snapshot (the ROUTER publishes them). The
        // completed set = those facts UNION any whose logic passes this eval (first-pass detection — the fact is
        // still round-tripping). The engine only DETECTS here; it never emits.
        long milestoneTs = System.currentTimeMillis();
        Map<String, String> doneMs = new HashMap<>();
        java.util.Set<String> newMs = new java.util.LinkedHashSet<>();   // TRANSITION: logic passes this eval, no durable fact yet
        for (String fk : f.keySet()) if (fk.startsWith("nba.milestone.")) {
            Object v = f.get(fk); doneMs.put(fk.substring("nba.milestone.".length()), v == null ? "0" : v.toString());
        }
        for (Map.Entry<String, JsonNode> me : milestones.entrySet()) {
            JsonNode logic = me.getValue().get("logic");
            if (logic != null && !logic.isNull() && treePass(logic, f)) {
                if (!doneMs.containsKey(me.getKey())) newMs.add(me.getKey());   // passed now, fact not yet on the snapshot
                doneMs.putIfAbsent(me.getKey(), String.valueOf(milestoneTs));
            }
        }

        // HARD COMPLETION — per ACTION, drives eligibility. Completed = the action's `completion` goal passes this
        // eval (first-pass detection) OR the durable nba.completion.{actionId} FACT is present (published by the
        // router, or an explicit API/lake signal). The engine only DETECTS here and reads the fact for durability;
        // it never emits. The completed set feeds the auto-exclude + the hardCompleted flag the router reads.
        Map<String, String> doneCompleted = new HashMap<>();
        java.util.Set<String> newCompleted = new java.util.LinkedHashSet<>();   // TRANSITION: goal passes this eval, no durable fact yet
        for (Map.Entry<String, JsonNode> ae : actions.entrySet()) {
            JsonNode cTree = ae.getValue().get("completion");
            boolean byCriterion = cTree != null && !cTree.isNull() && treePass(cTree, f);
            boolean signalled = isTruthy(f.get("nba.completion." + ae.getKey()));
            if (byCriterion || signalled) doneCompleted.put(ae.getKey(), String.valueOf(milestoneTs));
            if (byCriterion && !signalled) newCompleted.add(ae.getKey());        // first eval the goal passes (no fact yet) -> the router publishes it
        }

        // OPT-OUT (built-in compliance channel rule): a channel is opted out if ANY action's latest disposition on
        // it reached the channel's opt-out raw status (email Unsubscribe / sms STOP). Those disposition facts are
        // terminal + PERMANENT on the snapshot (a dead channel never re-fires, so the STOP/Unsubscribe fact never
        // disappears), so opt-out is derived straight from them — no side-latch, and it can never silently re-open.
        Map<String, String> optedOutChannels = new HashMap<>();
        for (String fk : f.keySet()) {
            if (!fk.startsWith("nba.disposition.")) continue;
            int dot = fk.lastIndexOf('.');
            if (dot < 0) continue;
            String ch = fk.substring(dot + 1);
            String optoutRaw = OPTOUT_RAW.get(ch);
            Object dv = f.get(fk);
            if (optoutRaw != null && dv != null && optoutRaw.equals(dv.toString()))
                optedOutChannels.put(ch, String.valueOf(milestoneTs));
        }

        // ELIGIBILITY EVAL. NBA_RULES_MODE=kie offloads the Drools fireAllRules to the standalone nba-kie-server
        // (scalable for load tests); embedded (default) runs the KieSession inline. Either way we get the same
        // hit list. Everything else below (enrichment, channel-saturation, dedup, emit, milestones) stays here.
        List<String> hits;
        if (KIE_MODE) {
            hits = kieServerEval(nbaId, f);
            if (hits == null) return;          // KIE server unreachable -> skip this eval (the snapshot re-flows)
        } else {
            KieBase kb = kieBase;
            if (kb == null) return;
            hits = new ArrayList<>();
            KieSession session = kb.newKieSession();
            try {
                session.setGlobal("results", hits);
                session.insert(new Snap(nbaId, f));
                session.fireAllRules();
            } finally {
                session.dispose();
            }
        }

        // An evaluation is the snapshot with raw facts dropped, rule results added, and the ML scores CARRIED
        // OVER — so the action router reads everything off the eval and never needs the snapshot. It carries a
        // single unified channelActions[] (per action-channel) + the member's milestones[]. Each ChannelAction
        // self-describes: { actionId, channel, name, ttlSeconds, contentKey, score, eligible, active,
        // cancellable, softCompleted, hardCompleted, workflowState } — score is null until ML scores it; the
        // live workflowState rides up on the snapshot (state fact -> snapshot-builder -> here).
        ObjectNode eval = M.createObjectNode();
        eval.put("nbaId", nbaId);
        eval.put("entityType", snap.path("entityType").asText(""));
        eval.put("entityId", snap.path("entityId").asText(""));
        eval.put("correlationId", snap.path("correlationId").asText(""));
        eval.put("evaluatedAt", System.currentTimeMillis());
        ArrayNode chans = eval.putArray("channelActions");
        java.util.List<String> eligSig = new java.util.ArrayList<>();   // identity of the ELIGIBLE set
        java.util.List<String> fullSig = new java.util.ArrayList<>();   // eligibility + scores + states
        java.util.Set<String> hitSlugs = new java.util.HashSet<>(hits); // "aid::ch" that passed the Drools rules

        // Candidate slugs = every Drools hit, PLUS any action-channel that still has a live workflow (so an
        // in-flight action that fell out of eligibility, or one walking to HARD_COMPLETED, stays on the eval).
        java.util.LinkedHashMap<String, String[]> slugs = new java.util.LinkedHashMap<>();
        for (String r : hits) {
            String[] p = r.split("::", 2);
            if (p.length == 2 && actions.containsKey(p[0])) slugs.put(r, p);
        }
        for (String fk : f.keySet()) {
            if (!fk.startsWith("nba.actionstate.")) continue;
            Object v = f.get(fk);
            String st = v == null ? null : v.toString();
            if (st == null || !ACTIVE_STATES.contains(st)) continue;           // only live (occupying) workflows
            String rem = fk.substring("nba.actionstate.".length());
            int dot = rem.lastIndexOf('.');
            if (dot < 0) continue;
            slugs.putIfAbsent(rem.substring(0, dot) + "::" + rem.substring(dot + 1),
                    new String[]{rem.substring(0, dot), rem.substring(dot + 1)});
        }

        for (Map.Entry<String, String[]> se : slugs.entrySet()) {
            String slug = se.getKey(), aid = se.getValue()[0], ch = se.getValue()[1];
            JsonNode a = actions.get(aid);                                     // may be null: an orphan live workflow
            Object stt = f.get("nba.actionstate." + aid + "." + ch);          // live state rides up on the snapshot
            String state = stt == null ? null : stt.toString();
            boolean activeWf = state != null && ACTIVE_STATES.contains(state);
            boolean completed = doneCompleted.containsKey(aid);
            // eligible = passed Drools, channel NOT throttle-saturated (HOT, can't clear today — ML reroutes),
            // NOT retired-on-completion, and NOT operator-suppressed. A completed auto-exclude action stays eligible
            // only while its workflow is still walking (so the router bridges HARD_COMPLETE); once terminal it flips
            // eligible:false and, with no live workflow, drops off the next eval. autoExcludeOnCompletion=false
            // keeps it eligible. Operator suppression IS applied here now: a suppressed action/action-channel goes
            // eligible:false, so the router's in-flight SUPPRESS (on !eligible && cancellable) cancels it on the next
            // eval. The read-edge filters remain defense-in-depth; immediate cancellation = Temporal SuppressionWorkflow.
            Long hotUntil = CHANNEL_HOT_UNTIL.get(ch);
            boolean throttleHot = hotUntil != null && System.currentTimeMillis() < hotUntil;
            boolean autoExcluded = completed && a != null && a.path("autoExcludeOnCompletion").asBoolean(true) && !activeWf;
            boolean channelOptedOut = optedOutChannels.containsKey(ch);        // BUILT-IN opt-out channel rule: ineligible
            boolean eligible = hitSlugs.contains(slug) && !throttleHot && !autoExcluded && !channelOptedOut
                    && !isOperatorSuppressed(aid, ch);
            if (!eligible && !activeWf) continue;                              // neither eligible nor in flight -> off the eval

            long ttl = a == null ? 0 : a.path("ttlSeconds").asLong(0);
            String contentKey = a == null ? "" : contentKeyFor(a, ch, f, nbaId);   // variant-selected per member
            ObjectNode ca = chans.addObject();
            ca.put("actionId", aid);
            ca.put("channel", ch);
            ca.put("name", a == null ? "" : a.path("name").asText());
            ca.put("ttlSeconds", ttl);
            ca.put("contentKey", contentKey);
            ca.put("eligible", eligible);
            Object sc = f.get("nba.score." + aid + "." + ch);                  // carry the score
            if (sc instanceof Number num) ca.put("score", num.doubleValue());
            else ca.putNull("score");
            if (state != null) ca.put("workflowState", state); else ca.putNull("workflowState");
            ca.put("active", activeWf);                                        // coarse in-flight flag for the router
            ca.put("cancellable", "CREATED".equals(state));                   // not sent yet -> router can SUPPRESS to replace
            ca.put("hardCompleted", completed);
            long hardTtl = a == null ? 0 : a.path("hardTtlSeconds").asLong(0);
            if (hardTtl > 0) ca.put("hardTtlSeconds", hardTtl);
            Object disp = f.get("nba.disposition." + aid + "." + ch);
            boolean soft = a != null && softCompleted(a, ch, disp == null ? null : disp.toString());
            ca.put("softCompleted", soft);
            if (channelOptedOut) ca.put("optedOut", true);                     // surfaced for the portal/observability

            String idSig = aid + "::" + ch + "::" + contentKey + "::" + ttl;
            if (eligible) eligSig.add(idSig);                                  // eligibility identity = the eligible set
            fullSig.add(idSig + "=" + (sc instanceof Number num ? num.doubleValue() : "null")
                    + "@" + state + "#e" + eligible + "#s" + soft + "#h" + completed);
        }
        // Ride the member's COMPLETED milestones on the eval — derived above from the durable nba.milestone.* facts,
        // so they're permanent and never dropped (even when a milestone's logic no longer holds). Added to fullSig
        // so a fresh completion triggers an emit, but NOT to eligSig (a milestone is not an eligibility change).
        if (!doneMs.isEmpty()) {
            ArrayNode ms = eval.putArray("milestones");
            for (String mid : new java.util.TreeSet<>(doneMs.keySet())) {
                JsonNode mdef = milestones.get(mid);
                ObjectNode mo = ms.addObject();
                mo.put("id", mid);
                mo.put("name", mdef != null ? mdef.path("name").asText(mid) : mid);
                long cat = 0; try { cat = Long.parseLong(doneMs.get(mid)); } catch (Exception ignore) {}
                mo.put("completedAt", cat);
                fullSig.add("milestone:" + mid);
            }
        }
        // TRANSITION list: milestones whose logic passed THIS eval with no durable fact yet — the set the ROUTER
        // publishes as nba.milestone.{id} facts (it carries completedAt so the router needn't cross-reference). NOT
        // in fullSig: it's transient (clears once the fact round-trips); the full milestones[] above drives change-
        // detection, and a fresh milestone already grows that list so the eval emits and the router sees this.
        if (!newMs.isEmpty()) {
            ArrayNode nm = eval.putArray("newMilestones");
            for (String mid : newMs) { ObjectNode mo = nm.addObject(); mo.put("id", mid); mo.put("completedAt", milestoneTs); }
        }
        // COMPLETED actions ride the eval as a `completed[]` array (the action IDs in doneCompleted). A hard
        // completion can prune from channelActions (completed + auto-excluded + no live workflow), so this un-pruned
        // list is what carries every completion; added to fullSig so a fresh completion triggers an emit (NOT to
        // eligSig — completing is not an eligibility change in itself).
        if (!doneCompleted.isEmpty()) {
            ArrayNode comp = eval.putArray("completed");
            for (String aid : new java.util.TreeSet<>(doneCompleted.keySet())) { comp.add(aid); fullSig.add("completed:" + aid); }
        }
        // TRANSITION list: actions whose goal passed THIS eval with no durable fact yet — what the ROUTER publishes
        // as nba.completion.{id} facts. NOT in fullSig (transient; the full completed[] above drives change-detection).
        if (!newCompleted.isEmpty()) {
            ArrayNode nc = eval.putArray("newCompleted");
            for (String aid : newCompleted) nc.add(aid);
        }

        java.util.Collections.sort(eligSig);
        java.util.Collections.sort(fullSig);
        String eligHash = String.join("|", eligSig);
        String fullHash = String.join("|", fullSig);

        // CHANGE DETECTION: skip the emit when nothing moved. "redis" rebuilds the prior signatures from the SINGLE
        // eligibility object (nba:eligibility:{nbaId}, written by the action router); "memory" uses this instance's
        // last-emitted signatures (no Redis read). Both yield [fullHash, eligHash]; a never-seen member uses the
        // null baseline so its first eval emits, identically in both modes.
        String[] prior;
        if ("memory".equalsIgnoreCase(CHANGE_DETECT)) {
            String[] cached = LAST_SIG.get(nbaId);
            prior = (cached != null) ? cached : sigsOf(null);
        } else {
            String priorJson = redis.get(ELIG_KEY + nbaId);
            prior = sigsOf(priorJson == null ? null : M.readTree(priorJson));
        }
        if (fullHash.equals(prior[0])) {
            log.info("no change, skip emit " + nbaId + " channelActions=" + chans.size());
            Metrics.counter("nba_rules_evaluations_total", "result", "unchanged").increment();
            return;
        }
        // eligibilityChanged tells the ML layer whether to re-score: TRUE = eligibility moved, re-score; FALSE =
        // only scores moved, IGNORE (re-scoring an unchanged eligibility is what loops score->snapshot->eval->score).
        boolean eligibilityChanged = !eligHash.equals(prior[1]);
        eval.put("eligibilityChanged", eligibilityChanged);

        String out = M.writeValueAsString(eval);
        // Type header: "eligibility" when the eligible set moved, "score" when only scores did.
        // Lets the ML layer discard score-only evals WITHOUT deserializing the body.
        ProducerRecord<String, String> rec = new ProducerRecord<>(evalTopic, nbaId, out);
        rec.headers().add("type", (eligibilityChanged ? "eligibility" : "score")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8));
        producer.send(rec);
        if ("memory".equalsIgnoreCase(CHANGE_DETECT)) LAST_SIG.put(nbaId, new String[]{fullHash, eligHash});   // remember what we emitted
        log.info("evaluated " + nbaId + " channelActions=" + chans.size()
                + " type=" + (eligibilityChanged ? "eligibility" : "score") + " " + hits);
        Metrics.counter("nba_rules_evaluations_total", "result", eligibilityChanged ? "eligibility" : "score").increment();
        io.micrometer.core.instrument.DistributionSummary.builder("nba_rules_eligible_actions")
                .register(Metrics.REGISTRY).record(chans.size());
    }

    /** Rebuild [fullHash, eligHash] from an eligibility object's channelActions + milestones — IDENTICAL to the
     *  inline signatures built during evaluate — so change-detection can diff the freshly-computed eval against the
     *  router-written object in Redis (the single source). */
    static String[] sigsOf(JsonNode obj) {
        if (obj == null) return new String[]{"", ""};
        java.util.List<String> full = new java.util.ArrayList<>(), elig = new java.util.ArrayList<>();
        for (JsonNode ca : obj.path("channelActions")) {
            String idSig = ca.path("actionId").asText() + "::" + ca.path("channel").asText() + "::"
                    + ca.path("contentKey").asText("") + "::" + ca.path("ttlSeconds").asLong(0);
            boolean eligible = ca.path("eligible").asBoolean(false);
            if (eligible) elig.add(idSig);
            String score = ca.path("score").isNumber() ? String.valueOf(ca.get("score").asDouble()) : "null";
            String state = ca.hasNonNull("workflowState") ? ca.get("workflowState").asText() : "null";
            full.add(idSig + "=" + score + "@" + state + "#e" + eligible
                    + "#s" + ca.path("softCompleted").asBoolean(false) + "#h" + ca.path("hardCompleted").asBoolean(false));
        }
        for (JsonNode m : obj.path("milestones")) full.add("milestone:" + m.path("id").asText());
        for (JsonNode c : obj.path("completed")) full.add("completed:" + c.asText());
        java.util.Collections.sort(full); java.util.Collections.sort(elig);
        return new String[]{String.join("|", full), String.join("|", elig)};
    }

    static Object typedValue(JsonNode fv) {
        JsonNode v = fv.get("value");
        if (v == null || v.isNull()) return null;
        if (v.isBoolean()) return v.asBoolean();
        if (v.isIntegralNumber()) return v.asLong();
        if (v.isNumber()) return v.asDouble();
        return v.asText();
    }

    /** Pick the content template for this member on this channel. A channel can carry CONTENT VARIANTS
     *  (A/B + targeting): each variant has its own contentKey plus an optional `percent` (a deterministic
     *  random split, stable per member) and optional `conditions` (a fact gate). Variants are tried in order;
     *  the FIRST whose conditions pass AND whose member-bucket falls under `percent` wins. None match -> the
     *  base contentKey. memberKey makes the % split stable + reproducible (same member always same variant). */
    static String contentKeyFor(JsonNode a, String ch, Map<String, Object> facts, String memberKey) {
        JsonNode channels = a.get("channels");
        if (channels == null) return "";
        for (JsonNode c : channels) {
            if (!ch.equals(c.path("channel").asText())) continue;
            String base = c.path("contentKey").asText("");
            JsonNode variants = c.get("variants");
            if (variants != null && variants.isArray()) {
                int idx = 0;
                for (JsonNode v : variants) {
                    idx++;
                    String vk = v.path("contentKey").asText("");
                    if (vk.isEmpty()) continue;
                    JsonNode conds = v.get("conditions");
                    if (conds != null && !conds.isNull() && !treePass(conds, facts)) continue;   // fact gate
                    JsonNode pct = v.get("percent");
                    if (pct != null && pct.isNumber() && pct.asInt() < 100) {                     // random split
                        int bucket = Math.floorMod((memberKey + ":" + ch + ":" + idx).hashCode(), 100);
                        if (bucket >= pct.asInt()) continue;
                    }
                    return vk;                                                                     // first match wins
                }
            }
            return base;
        }
        return "";
    }

    /** Evaluate a condition tree {op: all|any, conditions:[{fact,cmp,value}|<nested tree>]} against the
     *  member facts — the same semantics the DRL uses (missing fact = its type default), but in plain Java
     *  (variant selection runs post-eligibility, off the eval's fact map — no Drools session). */
    static boolean treePass(JsonNode tree, Map<String, Object> facts) {
        if (tree == null || tree.isNull()) return true;
        JsonNode conds = tree.get("conditions");
        if (conds == null || !conds.isArray() || conds.size() == 0) return true;
        boolean any = "any".equals(tree.path("op").asText("all"));
        for (JsonNode c : conds) {
            boolean ok = (c.has("conditions") && c.get("conditions").isArray()) ? treePass(c, facts) : condPass(c, facts);
            if (any && ok) return true;
            if (!any && !ok) return false;
        }
        return !any;   // all -> no failures = true; any -> nothing matched = false
    }

    static boolean condPass(JsonNode c, Map<String, Object> facts) {
        String fact = c.path("fact").asText("");
        if (fact.isEmpty()) return true;
        String cmp = c.path("cmp").asText("eq");
        Object actual = facts.get(fact);
        if ("exists".equals(cmp)) return actual != null;
        JsonNode val = c.get("value");
        if (val != null && val.isNumber() && (actual == null || actual instanceof Number || actual instanceof Boolean)) {
            // Boolean facts (the activation-layer emits driven/milestone facts as BOOLEAN true/false) coerce to 1/0
            // so numeric criteria (gte 1) latch them — covers both completion trees AND prereq eligibility.
            double x = actual == null ? 0.0
                     : actual instanceof Boolean ? (((Boolean) actual) ? 1.0 : 0.0)
                     : ((Number) actual).doubleValue();
            double y = val.asDouble();
            return switch (cmp) {
                case "ne" -> x != y; case "gt" -> x > y; case "gte" -> x >= y;
                case "lt" -> x < y;  case "lte" -> x <= y; default -> x == y;
            };
        }
        if (val != null && val.isBoolean()) {
            boolean x = actual instanceof Boolean b ? b : Boolean.parseBoolean(String.valueOf(actual)), y = val.asBoolean();
            return "ne".equals(cmp) ? x != y : x == y;
        }
        String x = actual == null ? "" : String.valueOf(actual), y = val == null ? "" : val.asText();
        if ("in".equals(cmp)) {                                  // value = comma-separated allow-list
            for (String part : y.split(",")) if (part.trim().equals(x)) return true;
            return false;
        }
        return "ne".equals(cmp) ? !x.equals(y) : x.equals(y);
    }

    /** Truthiness of the explicit nba.completion.{actionId} signal fact. */
    static boolean isTruthy(Object v) {
        if (v == null) return false;
        if (v instanceof Boolean b) return b;
        String s = String.valueOf(v);
        return s.equalsIgnoreCase("completed") || s.equalsIgnoreCase("true") || s.equals("1");
    }

    // Soft-completion funnel bars (mirror action-library's CHANNEL_FUNNEL). The soft bar defaults to the
    // channel funnel's TERMINAL stage; an action may override per channel via channels[].softCompletion.
    static final Map<String, List<String>> CHANNEL_FUNNEL = Map.of(
            "email", List.of("Delivered", "Opened", "LinkClicked"),
            "sms",   List.of("Delivered", "LinkClicked"),
            "push",  List.of("Delivered", "Opened"),
            "voice", List.of("Answered", "Completed"),
            "mail",  List.of("Delivered"));
    static final List<String> INBOUND_FUNNEL = List.of("Presented", "Accepted", "Completed");
    static List<String> funnelFor(String ch) { return CHANNEL_FUNNEL.getOrDefault(ch, INBOUND_FUNNEL); }

    // OPT-OUT = a BUILT-IN, always-on channel-eligibility rule (mirrors action-layer DECLINE_RAW). When a channel's
    // disposition reaches its opt-out raw status, the member has opted OUT of that channel — we latch it durably
    // (HSETNX, exactly like a milestone/hard-completion) and the channel goes INELIGIBLE for that member, permanently.
    // Only the hard, legally-binding opt-outs gate (email Unsubscribe / sms STOP); a voice Declined / push Dismissed is
    // a negative DISPOSITION the model learns from, not a permanent channel removal (those re-approach; isDNC is global).
    static final Map<String, String> OPTOUT_RAW = Map.of("email", "Unsubscribe", "sms", "STOP");

    /** soft_completed = the latest disposition status for this (action,channel) reached the channel's soft
     *  bar (>= its funnel index). Bar = terminal stage by default; per-action override via channels[].softCompletion. */
    static boolean softCompleted(JsonNode a, String channel, String status) {
        if (status == null || status.isEmpty()) return false;
        List<String> funnel = funnelFor(channel);
        String bar = funnel.get(funnel.size() - 1);
        JsonNode chans = a.get("channels");
        if (chans != null) for (JsonNode c : chans)
            if (channel.equals(c.path("channel").asText()) && c.hasNonNull("softCompletion")) { bar = c.get("softCompletion").asText(bar); break; }
        int si = funnel.indexOf(status), bi = funnel.indexOf(bar);
        if (bi < 0) bi = funnel.size() - 1;
        return si >= 0 && si >= bi;
    }

    /** Returns true if the change is STRUCTURAL (actions/rules changed) so the DRL must rebuild.
     *  A throttle-level update is eval-time data, not structure — it returns false (no rebuild). */
    static boolean applyDef(String key, String value) throws Exception {
        int i = key.indexOf(':');
        if (i < 0) return false;
        String type = key.substring(0, i);
        String id = key.substring(i + 1);
        if ("THROTTLE".equals(type)) { applyThrottle(value); return false; }
        if ("THROTTLE_HOT".equals(type)) { applyThrottleHot(id, value); return false; }
        if ("ACTION_SUPPRESS".equals(type)) { applyActionSuppress(id, value); return false; }   // eval-time data, not structure -> no rebuild
        if ("MILESTONE".equals(type)) {                     // evaluated in Java (treePass), not Drools -> no DRL rebuild
            if (value == null) milestones.remove(id); else milestones.put(id, M.readTree(value));
            return false;
        }
        Map<String, JsonNode> store = switch (type) {
            case "ACTION" -> actions;
            case "GLOBAL_RULE" -> globalRules;
            case "CHANNEL_RULE" -> channelRules;
            default -> null;
        };
        if (store == null) return false;
        if (value == null) store.remove(id);              // tombstone
        else store.put(id, M.readTree(value));
        return true;
    }

    /** A forwarded throttle fact {key:"nba.throttle.{ch}.daily", value:<level>, eventTs}, broadcast on
     *  the definitions stream. Retain the latest level per key (event-time LWW); applied to EVERY
     *  evaluation as a population-wide channel cap. */
    /** Channel saturated for the day (gate's throttle-suppress, forwarded by the snapshot-builder).
     *  Mark it ineligible until midnight — the daily budget resets then. Each fresh signal refreshes
     *  the TTL; once they stop (channel drained), it expires and the channel reopens on its own. */
    // Saturation TTL: 0 (default) = until midnight (daily budget resets). A positive override is for
    // testing (so the "channel reopens" path doesn't have to wait for midnight).
    static final long HOT_TTL_SECONDS = Long.parseLong(env("NBA_THROTTLE_HOT_TTL_SECONDS", "0"));
    static void applyThrottleHot(String channel, String value) throws Exception {
        // THROTTLE_HOT is a transient EVENT but it lands on the compacted definitions topic, so it
        // replays on restart. Ignore a stale event (its TTL already elapsed) so a restart doesn't
        // resurrect an old saturation. eventTs comes from the forwarded throttle-suppress fact.
        long now = System.currentTimeMillis();
        long eventTs = value == null ? now : M.readTree(value).path("eventTs").asLong(now);
        if (eventTs > now + 60_000) return;                                      // future ts (clock skew / bad data) -> ignore
        long until;
        if (HOT_TTL_SECONDS > 0) {
            if (now - eventTs >= HOT_TTL_SECONDS * 1000) return;                 // expired/replayed -> ignore
            until = eventTs + HOT_TTL_SECONDS * 1000;                            // anchor to the EVENT clock so a
                                                                                 // restart's replay doesn't refresh it
        } else {
            if (eventTs / 86_400_000L < now / 86_400_000L) return;              // from a previous day -> ignore
            until = ((now / 86_400_000L) + 1) * 86_400_000L;                     // until midnight
        }
        CHANNEL_HOT_UNTIL.put(channel, until);
        log.info("channel HOT (saturated for the day) " + channel + " -> ineligible "
                + (HOT_TTL_SECONDS > 0 ? "for " + HOT_TTL_SECONDS + "s" : "until midnight"));
    }

    /** Operator suppression toggle. target = {actionId} (whole action) or {actionId}.{channel} (one channel).
     *  value = JSON fact {value:<bool>, actionId, channel, eventTs, ...}. A null value (tombstone) OR value=false
     *  UN-suppresses; value=true suppresses. Retained in SUPPRESSED, AND-ed into eligibility on the next eval. */
    static void applyActionSuppress(String target, String value) throws Exception {
        boolean suppressed = value != null && M.readTree(value).path("value").asBoolean(false);
        if (suppressed) SUPPRESSED.add(target); else SUPPRESSED.remove(target);
        log.info("action " + (suppressed ? "SUPPRESSED" : "unsuppressed") + " " + target);
    }

    static void applyThrottle(String value) throws Exception {
        if (value == null) return;
        JsonNode t = M.readTree(value);
        String fkey = t.path("key").asText("");
        if (fkey.isEmpty()) return;
        long level = t.path("value").asLong(0), ts = t.path("eventTs").asLong(0);
        long[] cur = GLOBAL_THROTTLE.get(fkey);
        if (cur == null || ts >= cur[1]) {
            GLOBAL_THROTTLE.put(fkey, new long[]{level, ts});
            log.info("throttle level " + fkey + "=" + level);
            // Explicitly OPENING a channel (daily level -> 0) also lifts any saturation HOT on it: reopening
            // clears the cap AND the "can't clear today" prediction, keeping channel state idempotent — a
            // baseline throttle reset drops a stale saturation HOT instead of it lingering for the TTL and
            // bleeding into later/other evals. Gated to EXPLICIT opens (operator/test) — the routine lake
            // count emit (source=throttle-lake) is just telemetry and must NOT race-clear a live saturation.
            String src = t.path("source").asText("");
            if (level == 0 && !"throttle-lake".equals(src) && fkey.startsWith("nba.throttle.") && fkey.endsWith(".daily")) {
                String ch = fkey.substring("nba.throttle.".length(), fkey.length() - ".daily".length());
                if (CHANNEL_HOT_UNTIL.remove(ch) != null) log.info("channel reopened (saturation HOT cleared) " + ch);
            }
        }
    }

    static void rebuild() {
        String drl = buildDrl();
        try {
            KieHelper helper = new KieHelper();
            helper.addContent(drl, ResourceType.DRL);
            kieBase = helper.build();
            long ruleCount = actions.values().stream()
                    .mapToLong(a -> a.has("channels") ? a.get("channels").size() : 0).sum();
            log.info("KieBase rebuilt: actions=" + actions.size()
                    + " globalRules=" + globalRules.size() + " channelRules=" + channelRules.size()
                    + " eligibility-rules=" + ruleCount);
            log.info("----- DRL -----\n" + drl + "---------------");
        } catch (Exception e) {
            log.warn("DRL compile FAILED: " + e + "\n----- DRL -----\n" + drl);
        }
    }

    // ── DRL generation from the structured logic ──────────────────────────────
    static String buildDrl() {
        StringBuilder sb = new StringBuilder();
        sb.append("import ai.das.nba.rules.Snap;\n");
        sb.append("global java.util.List results;\n\n");

        String globalExpr = andRules(globalRules.values());   // all global rules must pass

        for (JsonNode a : actions.values()) {
            String aid = a.path("id").asText();
            String incl = exprForTree(a.get("inclusion"));
            String excl = exprForTree(a.get("exclusion"));
            JsonNode channels = a.get("channels");
            if (channels == null || !channels.isArray()) continue;
            for (JsonNode ch : channels) {
                String chName = ch.path("channel").asText("");
                String chExpr = andRules(channelRulesFor(chName));   // all channel rules for ch
                List<String> parts = new ArrayList<>();
                if (!incl.isEmpty()) parts.add(incl);
                if (!globalExpr.isEmpty()) parts.add(globalExpr);
                if (!chExpr.isEmpty()) parts.add(chExpr);
                String combined = String.join(" && ", parts);   // empty -> Snap() matches any
                sb.append("rule \"elig::").append(aid).append("::").append(chName).append("\"\n");
                sb.append("dialect \"mvel\"\nwhen\n");
                sb.append("  Snap(").append(combined).append(")\n");
                if (!excl.isEmpty()) sb.append("  not Snap(").append(excl).append(")\n");
                sb.append("then\n  results.add(\"").append(aid).append("::").append(chName).append("\");\nend\n\n");
            }
        }
        return sb.toString();
    }

    /** A condition tree {op: all|any, conditions:[{fact,cmp,value}]} -> an MVEL boolean expr. */
    static String exprForTree(JsonNode tree) {
        if (tree == null || tree.isNull()) return "";
        JsonNode conds = tree.get("conditions");
        if (conds == null || !conds.isArray() || conds.size() == 0) return "";
        String joiner = "any".equals(tree.path("op").asText("all")) ? " || " : " && ";
        List<String> parts = new ArrayList<>();
        for (JsonNode c : conds) {
            String e = exprForCond(c);
            if (!e.isEmpty()) parts.add(e);
        }
        return parts.isEmpty() ? "" : "(" + String.join(joiner, parts) + ")";
    }

    static String exprForCond(JsonNode c) {
        String fact = c.path("fact").asText("");
        if (fact.isEmpty()) return "";
        String cmp = c.path("cmp").asText("eq");
        // `exists` asks about presence — never coalesce it.
        if ("exists".equals(cmp)) return "get(\"" + fact + "\") != null";
        JsonNode value = c.get("value");
        // A missing fact behaves as its type's default (0 / false / ""), inferred from
        // the comparison value — so e.g. a member with no commsThisWeek count reads as 0.
        String lhs = "getOr(\"" + fact + "\", " + defaultFor(value) + ")";
        String val = renderVal(value);
        return switch (cmp) {
            case "ne" -> lhs + " != " + val;
            case "gt" -> lhs + " > " + val;
            case "gte" -> lhs + " >= " + val;
            case "lt" -> lhs + " < " + val;
            case "lte" -> lhs + " <= " + val;
            default -> lhs + " == " + val;     // eq
        };
    }

    /** Type-default for a comparison value: number→0, boolean→false, string→"". */
    static String defaultFor(JsonNode v) {
        if (v == null || v.isNull()) return "null";
        if (v.isBoolean()) return "false";
        if (v.isNumber()) return "0";
        return "\"\"";
    }

    static String renderVal(JsonNode v) {
        if (v == null || v.isNull()) return "null";
        if (v.isBoolean()) return String.valueOf(v.asBoolean());
        if (v.isNumber()) return v.asText();
        return "\"" + v.asText().replace("\"", "\\\"") + "\"";
    }

    static String andRules(Collection<JsonNode> rules) {
        List<String> parts = new ArrayList<>();
        for (JsonNode r : rules) {
            String e = exprForTree(r.get("logic"));
            if (!e.isEmpty()) parts.add(e);
        }
        return parts.isEmpty() ? "" : "(" + String.join(" && ", parts) + ")";
    }

    static Collection<JsonNode> channelRulesFor(String ch) {
        List<JsonNode> out = new ArrayList<>();
        for (JsonNode r : channelRules.values()) {
            // Skip .rate rules — those are GATE-ONLY pacing (the Temporal worker reads them to trickle
            // sends). Enforcing a rate cap as ELIGIBILITY would reroute the action the instant the rate
            // is hit, instead of letting it wait + trickle. The .daily rule IS eligibility (hard ceiling).
            if (ch.equals(r.path("channel").asText()) && !referencesRate(r)) out.add(r);
        }
        return out;
    }

    static boolean referencesRate(JsonNode rule) {
        JsonNode conds = rule.path("logic").path("conditions");
        if (conds.isArray()) for (JsonNode c : conds) if (c.path("fact").asText("").endsWith(".rate")) return true;
        return false;
    }

    // ── KIE-server mode: offload the Drools eval to the standalone decision service over HTTP ──────────
    static final boolean KIE_MODE = "kie".equalsIgnoreCase(env("NBA_RULES_MODE", "embedded"));
    static final String KIE_URL = env("NBA_KIE_URL", "http://nba-kie-server:7010");
    static final java.net.http.HttpClient HTTP = java.net.http.HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3)).build();

    /** POST {nbaId, facts} to nba-kie-server and return its hit slugs ("actionId::channel"). null on failure. */
    static List<String> kieServerEval(String nbaId, Map<String, Object> f) {
        try {
            ObjectNode req = M.createObjectNode();
            req.put("nbaId", nbaId);
            ObjectNode facts = req.putObject("facts");
            for (Map.Entry<String, Object> e : f.entrySet()) {
                Object v = e.getValue();
                if (v == null) facts.putNull(e.getKey());
                else if (v instanceof Boolean b) facts.put(e.getKey(), b);
                else if (v instanceof Long l) facts.put(e.getKey(), l);
                else if (v instanceof Integer iv) facts.put(e.getKey(), iv.longValue());
                else if (v instanceof Double d) facts.put(e.getKey(), d);
                else if (v instanceof Number n) facts.put(e.getKey(), n.doubleValue());
                else facts.put(e.getKey(), v.toString());
            }
            java.net.http.HttpRequest hr = java.net.http.HttpRequest.newBuilder(java.net.URI.create(KIE_URL + "/evaluate"))
                    .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                    .POST(java.net.http.HttpRequest.BodyPublishers.ofString(M.writeValueAsString(req))).build();
            java.net.http.HttpResponse<String> resp = HTTP.send(hr, java.net.http.HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) { log.warn("kie-server HTTP " + resp.statusCode()); return null; }
            JsonNode hitsNode = M.readTree(resp.body()).get("hits");
            List<String> hits = new ArrayList<>();
            if (hitsNode != null && hitsNode.isArray()) for (JsonNode h : hitsNode) hits.add(h.asText());
            return hits;
        } catch (Exception e) {
            log.warn("kie-server eval failed", e);
            return null;
        }
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
