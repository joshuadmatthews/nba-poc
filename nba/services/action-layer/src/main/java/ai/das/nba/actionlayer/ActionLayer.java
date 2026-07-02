package ai.das.nba.actionlayer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NBA Unified Activation Layer — the DISPOSITION BRAIN.
 *
 * Consumes the state machine's DISPATCH / CANCEL ops (nba.activations) and is the integration boundary
 * with the real channel providers (ESP / SMS gateway / voice / push). Its job is to be SMART ABOUT
 * DISPOSITIONS: it ingests each provider's raw disposition events (here SIMULATED — swap the simulator
 * for real webhooks) and CLASSIFIES them to the canonical lifecycle state, walking the per-action Temporal
 * workflow through the disposition states:
 *
 *   IN_PROCESS -> PRESENTED                                 (the delivery funnel)
 *                      \-> FAILED (bounce / no-answer)
 *                       \-> DECLINED (opt-out / unsubscribe)
 *
 * The WORKFLOW emits IN_PROCESS itself on DISPATCH ("activation sent, no response yet"); the activation layer
 * then walks the provider DELIVERY dispositions to PRESENTED ("it reached the member"). The raw engagement
 * status (LinkClicked / Opened / Completed-with-rep) rides the fact's `raw` field — the RULES ENGINE decides
 * SOFT_COMPLETED off it against the action's soft bar (a rule, defaulting to a per-channel default), and the
 * router bridges SOFT_COMPLETE in (just like HARD). So the activation layer owns only the DELIVERY states.
 * Each raw disposition is a MEMBER FACT nba.disposition.{actionId}.{channel}: VALUE = canonical delivery
 * state, `raw` = provider status. The temporal disposition-consumer signals the workflow.
 *
 * The per-channel funnel (CHANNEL_FUNNEL) is the raw->canonical taxonomy; in production it's the webhook
 * map. Single instance + in-memory pending map (POC).
 */
public class ActionLayer {
    private static final Logger log = LoggerFactory.getLogger(ActionLayer.class);
    static final ObjectMapper M = new ObjectMapper();

    // Canonical DELIVERY states the activation layer emits. SOFT_COMPLETED/HARD_COMPLETED are NOT ours — the
    // rules engine evaluates those off the raw disposition + the goal, and the router bridges them in.
    static final String IN_PROCESS = "IN_PROCESS", PRESENTED = "PRESENTED",
            DECLINED = "DECLINED", FAILED = "FAILED", SUPPRESSED = "SUPPRESSED";
    // Responses to a SUPPRESS (CANCEL) activation: SUPPRESSED if we caught it before the send, else SUPPRESS_FAILED.
    static final String SUPPRESS_FAILED = "SUPPRESS_FAILED";

    /** Per-channel disposition funnel: the ordered {rawDisposition, canonicalDeliveryState} steps the provider
     *  walks AFTER the workflow's own IN_PROCESS. Delivered/Opened/clicked all map to the delivery state
     *  PRESENTED; the RAW status rides the fact so the rules engine can decide soft-completion against the
     *  channel's bar. Real deployment swaps the simulator for provider webhooks; this map stays the classifier. */
    static final Map<String, String[][]> CHANNEL_FUNNEL = Map.of(
            "email", new String[][]{{"Delivered", PRESENTED}, {"Opened", PRESENTED}, {"LinkClicked", PRESENTED}},
            "sms",   new String[][]{{"Delivered", PRESENTED}, {"LinkClicked", PRESENTED}},
            "push",  new String[][]{{"Delivered", PRESENTED}, {"Opened", PRESENTED}},
            "voice", new String[][]{{"Answered", PRESENTED}, {"Completed", PRESENTED}});
    static final String[][] DEFAULT_FUNNEL = new String[][]{{"Presented", PRESENTED}, {"Accepted", PRESENTED}};
    /** Per-channel failure + decline raw labels (what a bounce / opt-out is called on that channel). */
    static final Map<String, String> FAIL_RAW = Map.of("email", "Bounced", "sms", "Undelivered", "push", "Failed", "voice", "NoAnswer");
    static final Map<String, String> DECLINE_RAW = Map.of("email", "Unsubscribe", "sms", "STOP", "push", "Dismissed", "voice", "Declined");

    static String[][] funnelFor(String channel) { return CHANNEL_FUNNEL.getOrDefault(channel, DEFAULT_FUNNEL); }

    // A send in flight: its activation + the funnel position it has walked to. A CANCEL that arrives before
    // the first disposition is dished wins (-> SUPPRESSED); after, it's too-late and the funnel proceeds.
    static final Map<String, Walk> WALKS = new ConcurrentHashMap<>();
    static final class Walk {
        final JsonNode act; final String channel; final String[][] funnel;
        volatile int step = -1;        // -1 = scheduled, no disposition dished yet
        volatile long nextAt;          // when the next disposition fires
        volatile boolean cancelled = false;
        volatile String outcome;       // null = happy walk; else FAILED/DECLINED chosen for this send
        Walk(JsonNode act, String channel, long firstAt) { this.act = act; this.channel = channel; this.funnel = funnelFor(channel); this.nextAt = firstAt; }
    }

    // ── SIM CONFIG (live-shiftable via Redis) ──────────────────────────────────────────────────────────────
    // The POC activation layer is a MOCK for real channels/people. Two modes, refreshed from Redis every ~5s:
    //   deterministic (default): delivery only; conversions come only from real /completion or rule criteria.
    //   stochastic: on delivery, sample a HARD conversion ~ Bernoulli(nba:sim:propensity[arm]) and emit a
    //     completion -> HARD_COMPLETED; non-converters EXPIRE at TTL (the negative label). Tests/demos SHIFT the
    //     propensity live (nba:sim:propensity = JSON {"actionId.channel": rate | "actionId": rate}) and watch the
    //     model adapt across retrains.
    static volatile String SIM_MODE = "deterministic";
    static volatile Map<String, Double> SIM_PROP = Map.of();
    // MEMBER-DEPENDENT f* (preferred over the flat SIM_PROP): nba:sim:fstar = JSON {actionId: {b, <featureCol>: w, ...}}.
    // Convert prob = sigmoid(b + Σ w·feature), with the isDNC + sms-consent gates. This is the GROUND-TRUTH world the
    // model learns — the SAME coefficients the simulator uses (nba_ml_common.FSTAR), so sim data and live outcomes share
    // one world. Designed so DIFFERENT arms win for DIFFERENT members (dormant→email, power-user→voice, viewer→push) —
    // a real per-(member,action) signal, not a flat ranking. Shift it live (the adaptation demo) and the model re-learns.
    static volatile JsonNode SIM_FSTAR = null;
    // MEMBER×CHANNEL AFFINITY (live-shiftable, nba:sim:channel_affinity = JSON {channel: {featureCol: w, ...}}): an a-priori
    // channel preference from member properties (digital -> email/push, age/SDOH -> voice/mail), ADDED to f*'s z. This is
    // GROUND-TRUTH member behavior, not a model component — the model LEARNS it by maximizing milestone value. Channel-level
    // (action-independent): a digital member prefers email whatever the intent.
    static volatile JsonNode SIM_CH_AFFINITY = null;
    // opt-out / bounce probabilities — LIVE-SHIFTABLE via Redis (nba:sim:decline_rate / nba:sim:fail_rate), so we can
    // turn on opt-out emission (Unsubscribe/STOP/Declined/Dismissed dispositions) without a redeploy. Startup defaults
    // come from NBA_SIM_DECLINE_RATE / NBA_SIM_FAIL_RATE. The disposition walker reads these statics fresh each step.
    static volatile double SIM_DECLINE_RATE = 0.0;
    static volatile double SIM_FAIL_RATE = 0.0;
    // INTELLIGENT opt-out escalation (mirrors nba_journey_env OPTOUT_ESC/OPTOUT_FREE): opt-out probability rises by
    // OPTOUT_ESC per contact beyond OPTOUT_FREE, so OVER-CONTACTING a member is what drives them to opt out. This makes
    // pacing both learnable (in the env) and observable (live): a badgering policy loses members; a paced one keeps them.
    static final double OPTOUT_ESC = Double.parseDouble(env("NBA_SIM_OPTOUT_ESC", "0.02"));
    static final double OPTOUT_FREE = Double.parseDouble(env("NBA_SIM_OPTOUT_FREE", "4.0"));
    // REALISTIC FUNNEL DROP-OFF: STAGE_PROB[channel][i] = P(reach funnel stage i | reached stage i-1). Stage 0 =
    // delivery/answer (a miss -> FAILED bounce/no-answer); later stages = open/click/answer-through (a miss -> the
    // member stalled, delivered-but-not-engaged -> the workflow EXPIRES at TTL = the realistic 'ignored' negative).
    // WITHOUT this the walker reaches every stage = 100% open/click (a deterministic happy-path, NOT real engagement,
    // which teaches the policy that every send succeeds). Defaults are realistic CTRs; live-shiftable + per-member-
    // calibratable via nba:sim:funnel = JSON {channel: [p0, p1, ...]}.
    static volatile Map<String, double[]> STAGE_PROB = defaultStageProb();
    static Map<String, double[]> defaultStageProb() {
        Map<String, double[]> m = new java.util.HashMap<>();
        m.put("email", new double[]{0.97, 0.30, 0.25});   // delivered 97% · opened 30% · clicked 25% of opens (~7% CTR)
        m.put("sms",   new double[]{0.98, 0.14});          // delivered 98% · clicked 14%
        m.put("push",  new double[]{0.92, 0.10});          // delivered 92% · opened 10%
        m.put("voice", new double[]{0.32, 0.80});          // answered 32% · completed 80% of answers
        return m;
    }
    static redis.clients.jedis.JedisPooled REDIS;   // member features for f* are read from the snapshot at convert time
    // (snapshot fact key, the FSTAR coefficient name) — the feature contract, mirrors nba_ml_common.FEATURE_KEYS.
    static final String[][] FEAT = {
            {"operator.activity.daysSinceLogin",  "daysSinceLogin"},
            {"operator.activity.completedTasks",  "completedTasks"},
            {"operator.activity.viewedDashboard", "viewedDashboard"},
            {"operator.activity.usedChat",        "usedChat"},
            {"operator.profile.isDNC",            "isDNC"},
            {"operator.profile.smsConsent",       "smsConsent"},
            {"operator.comms.totalThisWeek",      "totalThisWeek"},
            {"operator.comms.emailsThisWeek",     "emailsThisWeek"},
            // HEALTHCARE member properties — so f* can vary conversion by WHO the member is (the personalization
            // signal the model learns) and the channel-affinity term can read them. Mirrors nba_ml_common.FEATURE_KEYS.
            {"operator.profile.age",               "age"},
            {"operator.profile.riskScore",         "riskScore"},
            {"operator.profile.sdohBarrier",       "sdohBarrier"},
            {"operator.profile.diabetic",          "diabetic"},
            {"operator.clinical.comorbidityCount", "comorbidityCount"},
            {"operator.clinical.openCareGaps",     "openCareGaps"},
            {"operator.clinical.erVisits12mo",     "erVisits12mo"},
            {"operator.clinical.rxAdherencePDC",   "rxAdherencePDC"},
            {"operator.activity.portalLogins30d",  "portalLogins30d"},
            {"operator.activity.pagesViewed30d",   "pagesViewed30d"}};
    // ── JOURNEY mode (nba:sim:mode contains "journey"): the activation layer becomes a multi-step SIMULATOR of a
    // real member. On a delivered touch, a POSITIVE response (Bernoulli(f*), or f*>=0.5 in deterministic mode for
    // testing) ADVANCES the member's engagement features per the arm's effect; those facts flow back through the
    // medallion -> snapshot -> the next eval, so the member climbs Activated->Onboarded->Engaged->Upgraded under the
    // live policy. (In real production, real customers do this for free; this is the demo stand-in for real people.)
    // BOOL_FEATS = the driven facts that are BOOLEAN milestones (set ONCE to 1.0, not accumulated) — the healthcare
    // engagement/care facts that the milestone ladder reads.
    static final java.util.Set<String> BOOL_FEATS = java.util.Set.of(
            "respondedToOutreach", "registeredForPortal", "loggedIn", "viewedBenefits", "hraCompleted", "pcpSelected",
            "careTeamEngaged", "awvCompleted", "medAdherent", "mammogramDone", "a1cControlled", "colonoscopyDone");
    // nba:sim:effect (Redis, live-shiftable) = JSON {actionId: {feature: increment}} — the engagement progress the
    // journey simulator applies on a positive response, per action: the healthcare driven facts (respondedToOutreach,
    // registeredForPortal, hraCompleted, … → the milestone ladder). It is the SINGLE source of truth for the WHOLE
    // catalog (all 15 healthcare actions); there is NO hardcoded fallback — if it's unset, a member simply doesn't
    // advance (fail-safe). Seeded by the same world as nba_ml_common (see the live nba:sim:effect key).
    static volatile JsonNode SIM_EFFECT = null;
    static Map<String, Double> effectFor(String actionId) {
        JsonNode e = (SIM_EFFECT == null) ? null : SIM_EFFECT.get(actionId);
        if (e != null && e.isObject()) {
            Map<String, Double> m = new java.util.HashMap<>();
            e.fields().forEachRemaining(en -> m.put(en.getKey(), en.getValue().asDouble()));
            return m;
        }
        return null;   // nba:sim:effect is the source of truth for the full catalog; no demo-arm fallback
    }

    public static void main(String[] args) {
        String bootstrap   = env("NBA_BOOTSTRAP", "nba-redpanda:9092");
        String actTopic    = env("NBA_ACT_TOPIC", "nba.activations");
        String memberFacts = env("NBA_MEMBER_FACTS", "nba.member.facts");
        String dlq         = env("NBA_DLQ", "nba.dlq.action-layer");
        long   stepMs      = Long.parseLong(env("NBA_DISPOSITION_STEP_MS", "1500"));   // delay between funnel steps (sim)
        SIM_FAIL_RATE    = Double.parseDouble(env("NBA_SIM_FAIL_RATE", "0.0"));        // startup default (Redis-shiftable)
        SIM_DECLINE_RATE = Double.parseDouble(env("NBA_SIM_DECLINE_RATE", "0.0"));     // startup default (Redis-shiftable)

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(bootstrap));

        // SIM-CONFIG poller: refresh mode + per-arm conversion propensity from Redis every 5s (live-shiftable).
        String redisHost = env("NBA_REDIS_HOST", "nba-redis");
        int redisPort = Integer.parseInt(env("NBA_REDIS_PORT", "6379"));
        REDIS = new redis.clients.jedis.JedisPooled(redisHost, redisPort);

        // Prometheus /metrics on a side port (no HTTP surface otherwise).
        Metrics.serve(Integer.parseInt(env("NBA_METRICS_PORT", "9405")));

        Thread cfg = new Thread(() -> {
            while (true) {
                try {
                    String mode = REDIS.get("nba:sim:mode");
                    SIM_MODE = (mode == null || mode.isBlank()) ? "deterministic" : mode.trim();
                    String prop = REDIS.get("nba:sim:propensity");
                    if (prop != null && !prop.isBlank()) {
                        java.util.Map<String, Double> m = new java.util.HashMap<>();
                        M.readTree(prop).fields().forEachRemaining(e -> m.put(e.getKey(), e.getValue().asDouble()));
                        SIM_PROP = m;
                    } else { SIM_PROP = Map.of(); }
                    String fstar = REDIS.get("nba:sim:fstar");                 // member-dependent f* (preferred)
                    SIM_FSTAR = (fstar != null && !fstar.isBlank()) ? M.readTree(fstar) : null;
                    String chaff = REDIS.get("nba:sim:channel_affinity");      // member×channel affinity (added to f*'s z)
                    SIM_CH_AFFINITY = (chaff != null && !chaff.isBlank()) ? M.readTree(chaff) : null;
                    String ef = REDIS.get("nba:sim:effect");                   // per-action engagement effect (live-shiftable)
                    SIM_EFFECT = (ef != null && !ef.isBlank()) ? M.readTree(ef) : null;
                    String dr = REDIS.get("nba:sim:decline_rate");             // opt-out probability (live-shiftable)
                    if (dr != null && !dr.isBlank()) try { SIM_DECLINE_RATE = Double.parseDouble(dr.trim()); } catch (NumberFormatException ig) { }
                    String fr = REDIS.get("nba:sim:fail_rate");                // bounce probability (live-shiftable)
                    if (fr != null && !fr.isBlank()) try { SIM_FAIL_RATE = Double.parseDouble(fr.trim()); } catch (NumberFormatException ig) { }
                    String fn = REDIS.get("nba:sim:funnel");                   // realistic per-stage funnel drop-off
                    if (fn != null && !fn.isBlank()) {
                        Map<String, double[]> sp = new java.util.HashMap<>();
                        M.readTree(fn).fields().forEachRemaining(e -> {
                            double[] arr = new double[e.getValue().size()];
                            for (int i = 0; i < arr.length; i++) arr[i] = e.getValue().get(i).asDouble();
                            sp.put(e.getKey(), arr);
                        });
                        if (!sp.isEmpty()) STAGE_PROB = sp;
                    }
                } catch (Exception ignore) { }
                try { Thread.sleep(5000); } catch (InterruptedException ie) { return; }
            }
        }, "sim-config");
        cfg.setDaemon(true);
        cfg.start();

        // The DISPOSITION WALKER: drives each in-flight send through its channel funnel, emitting one
        // canonical-state disposition per step. (Replace this thread with provider webhooks in production —
        // they call the same `disposition(...)` emitter with the classified state.)
        Thread walker = new Thread(() -> {
            while (true) {
                try { Thread.sleep(150); } catch (InterruptedException ie) { return; }
                long now = System.currentTimeMillis();
                for (Map.Entry<String, Walk> e : WALKS.entrySet()) {
                    Walk w = e.getValue();
                    if (w.nextAt > now) continue;
                    advance(producer, memberFacts, e.getKey(), w, stepMs, SIM_FAIL_RATE, SIM_DECLINE_RATE);
                }
            }
        }, "disposition-walker");
        walker.setDaemon(true);
        walker.start();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(bootstrap));
        consumer.subscribe(List.of(actTopic));
        log.info("up (disposition brain): in=" + actTopic + " (DISPATCH/CANCEL) out=" + memberFacts
                + " (nba.disposition.* = canonical state) stepMs=" + stepMs);

        while (true) {
            ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> r : recs) {
                try {
                    maybeInjectFault(r.value());                 // test hook: force a DLQ on marked messages
                    JsonNode a = M.readTree(r.value());
                    String op = a.path("op").asText("");
                    if (!"DISPATCH".equals(op) && !"CANCEL".equals(op)) continue;   // ignore CREATE/SUPPRESS
                    boolean batch = a.has("actions") && a.get("actions").isArray() && a.get("actions").size() > 0;
                    String channel = a.path("channel").asText("");
                    Metrics.counter("nba_activations_total", "channel", channel, "kind", op).increment();
                    // keyed by memberId (the activation carries NO nbaId — it lives only inside trackingId), so
                    // concurrent sends of the SAME arm to DIFFERENT members don't collide in WALKS and overwrite each
                    // other. batch = ONE send keyed memberId:batch:channel; single = memberId:actionId:channel
                    String memberId = a.path("memberId").asText("");
                    String slug = batch ? memberId + ":batch:" + channel
                            : memberId + ":" + a.path("actionId").asText("") + ":" + channel;
                    if ("DISPATCH".equals(op)) {
                        // begin the disposition walk: workflow already emitted IN_PROCESS; first disposition (Delivered->PRESENTED) fires ~immediately.
                        WALKS.put(slug, new Walk(a, channel, System.currentTimeMillis()));
                        log.info("accept " + slug + (batch ? " (batch x" + a.get("actions").size() + ")" : "") + " -> walking " + channel + " funnel");
                    } else {  // CANCEL = a SUPPRESS activation; we answer SUPPRESSED or SUPPRESS_FAILED
                        Walk w = WALKS.get(slug);
                        if (w == null || w.step < 0) {                       // not sent yet -> suppress succeeds
                            WALKS.remove(slug);
                            log.info("SUPPRESS ok (pre-send) " + slug);
                            disposition(producer, memberFacts, a, SUPPRESSED, "Cancelled");
                        } else {                                             // already sent -> suppress failed; the send proceeds through its funnel
                            log.info("SUPPRESS_FAILED (already sent) " + slug);
                            disposition(producer, memberFacts, a, SUPPRESS_FAILED, "AlreadySent");
                        }
                    }
                } catch (Exception e) {
                    log.warn("DLQ <- " + e + " :: " + r.value());
                    producer.send(dlqEnvelope(dlq, "action-layer", r, String.valueOf(e)));
                }
            }
            if (!recs.isEmpty()) consumer.commitSync();
        }
    }

    /** Advance one send by one funnel step (or to its FAILED/DECLINED branch), emitting the canonical
     *  disposition. Removes the walk when it reaches a terminal/soft-complete step. */
    static void advance(Producer<String, String> producer, String memberFacts, String slug, Walk w,
                        long stepMs, double failRate, double declineRate) {
        // First move off "scheduled": decide once whether this send will FAIL or be DECLINED (sim only).
        if (w.step < 0 && w.outcome == null) {
            double roll = Math.random();
            if (roll < failRate) w.outcome = FAILED;
            else {
                // INTELLIGENT opt-out: the probability RISES with over-contact (mirrors the env's OPTOUT_ESC), so it's
                // OVER-REACHING on outbound that triggers opt-outs. This is what makes pacing learnable + observable:
                // a policy that badgers a member drives that member to opt out (forfeiting all their future value), so
                // the RL policy is rewarded for spacing touches. declineRate is the flat floor; deterministic mode skips
                // it (tests stay predictable). totalThisWeek = the member's recent contact load (read from the snapshot).
                boolean det = SIM_MODE == null || SIM_MODE.contains("det");
                double contacts = det ? 0.0 : readFeatures(w.act.path("memberId").asText("")).getOrDefault("totalThisWeek", 0.0);
                double pOut = det ? declineRate
                                  : Math.min(0.5, declineRate + OPTOUT_ESC * Math.max(0.0, contacts - OPTOUT_FREE));
                if (roll < failRate + pOut) w.outcome = DECLINED;
            }
        }
        w.step++;
        String[][] funnel = w.funnel;

        // FAILED branch: after the first delivery-ish step, bounce instead of progressing.
        if (FAILED.equals(w.outcome) && w.step >= 1) {
            WALKS.remove(slug);
            disposition(producer, memberFacts, w.act, FAILED, FAIL_RAW.getOrDefault(w.channel, "Failed"));
            return;
        }
        // DECLINED branch: once PRESENTED, the member opts out instead of engaging.
        if (DECLINED.equals(w.outcome) && w.step >= Math.min(2, funnel.length - 1)) {
            WALKS.remove(slug);
            disposition(producer, memberFacts, w.act, DECLINED, DECLINE_RAW.getOrDefault(w.channel, "Declined"));
            return;
        }
        // REALISTIC ENGAGEMENT DROP-OFF (see STAGE_PROB): roll whether the member reaches this funnel stage. Only for
        // happy-path walks (FAIL/DECLINE-destined walks are handled above). Stage 0 miss = FAILED (bounce / no-answer);
        // a later miss = delivered-but-not-engaged -> stall here -> the workflow EXPIRES at TTL (the 'ignored' negative).
        // DETERMINISTIC mode (tests + simple demos) walks the FULL happy-path funnel — drop-off would make the
        // lifecycle non-deterministic and break the integration suite's SOFT_COMPLETED assertions. Realistic funnels
        // apply only in stochastic/journey (non-"det") mode, where realistic generated data is the goal.
        boolean deterministic = SIM_MODE == null || SIM_MODE.contains("det");
        if (w.outcome == null && !deterministic) {
            double[] sp = STAGE_PROB.get(w.channel);
            if (sp != null && w.step < sp.length && Math.random() >= sp[w.step]) {
                if (w.step == 0) disposition(producer, memberFacts, w.act, FAILED, FAIL_RAW.getOrDefault(w.channel, "Failed"));
                WALKS.remove(slug); return;
            }
        }
        if (w.step >= funnel.length) { WALKS.remove(slug); return; }   // funnel exhausted (reached soft bar)

        String raw = funnel[w.step][0], canonical = funnel[w.step][1];
        disposition(producer, memberFacts, w.act, canonical, raw);
        if (w.step >= funnel.length - 1) {                                 // emitted the soft bar (delivered) -> done walking
            maybeConvert(producer, memberFacts, w.act);                    // STOCHASTIC mode: sample HARD conversion at delivery
            WALKS.remove(slug); return;
        }
        w.nextAt = System.currentTimeMillis() + stepMs;
    }

    // STOCHASTIC mode only: at delivery, sample whether the member CONVERTS for each action ~ Bernoulli(rate),
    // where rate = nba:sim:propensity["actionId.channel"] (falling back to ["actionId"], default 0). A converter
    // gets a completion fact -> HARD_COMPLETED; a non-converter is left to EXPIRE at TTL (the negative label).
    // This is the tunable f*_live the model learns; shift it in Redis and the model adapts across retrains.
    static void maybeConvert(Producer<String, String> producer, String memberFacts, JsonNode a) {
        boolean journey = SIM_MODE != null && SIM_MODE.contains("journey");
        if (!journey && !"stochastic".equals(SIM_MODE)) return;
        String channel = a.path("channel").asText("");
        if (a.has("actions") && a.get("actions").isArray() && a.get("actions").size() > 0) {
            for (JsonNode act : a.get("actions"))
                stepOne(journey, producer, memberFacts, a, act.path("actionId").asText(""), act.path("trackingId").asText(""), channel);
        } else {
            stepOne(journey, producer, memberFacts, a, a.path("actionId").asText(""), a.path("trackingId").asText(""), channel);
        }
    }

    static void stepOne(boolean journey, Producer<String, String> producer, String memberFacts, JsonNode a, String actionId, String trackingId, String channel) {
        if (journey) advanceJourney(producer, memberFacts, a, actionId, trackingId, channel);
        else convertOne(producer, memberFacts, a, actionId, trackingId, channel);
    }

    static void convertOne(Producer<String, String> producer, String memberFacts, JsonNode a, String actionId, String trackingId, String channel) {
        double rate = convertRate(actionId, channel, a.path("memberId").asText(""));
        if (Math.random() >= rate) return;                                 // did not convert -> workflow EXPIRES (negative)
        emitCompletion(producer, memberFacts, a, actionId, nbaFromTracking(trackingId));
    }

    /** JOURNEY mode: a delivered touch elicits a positive response ~ Bernoulli(f*) (or f*>=0.5 in deterministic mode
     *  for predictable tests); on a positive response, ADVANCE the member's engagement features per ARM_EFFECT and
     *  emit them so the medallion evolves the member's state (next eval sees the progress). When the member crosses
     *  the Upgraded bar (the deep milestone) emit the HARD conversion. A non-response = no progress (the action just
     *  expires). This turns the live pipeline into the multi-step journey the RL policy sequences through. */
    static void advanceJourney(Producer<String, String> producer, String memberFacts, JsonNode a, String actionId, String trackingId, String channel) {
        String memberId = a.path("memberId").asText("");
        Map<String, Double> eff = effectFor(actionId);                     // Redis-driven (nba:sim:effect) for the full catalog
        if (eff == null || memberId.isEmpty()) return;
        double p = convertRate(actionId, channel, memberId);
        // deterministic (testing): respond on ANY non-compliance-gated touch (p>0) -> predictable progression while
        // still respecting the DNC/consent gates (which set p=0). stochastic (demo): Bernoulli(f*) -> realistic.
        boolean respond = SIM_MODE.contains("det") ? (p > 0.0) : (Math.random() < p);
        if (!respond) return;                                              // ignored this touch -> no progress
        Map<String, Double> f = readFeatures(memberId);
        String entityType = a.path("entityType").asText("OPERATOR");
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Double> e : eff.entrySet()) {
            String s = e.getKey(); double cur = f.getOrDefault(s, 0.0);
            if (BOOL_FEATS.contains(s)) {
                if (cur < 1) { f.put(s, 1.0); emitFeatureFact(producer, memberFacts, entityType, memberId, fullKeyOf(s), 1.0, true, now); }
            } else {
                double nv = cur + e.getValue(); f.put(s, nv);
                emitFeatureFact(producer, memberFacts, entityType, memberId, fullKeyOf(s), nv, false, now);
            }
        }
        double days = Math.max(0.0, f.getOrDefault("daysSinceLogin", 0.0) - 5.0);   // re-engaged -> recency improves
        emitFeatureFact(producer, memberFacts, entityType, memberId, "operator.activity.daysSinceLogin", days, false, now);
        if (f.getOrDefault("completedTasks", 0.0) >= 9 && f.getOrDefault("usedChat", 0.0) >= 1 && f.getOrDefault("viewedDashboard", 0.0) >= 1)
            emitCompletion(producer, memberFacts, a, actionId, nbaFromTracking(trackingId));   // Upgraded -> HARD conversion
    }

    static String fullKeyOf(String shortName) {
        for (String[] kv : FEAT) if (kv[1].equals(shortName)) return kv[0];
        return "operator.activity." + shortName;
    }

    /** Emit one member feature fact (a journey step) onto member.facts -> medallion -> snapshot evolves. */
    static void emitFeatureFact(Producer<String, String> producer, String memberFacts, String entityType, String memberId, String key, double val, boolean asBool, long now) {
        try {
            ObjectNode fact = M.createObjectNode();
            fact.put("key", key);
            if (asBool) { fact.put("value", true); fact.put("valueType", "BOOLEAN"); }
            else { fact.put("value", (long) Math.round(val)); fact.put("valueType", "LONG"); }
            fact.put("eventTs", now);
            fact.put("source", "activation-journey");
            fact.put("entityType", entityType);
            fact.put("entityId", memberId);
            producer.send(new ProducerRecord<>(memberFacts, entityType + ":" + memberId, M.writeValueAsString(fact)));
        } catch (Exception ignore) { }
    }

    /** The ground-truth convert probability for (member, action, channel). Prefer member-dependent f* (nba:sim:fstar:
     *  sigmoid(b + Σ w·feature) with the compliance/consent gates); fall back to the flat per-arm SIM_PROP if f* isn't
     *  set. Member features are read live from the snapshot (resolve nbaId via the idmap), so the world the model learns
     *  is the same one the simulator generates from (nba_ml_common.FSTAR). */
    static double convertRate(String actionId, String channel, String memberId) {
        JsonNode spec = (SIM_FSTAR == null) ? null : SIM_FSTAR.get(actionId);
        if (spec == null)                                                  // no f* coeffs -> flat fallback
            return SIM_PROP.getOrDefault(actionId + "." + channel, SIM_PROP.getOrDefault(actionId, 0.0));
        Map<String, Double> f = readFeatures(memberId);
        if (f.getOrDefault("isDNC", 0.0) >= 1) return 0.0;                 // compliance gate
        if ("sms".equals(channel) && f.getOrDefault("smsConsent", 0.0) < 1) return 0.0;  // consent gate
        double z = spec.path("b").asDouble(0.0);
        var it = spec.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if ("b".equals(e.getKey())) continue;
            z += e.getValue().asDouble(0.0) * f.getOrDefault(e.getKey(), 0.0);
        }
        z += channelAffinity(channel, f);                                  // member×channel preference (added to f*'s z)
        return 1.0 / (1.0 + Math.exp(-z));
    }

    /** Member×channel affinity term (nba:sim:channel_affinity[channel] = {featureCol: w}). Added to f*'s z so the SAME
     *  intent converts differently by channel for a given member (digital->email/push, age/SDOH->voice/mail). */
    static double channelAffinity(String channel, Map<String, Double> f) {
        if (SIM_CH_AFFINITY == null) return 0.0;
        JsonNode w = SIM_CH_AFFINITY.get(channel);
        if (w == null) return 0.0;
        double a = 0.0;
        var fit = w.fields();
        while (fit.hasNext()) { Map.Entry<String, JsonNode> e = fit.next(); a += e.getValue().asDouble(0.0) * f.getOrDefault(e.getKey(), 0.0); }
        return a;
    }

    /** Read the member's feature vector from the snapshot (one hmget), keyed by the short FSTAR coeff names. */
    static Map<String, Double> readFeatures(String memberId) {
        Map<String, Double> out = new java.util.HashMap<>();
        try {
            String nbaId = REDIS.get("nba:idmap:OPERATOR:" + memberId);
            if (nbaId == null) return out;
            String[] hf = new String[FEAT.length];
            for (int i = 0; i < FEAT.length; i++) hf[i] = "fact:" + FEAT[i][0];
            List<String> vals = REDIS.hmget("nba:snapshot:" + nbaId, hf);
            for (int i = 0; i < FEAT.length; i++) {
                double v = 0.0;
                String j = vals.get(i);
                if (j != null) {
                    JsonNode node = M.readTree(j).path("value");
                    v = node.isBoolean() ? (node.asBoolean() ? 1.0 : 0.0) : node.asDouble(0.0);
                }
                out.put(FEAT[i][1], v);
            }
        } catch (Exception ignore) { }
        return out;
    }

    static String nbaFromTracking(String trackingId) {                     // "nba-ca:{nbaId}:{actionId}:{channel}|{corr}"
        if (trackingId == null) return "";
        int bar = trackingId.indexOf('|');
        String[] p = (bar > 0 ? trackingId.substring(0, bar) : trackingId).split(":");
        return p.length >= 2 ? p[1] : "";
    }

    // A completion fact: nba.completion.{actionId} = "completed" (kind=completion) — the temporal disposition
    // consumer fans hardComplete() out to the member's channel workflows -> HARD_COMPLETED. Same path real
    // conversions take via the action-library /completion endpoint; here the sim injects it.
    static void emitCompletion(Producer<String, String> producer, String memberFacts, JsonNode a, String actionId, String nbaId) {
        String entityType = a.path("entityType").asText(""), memberId = a.path("memberId").asText("");
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", entityType);
        fact.put("entityId", memberId);
        if (!nbaId.isEmpty()) fact.put("nbaId", nbaId);
        fact.put("key", "nba.completion." + actionId);
        fact.put("value", "completed");
        fact.put("valueType", "STRING");
        fact.put("actionId", actionId);
        fact.put("eventTs", System.currentTimeMillis());
        fact.put("source", "action-layer-sim");
        fact.put("correlationId", a.path("correlationId").asText(""));
        String json;
        try { json = M.writeValueAsString(fact); } catch (Exception e) { throw new RuntimeException(e); }
        ProducerRecord<String, String> rec = new ProducerRecord<>(memberFacts, entityType + ":" + memberId, json);
        rec.headers().add("kind", "completion".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        producer.send(rec);
        log.info("[action-layer][SIM] CONVERTED " + memberId + ":" + actionId + " -> completion emitted");
    }

    // A disposition is a MEMBER FACT: nba.disposition.{actionId}.{channel} = <canonicalState>, with the raw
    // provider status in `raw`. A BATCH fans out to ONE disposition per action (each with its trackingId so
    // the per-action tracker workflow gets it); a single emits one. Addressed by entityType + memberId.
    static void disposition(Producer<String, String> producer, String memberFacts, JsonNode a, String state, String raw) {
        if (a.has("actions") && a.get("actions").isArray() && a.get("actions").size() > 0) {
            for (JsonNode act : a.get("actions"))
                emitDisposition(producer, memberFacts, a, act.path("actionId").asText(""), act.path("contentKey").asText(""), act.path("trackingId").asText(""), state, raw);
        } else {
            emitDisposition(producer, memberFacts, a, a.path("actionId").asText(""), a.path("contentKey").asText(""), a.path("trackingId").asText(""), state, raw);
        }
    }

    static void emitDisposition(Producer<String, String> producer, String memberFacts, JsonNode a,
                                String actionId, String contentKey, String trackingId, String state, String raw) {
        String entityType = a.path("entityType").asText("");
        String memberId = a.path("memberId").asText("");
        String channel = a.path("channel").asText("");
        String key = "nba.disposition." + actionId + "." + channel;
        ObjectNode fact = M.createObjectNode();
        fact.put("entityType", entityType);
        fact.put("entityId", memberId);          // member fact addressed by memberId (no nbaId)
        fact.put("key", key);
        fact.put("value", raw);                  // VALUE = the RAW provider status — the ONLY thing the sender
                                                 // reports. The rules engine evaluates soft-completion off it,
                                                 // and the STATE MACHINE classifies raw -> canonical itself
                                                 // (DispositionClassifier); the sender no longer decides state.
        fact.put("valueType", "STRING");
        fact.put("eventTs", System.currentTimeMillis());
        fact.put("source", "action-layer");
        fact.put("correlationId", a.path("correlationId").asText(""));
        fact.put("memberId", memberId);
        fact.put("channel", channel);
        fact.put("contentKey", contentKey);
        fact.put("trackingId", trackingId);
        String json;
        try { json = M.writeValueAsString(fact); } catch (Exception e) { throw new RuntimeException(e); }
        // keyed by memberId (entityType:entityId) so all of a member's facts share a partition — the
        // snapshot-builder then owns a member race-free. The fact key lives in the body, not the kafka key.
        ProducerRecord<String, String> rec = new ProducerRecord<>(memberFacts, entityType + ":" + memberId, json);
        rec.headers().add("kind", "disposition".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        producer.send(rec);
        Metrics.counter("nba_dispositions_total", "status", state, "channel", channel).increment();
        log.info("disposition " + memberId + ":" + actionId + ":" + channel + " = " + state + " (" + raw + ")");
    }

    static Properties consumerProps(String bootstrap) {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, "action-layer");
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");   // act on live activations only
        cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return cp;
    }

    static Properties producerProps(String bootstrap) {
        Properties pp = new Properties();
        pp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        return pp;
    }

    /** Standard DLQ envelope: source coordinates + raw value + error (replayable to the source topic). */
    static ProducerRecord<String, String> dlqEnvelope(String dlqTopic, String consumer, ConsumerRecord<String, String> r, String error) {
        try {
            ObjectNode env = M.createObjectNode();
            env.put("consumer", consumer); env.put("topic", r.topic());
            env.put("partition", r.partition()); env.put("offset", r.offset());
            if (r.key() != null) env.put("key", r.key()); else env.putNull("key");
            env.put("value", r.value());
            ObjectNode h = env.putObject("headers");
            for (org.apache.kafka.common.header.Header hd : r.headers())
                if (hd.value() != null) h.put(hd.key(), new String(hd.value(), java.nio.charset.StandardCharsets.UTF_8));
            env.put("error", error); env.put("dlqTs", System.currentTimeMillis());
            return new ProducerRecord<>(dlqTopic, r.key(), M.writeValueAsString(env));
        } catch (Exception e) { return new ProducerRecord<>(dlqTopic, r.key(), r.value()); }
    }

    // Test hook: when NBA_FAULT_INJECT is set, any record whose raw value CONTAINS that substring throws —
    // exercises this consumer's DLQ + replay path on demand. Empty (default) = no-op, no prod impact.
    static final String FAULT_INJECT = env("NBA_FAULT_INJECT", "");
    static void maybeInjectFault(String raw) {
        if (!FAULT_INJECT.isEmpty() && raw != null && raw.contains(FAULT_INJECT))
            throw new RuntimeException("injected fault (NBA_FAULT_INJECT=" + FAULT_INJECT + ")");
    }

    static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }
}
