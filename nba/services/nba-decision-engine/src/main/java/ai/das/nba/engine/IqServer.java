package ai.das.nba.engine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Interactive-Query read surface — this is what REPLACES the Redis read stores. Readers call:
 *   GET /snapshot/{nbaId}    -> the member snapshot (built from the snapshot store), shape == old getSnapshot.
 *   GET /eligibility/{nbaId} -> the member's latest eval (the materialized nba.evaluations), == old nba:eligibility.
 * Both are served straight from this app's local RocksDB-backed state (microseconds). No Redis.
 *
 * Scale-correct across pods: each store is sharded by the member-partitioning, so a read for a member this pod
 * doesn't own is 307-redirected to the pod that does (queryMetadataForKey + the per-pod APPLICATION_SERVER
 * advertised address). A single pod owns everything -> always a local read.
 *
 * NB: this com.sun.net.httpserver server is a POC read surface — for production read QPS swap to a real server
 * (Javalin/Netty) + a routing-aware client (or co-locate the reader), retry on 503 (rebalance window), standbys.
 */
final class IqServer {
    private static final Logger log = LoggerFactory.getLogger(IqServer.class);
    private static final TypeReference<HashMap<String, String>> HASH = new TypeReference<>() {};
    private static final StringSerializer KEY_SER = new StringSerializer();

    /** Turn a stored value into the response body for a given key (snapshot: rebuild JSON; eligibility: raw). */
    @FunctionalInterface
    interface Transform { String apply(String nbaId, String stored) throws Exception; }

    private IqServer() {}

    static void serve(HostInfo self, KafkaStreams streams, String snapshotStore, String eligibilityStore) {
        try {
            HttpServer s = HttpServer.create(new InetSocketAddress(self.port()), 0);
            s.createContext("/snapshot/", ex -> handle(ex, self, streams, snapshotStore, "/snapshot/",
                    (id, stored) -> SnapshotLogic.buildSnapshotJson(id, SnapshotLogic.M.readValue(stored, HASH))));
            s.createContext("/eligibility/", ex -> handle(ex, self, streams, eligibilityStore, "/eligibility/",
                    (id, stored) -> stored));                       // the materialized eval is already the body
            s.createContext("/health", ex -> respond(ex, 200, "ok", false));
            s.setExecutor(null);
            s.start();
            log.info("IQ read surface up on :{} (GET /snapshot/{{id}}, /eligibility/{{id}}) — replaces the nba:snapshot + nba:eligibility Redis reads", self.port());
        } catch (Exception e) {
            log.warn("IQ endpoint failed to start on :{}", self.port(), e);
        }
    }

    private static void handle(HttpExchange ex, HostInfo self, KafkaStreams streams, String storeName,
                               String prefix, Transform transform) {
        try {
            String nbaId = ex.getRequestURI().getPath().substring(prefix.length());
            if (nbaId.isEmpty()) { respond(ex, 400, "{\"error\":\"nbaId required\"}", true); return; }

            // Which pod owns this member's partition? Redirect if it isn't us (no-op on a single pod).
            KeyQueryMetadata md = streams.queryMetadataForKey(storeName, nbaId, KEY_SER);
            if (md == null || md.equals(KeyQueryMetadata.NOT_AVAILABLE)) {
                respond(ex, 503, "{\"error\":\"state not available (rebalancing)\"}", true); return;
            }
            HostInfo owner = md.activeHost();
            if (!owner.equals(self)) {
                ex.getResponseHeaders().set("Location", "http://" + owner.host() + ":" + owner.port() + prefix + nbaId);
                respond(ex, 307, "", false); return;
            }

            ReadOnlyKeyValueStore<String, String> store =
                    streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
            String stored = store.get(nbaId);
            if (stored == null) { respond(ex, 404, "{\"error\":\"not found\",\"nbaId\":\"" + nbaId + "\"}", true); return; }
            respond(ex, 200, transform.apply(nbaId, stored), true);
            Metrics.counter("nba_engine_iq_reads_total", "store", storeName).increment();
        } catch (InvalidStateStoreException e) {
            respond(ex, 503, "{\"error\":\"store migrating\"}", true);            // store not yet (re)assigned
        } catch (Exception e) {
            log.warn("IQ read failed", e);
            respond(ex, 500, "{\"error\":\"read failed\"}", true);
        }
    }

    private static void respond(HttpExchange ex, int code, String body, boolean json) {
        try {
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            if (json) ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(code, b.length == 0 ? -1 : b.length);
            if (b.length > 0) try (OutputStream os = ex.getResponseBody()) { os.write(b); }
            else ex.close();
        } catch (IOException ignore) { ex.close(); }
    }
}
