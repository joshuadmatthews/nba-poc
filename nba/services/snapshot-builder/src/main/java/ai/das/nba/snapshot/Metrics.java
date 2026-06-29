package ai.das.nba.snapshot;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Shared Prometheus wiring for the NBA services. One process-wide {@link PrometheusMeterRegistry}
 * pre-bound with JVM + process gauges and a {@code service} common tag (NBA_SERVICE env).
 *
 * HTTP services mount {@link #scrape()} on their existing app (GET /metrics); workers with no HTTP
 * surface call {@link #serve(int)} once to expose /metrics (+ /health) on a side port. Meters are
 * looked up by name+tags and cached by Micrometer, so {@link #counter}/{@link #timer} are cheap to
 * call on the hot path.
 */
public final class Metrics {
    private static final Logger log = LoggerFactory.getLogger(Metrics.class);

    public static final PrometheusMeterRegistry REGISTRY =
            new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    static {
        REGISTRY.config().commonTags("service", System.getenv().getOrDefault("NBA_SERVICE", "snapshot-builder"));
        new ClassLoaderMetrics().bindTo(REGISTRY);
        new JvmMemoryMetrics().bindTo(REGISTRY);
        new JvmGcMetrics().bindTo(REGISTRY);
        new JvmThreadMetrics().bindTo(REGISTRY);
        new ProcessorMetrics().bindTo(REGISTRY);
        new UptimeMetrics().bindTo(REGISTRY);
    }

    private Metrics() {}

    /** Cached counter by name+tags (tags as k,v,k,v...). */
    public static Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(REGISTRY);
    }

    /** Cached timer (p50/p95/p99 client-side percentiles) by name+tags (tags as k,v,k,v...). */
    public static Timer timer(String name, String... tags) {
        return Timer.builder(name).publishPercentiles(0.5, 0.95, 0.99).tags(tags).register(REGISTRY);
    }

    public static String scrape() { return REGISTRY.scrape(); }

    /** Workers with no HTTP surface: expose GET /metrics (+ /health) on a side port. Best-effort. */
    public static void serve(int port) {
        try {
            HttpServer s = HttpServer.create(new InetSocketAddress(port), 0);
            s.createContext("/metrics", ex -> {
                byte[] body = REGISTRY.scrape().getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                ex.sendResponseHeaders(200, body.length);
                try (OutputStream os = ex.getResponseBody()) { os.write(body); }
            });
            s.createContext("/health", ex -> {
                byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
                ex.sendResponseHeaders(200, body.length);
                try (OutputStream os = ex.getResponseBody()) { os.write(body); }
            });
            s.setExecutor(null);
            s.start();
            log.info("metrics endpoint up on :{} (/metrics)", port);
        } catch (Exception e) {
            log.warn("metrics endpoint failed to start on :{}", port, e);
        }
    }
}
