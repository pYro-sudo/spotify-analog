package by.losik.router.proxy;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Slf4j
public class RouterProxy extends AbstractVerticle {
    @ConfigProperty(name = "inbound.address", defaultValue = "resource.proxy")
    String INBOUND_ADDRESS;
    @ConfigProperty(name = "router.address", defaultValue = "router.backend")
    String ROUTER_ADDRESS;

    @Inject
    EventBus eventBus;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @PostConstruct
    void init() {
        meterRegistry.counter("router.proxy.requests.total").increment(0);
        meterRegistry.counter("router.proxy.successes.total").increment(0);
        meterRegistry.counter("router.proxy.failures.total").increment(0);

        eventBus.<JsonObject>consumer(INBOUND_ADDRESS, msg -> {
            String requestId = UUID.randomUUID().toString();
            meterRegistry.counter("router.proxy.requests.total").increment();

            long startTime = System.currentTimeMillis();
            JsonObject enriched = enrichMessage(msg.body(), requestId);
            meterRegistry.timer("router.proxy.enrichment.time").record(Duration.ofMillis(System.currentTimeMillis() - startTime));

            log.debug("Forwarding request {} to routers", requestId);

            long routingStartTime = System.currentTimeMillis();
            eventBus.request(ROUTER_ADDRESS, enriched)
                    .subscribe().with(
                            reply -> {
                                long routingTime = System.currentTimeMillis() - routingStartTime;
                                meterRegistry.timer("router.proxy.routing.time").record(Duration.ofMillis(routingTime));
                                msg.reply(reply.body());
                                meterRegistry.counter("router.proxy.successes.total").increment();
                                meterRegistry.summary("router.proxy.total_time").record(System.currentTimeMillis() - startTime);
                            },
                            failure -> {
                                long routingTime = System.currentTimeMillis() - routingStartTime;
                                meterRegistry.timer("router.proxy.routing.time").record(Duration.ofNanos(routingTime));
                                msg.fail(500, "Routing failed: " + failure.getMessage());
                                meterRegistry.counter("router.proxy.failures.total").increment();
                                meterRegistry.counter("router.proxy.failures", "type", failure.getClass().getSimpleName()).increment();
                                meterRegistry.summary("router.proxy.total_time").record(System.currentTimeMillis() - startTime);
                            }
                    );
        });
    }

    private JsonObject enrichMessage(JsonObject original, String requestId) {
        return original.copy()
                .put("proxy_timestamp", Instant.now().toString())
                .put("request_id", requestId)
                .put("hops", original.getInteger("hops", 0) + 1);
    }
}