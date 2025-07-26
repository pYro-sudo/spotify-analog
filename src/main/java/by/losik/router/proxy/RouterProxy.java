package by.losik.router.proxy;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.shareddata.AsyncMap;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ApplicationScoped
@Slf4j
public class RouterProxy extends AbstractVerticle {
    private static final AtomicInteger activeShards = new AtomicInteger(0);
    private final ConcurrentMap<String, io.vertx.mutiny.core.eventbus.Message<JsonObject>> pendingRequests = new ConcurrentHashMap<>();

    @Inject EventBus eventBus;
    @Inject Vertx vertx;
    @Inject PrometheusMeterRegistry meterRegistry;

    @ConfigProperty(name = "router.instances", defaultValue = "4") int routerInstances;
    @ConfigProperty(name = "shard.map.name", defaultValue = "router-proxy-shards") String shardMapName;
    @ConfigProperty(name = "inbound.prefix", defaultValue = "resource.proxy-") String inboundPrefix;
    @ConfigProperty(name = "router.prefix", defaultValue = "router.backend-") String routerPrefix;
    @ConfigProperty(name = "proxy.response.address", defaultValue = "proxy.responses") String proxyResponseAddress;
    @ConfigProperty(name = "shard.key", defaultValue = "shard-") String shardKey;

    @Override
    public Uni<Void> asyncStart() {
        return vertx.sharedData().<String, String>getAsyncMap(shardMapName)
                .onItem().transformToUni(asyncMap -> Uni.combine().all().unis(
                                IntStream.range(0, routerInstances)
                                        .mapToObj(shardIndex -> acquireShard(asyncMap, shardIndex))
                                        .collect(Collectors.toList()))
                        .discardItems()
                        .onItem().invoke(() -> {
                            log.info("Acquired {} shards", activeShards.get());
                            setupResponseConsumer();
                        })
                        .onFailure().invoke(e -> log.error("Shard acquisition failed", e)));
    }

    private Uni<Void> acquireShard(AsyncMap<String, String> asyncMap, int shardIndex) {
        String instanceId = deploymentID();
        return asyncMap.putIfAbsent(shardKey.concat(String.valueOf(shardIndex)), instanceId)
                .onItem().transformToUni(previousOwner -> {
                    if (previousOwner == null || previousOwner.equals(instanceId)) {
                        log.info("Acquired shard {} (instance: {})", shardIndex, instanceId);
                        activeShards.incrementAndGet();
                        meterRegistry.gauge("router.proxy.shards.active", activeShards);
                        return setupShardHandler(shardIndex);
                    }
                    log.debug("Shard {} already owned by {}", shardIndex, previousOwner);
                    return Uni.createFrom().voidItem();
                })
                .onFailure().recoverWithUni(e -> {
                    log.error("Failed to acquire shard {}", shardIndex, e);
                    return Uni.createFrom().voidItem();
                });
    }

    private Uni<Void> setupShardHandler(int shardIndex) {
        String inboundAddress = inboundPrefix + shardIndex;
        String outboundAddress = routerPrefix + shardIndex;

        return vertx.eventBus().<JsonObject>consumer(inboundAddress)
                .handler(msg -> {
                    meterRegistry.counter("router.proxy.forwarded.messages").increment();
                    JsonObject messageBody = msg.body().copy();

                    if (msg.replyAddress() != null) {
                        String requestId = messageBody.getString("aggregateId", java.util.UUID.randomUUID().toString());
                        pendingRequests.put(requestId, msg);
                        messageBody.put("originalReplyAddress", msg.replyAddress());
                    }

                    forwardMessage(outboundAddress, messageBody);
                })
                .completionHandler()
                .onItem().invoke(() -> log.info("Handler registered for shard {}", shardIndex))
                .onFailure().invoke(e -> log.error("Failed to register handler for shard {}", shardIndex, e));
    }

    private void forwardMessage(String outboundAddress, JsonObject messageBody) {
        long startTime = System.currentTimeMillis();
        String aggregateId = messageBody.getString("aggregateId");

        eventBus.<JsonObject>request(outboundAddress, messageBody)
                .subscribe().with(
                        reply -> {
                            meterRegistry.timer("router.proxy.forward.time")
                                    .record(Duration.ofMillis(System.currentTimeMillis() - startTime));
                        },
                        failure -> {
                            meterRegistry.counter("router.proxy.forward.errors").increment();
                            log.error("Forward failed [shard:{}] [aggregateId:{}]", outboundAddress, aggregateId, failure);
                            // If forwarding fails, fail the original request
                            io.vertx.mutiny.core.eventbus.Message<JsonObject> originalMsg = pendingRequests.remove(aggregateId);
                            if (originalMsg != null) {
                                originalMsg.fail(500, "Forwarding failed: " + failure.getMessage());
                            }
                        }
                );
    }

    private void setupResponseConsumer() {
        vertx.eventBus().<JsonObject>consumer(proxyResponseAddress)
                .handler(msg -> {
                    JsonObject response = msg.body();
                    String aggregateId = response.getString("aggregateId");
                    String originalAddress = response.getString("originalReplyAddress");

                    if (originalAddress != null && aggregateId != null) {
                        io.vertx.mutiny.core.eventbus.Message<JsonObject> originalMsg = pendingRequests.remove(aggregateId);
                        if (originalMsg != null) {
                            originalMsg.reply(response);
                            log.debug("Routed response back to {}", originalAddress);
                            meterRegistry.counter("router.proxy.responses.routed").increment();
                        } else {
                            log.warn("No pending request found for aggregateId: {}", aggregateId);
                            meterRegistry.counter("router.proxy.responses.orphaned").increment();
                        }
                    } else {
                        log.error("Missing originalReplyAddress or aggregateId in response: {}", response);
                        meterRegistry.counter("router.proxy.responses.dropped").increment();
                    }
                })
                .exceptionHandler(e -> {
                    log.error("Error in response consumer", e);
                    meterRegistry.counter("router.proxy.responses.errors").increment();
                });
    }

    @Override
    public Uni<Void> asyncStop() {
        return releaseAllShards()
                .onItem().invoke(() -> {
                    log.info("Released all shards");
                    pendingRequests.clear();
                })
                .onFailure().invoke(e -> log.error("Failed to release shards", e));
    }

    private Uni<Void> releaseAllShards() {
        return vertx.sharedData().<String, String>getAsyncMap(shardMapName)
                .onItem().transformToUni(asyncMap -> {
                    String instanceId = deploymentID();
                    return Uni.combine().all().unis(
                            IntStream.range(0, routerInstances)
                                    .mapToObj(shardIndex ->
                                            asyncMap.removeIfPresent("shard-" + shardIndex, instanceId))
                                    .collect(Collectors.toList())
                    ).discardItems();
                })
                .onItem().invoke(() -> activeShards.set(0));
    }
}