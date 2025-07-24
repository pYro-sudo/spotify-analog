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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ApplicationScoped
@Slf4j
public class RouterProxy extends AbstractVerticle {
    private static final AtomicInteger activeShards = new AtomicInteger(0);

    @Inject
    EventBus eventBus;
    @Inject
    Vertx vertx;
    @Inject
    PrometheusMeterRegistry meterRegistry;

    @ConfigProperty(name = "router.instances", defaultValue = "4")
    int routerInstances;
    @ConfigProperty(name = "shard.map.name", defaultValue = "router-proxy-shards")
    String shardMapName;
    @ConfigProperty(name = "inbound.prefix", defaultValue = "resource.proxy-")
    String inboundPrefix;
    @ConfigProperty(name = "router.prefix", defaultValue = "router.backend-")
    String routerPrefix;

    @ConfigProperty(name = "shard.key", defaultValue = "shard-")
    String shardKey;

    @Override
    public Uni<Void> asyncStart() {
        return vertx.sharedData().<String, String>getAsyncMap(shardMapName)
                .onItem().transformToUni(asyncMap -> Uni.combine().all().unis(
                        IntStream.range(0, routerInstances)
                                .mapToObj(shardIndex -> acquireShard(asyncMap, shardIndex))
                                .collect(Collectors.toList())
                ).discardItems())
                .onItem().invoke(() -> log.info("Acquired {} shards", activeShards.get()))
                .onFailure().invoke(e -> log.error("Shard acquisition failed", e));
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
                    forwardMessage(outboundAddress, msg);
                })
                .completionHandler()
                .onItem().invoke(() -> log.info("Handler registered for shard {}", shardIndex))
                .onFailure().invoke(e -> log.error("Failed to register handler for shard {}", shardIndex, e));

    }

    private void forwardMessage(String outboundAddress, io.vertx.mutiny.core.eventbus.Message<JsonObject> msg) {
        long startTime = System.currentTimeMillis();
        JsonObject body = msg.body();
        String aggregateId = body.getString("aggregateId");

        eventBus.<JsonObject>request(outboundAddress, body)
                .subscribe().with(
                        reply -> {
                            meterRegistry.timer("router.proxy.forward.time")
                                    .record(Duration.ofMillis(System.currentTimeMillis() - startTime));
                            msg.reply(reply.body());
                        },
                        failure -> {
                            meterRegistry.counter("router.proxy.forward.errors").increment();
                            log.error("Forward failed [shard:{}] [aggregateId:{}]",
                                    outboundAddress, aggregateId, failure);
                            msg.fail(500, "Forwarding failed: " + failure.getMessage());
                        }
                );
    }

    @Override
    public Uni<Void> asyncStop() {
        return releaseAllShards()
                .onItem().invoke(() -> log.info("Released all shards"))
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