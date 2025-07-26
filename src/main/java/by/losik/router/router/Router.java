package by.losik.router.router;

import by.losik.entity.Status;
import by.losik.router.producer.DynamicEmitterFactory;
import by.losik.service.MessageService;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Duration;

@Slf4j
public class Router extends AbstractVerticle {
    @ConfigProperty(name = "proxy.response.address", defaultValue = "proxy.responses")
    String proxyResponseAddress;

    @Inject
    DynamicEmitterFactory emitterFactory;

    @Inject
    MessageService messageService;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @Override
    public Uni<Void> asyncStart() {
        String routerId = deploymentID();
        int shardIndex = extractShardIndex(routerId);
        String shardedAddress = "router.backend-" + shardIndex;

        meterRegistry.counter("router.messages.total").increment(0);
        meterRegistry.counter("router.messages.success").increment(0);
        meterRegistry.counter("router.messages.failed").increment(0);
        meterRegistry.counter("router.kafka.errors").increment(0);

        return vertx.eventBus().<JsonObject>consumer(shardedAddress)
                .handler(msg -> {
                    long startTime = System.currentTimeMillis();
                    meterRegistry.counter("router.messages.total").increment();
                    meterRegistry.gauge("router.shard." + shardIndex + ".active.messages", 1);

                    processMessage(msg, routerId, shardIndex)
                            .subscribe().with(
                                    item -> {
                                        long duration = System.currentTimeMillis() - startTime;
                                        meterRegistry.timer("router.processing.time").record(Duration.ofMillis(duration));
                                        meterRegistry.counter("router.messages.success").increment();
                                        meterRegistry.gauge("router.shard." + shardIndex + ".active.messages", 0);
                                        log.debug("[Shard {}] Message processed successfully", shardIndex);
                                    },
                                    failure -> {
                                        long duration = System.currentTimeMillis() - startTime;
                                        meterRegistry.timer("router.processing.time").record(Duration.ofMillis(duration));
                                        meterRegistry.counter("router.messages.failed").increment();
                                        meterRegistry.gauge("router.shard." + shardIndex + ".active.messages", 0);
                                        log.error("[Shard {}] Message processing failed", shardIndex, failure);
                                    }
                            );
                })
                .completionHandler()
                .onItem().invoke(() ->
                        log.info("Router {} started on shard {}", routerId, shardIndex))
                .onFailure().invoke(e ->
                        log.error("Router {} failed to start on shard {}", routerId, shardIndex, e));
    }

    private int extractShardIndex(String deploymentId) {
        String[] parts = deploymentId.split("_");
        return Integer.parseInt(parts[1].split("@")[0]);
    }

    private Uni<Void> processMessage(Message<JsonObject> msg, String routerId, int shardIndex) {
        JsonObject body = msg.body();
        Long messageId = body.getLong("id");
        String eventType = body.getString("eventType", "default");
        String targetChannel = "kafka-out-" + eventType.toLowerCase();

        String originalReplyAddress = body.getString("originalReplyAddress", msg.replyAddress());
        JsonObject kafkaMessage = body.copy()
                .put("originalReplyAddress", originalReplyAddress)
                .put("routerAddress", "router.backend-" + shardIndex)
                .put("routerId", routerId)
                .put("shardIndex", shardIndex);

        log.info("[Shard {}] Processing message {} for channel {}", shardIndex, messageId, targetChannel);

        return Uni.createFrom().item(() -> createResponse(kafkaMessage, routerId, shardIndex))
                .onItem().transformToUni(response -> {
                    long kafkaStartTime = System.currentTimeMillis();
                    return Uni.createFrom().completionStage(
                                    emitterFactory.getEmitter(targetChannel).send(kafkaMessage).toCompletableFuture())
                            .onItem().transformToUni(i -> {
                                long kafkaDuration = System.currentTimeMillis() - kafkaStartTime;
                                meterRegistry.timer("router.kafka.processing.time",
                                                "channel", targetChannel)
                                        .record(Duration.ofMillis(kafkaDuration));
                                log.info("[Shard {}] Message {} sent to Kafka channel {}",
                                        shardIndex, messageId, targetChannel);
                                return messageService.updateMessageStatus(messageId, Status.PROCESSED);
                            })
                            .onFailure().recoverWithUni(e -> {
                                long kafkaDuration = System.currentTimeMillis() - kafkaStartTime;
                                meterRegistry.timer("router.kafka.processing.time",
                                                "channel", targetChannel)
                                        .record(Duration.ofMillis(kafkaDuration));
                                meterRegistry.counter("router.kafka.errors",
                                        "channel", targetChannel).increment();
                                log.error("[Shard {}] Kafka send failed for message {} on channel {}",
                                        shardIndex, messageId, targetChannel, e);
                                return messageService.updateMessageStatus(messageId, Status.FAILED)
                                        .onItem().invoke(updated ->
                                                msg.fail(500, "Kafka error: " + e.getMessage()));
                            });
                })
                .replaceWithVoid();
    }

    private JsonObject createResponse(JsonObject body, String routerId, int shardIndex) {
        return new JsonObject()
                .put("status", "processed")
                .put("by", routerId)
                .put("shard", shardIndex)
                .mergeIn(body);
    }
}