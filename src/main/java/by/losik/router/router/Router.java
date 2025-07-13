package by.losik.router.router;

import by.losik.configuration.Status;
import by.losik.service.MessageService;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.Message;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.time.Duration;

@Slf4j
public class Router extends AbstractVerticle {
    @Inject
    @Channel("kafka-in")
    Emitter<JsonObject> kafkaEmitter;
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

        log.info("[Shard {}] Processing message {}", shardIndex, messageId);

        return Uni.createFrom().item(() -> createResponse(body, routerId, shardIndex))
                .onItem().transformToUni(response -> {
                    long kafkaStartTime = System.currentTimeMillis();
                    return Uni.createFrom().completionStage(kafkaEmitter.send(body).toCompletableFuture())
                            .onItem().transformToUni(i -> {
                                long kafkaDuration = System.currentTimeMillis() - kafkaStartTime;
                                meterRegistry.timer("router.kafka.processing.time").record(Duration.ofMillis(kafkaDuration));
                                log.info("[Shard {}] Message {} sent to Kafka", shardIndex, messageId);
                                return messageService.updateMessageStatus(messageId, Status.PROCESSED)
                                        .onItem().invoke(updated -> msg.reply(response));
                            })
                            .onFailure().recoverWithUni(e -> {
                                long kafkaDuration = System.currentTimeMillis() - kafkaStartTime;
                                meterRegistry.timer("router.kafka.processing.time").record(Duration.ofMillis(kafkaDuration));
                                meterRegistry.counter("router.kafka.errors").increment();
                                log.error("[Shard {}] Kafka send failed for message {}", shardIndex, messageId, e);
                                return messageService.updateMessageStatus(messageId, Status.FAILED)
                                        .onItem().invoke(updated -> msg.fail(500, "Kafka error: " + e.getMessage()));
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