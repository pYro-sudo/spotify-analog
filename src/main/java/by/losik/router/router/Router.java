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

        meterRegistry.counter("router.messages.total").increment(0);
        meterRegistry.counter("router.messages.success").increment(0);
        meterRegistry.counter("router.messages.failed").increment(0);
        meterRegistry.counter("router.kafka.errors").increment(0);

        return vertx.eventBus().<JsonObject>consumer("router.backend")
                .handler(msg -> {
                    long startTime = System.currentTimeMillis();
                    meterRegistry.counter("router.messages.total").increment();

                    processMessage(msg, routerId)
                            .subscribe().with(
                                    item -> {
                                        long duration = System.currentTimeMillis() - startTime;
                                        meterRegistry.timer("router.processing.time").record(Duration.ofMillis(duration));
                                        meterRegistry.counter("router.messages.success").increment();
                                        log.debug("Message processed successfully {}", item);
                                    },
                                    failure -> {
                                        long duration = System.currentTimeMillis() - startTime;
                                        meterRegistry.timer("router.processing.time").record(Duration.ofMillis(duration));
                                        meterRegistry.counter("router.messages.failed").increment();
                                        log.error("Message processing failed", failure);
                                    }
                            );
                })
                .completionHandler()
                .onItem().invoke(() -> log.info("Router {} started successfully", routerId))
                .onFailure().invoke(e -> log.error("Router {} failed to start", routerId, e));
    }

    private Uni<Void> processMessage(Message<JsonObject> msg, String routerId) {
        JsonObject body = msg.body();
        Long messageId = body.getLong("id");

        log.info("Router {} processing", routerId);

        return Uni.createFrom().item(() -> createResponse(body, routerId))
                .onItem().transformToUni(response -> {
                    long kafkaStartTime = System.currentTimeMillis();
                    return Uni.createFrom().completionStage(kafkaEmitter.send(JsonObject.mapFrom(msg)).toCompletableFuture())
                            .onItem().transformToUni(i -> {
                                long kafkaDuration = System.currentTimeMillis() - kafkaStartTime;
                                meterRegistry.timer("router.kafka.processing.time").record(Duration.ofMillis(kafkaDuration));

                                log.info("Message {} processed and sent to Kafka", messageId);
                                return messageService.updateMessageStatus(messageId, Status.PROCESSED)
                                        .onItem().invoke(updated -> msg.reply(response));
                            })
                            .onFailure().recoverWithUni(e -> {
                                long kafkaDuration = System.currentTimeMillis() - kafkaStartTime;
                                meterRegistry.timer("router.kafka.processing.time").record(Duration.ofMillis(kafkaDuration));
                                meterRegistry.counter("router.kafka.errors").increment();

                                log.error("Failed to send message {} to Kafka", messageId, e);
                                return messageService.updateMessageStatus(messageId, Status.FAILED)
                                        .onItem().invoke(updated -> msg.fail(500, "Failed to send to Kafka: " + e.getMessage()));
                            });
                })
                .replaceWithVoid();
    }

    private JsonObject createResponse(JsonObject body, String routerId) {
        return new JsonObject()
                .put("status", "processed")
                .put("by", routerId)
                .mergeIn(body);
    }
}