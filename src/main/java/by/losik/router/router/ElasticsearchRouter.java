package by.losik.router.router;

import by.losik.entity.Status;
import by.losik.service.AuthService;
import by.losik.service.MessageService;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

@Dependent
@Slf4j
@Retry(maxRetries = 4, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.4,
        delay = 10
)
public class ElasticsearchRouter extends AbstractVerticle {
    @ConfigProperty(name = "auth.token.field")
    String AUTH_TOKEN_FIELD;

    @Inject
    MessageService messageService;

    @Inject
    AuthService authService;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @Channel("rabbitmq-out")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    org.eclipse.microprofile.reactive.messaging.Emitter<JsonObject> rabbitMqEmitter;

    @Incoming("kafka-in")
    public Uni<Void> processKafkaMessage(org.eclipse.microprofile.reactive.messaging.Message<ConsumerRecords<String, JsonObject>> batch) {
        Timer.Sample batchTimer = Timer.start(meterRegistry);
        long messageCount = batch.getPayload().count();

        return Multi.createFrom().iterable(batch.getPayload())
                .onItem().transformToUniAndConcatenate(record -> {
                    JsonObject message = record.value();
                    String eventType = message.getString("eventType", "unknown");

                    meterRegistry.counter("messages.received", "eventType", eventType).increment();

                    String token = message.getString(AUTH_TOKEN_FIELD);
                    if (token == null) {
                        log.warn("Message missing authentication token");
                        meterRegistry.counter("messages.invalid.missing_token").increment();
                        return Uni.createFrom().voidItem();
                    }

                    Timer.Sample tokenValidationTimer = Timer.start(meterRegistry);
                    return authService.validateToken(token)
                            .onItem().transformToUni(valid -> {
                                tokenValidationTimer.stop(meterRegistry.timer("token.validation.time", "eventType", eventType));

                                if (!valid) {
                                    log.warn("Invalid authentication token in message");
                                    meterRegistry.counter("messages.invalid.token").increment();
                                    return Uni.createFrom().voidItem();
                                }

                                String aggregateId = message.getString("aggregateId");
                                String payload = message.toString();

                                Timer.Sample messageProcessingTimer = Timer.start(meterRegistry);
                                return messageService.createMessage(aggregateId, eventType, payload)
                                        .onItem().transformToUni(savedMessage -> {
                                            messageProcessingTimer.stop(meterRegistry.timer("message.processing.time", "eventType", eventType));

                                            log.debug("Forwarding JSON message: {}", message);
                                            Timer.Sample rabbitMqTimer = Timer.start(meterRegistry);
                                            return Uni.createFrom().completionStage(rabbitMqEmitter.send(message))
                                                    .onItem().transformToUni(ignored -> {
                                                        rabbitMqTimer.stop(meterRegistry.timer("rabbitmq.send.time", "eventType", eventType));
                                                        meterRegistry.counter("messages.processed.success", "eventType", eventType).increment();
                                                        return messageService.updateMessageStatus(savedMessage.getId(), Status.PROCESSED);
                                                    })
                                                    .onFailure().recoverWithUni(e -> {
                                                        rabbitMqTimer.stop(meterRegistry.timer("rabbitmq.send.time", "eventType", eventType));
                                                        meterRegistry.counter("messages.processed.failed", "eventType", eventType).increment();
                                                        log.error("Failed to send message", e);
                                                        return messageService.updateMessageStatus(savedMessage.getId(), Status.FAILED);
                                                    });
                                        });
                            });
                })
                .collect().asList()
                .onItem().transformToUni(ignored -> {
                    batchTimer.stop(meterRegistry.timer("batch.processing.time", "batchSize", String.valueOf(messageCount)));
                    meterRegistry.counter("batches.processed.success").increment();
                    log.debug("All messages processed successfully");
                    return Uni.createFrom().completionStage(batch.ack());
                })
                .onFailure().recoverWithUni(e -> {
                    batchTimer.stop(meterRegistry.timer("batch.processing.time", "batchSize", String.valueOf(messageCount)));
                    meterRegistry.counter("batches.processed.failed").increment();
                    log.error("Batch processing failed", e);
                    return Uni.createFrom().completionStage(batch.nack(e));
                });
    }
}