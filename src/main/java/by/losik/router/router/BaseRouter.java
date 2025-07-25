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
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Dependent
@Slf4j
@Retry(maxRetries = 4, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.4,
        delay = 10
)
public class BaseRouter extends AbstractVerticle {
    @ConfigProperty(name = "auth.token.field")
    String AUTH_TOKEN_FIELD;

    @Inject
    MessageService messageService;

    @Inject
    AuthService authService;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    private final Map<String, Emitter<JsonObject>> emitters = new ConcurrentHashMap<>();
    private final Map<String, String> channelMappings = new ConcurrentHashMap<>();

    protected void registerEmitter(String channelName, Emitter<JsonObject> emitter) {
        emitters.put(channelName, emitter);
    }

    protected void addChannelMapping(String inputChannel, String outputChannel) {
        channelMappings.put(inputChannel, outputChannel);
    }

    protected Uni<Void> processMessage(org.eclipse.microprofile.reactive.messaging.Message<ConsumerRecords<String, JsonObject>> batch,
                                       String inputChannel) {
        Timer.Sample batchTimer = Timer.start(meterRegistry);
        long messageCount = batch.getPayload().count();

        String outputChannel = channelMappings.get(inputChannel);
        if (outputChannel == null) {
            log.error("No output channel mapping found for input channel: {}", inputChannel);
            return Uni.createFrom().failure(new RuntimeException("No output channel mapping for: " + inputChannel));
        }

        if (!emitters.containsKey(outputChannel)) {
            log.error("No emitter registered for channel: {}", outputChannel);
            return Uni.createFrom().failure(new RuntimeException("No emitter for channel: " + outputChannel));
        }

        return Multi.createFrom().iterable(batch.getPayload())
                .onItem().transformToUniAndConcatenate(record -> {
                    JsonObject message = record.value();
                    String eventType = message.getString("eventType", "unknown");

                    meterRegistry.counter("messages.received", "eventType", eventType, "channel", inputChannel).increment();

                    String token = message.getString(AUTH_TOKEN_FIELD);
                    if (token == null) {
                        log.warn("Message missing authentication token");
                        meterRegistry.counter("messages.invalid.missing_token", "channel", inputChannel).increment();
                        return Uni.createFrom().voidItem();
                    }

                    Timer.Sample tokenValidationTimer = Timer.start(meterRegistry);
                    return authService.validateToken(token)
                            .onItem().transformToUni(valid -> {
                                tokenValidationTimer.stop(meterRegistry.timer("token.validation.time", "eventType", eventType, "channel", inputChannel));

                                if (!valid) {
                                    log.warn("Invalid authentication token in message");
                                    meterRegistry.counter("messages.invalid.token", "channel", inputChannel).increment();
                                    return Uni.createFrom().voidItem();
                                }

                                String aggregateId = message.getString("aggregateId");
                                String payload = message.toString();

                                Timer.Sample messageProcessingTimer = Timer.start(meterRegistry);
                                return messageService.createMessage(aggregateId, eventType, payload)
                                        .onItem().transformToUni(savedMessage -> {
                                            messageProcessingTimer.stop(meterRegistry.timer("message.processing.time", "eventType", eventType, "channel", inputChannel));

                                            log.debug("Forwarding JSON message from {} to {}: {}", inputChannel, outputChannel, message);
                                            Timer.Sample outputTimer = Timer.start(meterRegistry);
                                            return Uni.createFrom().completionStage(emitters.get(outputChannel).send(message))
                                                    .onItem().transformToUni(ignored -> {
                                                        outputTimer.stop(meterRegistry.timer(outputChannel + ".send.time", "eventType", eventType));
                                                        meterRegistry.counter("messages.processed.success", "eventType", eventType, "channel", inputChannel).increment();
                                                        return messageService.updateMessageStatus(savedMessage.getId(), Status.PROCESSED);
                                                    })
                                                    .onFailure().recoverWithUni(e -> {
                                                        outputTimer.stop(meterRegistry.timer(outputChannel + ".send.time", "eventType", eventType));
                                                        meterRegistry.counter("messages.processed.failed", "eventType", eventType, "channel", inputChannel).increment();
                                                        log.error("Failed to send message from {} to {}", inputChannel, outputChannel, e);
                                                        return messageService.updateMessageStatus(savedMessage.getId(), Status.FAILED);
                                                    });
                                        });
                            });
                })
                .collect().asList()
                .onItem().transformToUni(ignored -> {
                    batchTimer.stop(meterRegistry.timer("batch.processing.time", "batchSize", String.valueOf(messageCount), "channel", inputChannel));
                    meterRegistry.counter("batches.processed.success", "channel", inputChannel).increment();
                    log.debug("All messages processed successfully from channel {}", inputChannel);
                    return Uni.createFrom().completionStage(batch.ack());
                })
                .onFailure().recoverWithUni(e -> {
                    batchTimer.stop(meterRegistry.timer("batch.processing.time", "batchSize", String.valueOf(messageCount), "channel", inputChannel));
                    meterRegistry.counter("batches.processed.failed", "channel", inputChannel).increment();
                    log.error("Batch processing failed for channel {}", inputChannel, e);
                    return Uni.createFrom().completionStage(batch.nack(e));
                });
    }

    @Override
    public void stop() {
        emitters.clear();
        channelMappings.clear();
    }
}