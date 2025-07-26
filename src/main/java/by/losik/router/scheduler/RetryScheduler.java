package by.losik.router.scheduler;

import by.losik.entity.Message;
import by.losik.entity.Status;
import by.losik.router.producer.DynamicEmitterFactory;
import by.losik.service.MessageService;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Dependent
@Slf4j
@Retry(maxRetries = 3, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.5,
        delay = 10
)
public class RetryScheduler extends AbstractVerticle {

    @Inject
    MessageService messageService;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @Inject
    DynamicEmitterFactory emitterFactory;

    private final Map<String, String> channelMappings = new ConcurrentHashMap<>();

    @Override
    public void start() throws Exception {
        super.start();
        log.info("RetryScheduler started");
    }

    @Scheduled(every = "5m")
    public Uni<Void> retryFailedMessages() {
        Timer.Sample batchTimer = Timer.start(meterRegistry);

        return messageService.getMessagesByStatus(Status.FAILED)
                .onItem().transformToUni(messages -> {
                    meterRegistry.gauge("messages.failed.available", messages.size());

                    if (messages.isEmpty()) {
                        meterRegistry.counter("retry.operations.empty").increment();
                        batchTimer.stop(meterRegistry.timer("retry.operation.time", "result", "empty"));
                        return Uni.createFrom().voidItem();
                    }

                    meterRegistry.counter("retry.operations.started", "messageCount", String.valueOf(messages.size())).increment();

                    return Uni.combine().all().unis(
                                    messages.stream()
                                            .map(message -> {
                                                Timer.Sample statusUpdateTimer = Timer.start(meterRegistry);
                                                return messageService.updateMessageStatus(message.getId(), Status.RETRY)
                                                        .onItem().invoke(() -> {
                                                            statusUpdateTimer.stop(meterRegistry.timer("retry.status.update.time"));
                                                            meterRegistry.counter("messages.marked.for.retry").increment();
                                                        })
                                                        .onItem().transformToUni(updatedMessage -> {
                                                            JsonObject json = new JsonObject(message.getPayload());
                                                            String channelName = json.getString("channel", "default-retry-channel");

                                                            if (!channelMappings.containsKey(channelName)) {
                                                                channelMappings.put(channelName, channelName);
                                                                emitterFactory.getEmitter(channelName);
                                                                log.info("Registered retry channel: {}", channelName);
                                                            }

                                                            return processRetryMessage(json, channelName, message);
                                                        });
                                            })
                                            .toList()
                            ).discardItems()
                            .onItem().invoke(() -> {
                                batchTimer.stop(meterRegistry.timer("retry.operation.time", "result", "success"));
                                meterRegistry.counter("retry.operations.success").increment();
                                log.info("Successfully processed retry for {} messages", messages.size());
                            });
                })
                .onFailure().invoke(e -> {
                    batchTimer.stop(meterRegistry.timer("retry.operation.time", "result", "failure"));
                    meterRegistry.counter("retry.operations.failed").increment();
                    log.error("Failed to retry messages", e);
                });
    }

    private Uni<Message> processRetryMessage(JsonObject message, String channelName, Message dbMessage) {
        Timer.Sample messageRetryTimer = Timer.start(meterRegistry);

        return Uni.createFrom().completionStage(emitterFactory.getEmitter(channelName).send(message))
                .onItem().transformToUni(i -> {
                    messageRetryTimer.stop(meterRegistry.timer("retry.message.processing.time", "result", "success"));
                    meterRegistry.counter("messages.retry.success").increment();
                    return messageService.updateMessageStatus(dbMessage.getId(), Status.PROCESSED);
                })
                .onFailure().recoverWithUni(e -> {
                    messageRetryTimer.stop(meterRegistry.timer("retry.message.processing.time", "result", "failure"));
                    meterRegistry.counter("messages.retry.failed").increment();
                    log.error("Retry failed for message {} on channel {}", dbMessage.getId(), channelName, e);
                    return messageService.updateMessageStatus(dbMessage.getId(), Status.FAILED);
                });
    }

    @Override
    public void stop() {
        channelMappings.clear();
        log.info("RetryScheduler stopped");
    }
}