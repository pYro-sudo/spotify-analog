package by.losik.router.scheduler;

import by.losik.entity.Status;
import by.losik.service.MessageService;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

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

    @Channel("rabbitmq-out")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    Emitter<JsonObject> rabbitMqEmitter;

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
                                                        });
                                            })
                                            .toList()
                            ).discardItems()
                            .onItem().transformToUni(ignored -> Uni.combine().all().unis(
                                            messages.stream()
                                                    .map(message -> {
                                                        Timer.Sample messageRetryTimer = Timer.start(meterRegistry);
                                                        JsonObject json = new JsonObject(message.getPayload());
                                                        return Uni.createFrom().completionStage(rabbitMqEmitter.send(json))
                                                                .onItem().transformToUni(i -> {
                                                                    messageRetryTimer.stop(meterRegistry.timer("retry.message.processing.time", "result", "success"));
                                                                    meterRegistry.counter("messages.retry.success").increment();
                                                                    return messageService.updateMessageStatus(message.getId(), Status.PROCESSED);
                                                                })
                                                                .onFailure().recoverWithUni(e -> {
                                                                    messageRetryTimer.stop(meterRegistry.timer("retry.message.processing.time", "result", "failure"));
                                                                    meterRegistry.counter("messages.retry.failed").increment();
                                                                    log.error("Retry failed for message {}", message.getId(), e);
                                                                    return messageService.updateMessageStatus(message.getId(), Status.FAILED);
                                                                });
                                                    })
                                                    .toList()
                                    ).discardItems()
                                    .onItem().invoke(() -> {
                                        batchTimer.stop(meterRegistry.timer("retry.operation.time", "result", "success"));
                                        meterRegistry.counter("retry.operations.success").increment();
                                        log.info("Successfully retried {} messages", messages.size());
                                    }));
                })
                .onFailure().invoke(e -> {
                    batchTimer.stop(meterRegistry.timer("retry.operation.time", "result", "failure"));
                    meterRegistry.counter("retry.operations.failed").increment();
                    log.error("Failed to retry messages", e);
                });
    }
}