package by.losik.router.scheduler;

import by.losik.configuration.Status;
import by.losik.service.MessageService;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.core.AbstractVerticle;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;

import java.sql.Timestamp;

@Dependent
@Slf4j
@Retry(maxRetries = 4, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.4,
        delay = 10
)
public class ArchiveScheduler extends AbstractVerticle {

    @Inject
    MessageService messageService;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @ConfigProperty(name = "archive.delay", defaultValue = "1800000")
    long ARCHIVE_DELAY_MS;

    @Scheduled(every = "10m")
    public Uni<Void> archiveOldProcessedMessages() {
        Timer.Sample timer = Timer.start(meterRegistry);
        Timestamp thirtyMinutesAgo = new Timestamp(System.currentTimeMillis() - ARCHIVE_DELAY_MS);

        return messageService.getMessagesByStatus(Status.PROCESSED)
                .onItem().transformToUni(messages -> {
                    meterRegistry.gauge("messages.considered.for.archive", messages.size());

                    if (messages.isEmpty()) {
                        meterRegistry.counter("archive.operations.empty").increment();
                        timer.stop(meterRegistry.timer("archive.operation.time", "result", "empty"));
                        return Uni.createFrom().voidItem();
                    }

                    long toArchiveCount = messages.stream()
                            .filter(message -> message.getCreatedAt().before(thirtyMinutesAgo))
                            .count();

                    if (toArchiveCount == 0) {
                        meterRegistry.counter("archive.operations.none.eligible").increment();
                        timer.stop(meterRegistry.timer("archive.operation.time", "result", "none_eligible"));
                        return Uni.createFrom().voidItem();
                    }

                    return Uni.combine().all().unis(
                                    messages.stream()
                                            .filter(message -> message.getCreatedAt().before(thirtyMinutesAgo))
                                            .map(message -> {
                                                Timer.Sample messageTimer = Timer.start(meterRegistry);
                                                return messageService.updateMessageStatus(message.getId(), Status.ARCHIVED)
                                                        .onItem().invoke(() -> {
                                                            messageTimer.stop(meterRegistry.timer("archive.message.time"));
                                                            meterRegistry.counter("messages.archived").increment();
                                                        });
                                            })
                                            .toList()
                            ).discardItems()
                            .onItem().invoke(() -> {
                                timer.stop(meterRegistry.timer("archive.operation.time", "result", "success"));
                                meterRegistry.counter("archive.operations.success").increment();
                                log.info("Successfully archived {} messages", toArchiveCount);
                            });
                })
                .onFailure().invoke(e -> {
                    timer.stop(meterRegistry.timer("archive.operation.time", "result", "failure"));
                    meterRegistry.counter("archive.operations.failed").increment();
                    log.error("Failed to archive messages", e);
                });
    }
}