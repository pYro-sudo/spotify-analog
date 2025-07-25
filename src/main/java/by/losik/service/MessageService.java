package by.losik.service;

import by.losik.entity.Message;
import by.losik.entity.Status;
import by.losik.repository.MessageRepository;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;
import io.quarkus.cache.CacheResult;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.hibernate.service.spi.ServiceException;

import java.sql.Timestamp;
import java.util.List;

@ApplicationScoped
@Slf4j
@Retry(maxRetries = 4, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.4,
        delay = 10
)
public class MessageService {
    @Inject
    MessageRepository messageRepository;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @Transactional
    @CacheResult(cacheName = "messages")
    public Uni<Message> createMessage(String aggregateId, String eventType, String payload) {
        Timer.Sample timer = Timer.start(meterRegistry);
        Message message = new Message();
        message.setAggregateId(aggregateId);
        message.setEventType(eventType);
        message.setPayload(payload);
        message.setStatus(Status.PENDING);
        message.setCreatedAt(new Timestamp(System.currentTimeMillis()));

        return messageRepository.persist(message)
                .onItem().invoke(msg -> {
                    timer.stop(meterRegistry.timer("message.service.create.time", "eventType", eventType));
                    meterRegistry.counter("message.service.operations", "operation", "create", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.create.time", "eventType", eventType));
                    meterRegistry.counter("message.service.operations", "operation", "create", "status", "failed").increment();
                    log.error("Failed to create message for aggregate {}", aggregateId, throwable);
                })
                .onFailure().transform(throwable ->
                        new ServiceException("Failed to create message", throwable));
    }
    @Transactional
    @CacheResult(cacheName = "messages")
    public Uni<Message> getMessageById(Long id) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.findById(id)
                .onItem().invoke(msg -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.id.time"));
                    meterRegistry.counter("message.service.operations", "operation", "getById", "status", "success").increment();
                    if (msg != null) {
                        meterRegistry.counter("message.service.cache", "cache", "messages", "result", "hit").increment();
                    }
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.id.time"));
                    meterRegistry.counter("message.service.operations", "operation", "getById", "status", "failed").increment();
                    meterRegistry.counter("message.service.cache", "cache", "messages", "result", "miss").increment();
                    log.error("Failed to fetch message {}", id, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "messages-list")
    public Uni<List<Message>> getAllMessages() {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.listAll()
                .onItem().invoke(messages -> {
                    timer.stop(meterRegistry.timer("message.service.get.all.time"));
                    meterRegistry.counter("message.service.operations", "operation", "getAll", "status", "success").increment();
                    meterRegistry.gauge("message.service.count.all", messages.size());
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.get.all.time"));
                    meterRegistry.counter("message.service.operations", "operation", "getAll", "status", "failed").increment();
                    log.error("Failed to fetch all messages", throwable);
                });
    }
    @Transactional
    @CacheResult(cacheName = "messages-by-status")
    public Uni<List<Message>> getMessagesByStatus(Status status) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.findByStatus(status.name())
                .onItem().invoke(messages -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.status.time", "status", status.name()));
                    meterRegistry.counter("message.service.operations", "operation", "getByStatus", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.status.time", "status", status.name()));
                    meterRegistry.counter("message.service.operations", "operation", "getByStatus", "status", "failed").increment();
                    log.error("Failed to fetch messages by status {}", status, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "messages-by-event")
    public Uni<List<Message>> getMessagesByEventType(String eventType) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.findByEventType(eventType)
                .onItem().invoke(messages -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.event.time", "eventType", eventType));
                    meterRegistry.counter("message.service.operations", "operation", "getByEvent", "status", "success").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.event.time", "eventType", eventType));
                    meterRegistry.counter("message.service.operations", "operation", "getByEvent", "status", "failed").increment();
                    log.error("Failed to fetch messages by event type {}", eventType, throwable);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "messages")
    @CacheInvalidate(cacheName = "messages-by-status")
    @CacheInvalidate(cacheName = "messages-by-event")
    public Uni<Message> updateMessageStatus(Long id, Status newStatus) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.updateStatus(id, newStatus.name())
                .onItem().invoke(msg -> {
                    timer.stop(meterRegistry.timer("message.service.update.status.time", "newStatus", newStatus.name()));
                    meterRegistry.counter("message.service.operations", "operation", "updateStatus", "status", "success").increment();
                    meterRegistry.counter("message.service.cache.invalidations").increment();
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.update.status.time", "newStatus", newStatus.name()));
                    meterRegistry.counter("message.service.operations", "operation", "updateStatus", "status", "failed").increment();
                    log.error("Failed to update status for message {}", id, throwable);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "messages")
    @CacheInvalidate(cacheName = "messages-list")
    @CacheInvalidate(cacheName = "messages-by-status")
    @CacheInvalidate(cacheName = "messages-by-event")
    public Uni<Boolean> deleteMessage(Long id) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.deleteById(id)
                .onItem().invoke(deleted -> {
                    timer.stop(meterRegistry.timer("message.service.delete.time"));
                    meterRegistry.counter("message.service.operations", "operation", "delete", "status", "success").increment();
                    meterRegistry.counter("message.service.cache.invalidations").increment();
                    if (deleted) {
                        meterRegistry.counter("message.service.deleted").increment();
                    }
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.delete.time"));
                    meterRegistry.counter("message.service.operations", "operation", "delete", "status", "failed").increment();
                    log.error("Failed to delete message {}", id, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "messages-by-aggregate")
    public Uni<List<Message>> getMessagesByAggregateId(String aggregateId) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.findByAggregateId(aggregateId)
                .onItem().invoke(messages -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.aggregate.time", "aggregateId", aggregateId));
                    meterRegistry.counter("message.service.operations", "operation", "getByAggregate", "status", "success").increment();
                    meterRegistry.gauge("message.service.count.by.aggregate", messages.size());
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.aggregate.time", "aggregateId", aggregateId));
                    meterRegistry.counter("message.service.operations", "operation", "getByAggregate", "status", "failed").increment();
                    log.error("Failed to fetch messages by aggregate ID {}", aggregateId, throwable);
                });
    }

    @Transactional
    @CacheResult(cacheName = "messages-by-payload")
    public Uni<List<Message>> getMessagesByPayloadField(String jsonPath, String value) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.findByPayloadField(jsonPath, value)
                .onItem().invoke(messages -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.payload.time",
                            "jsonPath", jsonPath, "value", value));
                    meterRegistry.counter("message.service.operations", "operation", "getByPayload", "status", "success").increment();
                    meterRegistry.gauge("message.service.count.by.payload", messages.size());
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.get.by.payload.time",
                            "jsonPath", jsonPath, "value", value));
                    meterRegistry.counter("message.service.operations", "operation", "getByPayload", "status", "failed").increment();
                    log.error("Failed to fetch messages by payload field {}: {}", jsonPath, value, throwable);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "messages")
    @CacheInvalidate(cacheName = "messages-list")
    @CacheInvalidate(cacheName = "messages-by-status")
    @CacheInvalidate(cacheName = "messages-by-event")
    @CacheInvalidate(cacheName = "messages-by-aggregate")
    @CacheInvalidate(cacheName = "messages-by-payload")
    public Uni<Long> deleteMessagesOlderThan(Timestamp cutoffDate) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return messageRepository.deleteOldMessages(cutoffDate)
                .onItem().invoke(count -> {
                    timer.stop(meterRegistry.timer("message.service.delete.old.time",
                            "cutoffDate", cutoffDate.toString()));
                    meterRegistry.counter("message.service.operations", "operation", "deleteOld", "status", "success").increment();
                    meterRegistry.counter("message.service.cache.invalidations").increment();
                    meterRegistry.counter("message.service.deleted.old", "count", count.toString()).increment(count);
                })
                .onFailure().invoke(throwable -> {
                    timer.stop(meterRegistry.timer("message.service.delete.old.time",
                            "cutoffDate", cutoffDate.toString()));
                    meterRegistry.counter("message.service.operations", "operation", "deleteOld", "status", "failed").increment();
                    log.error("Failed to delete messages older than {}", cutoffDate, throwable);
                });
    }

    @Transactional
    @Scheduled(every = "10m")
    @CacheInvalidateAll(cacheName = "messages")
    @CacheInvalidateAll(cacheName = "messages-list")
    @CacheInvalidateAll(cacheName = "messages-by-status")
    @CacheInvalidateAll(cacheName = "messages-by-event")
    @CacheInvalidate(cacheName = "messages-by-aggregate")
    @CacheInvalidate(cacheName = "messages-by-payload")
    public Uni<Void> purge() {
        Timer.Sample timer = Timer.start(meterRegistry);
        return Uni.createFrom().voidItem()
                .onItem().invoke(() -> {
                    timer.stop(meterRegistry.timer("message.service.purge.time"));
                    meterRegistry.counter("message.service.cache.invalidations.all").increment();
                    log.info("Purge completed!");
                })
                .onFailure().invoke(item -> {
                    timer.stop(meterRegistry.timer("message.service.purge.time"));
                    log.error("Exception occurred", item);
                });
    }
}