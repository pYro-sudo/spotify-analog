package by.losik.repository;

import by.losik.entity.Message;
import by.losik.entity.Status;
import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.sql.Timestamp;
import java.util.List;

@ApplicationScoped
public class MessageRepository implements PanacheRepository<Message> {
    public Uni<List<Message>> findByEventType(String eventType) {
        return list("event_type", eventType);
    }

    public Uni<List<Message>> findByStatus(String status) {
        return list("status", status);
    }

    public Uni<List<Message>> findCreatedAfter(Timestamp date) {
        return list("created_at > ?1", date);
    }

    public Uni<Message> updateStatus(Long id, String newStatus) {
        return findById(id)
                .onItem().transformToUni(message -> {
                    if (message != null) {
                        message.setStatus(Status.valueOf(newStatus));
                        return persist(message);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<List<Message>> findByAggregateId(String aggregateId) {
        return list("aggregate_id", aggregateId);
    }

    public Uni<List<Message>> findByPayloadField(String jsonPath, String value) {
        return list("payload->>?1 = ?2", jsonPath, value);
    }

    public Uni<Long> deleteOldMessages(Timestamp cutoffDate) {
        return delete("created_at < ?1", cutoffDate);
    }
}