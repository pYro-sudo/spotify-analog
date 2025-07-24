package by.losik.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.sql.Timestamp;

@Data
@EqualsAndHashCode(callSuper = false)
@Entity
@Table(name = "outbox", schema = "message_table")
public class Message extends PanacheEntityBase {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", nullable = false)
    protected Long id;

    @Column(name = "aggregate_id", columnDefinition = "varchar", nullable = false)
    protected String aggregateId;

    @Column(name = "event_type", columnDefinition = "varchar", nullable = false, length = 50)
    protected String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", columnDefinition = "jsonb", nullable = false)
    protected String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", columnDefinition = "varchar", nullable = false, length = 20)
    protected Status status;

    @Column(name = "created_at", columnDefinition = "timestamp", nullable = false)
    protected Timestamp createdAt;
}