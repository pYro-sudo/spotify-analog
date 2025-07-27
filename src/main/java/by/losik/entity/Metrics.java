package by.losik.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import io.smallrye.mutiny.Uni;
import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Entity
@Data
@Table(name = "metrics", schema = "metrics")
public class Metrics extends PanacheEntityBase {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;

    @Column(name = "timestamp", nullable = false)
    public LocalDateTime timestamp;

    @Column(name = "metrics_data", columnDefinition = "TEXT", nullable = false)
    public String metricsData;

    @Column(name = "source", length = 50)
    public String source;

    public static Uni<Metrics> create(String metricsData, String source) {
        Metrics metric = new Metrics();
        metric.timestamp = LocalDateTime.now();
        metric.metricsData = metricsData;
        metric.source = source;
        return metric.persistAndFlush();
    }
}