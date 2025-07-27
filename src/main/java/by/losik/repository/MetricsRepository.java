package by.losik.repository;

import by.losik.entity.Metrics;
import io.quarkus.hibernate.reactive.panache.PanacheRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import java.time.LocalDateTime;
import java.util.List;

@ApplicationScoped
public class MetricsRepository implements PanacheRepository<Metrics> {

    public Uni<List<Metrics>> findBySource(String source) {
        return list("source", source);
    }

    public Uni<List<Metrics>> findCreatedAfter(LocalDateTime date) {
        return list("timestamp > ?1", date);
    }

    public Uni<List<Metrics>> findLatest(int limit) {
        return find("order by timestamp desc").page(0, limit).list();
    }

    public Uni<Metrics> updateMetricsData(Long id, String newMetricsData) {
        return findById(id)
                .onItem().transformToUni(metrics -> {
                    if (metrics != null) {
                        metrics.metricsData = newMetricsData;
                        return persist(metrics);
                    }
                    return Uni.createFrom().nullItem();
                });
    }

    public Uni<List<Metrics>> findByMetricsDataContaining(String text) {
        return list("metricsData like ?1", "%" + text + "%");
    }

    public Uni<Long> deleteMetricsBefore(LocalDateTime cutoffDate) {
        return delete("timestamp < ?1", cutoffDate);
    }

    public Uni<Long> deleteBySource(String source) {
        return delete("source", source);
    }
}