package by.losik.service;

import by.losik.entity.Metrics;
import by.losik.repository.MetricsRepository;
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

import java.time.LocalDateTime;
import java.util.List;

@ApplicationScoped
@Slf4j
@Retry(maxRetries = 4, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.4,
        delay = 10000
)
public class MetricsService {

    @Inject
    MetricsRepository metricsRepository;

    @Inject
    PrometheusMeterRegistry meterRegistry;

    @Transactional
    @CacheResult(cacheName = "metrics")
    public Uni<Metrics> createMetric(String metricsData, String source) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return Metrics.create(metricsData, source)
                .onItem().invoke(metric -> {
                    timer.stop(meterRegistry.timer("metrics.service.create.time", "source", source));
                    meterRegistry.counter("metrics.service.operations", "operation", "create", "status", "success").increment();
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.create.time", "source", source));
                    meterRegistry.counter("metrics.service.operations", "operation", "create", "status", "failed").increment();
                    log.error("Failed to create metric for source {}", source, failure);
                })
                .onFailure().transform(failure ->
                        new ServiceException("Failed to create metric", failure));
    }

    @Transactional
    @CacheResult(cacheName = "metrics")
    public Uni<Metrics> getMetricById(Long id) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return metricsRepository.findById(id)
                .onItem().invoke(metric -> {
                    timer.stop(meterRegistry.timer("metrics.service.get.by.id.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "getById", "status", "success").increment();
                    if (metric != null) {
                        meterRegistry.counter("metrics.service.cache", "cache", "metrics", "result", "hit").increment();
                    }
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.get.by.id.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "getById", "status", "failed").increment();
                    meterRegistry.counter("metrics.service.cache", "cache", "metrics", "result", "miss").increment();
                    log.error("Failed to fetch metric {}", id, failure);
                });
    }

    @Transactional
    @CacheResult(cacheName = "metrics-list")
    public Uni<List<Metrics>> getAllMetrics() {
        Timer.Sample timer = Timer.start(meterRegistry);
        return metricsRepository.listAll()
                .onItem().invoke(metrics -> {
                    timer.stop(meterRegistry.timer("metrics.service.get.all.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "getAll", "status", "success").increment();
                    meterRegistry.gauge("metrics.service.count.all", metrics.size());
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.get.all.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "getAll", "status", "failed").increment();
                    log.error("Failed to fetch all metrics", failure);
                });
    }

    @Transactional
    @CacheResult(cacheName = "metrics-by-source")
    public Uni<List<Metrics>> getMetricsBySource(String source) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return metricsRepository.findBySource(source)
                .onItem().invoke(metrics -> {
                    timer.stop(meterRegistry.timer("metrics.service.get.by.source.time", "source", source));
                    meterRegistry.counter("metrics.service.operations", "operation", "getBySource", "status", "success").increment();
                    meterRegistry.gauge("metrics.service.count.by.source", metrics.size());
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.get.by.source.time", "source", source));
                    meterRegistry.counter("metrics.service.operations", "operation", "getBySource", "status", "failed").increment();
                    log.error("Failed to fetch metrics by source {}", source, failure);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "metrics")
    @CacheInvalidate(cacheName = "metrics-by-source")
    public Uni<Metrics> updateMetricData(Long id, String newMetricsData) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return metricsRepository.updateMetricsData(id, newMetricsData)
                .onItem().invoke(metric -> {
                    timer.stop(meterRegistry.timer("metrics.service.update.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "update", "status", "success").increment();
                    meterRegistry.counter("metrics.service.cache.invalidations").increment();
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.update.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "update", "status", "failed").increment();
                    log.error("Failed to update metric {}", id, failure);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "metrics")
    @CacheInvalidate(cacheName = "metrics-list")
    @CacheInvalidate(cacheName = "metrics-by-source")
    public Uni<Boolean> deleteMetric(Long id) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return metricsRepository.deleteById(id)
                .onItem().invoke(deleted -> {
                    timer.stop(meterRegistry.timer("metrics.service.delete.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "delete", "status", "success").increment();
                    meterRegistry.counter("metrics.service.cache.invalidations").increment();
                    if (deleted) {
                        meterRegistry.counter("metrics.service.deleted").increment();
                    }
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.delete.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "delete", "status", "failed").increment();
                    log.error("Failed to delete metric {}", id, failure);
                });
    }

    @Transactional
    @CacheResult(cacheName = "metrics-by-content")
    public Uni<List<Metrics>> searchMetricsByContent(String text) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return metricsRepository.findByMetricsDataContaining(text)
                .onItem().invoke(metrics -> {
                    timer.stop(meterRegistry.timer("metrics.service.search.time", "query", text));
                    meterRegistry.counter("metrics.service.operations", "operation", "search", "status", "success").increment();
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.search.time", "query", text));
                    meterRegistry.counter("metrics.service.operations", "operation", "search", "status", "failed").increment();
                    log.error("Failed to search metrics with text {}", text, failure);
                });
    }

    @Transactional
    @CacheInvalidate(cacheName = "metrics")
    @CacheInvalidate(cacheName = "metrics-list")
    @CacheInvalidate(cacheName = "metrics-by-source")
    @CacheInvalidate(cacheName = "metrics-by-content")
    public Uni<Long> cleanupOldMetrics(LocalDateTime cutoffDate) {
        Timer.Sample timer = Timer.start(meterRegistry);
        return metricsRepository.deleteMetricsBefore(cutoffDate)
                .onItem().invoke(count -> {
                    timer.stop(meterRegistry.timer("metrics.service.cleanup.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "cleanup", "status", "success").increment();
                    meterRegistry.counter("metrics.service.cache.invalidations").increment();
                    meterRegistry.counter("metrics.service.cleaned.up", "count", count.toString()).increment(count);
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.cleanup.time"));
                    meterRegistry.counter("metrics.service.operations", "operation", "cleanup", "status", "failed").increment();
                    log.error("Failed to cleanup metrics older than {}", cutoffDate, failure);
                });
    }

    @Transactional
    @Scheduled(every = "10m")
    @CacheInvalidateAll(cacheName = "metrics")
    @CacheInvalidateAll(cacheName = "metrics-list")
    @CacheInvalidateAll(cacheName = "metrics-by-source")
    @CacheInvalidateAll(cacheName = "metrics-by-content")
    public Uni<Void> purgeCaches() {
        Timer.Sample timer = Timer.start(meterRegistry);
        return Uni.createFrom().voidItem()
                .onItem().invoke(() -> {
                    timer.stop(meterRegistry.timer("metrics.service.purge.time"));
                    meterRegistry.counter("metrics.service.cache.invalidations.all").increment();
                    log.info("Metrics caches purged successfully");
                })
                .onFailure().invoke(failure -> {
                    timer.stop(meterRegistry.timer("metrics.service.purge.time"));
                    log.error("Failed to purge metrics caches", failure);
                });
    }

    @Transactional
    @Scheduled(cron = "0 0 3 * * ?")
    public Uni<Void> performDailyMaintenance() {
        return cleanupOldMetrics(LocalDateTime.now().minusDays(30))
                .onItem().invoke(count ->
                        log.info("Daily maintenance removed {} old metrics", count))
                .replaceWithVoid();
    }
}