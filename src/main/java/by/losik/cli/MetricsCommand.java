package by.losik.cli;

import by.losik.entity.Metrics;
import by.losik.resources.interfaces.*;
import by.losik.service.MetricsService;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@TopCommand
@Command(name = "metrics", mixinStandardHelpOptions = true, version = "Metrics CLI 1.0")
public class MetricsCommand implements Callable<Integer> {

    private final AtomicInteger result = new AtomicInteger(0);

    @Inject
    MetricsService metricsService;

    @Option(names = {"-f", "--front"}, description = "Use frontend metrics source")
    boolean front;
    @Option(names = {"-e", "--elastic"}, description = "Use elastic metrics source")
    boolean elastic;
    @Option(names = {"-d", "--distributor"}, description = "Use distributor metrics source")
    boolean distributor;
    @Option(names = {"-p", "--postgres"}, description = "Use postgres metrics source")
    boolean postgres;

    @Option(names = {"-c", "--cleanup"}, description = "Cleanup old metrics")
    boolean cleanup;
    @Option(names = {"-l", "--list"}, description = "List stored metrics")
    boolean list;
    @Option(names = {"-s", "--search"}, description = "Search metrics by content")
    String searchText;
    @Option(names = {"-g", "--get"}, description = "Get metric by ID")
    Long metricId;
    @Option(names = {"-u", "--update"}, description = "Update metric data")
    String updateData;
    @Option(names = {"-r", "--remove"}, description = "Delete metric by ID")
    Long deleteId;

    @Option(names = {"-o", "--output"}, description = "Output directory for metrics files")
    String outputDir;

    @Inject @RestClient FrontMetricsResource frontMetrics;
    @Inject @RestClient ElasticMetricsResource elasticMetrics;
    @Inject @RestClient DistributorMetricsResource distributorMetrics;
    @Inject @RestClient PostgresMetricsResource postgresMetrics;

    public Integer call() {
        if (cleanup) {
            handleCleanup();
        } else if (list) {
            handleListMetrics();
        } else if (searchText != null) {
            handleSearch();
        } else if (metricId != null && updateData != null) {
            handleUpdateMetric();
        } else if (deleteId != null) {
            handleDeleteMetric();
        } else if (metricId != null) {
            handleGetMetric();
        } else {
            handleCollection();
        }
        return result.get();
    }

    private void handleCleanup() {
        metricsService.cleanupOldMetrics(LocalDateTime.now().minusDays(30))
                .subscribe().with(
                        count -> log.info("Cleaned up {} metrics", count),
                        failure -> {
                            log.error("Cleanup failed", failure);
                            result.set(1);
                        }
                );
    }

    private void handleListMetrics() {
        if (front) {
            metricsService.getMetricsBySource("front")
                    .subscribe().with(
                            metrics -> metrics.forEach(this::logMetric),
                            failure -> handleError("Failed to list front metrics", failure)
                    );
        } else if (elastic) {
            metricsService.getMetricsBySource("elastic")
                    .subscribe().with(
                            metrics -> metrics.forEach(this::logMetric),
                            failure -> handleError("Failed to list elastic metrics", failure)
                    );
        } else if (distributor) {
            metricsService.getMetricsBySource("distributor")
                    .subscribe().with(
                            metrics -> metrics.forEach(this::logMetric),
                            failure -> handleError("Failed to list distributor metrics", failure)
                    );
        } else if (postgres) {
            metricsService.getMetricsBySource("postgres")
                    .subscribe().with(
                            metrics -> metrics.forEach(this::logMetric),
                            failure -> handleError("Failed to list postgres metrics", failure)
                    );
        } else {
            metricsService.getAllMetrics()
                    .subscribe().with(
                            metrics -> metrics.forEach(this::logMetric),
                            failure -> handleError("Failed to list all metrics", failure)
                    );
        }
    }

    private void handleSearch() {
        metricsService.searchMetricsByContent(searchText)
                .subscribe().with(
                        results -> results.forEach(this::logMetric),
                        failure -> handleError("Search failed", failure)
                );
    }

    private void handleUpdateMetric() {
        metricsService.updateMetricData(metricId, updateData)
                .subscribe().with(
                        updated -> log.info("Updated metric {}", metricId),
                        failure -> handleError("Update failed", failure)
                );
    }

    private void handleDeleteMetric() {
        metricsService.deleteMetric(deleteId)
                .subscribe().with(
                        deleted -> {
                            if (deleted) log.info("Deleted metric {}", deleteId);
                            else log.warn("Metric not found");
                        },
                        failure -> handleError("Deletion failed", failure)
                );
    }

    private void handleGetMetric() {
        metricsService.getMetricById(metricId)
                .subscribe().with(
                        metric -> {
                            if (metric != null) log.info("Metric details:\n{}", formatMetric(metric));
                            else log.warn("Metric not found");
                        },
                        failure -> handleError("Fetch failed", failure)
                );
    }

    private void handleCollection() {
        if (front) {
            collectFromSource("front", frontMetrics.getMetrics());
        } else if (elastic) {
            collectFromSource("elastic", elasticMetrics.getMetrics());
        } else if (distributor) {
            collectFromSource("distributor", distributorMetrics.getMetrics());
        } else if (postgres) {
            collectFromSource("postgres", postgresMetrics.getMetrics());
        } else {
            log.error("No metrics source specified");
            result.set(1);
        }
    }

    private void collectFromSource(String source, Uni<String> metricsUni) {
        metricsUni
                .onItem().transformToUni(data ->
                        metricsService.createMetric(data, source)
                                .onItem().invoke(metric -> {
                                    log.info("Stored {} metrics (ID: {})", source, metric.id);
                                    if (outputDir != null) writeToFile(source, data);
                                })
                )
                .onFailure().invoke(e ->
                        log.error("Failed to collect {} metrics: {}", source, e.getMessage())
                )
                .replaceWithVoid()
                .subscribe().with(
                        success -> {
                        },
                        failure -> result.set(1)
                );
    }

    private void writeToFile(String source, String data) {
        try {
            Path path = Path.of(outputDir, String.format("%s-%s.txt", source, LocalDateTime.now()));
            Files.writeString(path, data);
            log.debug("Saved metrics to: {}", path);
        } catch (Exception e) {
            log.error("Failed to write metrics file: {}", e.getMessage());
        }
    }

    private void logMetric(Metrics metric) {
        log.info("Metric [ID: {}, Source: {}, Timestamp: {}]",
                metric.id, metric.source, metric.timestamp);
    }

    private String formatMetric(Metrics metric) {
        return String.format(
                "ID: %d\nSource: %s\nTimestamp: %s\nData Length: %d chars",
                metric.id, metric.source, metric.timestamp, metric.metricsData.length()
        );
    }

    private void handleError(String message, Throwable failure) {
        log.error(message, failure);
        result.set(1);
    }
}