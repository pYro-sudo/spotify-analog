package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.LogDTO;
import by.losik.repository.LogRepository;
import io.smallrye.faulttolerance.api.CircuitBreakerName;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.faulttolerance.Bulkhead;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Log Service", description = "Operations for application logging and log management")
public class LogService extends ServiceConfig {

    @Inject
    LogRepository logRepository;

    @Operation(summary = "Log single event",
            description = "Creates and stores a new log entry with automatic ID generation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Event logged successfully"),
            @APIResponse(responseCode = "400", description = "Invalid log message")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("log-event-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<LogDTO> logEvent(
            @Parameter(description = "Log message content", example = "User logged in", required = true)
            String message) {
        LogDTO logEntry = new LogDTO();
        logEntry.setId(generateLogId());
        logEntry.setLog(message);
        logEntry.setDate(LocalDateTime.from(Instant.now()));

        return logRepository.logEvent(logEntry)
                .onItem().invoke(entry -> log.debug("Logged event: {}", entry.getId()))
                .onFailure().transform(e -> new RuntimeException("Failed to log event: " + message, e));
    }

    @Operation(summary = "Bulk log events",
            description = "Efficiently logs multiple messages in one operation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "All events logged successfully"),
            @APIResponse(responseCode = "400", description = "Invalid messages in batch")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-log-events-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    public Uni<List<LogDTO>> bulkLogEvents(
            @Parameter(description = "List of log messages", required = true)
            List<String> messages) {
        List<LogDTO> logEntries = messages.stream()
                .map(message -> {
                    LogDTO entry = new LogDTO();
                    entry.setId(generateLogId());
                    entry.setLog(message);
                    entry.setDate(LocalDateTime.from(Instant.now()));
                    return entry;
                })
                .toList();

        return logRepository.bulkLogEvents(logEntries)
                .onItem().invoke(entries -> log.debug("Bulk logged {} events", entries.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to bulk log events", e));
    }

    @Operation(summary = "Get log by ID",
            description = "Retrieves a specific log entry by its unique identifier")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Log entry found"),
            @APIResponse(responseCode = "404", description = "Log entry not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-log-by-id-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<LogDTO> getLogById(
            @Parameter(description = "Log entry ID", example = "log-123456789", required = true)
            String id) {
        return logRepository.getLogById(id)
                .onItem().ifNotNull().invoke(entry -> log.debug("Retrieved log entry: {}", entry.getId()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("Log entry not found: " + id));
    }

    @Operation(summary = "Search logs by message content",
            description = "Finds logs containing the search term (partial match)")
    @APIResponse(responseCode = "200", description = "Search completed (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-logs-by-message-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<List<LogDTO>> searchLogsByMessage(
            @Parameter(description = "Search term", example = "error", required = true)
            String searchTerm) {
        return logRepository.searchLogsByMessage(searchTerm)
                .onItem().invoke(entries -> log.debug("Found {} logs matching '{}'", entries.size(), searchTerm))
                .onFailure().transform(e -> new RuntimeException("Failed to search logs by message: " + searchTerm, e));
    }

    @Operation(summary = "Search logs by time range",
            description = "Finds logs between two timestamps (inclusive)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed"),
            @APIResponse(responseCode = "400", description = "Invalid date range")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-logs-by-time-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<List<LogDTO>> searchLogsByTimeRange(
            @Parameter(description = "Start date", example = "2023-01-01T00:00:00Z", required = true)
            Date from,
            @Parameter(description = "End date", example = "2023-01-02T00:00:00Z", required = true)
            Date to) {
        if (from.after(to)) {
            return Uni.createFrom().failure(new IllegalArgumentException("From date cannot be after to date"));
        }

        return logRepository.searchLogsByTimeRange(from, to)
                .onItem().invoke(entries -> log.debug("Found {} logs between {} and {}", entries.size(), from, to))
                .onFailure().transform(e -> new RuntimeException("Failed to search logs by time range", e));
    }

    @Operation(summary = "Search logs by exact message",
            description = "Finds logs with exact message match")
    @APIResponse(responseCode = "200", description = "Search completed")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-logs-exact-message-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<List<LogDTO>> searchLogsByExactMessage(
            @Parameter(description = "Exact message text", example = "Database connection failed", required = true)
            String exactMessage) {
        return logRepository.searchLogsByExactMessage(exactMessage)
                .onItem().invoke(entries -> log.debug("Found {} logs with exact message '{}'", entries.size(), exactMessage))
                .onFailure().transform(e -> new RuntimeException("Failed to search logs by exact message: " + exactMessage, e));
    }

    @Operation(summary = "Get recent logs",
            description = "Retrieves logs from the last X minutes")
    @APIResponse(responseCode = "200", description = "Recent logs returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-recent-logs-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<List<LogDTO>> getRecentLogs(
            @Parameter(description = "Minutes to look back", example = "60", required = true)
            int minutes) {
        Date to = Date.from(Instant.now());
        Date from = Date.from(Instant.now().minusSeconds(minutes * 60L));
        return searchLogsByTimeRange(from, to);
    }

    @Operation(summary = "Get error logs",
            description = "Finds all logs containing 'ERROR' or 'exception'")
    @APIResponse(responseCode = "200", description = "Error logs returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-error-logs-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<List<LogDTO>> getErrorLogs() {
        return searchLogsByMessage("ERROR")
                .onItem().transformToUni(errorLogs ->
                        searchLogsByMessage("exception")
                                .onItem().transform(exceptionLogs -> {
                                    errorLogs.addAll(exceptionLogs);
                                    return errorLogs;
                                })
                );
    }

    private String generateLogId() {
        return "log-" + Instant.now().toEpochMilli() + "-" + (int)(Math.random() * 1000);
    }

    @Operation(summary = "Purge old logs",
            description = "Simulated operation to remove logs before cutoff date")
    @APIResponse(responseCode = "200", description = "Number of logs that would be purged")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("purge-old-logs-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<Long> purgeOldLogs(
            @Parameter(description = "Cutoff date", example = "2022-12-31T00:00:00Z", required = true)
            Date cutoffDate) {
        return searchLogsByTimeRange(new Date(0), cutoffDate)
                .onItem().transformToUni(logs -> {
                    if (logs.isEmpty()) {
                        return Uni.createFrom().item(0L);
                    }

                    log.warn("Purge operation would delete {} logs", logs.size());
                    return Uni.createFrom().item((long) logs.size());
                });
    }
}