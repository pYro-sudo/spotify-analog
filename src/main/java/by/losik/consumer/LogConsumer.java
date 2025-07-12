package by.losik.consumer;

import by.losik.service.AuthServiceImpl;
import by.losik.service.LogService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.Date;
import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Log Consumer", description = "Message processing endpoints for log management operations")
public class LogConsumer extends BaseConsumer {
    @Inject
    LogService logService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "log";
    }

    @Operation(summary = "Process log event",
            description = "Handles incoming messages for logging single events. " +
                    "Requires 'log' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Log event successfully processed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid log message")
    })
    @Parameter(name = "message", description = "Log message content", required = true)
    @Incoming("log-event-in")
    @Outgoing("log-event-out")
    public Uni<ProcessingResult> processLogEvent(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("event", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "log")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "event", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        String logMessage = message.getPayload();
                        return logService.logEvent(logMessage)
                                .onItem().transform(logEntry -> createSuccessResult(
                                        logEntry.getId(),
                                        "Log event processed successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "unknown-log-id",
                                        error,
                                        "Failed to process log event"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "event", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process log event"
                        );
                        recordMetrics(timer, "event", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Process bulk log events",
            description = "Handles batch processing of multiple log messages in a single operation. " +
                    "Requires 'bulk-log' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Log events successfully processed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid log messages in batch")
    })
    @Parameter(name = "messages", description = "List of log messages", required = true)
    @Incoming("log-bulk-event-in")
    @Outgoing("log-bulk-event-out")
    public Uni<ProcessingResult> processBulkLogEvents(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("bulk-event", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "bulk-log")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "bulk-event", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        List<String> messages = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return logService.bulkLogEvents(messages)
                                .onItem().transform(logEntries -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully logged " + logEntries.size() + " events"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "bulk-operation",
                                        error,
                                        "Failed to process bulk log events"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "bulk-event", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "bulk-event", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get log by ID",
            description = "Retrieves a specific log entry by its unique identifier. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Log entry successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Log entry not found")
    })
    @Parameter(name = "logId", description = "ID of the log entry to retrieve", required = true)
    @Incoming("log-get-by-id-in")
    @Outgoing("log-get-by-id-out")
    public Uni<ProcessingResult> processGetLogById(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-id", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-id", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String logId = message.getPayload();
                        return logService.getLogById(logId)
                                .onItem().transform(logEntry -> createSuccessResult(
                                        logId,
                                        "Log entry retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        logId,
                                        error,
                                        "Failed to retrieve log entry"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-id", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-id", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search logs by message content",
            description = "Finds logs containing the search term (partial match). " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "searchTerm", description = "Term to search in log messages", required = true)
    @Incoming("log-search-by-message-in")
    @Outgoing("log-search-by-message-out")
    public Uni<ProcessingResult> processSearchLogsByMessage(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search-by-message", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search-by-message", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String searchTerm = message.getPayload();
                        return logService.searchLogsByMessage(searchTerm)
                                .onItem().transform(logEntries -> createSuccessResult(
                                        searchTerm,
                                        "Found " + logEntries.size() + " matching logs"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        searchTerm,
                                        error,
                                        "Failed to search logs"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search-by-message", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "search-by-message", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search logs by time range",
            description = "Finds logs between two timestamps (inclusive). " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid date range")
    })
    @Parameters({
            @Parameter(name = "from", description = "Start timestamp in milliseconds", required = true),
            @Parameter(name = "to", description = "End timestamp in milliseconds", required = true)
    })
    @Incoming("log-search-by-time-in")
    @Outgoing("log-search-by-time-out")
    public Uni<ProcessingResult> processSearchLogsByTimeRange(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search-by-time", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search-by-time", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        Date from = new Date(payload.getLong("from"));
                        Date to = new Date(payload.getLong("to"));

                        return logService.searchLogsByTimeRange(from, to)
                                .onItem().transform(logEntries -> createSuccessResult(
                                        "time-range-search",
                                        "Found " + logEntries.size() + " logs in range"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "time-range-search",
                                        error,
                                        "Failed to search logs by time range"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search-by-time", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process time range search"
                        );
                        recordMetrics(timer, "search-by-time", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search logs by exact message",
            description = "Finds logs with exact message match. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "exactMessage", description = "Exact log message to match", required = true)
    @Incoming("log-search-exact-message-in")
    @Outgoing("log-search-exact-message-out")
    public Uni<ProcessingResult> processSearchLogsByExactMessage(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search-exact-message", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search-exact-message", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String exactMessage = message.getPayload();
                        return logService.searchLogsByExactMessage(exactMessage)
                                .onItem().transform(logEntries -> createSuccessResult(
                                        exactMessage,
                                        "Found " + logEntries.size() + " exact matches"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        exactMessage,
                                        error,
                                        "Failed to search logs by exact message"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search-exact-message", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process exact message search"
                        );
                        recordMetrics(timer, "search-exact-message", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get recent logs",
            description = "Retrieves logs from the last X minutes. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Recent logs successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "minutes", description = "Number of minutes to look back", required = true)
    @Incoming("log-get-recent-in")
    @Outgoing("log-get-recent-out")
    public Uni<ProcessingResult> processGetRecentLogs(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-recent", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-recent", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        int minutes = Integer.parseInt(message.getPayload());
                        return logService.getRecentLogs(minutes)
                                .onItem().transform(logEntries -> createSuccessResult(
                                        "recent-logs",
                                        "Found " + logEntries.size() + " recent logs"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "recent-logs",
                                        error,
                                        "Failed to get recent logs"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-recent", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process recent logs request"
                        );
                        recordMetrics(timer, "get-recent", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get error logs",
            description = "Finds all logs containing 'ERROR' or 'exception'. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Error logs successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Incoming("log-get-errors-in")
    @Outgoing("log-get-errors-out")
    public Uni<ProcessingResult> processGetErrorLogs(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-errors", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-errors", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        return logService.getErrorLogs()
                                .onItem().transform(logEntries -> createSuccessResult(
                                        "error-logs",
                                        "Found " + logEntries.size() + " error logs"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "error-logs",
                                        error,
                                        "Failed to get error logs"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-errors", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process error logs request"
                        );
                        recordMetrics(timer, "get-errors", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Purge old logs",
            description = "Simulates removal of logs before cutoff date (dry-run). " +
                    "Requires 'purge' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Purge simulation completed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions")
    })
    @Parameter(name = "cutoffDate", description = "Timestamp in milliseconds for oldest logs to keep", required = true)
    @Incoming("log-purge-in")
    @Outgoing("log-purge-out")
    public Uni<ProcessingResult> processPurgeOldLogs(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("purge", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "purge")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "purge", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        Date cutoffDate = new Date(Long.parseLong(message.getPayload()));
                        return logService.purgeOldLogs(cutoffDate)
                                .onItem().transform(count -> createSuccessResult(
                                        "purge-operation",
                                        "Would purge " + count + " logs"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "purge-operation",
                                        error,
                                        "Failed to process purge operation"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "purge", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process purge request"
                        );
                        recordMetrics(timer, "purge", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }
}