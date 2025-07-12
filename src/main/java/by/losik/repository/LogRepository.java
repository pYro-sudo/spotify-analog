package by.losik.repository;

import by.losik.entity.GroupDTO;
import by.losik.entity.LogDTO;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Duration;
import java.util.Date;
import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Log Repository", description = "Operations for managing application logs in Elasticsearch")
public class LogRepository extends ReactiveElasticsearchRepository<LogDTO> {

    @Override
    protected String getIndexName() {
        return "application_logs";
    }

    @Override
    protected String getId(LogDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<LogDTO> getEntityType() {
        return LogDTO.class;
    }

    @Operation(
            summary = "Log a single event",
            description = "Creates a new log entry in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Log event successfully recorded",
                    content = @Content(schema = @Schema(implementation = LogDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or logging failed after retries"
            )
    })
    public Uni<LogDTO> logEvent(LogDTO logEntry) {
        return index(logEntry)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to log event {}", logEntry.getId(), e));
    }

    @Operation(
            summary = "Bulk log multiple events",
            description = "Creates multiple log entries in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Log events successfully recorded",
                    content = @Content(schema = @Schema(implementation = LogDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk logging failed after retries"
            )
    })
    public Uni<List<LogDTO>> bulkLogEvents(List<LogDTO> logEntries) {
        return bulkIndex(logEntries)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk log failed for {} entries", logEntries.size(), e));
    }

    @Operation(
            summary = "Get log entry by ID",
            description = "Retrieves a single log entry from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Log entry found and returned",
                    content = @Content(schema = @Schema(implementation = LogDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Log entry not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<LogDTO> getLogById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the log entry",
                    required = true,
                    example = "log-2023-01-01-12345"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch log entry {}", id, e));
    }

    @Operation(
            summary = "Search logs by message content",
            description = "Searches for log entries containing the specified term in their message using match query"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = LogDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<LogDTO>> searchLogsByMessage(
            @Parameter(
                    name = "searchTerm",
                    description = "Term to search for in log messages",
                    required = true,
                    example = "error"
            ) String searchTerm) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match", new JsonObject()
                                .put("log", searchTerm)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by message failed for '{}'", searchTerm, e));
    }

    @Operation(
            summary = "Search logs by time range",
            description = "Filters log entries by their timestamp within the specified range"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = LogDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<LogDTO>> searchLogsByTimeRange(
            @Parameter(
                    name = "from",
                    description = "Start of time range (inclusive)",
                    required = true,
                    example = "2023-01-01T00:00:00Z"
            ) Date from,
            @Parameter(
                    name = "to",
                    description = "End of time range (inclusive)",
                    required = true,
                    example = "2023-01-02T00:00:00Z"
            ) Date to) {
        JsonObject rangeQuery = new JsonObject()
                .put("gte", from.getTime())
                .put("lte", to.getTime());

        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("range", new JsonObject()
                                .put("date", rangeQuery)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by time range failed from {} to {}", from, to, e));
    }

    @Operation(
            summary = "Search logs by exact message",
            description = "Searches for log entries containing the exact specified phrase in their message"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = LogDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<LogDTO>> searchLogsByExactMessage(
            @Parameter(
                    name = "exactMessage",
                    description = "Exact phrase to search for in log messages",
                    required = true,
                    example = "Database connection failed"
            ) String exactMessage) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match_phrase", new JsonObject()
                                .put("log", exactMessage)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by exact message failed for '{}'", exactMessage, e));
    }
}