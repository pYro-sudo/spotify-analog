package by.losik.repository;

import by.losik.entity.MessageDTO;
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
import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Message Repository", description = "Operations for managing messages in Elasticsearch")
public class MessageRepository extends ReactiveElasticsearchRepository<MessageDTO> {

    @Override
    protected String getIndexName() {
        return "messages";
    }

    @Override
    protected String getId(MessageDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<MessageDTO> getEntityType() {
        return MessageDTO.class;
    }

    @Operation(
            summary = "Save a single message",
            description = "Creates or updates a message in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Message successfully saved",
                    content = @Content(schema = @Schema(implementation = MessageDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed after retries"
            )
    })
    public Uni<MessageDTO> saveMessage(MessageDTO message) {
        return index(message)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to save message {}", message.getId(), e));
    }

    @Operation(
            summary = "Save multiple messages",
            description = "Creates or updates multiple messages in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Messages successfully saved",
                    content = @Content(schema = @Schema(implementation = MessageDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk save operation failed after retries"
            )
    })
    public Uni<List<MessageDTO>> saveAllMessages(List<MessageDTO> messages) {
        return bulkIndex(messages)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk save failed for {} messages", messages.size(), e));
    }

    @Operation(
            summary = "Find message by ID",
            description = "Retrieves a single message from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Message found and returned",
                    content = @Content(schema = @Schema(implementation = MessageDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Message not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<MessageDTO> findMessageById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the message",
                    required = true,
                    example = "msg-123"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch message {}", id, e));
    }

    @Operation(
            summary = "Find messages by user ID",
            description = "Searches for messages belonging to a specific user using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = MessageDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<MessageDTO>> findMessagesByUserId(
            @Parameter(
                    name = "userId",
                    description = "Exact user ID to search for in messages",
                    required = true,
                    example = "user-456"
            ) String userId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("userId.keyword", userId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by user failed for {}", userId, e));
    }

    @Operation(
            summary = "Search messages by payload",
            description = "Performs a full-text search on message payloads"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = MessageDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<MessageDTO>> searchMessagesByPayload(
            @Parameter(
                    name = "searchTerm",
                    description = "Term to search for in message payloads",
                    required = true,
                    example = "error"
            ) String searchTerm) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match", new JsonObject()
                                .put("payload", searchTerm)));

        return search(query)
                .onFailure().invoke(e -> log.error("Payload search failed for term {}", searchTerm, e));
    }

    @Operation(
            summary = "Delete message by ID",
            description = "Removes a message from Elasticsearch by its ID"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Message successfully deleted",
                    content = @Content(schema = @Schema(implementation = Boolean.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Message not found"
            )
    })
    public Uni<Boolean> deleteMessageById(
            @Parameter(
                    name = "id",
                    description = "ID of the message to delete",
                    required = true,
                    example = "msg-123"
            ) String id) {
        return deleteById(id)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted message: {}", id);
                    } else {
                        log.debug("Message not found for deletion: {}", id);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete message {} after retries: {}", id, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for message: {}", id);
                    return false;
                });
    }
}
