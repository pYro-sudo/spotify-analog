package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.MessageDTO;
import by.losik.repository.MessageRepository;
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
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Message Service", description = "Business logic for managing messages")
public class MessageService extends ServiceConfig {

    @Inject
    MessageRepository messageRepository;

    @Operation(
            summary = "Create or update a message",
            description = "Saves a message to the database with retry logic"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Message successfully saved",
                    content = @Content(schema = @Schema(implementation = MessageDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed"
            )
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("save-message-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "message-cache")
    public Uni<MessageDTO> saveMessage(MessageDTO message) {
        log.debug("Saving message with ID: {}", message.getId());
        return messageRepository.saveMessage(message)
                .onItem().invoke(msg -> log.debug("Successfully saved message: {}", msg.getId()))
                .onFailure().invoke(e -> log.error("Error saving message", e));
    }

    @Operation(
            summary = "Create or update multiple messages",
            description = "Saves multiple messages to the database in a single operation"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Messages successfully saved",
                    content = @Content(schema = @Schema(implementation = MessageDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed"
            )
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-save-messages-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidateAll(cacheName = "message-cache")
    public Uni<List<MessageDTO>> saveAllMessages(List<MessageDTO> messages) {
        log.debug("Saving {} messages", messages.size());
        return messageRepository.saveAllMessages(messages)
                .onItem().invoke(msgs -> log.debug("Successfully saved {} messages", msgs.size()))
                .onFailure().invoke(e -> log.error("Error saving messages", e));
    }

    @Operation(
            summary = "Get message by ID",
            description = "Retrieves a message by its unique identifier"
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
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-message-by-id-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "message-cache")
    public Uni<MessageDTO> getMessageById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the message",
                    required = true,
                    example = "msg-123"
            ) String id) {
        log.debug("Fetching message with ID: {}", id);
        return messageRepository.findMessageById(id)
                .onItem().invoke(msg -> log.debug("Successfully fetched message: {}", msg.getId()))
                .onFailure().invoke(e -> log.error("Error fetching message with ID: {}", id, e));
    }

    @Operation(
            summary = "Get messages by user ID",
            description = "Retrieves all messages belonging to a specific user"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Messages found and returned",
                    content = @Content(schema = @Schema(implementation = MessageDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-messages-by-user-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "messages-by-user-cache")
    public Uni<List<MessageDTO>> getMessagesByUserId(
            @Parameter(
                    name = "userId",
                    description = "ID of the user whose messages to retrieve",
                    required = true,
                    example = "user-456"
            ) String userId) {
        log.debug("Fetching messages for user ID: {}", userId);
        return messageRepository.findMessagesByUserId(userId)
                .onItem().invoke(msgs -> log.debug("Found {} messages for user {}", msgs.size(), userId))
                .onFailure().invoke(e -> log.error("Error fetching messages for user: {}", userId, e));
    }

    @Operation(
            summary = "Search messages by content",
            description = "Performs a full-text search on message content"
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
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-messages-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "messages-search-cache")
    public Uni<List<MessageDTO>> searchMessages(
            @Parameter(
                    name = "searchTerm",
                    description = "Term to search for in message content",
                    required = true,
                    example = "error"
            ) String searchTerm) {
        log.debug("Searching messages for term: {}", searchTerm);
        return messageRepository.searchMessagesByPayload(searchTerm)
                .onItem().invoke(msgs -> log.debug("Found {} messages matching term '{}'", msgs.size(), searchTerm))
                .onFailure().invoke(e -> log.error("Error searching messages for term: {}", searchTerm, e));
    }

    @Operation(
            summary = "Delete message by ID",
            description = "Removes a message from the database"
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
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or deletion failed"
            )
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("delete-message-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "message-cache")
    public Uni<Boolean> deleteMessage(
            @Parameter(
                    name = "id",
                    description = "ID of the message to delete",
                    required = true,
                    example = "msg-123"
            ) String id) {
        log.debug("Attempting to delete message with ID: {}", id);
        return messageRepository.deleteMessageById(id)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted message: {}", id);
                    } else {
                        log.debug("Message not found for deletion: {}", id);
                    }
                })
                .onFailure().invoke(e -> log.error("Error deleting message with ID: {}", id, e));
    }
}