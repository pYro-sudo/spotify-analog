package by.losik.consumer;

import by.losik.entity.MessageDTO;
import by.losik.service.AuthServiceImpl;
import by.losik.service.MessageService;
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

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Message Consumer", description = "Message processing endpoints for message management operations")
public class MessageConsumer extends BaseConsumer {
    @Inject
    MessageService messageService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "message";
    }

    @Operation(summary = "Save message",
            description = "Processes messages for creating or updating a single message. " +
                    "Requires 'save' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message successfully saved"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid message data")
    })
    @Incoming("message-save-in")
    @Outgoing("message-save-out")
    public Uni<ProcessingResult> processSaveMessage(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("save", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "save")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "save", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        MessageDTO messageDTO = parseMessage(message.getPayload(), MessageDTO.class);
                        return messageService.saveMessage(messageDTO)
                                .onItem().transform(item -> createSuccessResult(
                                        messageDTO.getId(),
                                        "Message successfully saved"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        messageDTO.getId(),
                                        error,
                                        "Failed to save message"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "save", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "save", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Bulk save messages",
            description = "Processes batch saving of multiple messages in a single operation. " +
                    "Requires 'bulk-save' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Messages successfully saved"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid message data in batch")
    })
    @Incoming("message-bulk-save-in")
    @Outgoing("message-bulk-save-out")
    public Uni<ProcessingResult> processBulkSaveMessages(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("bulk-save", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "bulk-save")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "bulk-save", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        List<MessageDTO> messages = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return messageService.saveAllMessages(messages)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully saved " + items.size() + " messages"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "bulk-operation",
                                        error,
                                        "Bulk processing failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "bulk-save", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "bulk-save", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get message by ID",
            description = "Retrieves a specific message by its unique identifier. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Message not found")
    })
    @Parameter(name = "messageId", description = "ID of the message to retrieve", required = true)
    @Incoming("message-get-by-id-in")
    @Outgoing("message-get-by-id-out")
    public Uni<ProcessingResult> processGetMessageById(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String messageId = message.getPayload();
                        return messageService.getMessageById(messageId)
                                .onItem().transform(msg -> createSuccessResult(
                                        messageId,
                                        "Message retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        messageId,
                                        error,
                                        "Failed to retrieve message"
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

    @Operation(summary = "Get messages by user ID",
            description = "Retrieves messages associated with a specific user. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Messages successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "userId", description = "ID of the user whose messages to retrieve", required = true)
    @Incoming("message-get-by-user-in")
    @Outgoing("message-get-by-user-out")
    public Uni<ProcessingResult> processGetMessagesByUserId(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-user", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-user", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String userId = message.getPayload();
                        return messageService.getMessagesByUserId(userId)
                                .onItem().transform(messages -> createSuccessResult(
                                        userId,
                                        "Messages retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to retrieve messages"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-user", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-user", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search messages",
            description = "Performs a full-text search on message content. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "searchTerm", description = "Term to search for in message content", required = true)
    @Incoming("message-search-in")
    @Outgoing("message-search-out")
    public Uni<ProcessingResult> processSearchMessages(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String searchTerm = message.getPayload();
                        return messageService.searchMessages(searchTerm)
                                .onItem().transform(messages -> createSuccessResult(
                                        searchTerm,
                                        "Search completed successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        searchTerm,
                                        error,
                                        "Failed to search messages"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "search", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete message",
            description = "Removes a message from the system. " +
                    "Requires 'delete' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message successfully deleted"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Message not found")
    })
    @Parameter(name = "messageId", description = "ID of the message to delete", required = true)
    @Incoming("message-delete-in")
    @Outgoing("message-delete-out")
    public Uni<ProcessingResult> processDeleteMessage(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("delete", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "delete")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "delete", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        String messageId = message.getPayload();
                        return messageService.deleteMessage(messageId)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(messageId, "Message successfully deleted") :
                                        createNotFoundResult(messageId)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        messageId,
                                        error,
                                        "Delete failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "delete", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during delete"
                        );
                        recordMetrics(timer, "delete", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }
}