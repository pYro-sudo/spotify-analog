package by.losik.consumer;

import by.losik.entity.LexemeDTO;
import by.losik.service.LexemeService;
import by.losik.service.AuthServiceImpl;
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
@Tag(name = "Lexeme Consumer", description = "Message processing endpoints for lexeme operations")
public class LexemeConsumer extends BaseConsumer {
    @Inject
    LexemeService lexemeService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "lexeme";
    }

    @Operation(summary = "Process lexeme indexing",
            description = "Handles incoming messages for indexing single lexeme records. " +
                    "Requires 'index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid lexeme data")
    })
    @Incoming("lexeme-index-in")
    @Outgoing("lexeme-index-out")
    public Uni<ProcessingResult> processLexemeIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("index", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "index")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "index", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        LexemeDTO lexeme = parseMessage(message.getPayload(), LexemeDTO.class);
                        return lexemeService.saveLexeme(lexeme)
                                .onFailure().retry()
                                .withBackOff(INITIAL_BACKOFF)
                                .atMost(MAX_RETRIES)
                                .onItem().transform(item -> createSuccessResult(
                                        lexeme.getId(),
                                        "Lexeme successfully indexed"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        lexeme.getId(),
                                        error,
                                        "Indexing failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "index", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "index", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Process bulk lexeme indexing",
            description = "Handles batch processing of multiple lexeme records in a single operation. " +
                    "Requires 'bulk-index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexemes successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid lexeme data in batch")
    })
    @Incoming("lexeme-bulk-index-in")
    @Outgoing("lexeme-bulk-index-out")
    public Uni<ProcessingResult> processBulkLexemeIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("bulk-index", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "bulk-index")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "bulk-index", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        List<LexemeDTO> lexemes = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return lexemeService.saveAllLexemes(lexemes)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully indexed " + items.size() + " lexemes"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "bulk-operation",
                                        error,
                                        "Bulk processing failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "bulk-index", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "bulk-index", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Retrieve lexeme by ID",
            description = "Processes requests to fetch a specific lexeme by its unique identifier. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Lexeme not found")
    })
    @Parameter(name = "id", description = "Unique identifier of the lexeme", required = true)
    @Incoming("lexeme-get-by-id-in")
    @Outgoing("lexeme-get-by-id-out")
    public Uni<ProcessingResult> processGetLexemeById(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String lexemeId = message.getPayload();
                        return lexemeService.getLexemeById(lexemeId)
                                .onItem().transform(lexeme -> createSuccessResult(
                                        lexemeId,
                                        "Lexeme retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        lexemeId,
                                        error,
                                        "Failed to retrieve lexeme"
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

    @Operation(summary = "Search lexemes by exact substring",
            description = "Processes search requests for lexemes matching the exact provided substring. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "substring", description = "Exact substring to match against lexemes", required = true)
    @Incoming("lexeme-search-exact-in")
    @Outgoing("lexeme-search-exact-out")
    public Uni<ProcessingResult> processSearchLexemesByExactSubstring(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search-exact", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search-exact", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String substring = message.getPayload();
                        return lexemeService.findLexemesByExactSubstring(substring)
                                .onItem().transform(lexemes -> createSuccessResult(
                                        substring,
                                        "Found " + lexemes.size() + " lexemes"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        substring,
                                        error,
                                        "Failed to search lexemes"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search-exact", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "search-exact", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get lexemes by accord",
            description = "Retrieves all lexemes associated with a specific accord. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexemes retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "accordId", description = "ID of the accord to find associated lexemes", required = true)
    @Incoming("lexeme-get-by-accord-in")
    @Outgoing("lexeme-get-by-accord-out")
    public Uni<ProcessingResult> processGetLexemesByAccord(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-accord", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-accord", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String accordId = message.getPayload();
                        return lexemeService.findLexemesByAccord(accordId)
                                .onItem().transform(lexemes -> createSuccessResult(
                                        accordId,
                                        "Found " + lexemes.size() + " lexemes"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        accordId,
                                        error,
                                        "Failed to retrieve lexemes"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-accord", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-accord", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search lexemes by prefix",
            description = "Processes search requests for lexemes starting with the provided prefix. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "prefix", description = "Prefix to match against lexemes", required = true)
    @Incoming("lexeme-search-prefix-in")
    @Outgoing("lexeme-search-prefix-out")
    public Uni<ProcessingResult> processSearchLexemesByPrefix(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search-prefix", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search-prefix", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String prefix = message.getPayload();
                        return lexemeService.findLexemesBySubstringPrefix(prefix)
                                .onItem().transform(lexemes -> createSuccessResult(
                                        prefix,
                                        "Found " + lexemes.size() + " lexemes"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        prefix,
                                        error,
                                        "Failed to search lexemes"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search-prefix", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "search-prefix", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Create new lexeme",
            description = "Handles lexeme creation requests with validation. " +
                    "Requires 'create' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme successfully created"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid lexeme data")
    })
    @Incoming("lexeme-create-in")
    @Outgoing("lexeme-create-out")
    public Uni<ProcessingResult> processCreateLexeme(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("create", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "create")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "create", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        LexemeDTO lexeme = parseMessage(message.getPayload(), LexemeDTO.class);
                        return lexemeService.createLexeme(lexeme)
                                .onItem().transform(item -> createSuccessResult(
                                        lexeme.getId(),
                                        "Lexeme created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        lexeme.getId(),
                                        error,
                                        "Failed to create lexeme"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "create", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "create", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Update lexeme substring",
            description = "Processes requests to update the substring of an existing lexeme. " +
                    "Requires 'update-substring' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme substring successfully updated"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Lexeme not found")
    })
    @Parameters({
            @Parameter(name = "lexemeId", description = "ID of the lexeme to update", required = true),
            @Parameter(name = "newSubstring", description = "New substring value for the lexeme", required = true)
    })
    @Incoming("lexeme-update-substring-in")
    @Outgoing("lexeme-update-substring-out")
    public Uni<ProcessingResult> processUpdateLexemeSubstring(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("update-substring", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "update-substring")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "update-substring", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String lexemeId = payload.getString("lexemeId");
                        String newSubstring = payload.getString("newSubstring");

                        return lexemeService.updateLexemeSubstring(lexemeId, newSubstring)
                                .onItem().transform(updated -> createSuccessResult(
                                        lexemeId,
                                        "Lexeme substring updated successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        lexemeId,
                                        error,
                                        "Failed to update lexeme substring"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "update-substring", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during substring update"
                        );
                        recordMetrics(timer, "update-substring", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Update lexeme accord",
            description = "Processes requests to update the accord reference of an existing lexeme. " +
                    "Requires 'update-accord' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme accord successfully updated"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Lexeme or accord not found")
    })
    @Parameters({
            @Parameter(name = "lexemeId", description = "ID of the lexeme to update", required = true),
            @Parameter(name = "newAccordId", description = "ID of the new accord reference", required = true)
    })
    @Incoming("lexeme-update-accord-in")
    @Outgoing("lexeme-update-accord-out")
    public Uni<ProcessingResult> processUpdateLexemeAccord(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("update-accord", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "update-accord")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "update-accord", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String lexemeId = payload.getString("lexemeId");
                        String newAccordId = payload.getString("newAccordId");

                        return lexemeService.updateLexemeAccord(lexemeId, newAccordId)
                                .onItem().transform(updated -> createSuccessResult(
                                        lexemeId,
                                        "Lexeme accord updated successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        lexemeId,
                                        error,
                                        "Failed to update lexeme accord"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "update-accord", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during accord update"
                        );
                        recordMetrics(timer, "update-accord", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search lexemes containing text fragment",
            description = "Processes search requests for lexemes containing the provided text fragment. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "textFragment", description = "Text fragment to search within lexemes", required = true)
    @Incoming("lexeme-search-containing-in")
    @Outgoing("lexeme-search-containing-out")
    public Uni<ProcessingResult> processSearchLexemesContaining(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search-containing", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search-containing", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String textFragment = message.getPayload();
                        return lexemeService.searchLexemesContaining(textFragment)
                                .onItem().transform(lexemes -> createSuccessResult(
                                        textFragment,
                                        "Found " + lexemes.size() + " lexemes"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        textFragment,
                                        error,
                                        "Failed to search lexemes"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search-containing", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "search-containing", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete lexeme",
            description = "Processes requests to remove a lexeme by its ID. " +
                    "Requires 'delete' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme successfully deleted"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Lexeme not found")
    })
    @Parameter(name = "id", description = "ID of the lexeme to delete", required = true)
    @Incoming("lexeme-delete-in")
    @Outgoing("lexeme-delete-out")
    public Uni<ProcessingResult> processDeleteLexeme(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String lexemeId = message.getPayload();
                        return lexemeService.removeLexeme(lexemeId)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(lexemeId, "Lexeme successfully deleted") :
                                        createNotFoundResult(lexemeId)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        lexemeId,
                                        error,
                                        "Delete failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "delete", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during delete"
                        );
                        recordMetrics(timer, "delete", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }
}