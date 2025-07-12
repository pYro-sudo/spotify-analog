package by.losik.consumer;

import by.losik.entity.AnalysisDTO;
import by.losik.entity.LexemeDTO;
import by.losik.service.AnalysisService;
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
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Analysis Consumer", description = "Message consumers for text analysis operations")
public class AnalysisConsumer extends BaseConsumer {
    @Inject
    AnalysisService analysisService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "analysis";
    }

    @Operation(summary = "Process analysis indexing",
            description = "Consumes analysis indexing messages, validates authorization, and processes the analysis")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis indexed successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - insufficient permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid message format")
    })
    @Incoming("analysis-index-in")
    @Outgoing("analysis-index-out")
    public Uni<ProcessingResult> processAnalysisIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        AnalysisDTO analysis = parseMessage(message.getPayload(), AnalysisDTO.class);
                        return analysisService.saveAnalysis(analysis)
                                .onFailure().retry()
                                .withBackOff(INITIAL_BACKOFF)
                                .atMost(MAX_RETRIES)
                                .onItem().transform(item -> createSuccessResult(
                                        analysis.getId(),
                                        "Analysis successfully indexed"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        analysis.getId(),
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

    @Operation(summary = "Process bulk analysis indexing",
            description = "Consumes bulk analysis indexing messages and processes multiple analyses at once")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Bulk indexing completed successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - insufficient permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid message format")
    })
    @Incoming("analysis-bulk-index-in")
    @Outgoing("analysis-bulk-index-out")
    public Uni<ProcessingResult> processBulkAnalysisIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        List<AnalysisDTO> analyses = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return analysisService.saveAllAnalyses(analyses)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully indexed " + items.size() + " analyses"
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

    @Operation(summary = "Process get analysis by ID",
            description = "Retrieves an analysis by its unique identifier")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - invalid credentials"),
            @APIResponse(responseCode = "404", description = "Analysis not found")
    })
    @Incoming("analysis-get-by-id-in")
    @Outgoing("analysis-get-by-id-out")
    public Uni<ProcessingResult> processGetAnalysisById(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String analysisId = message.getPayload();
                        return analysisService.getAnalysisById(analysisId)
                                .onItem().transform(analysis -> createSuccessResult(
                                        analysisId,
                                        "Analysis retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        analysisId,
                                        error,
                                        "Failed to retrieve analysis"
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

    @Operation(summary = "Process get analyses by user",
            description = "Retrieves all analyses belonging to a specific user")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analyses retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - invalid credentials")
    })
    @Incoming("analysis-get-by-user-in")
    @Outgoing("analysis-get-by-user-out")
    public Uni<ProcessingResult> processGetAnalysisByUser(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        return analysisService.getUserAnalyses(userId)
                                .onItem().transform(analyses -> createSuccessResult(
                                        userId,
                                        "Retrieved " + analyses.size() + " analyses"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to retrieve analyses"
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

    @Operation(summary = "Process get analyses by lexeme",
            description = "Retrieves analyses containing a specific lexeme type")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analyses retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - invalid credentials")
    })
    @Incoming("analysis-get-by-lexeme-in")
    @Outgoing("analysis-get-by-lexeme-out")
    public Uni<ProcessingResult> processGetAnalysisByLexeme(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-lexeme", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-lexeme", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String lexemeType = message.getPayload();
                        return analysisService.findAnalysesWithLexemeType(lexemeType)
                                .onItem().transform(analyses -> createSuccessResult(
                                        lexemeType,
                                        "Found " + analyses.size() + " analyses"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        lexemeType,
                                        error,
                                        "Failed to retrieve analyses"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-lexeme", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-lexeme", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Process get long analyses",
            description = "Retrieves analyses with text length exceeding the specified minimum")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analyses retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - invalid credentials"),
            @APIResponse(responseCode = "400", description = "Invalid minimum length parameter")
    })
    @Incoming("analysis-get-long-in")
    @Outgoing("analysis-get-long-out")
    public Uni<ProcessingResult> processGetLongAnalyses(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-long", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-long", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        int minLength = Integer.parseInt(message.getPayload());
                        return analysisService.findLongAnalyses(minLength)
                                .onItem().transform(analyses -> createSuccessResult(
                                        String.valueOf(minLength),
                                        "Found " + analyses.size() + " long analyses"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        String.valueOf(minLength),
                                        error,
                                        "Failed to retrieve analyses"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-long", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-long", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Process create analysis",
            description = "Creates a new analysis record")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis created successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - insufficient permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid analysis data")
    })
    @Incoming("analysis-create-in")
    @Outgoing("analysis-create-out")
    public Uni<ProcessingResult> processCreateAnalysis(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        AnalysisDTO analysis = parseMessage(message.getPayload(), AnalysisDTO.class);
                        return analysisService.createAnalysis(analysis)
                                .onItem().transform(item -> createSuccessResult(
                                        analysis.getId(),
                                        "Analysis created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        analysis.getId(),
                                        error,
                                        "Failed to create analysis"
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

    @Operation(summary = "Process add lexeme to analysis",
            description = "Adds a lexeme to an existing analysis at specified position")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme added successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - insufficient permissions"),
            @APIResponse(responseCode = "404", description = "Analysis not found"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid lexeme data")
    })
    @Incoming("analysis-add-lexeme-in")
    @Outgoing("analysis-add-lexeme-out")
    public Uni<ProcessingResult> processAddLexemeToAnalysis(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-lexeme", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-lexeme")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-lexeme", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String analysisId = payload.getString("analysisId");
                        int position = payload.getInteger("position");
                        LexemeDTO lexeme = parseMessage(payload.getJsonObject("lexeme").toString(), LexemeDTO.class);

                        return analysisService.addLexemeToAnalysis(analysisId, position, lexeme)
                                .onItem().transform(updated -> createSuccessResult(
                                        analysisId,
                                        "Lexeme added successfully to analysis"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        analysisId,
                                        error,
                                        "Failed to add lexeme to analysis"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-lexeme", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during lexeme addition"
                        );
                        recordMetrics(timer, "add-lexeme", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Process delete analysis",
            description = "Deletes an analysis by its ID")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis deleted successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - insufficient permissions"),
            @APIResponse(responseCode = "404", description = "Analysis not found")
    })
    @Incoming("analysis-delete-in")
    @Outgoing("analysis-delete-out")
    public Uni<ProcessingResult> processDeleteAnalysis(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String analysisId = message.getPayload();
                        return analysisService.removeAnalysis(analysisId)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(analysisId, "Analysis successfully deleted") :
                                        createNotFoundResult(analysisId)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        analysisId,
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