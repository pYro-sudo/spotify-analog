package by.losik.consumer;

import by.losik.entity.AccordDTO;
import by.losik.service.AccordService;
import by.losik.service.AuthService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Accord Consumer", description = "Message processing endpoints for accord operations")
public class AccordConsumer extends BaseConsumer {

    @Inject
    AccordService accordService;

    @Inject
    AuthService authService;

    @Override
    protected String getMetricPrefix() {
        return "accord";
    }

    @Operation(summary = "Process accord indexing",
            description = "Handles incoming messages for indexing single accord records. " +
                    "Requires 'index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid accord data")
    })
    @Incoming("accords-index-in")
    @Outgoing("accords-index-out")
    public Uni<ProcessingResult> processAccordIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        AccordDTO accord = parseMessage(message.getPayload(), AccordDTO.class);
                        return accordService.indexAccord(accord)
                                .onFailure().retry()
                                .withBackOff(INITIAL_BACKOFF)
                                .atMost(MAX_RETRIES)
                                .onItem().transform(item -> createSuccessResult(
                                        accord.getName(),
                                        "Accord successfully indexed"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        accord.getName(),
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

    @Operation(summary = "Process bulk accord indexing",
            description = "Handles batch processing of multiple accord records in a single operation. " +
                    "Requires 'bulk-index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accords successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid accord data in batch")
    })
    @Incoming("accords-bulk-index-in")
    @Outgoing("accords-bulk-index-out")
    public Uni<ProcessingResult> processBulkAccordIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        List<AccordDTO> accords = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return accordService.bulkIndexAccords(accords)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully indexed " + items.size() + " accords"
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

    @Operation(summary = "Retrieve accord by name",
            description = "Processes requests to fetch a specific accord by its exact name. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Accord not found")
    })
    @Incoming("accords-get-in")
    @Outgoing("accords-get-out")
    public Uni<ProcessingResult> processGetAccord(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String accordName = message.getPayload();
                        return accordService.getAccordByName(accordName)
                                .onItem().transform(accord -> createSuccessResult(
                                        accordName,
                                        "Accord retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        accordName,
                                        error,
                                        "Failed to retrieve accord"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search accords",
            description = "Processes search requests for accords matching the provided term. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "term", description = "Search term to match against accord names", required = true)
    @Incoming("accords-search-in")
    @Outgoing("accords-search-out")
    public Uni<ProcessingResult> processSearchAccords(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        return accordService.searchAccords(searchTerm)
                                .onItem().transform(results -> createSuccessResult(
                                        searchTerm,
                                        "Found " + results.size() + " accords"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        searchTerm,
                                        error,
                                        "Search failed"
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

    @Operation(summary = "Create new accord",
            description = "Handles accord creation requests with validation. " +
                    "Requires 'create' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord successfully created"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid accord data")
    })
    @Incoming("accords-create-in")
    @Outgoing("accords-create-out")
    public Uni<ProcessingResult> processCreateAccord(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        AccordDTO accord = parseMessage(message.getPayload(), AccordDTO.class);
                        return accordService.createAccord(accord)
                                .onItem().transform(item -> createSuccessResult(
                                        accord.getName(),
                                        "Accord created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        accord.getName(),
                                        error,
                                        "Failed to create accord"
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

    @Operation(summary = "Delete accord",
            description = "Processes requests to remove an accord by name. " +
                    "Requires 'delete' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord successfully deleted"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Accord not found")
    })
    @Parameter(name = "name", description = "Name of the accord to delete", required = true)
    @Incoming("accords-delete-in")
    @Outgoing("accords-delete-out")
    public Uni<ProcessingResult> processDeleteAccord(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String accordName = message.getPayload();
                        return accordService.deleteAccord(accordName)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(accordName, "Accord successfully deleted") :
                                        createNotFoundResult(accordName)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        accordName,
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