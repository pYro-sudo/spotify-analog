package by.losik.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Duration;

@Slf4j
@Tag(name = "Base Consumer", description = "Provides common functionality and utilities for all message consumers")
public abstract class BaseConsumer {
    protected static final int MAX_RETRIES = 3;
    protected static final Duration INITIAL_BACKOFF = Duration.ofMillis(500);
    protected static final String UNKNOWN = "unknown";
    protected static final String COUNT_SUFFIX = ".messages.total";
    protected static final String TIME_SUFFIX = ".process.time";
    protected static final String OPERATION_TAG = "op";
    protected static final String SUCCESS_TAG = "success";
    protected static final String ENTITY_TAG = "entity";

    @Inject
    ObjectMapper objectMapper;
    @Inject
    PrometheusMeterRegistry meterRegistry;

    protected abstract String getMetricPrefix();

    protected Timer.Sample startTimer() {
        return Timer.start(meterRegistry);
    }

    protected void recordCount(String operation, boolean success, String entity) {
        meterRegistry.counter(getMetricPrefix() + COUNT_SUFFIX,
                        OPERATION_TAG, operation,
                        SUCCESS_TAG, String.valueOf(success),
                        ENTITY_TAG, entity != null ? entity : UNKNOWN)
                .increment();
    }

    protected void recordTime(Timer.Sample timer, String operation, boolean success, String entity) {
        if (timer != null) {
            timer.stop(meterRegistry.timer(getMetricPrefix() + TIME_SUFFIX,
                    OPERATION_TAG, operation,
                    SUCCESS_TAG, String.valueOf(success),
                    ENTITY_TAG, entity != null ? entity : UNKNOWN));
        }
    }

    protected void recordMetrics(Timer.Sample timer, String operation, ProcessingResult result) {
        boolean success = result.status() == Status.SUCCESS;
        recordTime(timer, operation, success, result.name());
        recordCount(operation, success, result.name());
    }

    @Operation(summary = "Parse message payload",
            description = "Deserializes the message payload into the specified class type")
    @Parameters({
            @Parameter(name = "payload", description = "The JSON payload to be parsed", required = true),
            @Parameter(name = "clazz", description = "The target class type for deserialization", required = true)
    })
    protected <T> T parseMessage(String payload, Class<T> clazz) throws JsonProcessingException {
        try {
            return objectMapper.readValue(payload, clazz);
        } catch (JsonProcessingException e) {
            log.error("Failed to parse message payload: {}", payload);
            throw e;
        }
    }

    @Operation(summary = "Acknowledge message",
            description = "Handles message acknowledgment based on processing status")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message processed successfully"),
            @APIResponse(responseCode = "400", description = "Message processing failed")
    })
    @Parameters({
            @Parameter(name = "message", description = "The incoming RabbitMQ message", required = true),
            @Parameter(name = "status", description = "The processing status of the message", required = true)
    })
    protected void acknowledgeMessage(IncomingRabbitMQMessage<?> message, Status status) {
        if (status == Status.SUCCESS) {
            message.ack();
        } else {
            message.nack(new RuntimeException("Processing failed with status: " + status));
        }
    }

    @Operation(summary = "Handle parsing error",
            description = "Creates an error result for message parsing failures")
    @APIResponse(responseCode = "400", description = "Message parsing failed")
    @Parameters({
            @Parameter(name = "message", description = "The incoming message that failed parsing", required = true),
            @Parameter(name = "e", description = "The parsing exception that occurred", required = true)
    })
    protected ProcessingResult handleParsingError(IncomingRabbitMQMessage<?> message, JsonProcessingException e) {
        message.nack(e);
        return new ProcessingResult(
                UNKNOWN,
                Status.ERROR,
                "Invalid message format: " + e.getMessage()
        );
    }

    @Operation(summary = "Create success result",
            description = "Constructs a successful processing result")
    @APIResponse(responseCode = "200", description = "Success result created")
    @Parameters({
            @Parameter(name = "entityName", description = "Name of the processed entity", required = true),
            @Parameter(name = "successMessage", description = "Description of the successful operation", required = true)
    })
    protected ProcessingResult createSuccessResult(String entityName, String successMessage) {
        log.info("Operation successful - Entity: {}, Message: {}", entityName, successMessage);
        return new ProcessingResult(
                entityName,
                Status.SUCCESS,
                successMessage
        );
    }

    @Operation(summary = "Create error result",
            description = "Constructs an error processing result")
    @APIResponse(responseCode = "400", description = "Error result created")
    @Parameters({
            @Parameter(name = "entityName", description = "Name of the entity that failed processing"),
            @Parameter(name = "error", description = "The exception that caused the failure", required = true),
            @Parameter(name = "errorContext", description = "Contextual information about the error", required = true)
    })
    protected ProcessingResult createErrorResult(String entityName, Throwable error, String errorContext) {
        log.error("Failed to process {} after {} retries: {}", entityName, MAX_RETRIES, error.getMessage());
        return new ProcessingResult(
                entityName != null ? entityName : UNKNOWN,
                Status.ERROR,
                errorContext + ": " + error.getMessage()
        );
    }

    @Operation(summary = "Create not found result",
            description = "Constructs a processing result for entity not found cases")
    @APIResponse(responseCode = "404", description = "Entity not found result created")
    @Parameter(name = "entityName", description = "Name of the entity that was not found", required = true)
    protected ProcessingResult createNotFoundResult(String entityName) {
        log.warn("Entity not found: {}", entityName);
        return new ProcessingResult(
                entityName,
                Status.ERROR,
                "Entity not found"
        );
    }

    @Operation(summary = "Log processing result",
            description = "Logs the outcome of a message processing operation")
    @APIResponse(responseCode = "200", description = "Result logged successfully")
    @Parameter(name = "result", description = "The processing result to log", required = true)
    protected void logProcessingResult(ProcessingResult result) {
        if (result.status() == Status.SUCCESS) {
            log.info("Operation successful - Entity: {}, Message: {}",
                    result.name(), result.message());
        } else {
            log.warn("Operation failed - Entity: {}, Error: {}",
                    result.name(), result.message());
        }
    }

    @Operation(summary = "Create unauthorized result",
            description = "Constructs a processing result for unauthorized access attempts")
    @APIResponse(responseCode = "401", description = "Unauthorized access result created")
    protected ProcessingResult createUnauthorizedResult() {
        return new ProcessingResult(
                UNKNOWN,
                Status.ERROR,
                "Unauthorized access"
        );
    }

    @Operation(summary = "Create forbidden result",
            description = "Constructs a processing result for insufficient privilege cases")
    @APIResponse(responseCode = "403", description = "Forbidden access result created")
    protected ProcessingResult createForbiddenResult() {
        return new ProcessingResult(
                UNKNOWN,
                Status.ERROR,
                "Insufficient privileges"
        );
    }
}