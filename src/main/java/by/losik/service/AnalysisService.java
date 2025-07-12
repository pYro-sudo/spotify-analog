package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.AnalysisDTO;
import by.losik.entity.LexemeDTO;
import by.losik.repository.AnalysisRepository;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
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

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Analysis Service", description = "Operations for text analysis processing")
public class AnalysisService extends ServiceConfig {

    @Inject
    AnalysisRepository analysisRepository;

    @Operation(summary = "Save analysis",
            description = "Stores analysis with validation (requires user ID and text)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis saved successfully"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("save-analysis-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "analysis-cache") // Invalidate single analysis cache
    @CacheInvalidate(cacheName = "user-analyses-cache") // Invalidate user's analyses cache
    public Uni<AnalysisDTO> saveAnalysis(AnalysisDTO analysis) {
        log.debug("Saving analysis with ID: {}", analysis.getId());
        validateAnalysis(analysis);
        return analysisRepository.saveAnalysis(analysis)
                .onItem().invoke(saved -> log.info("Successfully saved analysis: {}", saved.getId()))
                .onFailure().transform(e -> new RuntimeException("Failed to save analysis: " + analysis.getId(), e));
    }

    @Operation(summary = "Bulk save analyses",
            description = "Saves multiple analyses with batch validation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "All analyses saved"),
            @APIResponse(responseCode = "400", description = "Batch validation failed")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-save-analyses-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "analysis-cache") // Clear entire analysis cache
    @CacheInvalidate(cacheName = "user-analyses-cache") // Clear all user analyses caches
    public Uni<List<AnalysisDTO>> saveAllAnalyses(List<AnalysisDTO> analyses) {
        log.debug("Bulk saving {} analyses", analyses.size());
        analyses.forEach(this::validateAnalysis);
        return analysisRepository.saveAllAnalyses(analyses)
                .onItem().invoke(saved -> log.info("Successfully saved {} analyses", saved.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to save analyses batch", e));
    }

    @Operation(summary = "Get analysis by ID",
            description = "Retrieves complete analysis data")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis found"),
            @APIResponse(responseCode = "404", description = "Analysis not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-analysis-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "analysis-cache") // Cache individual analyses by ID
    public Uni<AnalysisDTO> getAnalysisById(
            @Parameter(description = "Analysis ID", example = "analysis-123", required = true)
            @CacheKey String id) {
        log.debug("Fetching analysis by ID: {}", id);
        return analysisRepository.findAnalysisById(id)
                .onItem().ifNotNull().invoke(analysis -> log.debug("Found analysis: {}", analysis.getId()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("Analysis not found with ID: " + id));
    }

    @Operation(summary = "Get user's analyses",
            description = "Lists all analyses belonging to a user")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-user-analyses-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "user-analyses-cache") // Cache user's analyses
    public Uni<List<AnalysisDTO>> getUserAnalyses(
            @Parameter(description = "User ID", example = "user-456", required = true)
            @CacheKey String userId) {
        log.debug("Fetching analyses for user: {}", userId);
        return analysisRepository.findAnalysesByUserId(userId)
                .onItem().invoke(analyses -> log.debug("Found {} analyses for user {}", analyses.size(), userId))
                .onFailure().transform(e -> new RuntimeException("Failed to get analyses for user: " + userId, e));
    }

    @Operation(summary = "Find analyses by lexeme type",
            description = "Searches analyses containing specific lexeme types")
    @APIResponse(responseCode = "200", description = "Search results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("find-analyses-by-lexeme-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "analyses-by-lexeme-cache") // Cache analyses by lexeme type
    public Uni<List<AnalysisDTO>> findAnalysesWithLexemeType(
            @Parameter(description = "Lexeme type", example = "NOUN", required = true)
            @CacheKey String lexemeType) {
        log.debug("Searching analyses with lexeme type: {}", lexemeType);
        return analysisRepository.findAnalysesContainingLexeme(lexemeType)
                .onItem().invoke(analyses -> log.debug("Found {} analyses with lexeme type {}", analyses.size(), lexemeType))
                .onFailure().transform(e -> new RuntimeException("Failed to search analyses by lexeme type: " + lexemeType, e));
    }

    @Operation(summary = "Find long analyses",
            description = "Filters analyses by minimum text length")
    @APIResponse(responseCode = "200", description = "Filtered results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("find-long-analyses-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "long-analyses-cache") // Cache long analyses by length threshold
    public Uni<List<AnalysisDTO>> findLongAnalyses(
            @Parameter(description = "Minimum text length", example = "1000", required = true)
            @CacheKey int minLength) {
        log.debug("Searching analyses with text length > {}", minLength);
        return analysisRepository.findAnalysesWithTextLengthGreaterThan(minLength)
                .onItem().invoke(analyses -> log.debug("Found {} analyses with text length > {}", analyses.size(), minLength))
                .onFailure().transform(e -> new RuntimeException("Failed to search analyses by text length: " + minLength, e));
    }

    @Operation(summary = "Add lexeme to analysis",
            description = "Inserts lexeme at specified position in analysis")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis updated"),
            @APIResponse(responseCode = "404", description = "Analysis not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-lexeme-to-analysis-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "analysis-cache") // Invalidate this analysis's cache
    @CacheInvalidate(cacheName = "analyses-by-lexeme-cache") // Invalidate lexeme type cache
    public Uni<AnalysisDTO> addLexemeToAnalysis(
            @Parameter(description = "Analysis ID", example = "analysis-123", required = true)
            String analysisId,
            @Parameter(description = "Position index", example = "0", required = true)
            Integer position,
            @Parameter(description = "Lexeme to add", required = true)
            LexemeDTO lexeme) {
        log.debug("Adding lexeme to analysis {} at position {}", analysisId, position);
        return getAnalysisById(analysisId)
                .onItem().transformToUni(analysis -> {
                    analysis.addLexeme(position, lexeme);
                    return saveAnalysis(analysis);
                });
    }

    private void validateAnalysis(AnalysisDTO analysis) {
        if (analysis.getUserId().isEmpty()) {
            throw new IllegalArgumentException("Analysis must have at least one user ID");
        }
        if (analysis.getText().isEmpty()) {
            throw new IllegalArgumentException("Analysis text cannot be empty");
        }
    }

    @Operation(summary = "Create new analysis",
            description = "Validates and creates analysis record")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis created"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("create-analysis-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-analyses-cache") // Invalidate user's analyses cache
    public Uni<AnalysisDTO> createAnalysis(AnalysisDTO analysis) {
        log.info("Creating new analysis with ID: {}", analysis.getId());
        validateAnalysis(analysis);
        return saveAnalysis(analysis);
    }

    @Operation(summary = "Remove the analysis",
            description = "Idempotent operation to remove an analysis")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis deleted"),
            @APIResponse(responseCode = "404", description = "Analysis not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-analysis-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "analysis-cache") // Invalidate this analysis' cache
    public Uni<Boolean> removeAnalysis(String analysisId) {
        log.debug("Removing analysis {}", analysisId);
        return analysisRepository.deleteAnalysisById(analysisId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.info("Successfully deleted analysis: {}", analysisId);
                    } else {
                        log.warn("Analysis not found for deletion: {}", analysisId);
                    }
                });
    }
}