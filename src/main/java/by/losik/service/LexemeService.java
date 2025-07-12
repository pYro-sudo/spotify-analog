package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.LexemeDTO;
import by.losik.repository.LexemeRepository;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheResult;
import io.smallrye.faulttolerance.api.CircuitBreakerName;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
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
@Tag(name = "Lexeme Service", description = "Operations for linguistic lexemes management")
public class LexemeService extends ServiceConfig {
    @Inject
    LexemeRepository lexemeRepository;

    @Operation(summary = "Save lexeme",
            description = "Stores lexeme with validation (requires ID, substring and accord)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme saved successfully"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("save-lexeme-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "lexeme-cache")
    @CacheInvalidate(cacheName = "lexemes-by-accord-cache")
    public Uni<LexemeDTO> saveLexeme(LexemeDTO lexeme) {
        validateLexeme(lexeme);
        log.debug("Saving lexeme with ID: {}", lexeme.getId());
        return lexemeRepository.saveLexeme(lexeme)
                .onItem().invoke(saved -> log.info("Successfully saved lexeme: {}", saved.getId()))
                .onFailure().transform(e -> new RuntimeException("Failed to save lexeme: " + lexeme.getId(), e));
    }

    @Operation(summary = "Bulk save lexemes",
            description = "Saves multiple lexemes with batch validation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "All lexemes saved"),
            @APIResponse(responseCode = "400", description = "Batch validation failed")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-save-lexemes-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "lexeme-cache")
    @CacheInvalidate(cacheName = "lexemes-by-accord-cache")
    public Uni<List<LexemeDTO>> saveAllLexemes(List<LexemeDTO> lexemes) {
        lexemes.forEach(this::validateLexeme);
        log.debug("Bulk saving {} lexemes", lexemes.size());
        return lexemeRepository.saveAllLexemes(lexemes)
                .onItem().invoke(saved -> log.info("Successfully saved {} lexemes", saved.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to save lexemes batch", e));
    }

    @Operation(summary = "Get lexeme by ID",
            description = "Retrieves complete lexeme data")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme found"),
            @APIResponse(responseCode = "404", description = "Lexeme not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-lexeme-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "lexeme-cache")
    public Uni<LexemeDTO> getLexemeById(
            @Parameter(description = "Lexeme ID", example = "lex-123", required = true)
            String id) {
        log.debug("Fetching lexeme by ID: {}", id);
        return lexemeRepository.findLexemeById(id)
                .onItem().ifNotNull().invoke(lexeme -> log.debug("Found lexeme: {}", lexeme.getId()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("Lexeme not found with ID: " + id));
    }

    @Operation(summary = "Find lexemes by exact substring",
            description = "Searches lexemes with exact substring match")
    @APIResponse(responseCode = "200", description = "Search results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("find-lexemes-by-substring-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "lexemes-by-substring-cache")
    public Uni<List<LexemeDTO>> findLexemesByExactSubstring(
            @Parameter(description = "Exact substring", example = "hello", required = true)
            String substring) {
        log.debug("Searching lexemes by exact substring: {}", substring);
        return lexemeRepository.findLexemesBySubstring(substring)
                .onItem().invoke(lexemes -> log.debug("Found {} lexemes with exact substring '{}'", lexemes.size(), substring))
                .onFailure().transform(e -> new RuntimeException("Failed to search lexemes by substring: " + substring, e));
    }

    @Operation(summary = "Find lexemes by accord",
            description = "Lists lexemes associated with specific accord")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("find-lexemes-by-accord-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "lexemes-by-accord-cache")
    public Uni<List<LexemeDTO>> findLexemesByAccord(
            @Parameter(description = "Accord ID", example = "accord-456", required = true)
            String accordId) {
        log.debug("Finding lexemes for accord: {}", accordId);
        return lexemeRepository.findLexemesByAccord(accordId)
                .onItem().invoke(lexemes -> log.debug("Found {} lexemes for accord {}", lexemes.size(), accordId))
                .onFailure().transform(e -> new RuntimeException("Failed to find lexemes by accord: " + accordId, e));
    }

    @Operation(summary = "Find lexemes by prefix",
            description = "Searches lexemes starting with given substring")
    @APIResponse(responseCode = "200", description = "Search results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("find-lexemes-by-prefix-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "lexemes-by-prefix-cache")
    public Uni<List<LexemeDTO>> findLexemesBySubstringPrefix(
            @Parameter(description = "Substring prefix", example = "hel", required = true)
            String prefix) {
        log.debug("Searching lexemes by prefix: {}", prefix);
        return lexemeRepository.findLexemesBySubstringPrefix(prefix)
                .onItem().invoke(lexemes -> log.debug("Found {} lexemes with prefix '{}'", lexemes.size(), prefix))
                .onFailure().transform(e -> new RuntimeException("Failed to search lexemes by prefix: " + prefix, e));
    }

    @Operation(summary = "Create new lexeme",
            description = "Validates and creates a new lexeme")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme created"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("create-lexeme-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "lexemes-by-accord-cache")
    public Uni<LexemeDTO> createLexeme(LexemeDTO lexeme) {
        log.info("Creating new lexeme with ID: {}", lexeme.getId());
        validateLexeme(lexeme);
        return saveLexeme(lexeme);
    }

    @Operation(summary = "Update lexeme substring",
            description = "Modifies the substring of an existing lexeme")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme updated"),
            @APIResponse(responseCode = "404", description = "Lexeme not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("update-lexeme-substring-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "lexeme-cache")
    @CacheInvalidate(cacheName = "lexemes-by-substring-cache")
    @CacheInvalidate(cacheName = "lexemes-by-prefix-cache")
    public Uni<LexemeDTO> updateLexemeSubstring(
            @Parameter(description = "Lexeme ID", example = "lex-123", required = true)
            String lexemeId,
            @Parameter(description = "New substring value", example = "world", required = true)
            String newSubstring) {
        return getLexemeById(lexemeId)
                .onItem().transformToUni(lexeme -> {
                    lexeme.setSubstring(newSubstring);
                    return saveLexeme(lexeme);
                });
    }

    @Operation(summary = "Update lexeme accord",
            description = "Changes the accord reference of a lexeme")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Lexeme updated"),
            @APIResponse(responseCode = "404", description = "Lexeme not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("update-lexeme-accord-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "lexeme-cache")
    @CacheInvalidate(cacheName = "lexemes-by-accord-cache")
    public Uni<LexemeDTO> updateLexemeAccord(
            @Parameter(description = "Lexeme ID", example = "lex-123", required = true)
            String lexemeId,
            @Parameter(description = "New accord ID", example = "C#", required = true)
            String newAccordId) {
        return getLexemeById(lexemeId)
                .onItem().transformToUni(lexeme -> {
                    lexeme.setAccordId(newAccordId);
                    return saveLexeme(lexeme);
                });
    }

    private void validateLexeme(LexemeDTO lexeme) {
        if (lexeme.getId().isBlank()) {
            throw new IllegalArgumentException("Lexeme ID cannot be empty");
        }
        if (lexeme.getSubstring().isBlank()) {
            throw new IllegalArgumentException("Lexeme substring cannot be empty");
        }
        if (lexeme.getAccordId().isBlank()) {
            throw new IllegalArgumentException("Lexeme accord reference cannot be empty");
        }
    }

    @Operation(summary = "Search lexemes containing text",
            description = "Finds lexemes containing text fragment using wildcard search")
    @APIResponse(responseCode = "200", description = "Search results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-lexemes-containing-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "lexemes-containing-cache")
    public Uni<List<LexemeDTO>> searchLexemesContaining(
            @Parameter(description = "Text fragment", example = "ello", required = true)
            String textFragment) {
        log.debug("Searching lexemes containing: {}", textFragment);
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("wildcard", new JsonObject()
                                .put("substring", "*" + textFragment + "*")));

        return lexemeRepository.search(query)
                .onItem().invoke(lexemes -> log.debug("Found {} lexemes containing '{}'", lexemes.size(), textFragment))
                .onFailure().transform(e -> new RuntimeException("Failed to search lexemes containing: " + textFragment, e));
    }

    @Operation(summary = "Remove the lexeme",
            description = "Idempotent operation to remove a lexeme")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song deleted"),
            @APIResponse(responseCode = "404", description = "SOng not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-lexeme-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "lexeme-cache") // Invalidate this song's cache
    public Uni<Boolean> removeLexeme(String lexemeId) {
        log.debug("Removing analysis {}", lexemeId);
        return lexemeRepository.deleteLexemeById(lexemeId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.info("Successfully deleted lexeme: {}", lexemeId);
                    } else {
                        log.warn("Analysis not found for lexeme: {}", lexemeId);
                    }
                });
    }
}