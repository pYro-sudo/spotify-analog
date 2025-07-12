package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.AccordDTO;
import by.losik.repository.AccordRepository;
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
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;

@ApplicationScoped
@Slf4j
@Tag(name = "Accord Service", description = "Business operations for musical accords")
public class AccordService extends ServiceConfig {
    @Inject
    AccordRepository accordRepository;

    @Operation(summary = "Index a single accord",
            description = "Stores or updates an accord in the search index")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord successfully indexed"),
            @APIResponse(responseCode = "400", description = "Invalid accord data")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("index-accord-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "accord-cache")
    @CacheInvalidateAll(cacheName = "accord-search-cache")
    public Uni<AccordDTO> indexAccord(AccordDTO accord) {
        log.debug("Indexing accord: {}", accord.getName());
        return accordRepository.indexAccord(accord)
                .onItem().invoke(item -> log.info("Successfully indexed accord: {}", item.getName()));
    }

    @Operation(summary = "Bulk index multiple accords",
            description = "Efficiently indexes multiple accords in one operation")
    @APIResponses(value = {
            @APIResponse(responseCode = "200", description = "Accords successfully indexed"),
            @APIResponse(responseCode = "400", description = "Invalid accord data in batch")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-index-accord-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidateAll(cacheName = "accord-cache")
    @CacheInvalidateAll(cacheName = "accord-search-cache")
    public Uni<List<AccordDTO>> bulkIndexAccords(List<AccordDTO> accords) {
        log.debug("Bulk indexing {} accords", accords.size());
        return accordRepository.bulkIndexAccords(accords)
                .onItem().invoke(items -> log.info("Successfully indexed {} accords", items.size()));
    }

    @Operation(summary = "Get accord by name",
            description = "Retrieves a specific accord by its exact name")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord found and returned"),
            @APIResponse(responseCode = "404", description = "Accord not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-accord-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "accord-cache")
    public Uni<AccordDTO> getAccordByName(String name) {
        log.debug("Fetching accord: {}", name);
        return accordRepository.getAccord(name)
                .onItem().ifNotNull().invoke(accord -> log.debug("Found accord: {}", accord.getName()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("Accord not found: " + name));
    }

    @Operation(summary = "Search accords",
            description = "Finds accords matching the search term in their name")
    @APIResponse(responseCode = "200", description = "Search completed, may return empty list")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-accord-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "accord-search-cache")
    public Uni<List<AccordDTO>> searchAccords(String term) {
        log.debug("Searching for accords with term: {}", term);
        return accordRepository.searchAccords(term)
                .onItem().invoke(results -> log.debug("Found {} accords matching '{}'", results.size(), term));
    }

    @Operation(summary = "Create new accord",
            description = "Simple wrapper around indexing with potential for future validation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord created successfully"),
            @APIResponse(responseCode = "400", description = "Invalid accord data")
    })
    @CacheInvalidate(cacheName = "accord-cache")
    @CacheInvalidateAll(cacheName = "accord-search-cache")
    public Uni<AccordDTO> createAccord(AccordDTO accord) {
        log.info("Creating new accord: {}", accord.getName());
        return indexAccord(accord);
    }

    @Operation(summary = "Delete accord by name",
            description = "Removes an accord from the search index")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Accord successfully deleted"),
            @APIResponse(responseCode = "404", description = "Accord not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("delete-accord-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "accord-cache")
    @CacheInvalidateAll(cacheName = "accord-search-cache")
    public Uni<Boolean> deleteAccord(String name) {
        log.debug("Deleting accord: {}", name);
        return accordRepository.deleteAccord(name)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.info("Successfully deleted accord: {}", name);
                    } else {
                        log.warn("Accord not found for deletion: {}", name);
                    }
                });
    }
}