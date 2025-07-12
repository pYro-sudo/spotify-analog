package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.SongDTO;
import by.losik.repository.SongRepository;
import io.quarkus.cache.CacheInvalidate;
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
@Tag(name = "Song Service", description = "Operations for managing songs and their metadata")
public class SongService extends ServiceConfig {

    @Inject
    SongRepository songRepository;

    @Operation(summary = "Save song",
            description = "Stores song with validation (requires name, content, and contributors)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song saved successfully"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("save-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache")
    @CacheInvalidate(cacheName = "songs-by-user-cache")
    @CacheInvalidate(cacheName = "songs-with-analysis-cache")
    public Uni<SongDTO> saveSong(SongDTO song) {
        validateSong(song);
        log.debug("Saving song with ID: {}", song.getId());
        return songRepository.saveSong(song)
                .onItem().invoke(saved -> log.info("Successfully saved song: {}", saved.getId()))
                .onFailure().transform(e -> new RuntimeException("Failed to save song: " + song.getId(), e));
    }

    @Operation(summary = "Bulk save songs",
            description = "Saves multiple songs with batch validation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "All songs saved"),
            @APIResponse(responseCode = "400", description = "Batch validation failed")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-save-songs-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache")
    @CacheInvalidate(cacheName = "songs-by-user-cache")
    @CacheInvalidate(cacheName = "songs-with-analysis-cache")
    public Uni<List<SongDTO>> saveAllSongs(List<SongDTO> songs) {
        songs.forEach(this::validateSong);
        log.debug("Bulk saving {} songs", songs.size());
        return songRepository.saveAllSongs(songs)
                .onItem().invoke(saved -> log.info("Successfully saved {} songs", saved.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to save songs batch", e));
    }

    @Operation(summary = "Get song by ID",
            description = "Retrieves complete song metadata")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song found"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-song-by-id-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "song-cache")
    public Uni<SongDTO> getSongById(
            @Parameter(description = "Song ID", example = "song-123", required = true)
            String id) {
        log.debug("Fetching song by ID: {}", id);
        return songRepository.findSongById(id)
                .onItem().ifNotNull().invoke(song -> log.debug("Found song: {}", song.getId()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("Song not found with ID: " + id));
    }

    @Operation(summary = "Get user's songs",
            description = "Lists songs where user is a contributor")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-songs-by-user-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "songs-by-user-cache")
    public Uni<List<SongDTO>> getSongsByUser(
            @Parameter(description = "User ID", example = "user-456", required = true)
            String userId) {
        log.debug("Fetching songs for user: {}", userId);
        return songRepository.findSongsByUser(userId)
                .onItem().invoke(songs -> log.debug("Found {} songs for user {}", songs.size(), userId))
                .onFailure().transform(e -> new RuntimeException("Failed to get songs for user: " + userId, e));
    }

    @Operation(summary = "Search songs by name",
            description = "Finds songs using prefix matching on names")
    @APIResponse(responseCode = "200", description = "Search results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-songs-by-name-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "songs-by-name-cache")
    public Uni<List<SongDTO>> searchSongsByName(
            @Parameter(description = "Name or partial name", example = "Bohemian", required = true)
            String name) {
        log.debug("Searching songs by name: {}", name);
        return songRepository.findSongsByName(name)
                .onItem().invoke(songs -> log.debug("Found {} songs matching name '{}'", songs.size(), name))
                .onFailure().transform(e -> new RuntimeException("Failed to search songs by name: " + name, e));
    }

    @Operation(summary = "Get songs with analysis",
            description = "Lists songs containing specific analysis")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-songs-with-analysis-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "songs-with-analysis-cache")
    public Uni<List<SongDTO>> getSongsWithAnalysis(
            @Parameter(description = "Analysis ID", example = "analysis-789", required = true)
            String analysisId) {
        log.debug("Finding songs with analysis: {}", analysisId);
        return songRepository.findSongsWithAnalysis(analysisId)
                .onItem().invoke(songs -> log.debug("Found {} songs with analysis {}", songs.size(), analysisId))
                .onFailure().transform(e -> new RuntimeException("Failed to find songs by analysis: " + analysisId, e));
    }

    @Operation(summary = "Get songs by contributors",
            description = "Finds songs with any of the specified contributors")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-songs-by-contributors-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "songs-by-contributors-cache")
    public Uni<List<SongDTO>> getSongsByContributors(
            @Parameter(description = "List of contributor IDs", example = "[\"user-1\", \"user-2\"]", required = true)
            List<String> userIds) {
        log.debug("Finding songs by {} contributors", userIds.size());
        return songRepository.findSongsByContributors(userIds)
                .onItem().invoke(songs -> log.debug("Found {} songs by contributors", songs.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to find songs by contributors", e));
    }

    @Operation(summary = "Add contributor to song",
            description = "Idempotent operation to add contributor")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song updated"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-contributor-to-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache")
    @CacheInvalidate(cacheName = "songs-by-user-cache")
    @CacheInvalidate(cacheName = "songs-by-contributors-cache")
    public Uni<SongDTO> addContributorToSong(
            @Parameter(description = "Song ID", example = "song-123", required = true)
            String songId,
            @Parameter(description = "User ID to add", example = "user-456", required = true)
            String userId) {
        log.debug("Adding contributor {} to song {}", userId, songId);
        return getSongById(songId)
                .onItem().transformToUni(song -> {
                    if (song.getUserListId().contains(userId)) {
                        log.warn("User {} already contributes to song {}", userId, songId);
                        return Uni.createFrom().item(song);
                    }
                    song.getUserListId().add(userId);
                    return saveSong(song);
                });
    }

    @Operation(summary = "Remove contributor from song",
            description = "Idempotent operation to remove contributor")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song updated"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-contributor-from-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache")
    @CacheInvalidate(cacheName = "songs-by-user-cache")
    @CacheInvalidate(cacheName = "songs-by-contributors-cache")
    public Uni<SongDTO> removeContributorFromSong(
            @Parameter(description = "Song ID", example = "song-123", required = true)
            String songId,
            @Parameter(description = "User ID to remove", example = "user-456", required = true)
            String userId) {
        log.debug("Removing contributor {} from song {}", userId, songId);
        return getSongById(songId)
                .onItem().transformToUni(song -> {
                    if (!song.getUserListId().contains(userId)) {
                        log.warn("User {} not found in song {}", userId, songId);
                        return Uni.createFrom().item(song);
                    }
                    song.getUserListId().remove(userId);
                    return saveSong(song);
                });
    }

    @Operation(summary = "Add analysis to song",
            description = "Idempotent operation to add analysis reference")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song updated"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-analysis-to-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache")
    @CacheInvalidate(cacheName = "songs-with-analysis-cache")
    public Uni<SongDTO> addAnalysisToSong(
            @Parameter(description = "Song ID", example = "song-123", required = true)
            String songId,
            @Parameter(description = "Analysis ID to add", example = "analysis-789", required = true)
            String analysisId) {
        log.debug("Adding analysis {} to song {}", analysisId, songId);
        return getSongById(songId)
                .onItem().transformToUni(song -> {
                    if (song.getAnalysisListId().contains(analysisId)) {
                        log.warn("Analysis {} already exists in song {}", analysisId, songId);
                        return Uni.createFrom().item(song);
                    }
                    song.getAnalysisListId().add(analysisId);
                    return saveSong(song);
                });
    }

    @Operation(summary = "Remove analysis from song",
            description = "Idempotent operation to remove analysis reference")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song updated"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-analysis-from-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache")
    @CacheInvalidate(cacheName = "songs-with-analysis-cache")
    public Uni<SongDTO> removeAnalysisFromSong(
            @Parameter(description = "Song ID", example = "song-123", required = true)
            String songId,
            @Parameter(description = "Analysis ID to remove", example = "analysis-789", required = true)
            String analysisId) {
        log.debug("Removing analysis {} from song {}", analysisId, songId);
        return getSongById(songId)
                .onItem().transformToUni(song -> {
                    if (!song.getAnalysisListId().contains(analysisId)) {
                        log.warn("Analysis {} not found in song {}", analysisId, songId);
                        return Uni.createFrom().item(song);
                    }
                    song.getAnalysisListId().remove(analysisId);
                    return saveSong(song);
                });
    }

    private void validateSong(SongDTO song) {
        if (song.getName().isBlank()) {
            throw new IllegalArgumentException("Song name cannot be empty");
        }
        if (song.getUserListId().isEmpty()) {
            throw new IllegalArgumentException("Song must have at least one contributor");
        }
        if (song.getSongContent() == null || song.getSongContent().length == 0) {
            throw new IllegalArgumentException("Song content cannot be empty");
        }
    }

    @Operation(summary = "Create new song",
            description = "Validates and creates a new song record")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song created"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("create-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "songs-by-user-cache")
    public Uni<SongDTO> createSong(SongDTO song) {
        log.info("Creating new song with ID: {}", song.getId());
        validateSong(song);
        return saveSong(song);
    }

    @Operation(summary = "Get song content",
            description = "Retrieves binary song content (audio data)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song content returned"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-song-content-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "song-content-cache")
    public Uni<byte[]> getSongContent(
            @Parameter(description = "Song ID", example = "song-123", required = true)
            String songId) {
        return getSongById(songId)
                .onItem().transform(SongDTO::getSongContent);
    }

    @Operation(summary = "Update song content",
            description = "Replaces the binary content of a song")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Content updated"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("update-song-content-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache")
    @CacheInvalidate(cacheName = "song-content-cache")
    public Uni<SongDTO> updateSongContent(
            @Parameter(description = "Song ID", example = "song-123", required = true)
            String songId,
            @Parameter(description = "New binary content", required = true)
            byte[] content) {
        return getSongById(songId)
                .onItem().transformToUni(song -> {
                    song.setSongContent(content);
                    return saveSong(song);
                });
    }

    @Operation(summary = "Remove the song",
            description = "Idempotent operation to remove a song")
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
    @CircuitBreakerName("remove-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "song-cache") // Invalidate this song's cache
    public Uni<Boolean> removeSong(String songId) {
        log.debug("Removing analysis {}", songId);
        return songRepository.deleteSongById(songId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.info("Successfully deleted analysis: {}", songId);
                    } else {
                        log.warn("Analysis not found for deletion: {}", songId);
                    }
                });
    }
}