package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.AlbumDTO;
import by.losik.repository.AlbumRepository;
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
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Album Service", description = "Operations for managing music albums and playlists")
public class AlbumService extends ServiceConfig {

    @Inject
    AlbumRepository albumRepository;

    @Operation(summary = "Save album",
            description = "Persists an album with validation and error handling")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album saved successfully"),
            @APIResponse(responseCode = "400", description = "Invalid album data")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("save-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "album-cache") // Invalidate single album cache
    @CacheInvalidate(cacheName = "albums-by-type-cache") // Invalidate type-based cache
    public Uni<AlbumDTO> saveAlbum(AlbumDTO album) {
        log.debug("Saving album with ID: {}", album.getId());
        return albumRepository.saveAlbum(album)
                .onItem().invoke(savedAlbum -> log.info("Successfully saved album: {}", savedAlbum.getId()))
                .onFailure().transform(e -> new RuntimeException("Failed to save album: " + album.getId(), e));
    }

    @Operation(summary = "Bulk save albums",
            description = "Efficiently saves multiple albums with transaction-like behavior")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "All albums saved successfully"),
            @APIResponse(responseCode = "400", description = "Batch contained invalid data")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-save-albums-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "album-cache") // Clear entire album cache
    @CacheInvalidate(cacheName = "albums-by-type-cache") // Clear type cache
    public Uni<List<AlbumDTO>> saveAllAlbums(List<AlbumDTO> albums) {
        log.debug("Bulk saving {} albums", albums.size());
        return albumRepository.saveAllAlbums(albums)
                .onItem().invoke(savedAlbums -> log.info("Successfully saved {} albums", savedAlbums.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to save albums batch", e));
    }

    @Operation(summary = "Get album by ID",
            description = "Retrieves an album with proper null checking")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album found"),
            @APIResponse(responseCode = "404", description = "Album not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "album-cache")
    public Uni<AlbumDTO> getAlbumById(String id) {
        log.debug("Fetching album by ID: {}", id);
        return albumRepository.findAlbumById(id)
                .onItem().ifNotNull().invoke(album -> log.debug("Found album: {}", album.getId()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("Album not found with ID: " + id));
    }

    @Operation(summary = "Search albums by name",
            description = "Finds albums using prefix matching on names")
    @APIResponse(responseCode = "200", description = "Search completed (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-albums-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "album-search-cache") // Cache search results
    public Uni<List<AlbumDTO>> searchAlbumsByName(String name) {
        log.debug("Searching albums by name: {}", name);
        return albumRepository.findAlbumsByName(name)
                .onItem().invoke(albums -> log.debug("Found {} albums matching name '{}'", albums.size(), name))
                .onFailure().transform(e -> new RuntimeException("Failed to search albums by name: " + name, e));
    }

    @Operation(summary = "Find albums containing song",
            description = "Lists all albums that include a specific song")
    @APIResponse(responseCode = "200", description = "Search completed")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("find-albums-by-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "albums-by-song-cache") // Cache albums by song
    public Uni<List<AlbumDTO>> findAlbumsContainingSong(String songId) {
        log.debug("Finding albums containing song ID: {}", songId);
        return albumRepository.findAlbumsBySongId(songId)
                .onItem().invoke(albums -> log.debug("Found {} albums containing song {}", albums.size(), songId))
                .onFailure().transform(e -> new RuntimeException("Failed to find albums by song ID: " + songId, e));
    }

    @Operation(summary = "Get albums by type",
            description = "Filters albums by type (true for albums, false for playlists)")
    @APIResponse(responseCode = "200", description = "Filtered list returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-albums-by-type-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "albums-by-type-cache") // Cache albums by type
    public Uni<List<AlbumDTO>> getAlbumsByType(boolean isAlbum) {
        String type = isAlbum ? "albums" : "playlists";
        log.debug("Fetching all {} from repository", type);
        return albumRepository.findAlbumsByType(isAlbum)
                .onItem().invoke(albums -> log.debug("Found {} {}", albums.size(), type))
                .onFailure().transform(e -> new RuntimeException("Failed to fetch " + type, e));
    }

    @Operation(summary = "Create new album",
            description = "Validates and creates a new album (requires at least one song)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album created"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("create-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "album-cache") // Invalidate cache for new album
    @CacheInvalidate(cacheName = "albums-by-type-cache") // Invalidate type cache
    public Uni<AlbumDTO> createAlbum(AlbumDTO album) {
        log.info("Creating new album with ID: {}", album.getId());
        if (album.getSongListId().isEmpty()) {
            log.warn("Attempt to create empty album: {}", album.getId());
            return Uni.createFrom().failure(new IllegalArgumentException("Album must contain at least one song"));
        }
        return saveAlbum(album);
    }

    @Operation(summary = "Add song to album",
            description = "Idempotent operation to add a song to an album")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album updated"),
            @APIResponse(responseCode = "404", description = "Album not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-song-to-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "album-cache")
    @CacheInvalidate(cacheName = "albums-by-song-cache")
    public Uni<AlbumDTO> addSongToAlbum(String albumId, String songId) {
        log.debug("Adding song {} to album {}", songId, albumId);
        return getAlbumById(albumId)
                .onItem().transformToUni(album -> {
                    if (album.getSongListId().contains(songId)) {
                        log.warn("Song {} already exists in album {}", songId, albumId);
                        return Uni.createFrom().item(album);
                    }
                    album.getSongListId().add(songId);
                    return saveAlbum(album);
                });
    }

    @Operation(summary = "Remove song from album",
            description = "Idempotent operation to remove a song from an album")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album updated"),
            @APIResponse(responseCode = "404", description = "Album not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-song-from-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "album-cache") // Invalidate this album's cache
    @CacheInvalidate(cacheName = "albums-by-song-cache") // Invalidate song-based cache
    public Uni<AlbumDTO> removeSongFromAlbum(String albumId, String songId) {
        log.debug("Removing song {} from album {}", songId, albumId);
        return getAlbumById(albumId)
                .onItem().transformToUni(album -> {
                    if (!album.getSongListId().contains(songId)) {
                        log.warn("Song {} not found in album {}", songId, albumId);
                        return Uni.createFrom().item(album);
                    }
                    album.getSongListId().remove(songId);
                    return saveAlbum(album);
                });
    }

    @Operation(summary = "Remove the album",
            description = "Idempotent operation to remove an album")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album deleted"),
            @APIResponse(responseCode = "404", description = "Album not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "album-cache") // Invalidate this album's cache
    @CacheInvalidate(cacheName = "albums-by-song-cache") // Invalidate song-based cache
    public Uni<Boolean> removeAlbum(String albumId) {
        log.debug("Removing album {}", albumId);
        return albumRepository.deleteAlbum(albumId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.info("Successfully deleted album: {}", albumId);
                    } else {
                        log.warn("Album not found for deletion: {}", albumId);
                    }
                });
    }
}