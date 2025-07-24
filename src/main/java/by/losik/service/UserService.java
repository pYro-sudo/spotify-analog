package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.UserDTO;
import by.losik.repository.UserRepository;
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
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheInvalidateAll;

@ApplicationScoped
@Slf4j
@Tag(name = "User Service", description = "Operations for user management and music collections")
public class UserService extends ServiceConfig {
    @Inject
    UserRepository userRepository;

    @Operation(summary = "Save user",
            description = "Stores user with validation (requires non-empty ID)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User saved successfully"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("save-user-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    @CacheInvalidate(cacheName = "user-songs-cache")
    @CacheInvalidate(cacheName = "user-albums-cache")
    public Uni<UserDTO> saveUser(UserDTO user) {
        validateUser(user);
        log.debug("Saving user with ID: {}", user.getId());
        return userRepository.saveUser(user)
                .onItem().invoke(saved -> log.info("Successfully saved user: {}", saved.getId()))
                .onFailure().transform(e -> new RuntimeException("Failed to save user: " + user.getId(), e));
    }

    @Operation(summary = "Bulk save users",
            description = "Saves multiple users with batch validation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "All users saved"),
            @APIResponse(responseCode = "400", description = "Batch validation failed")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-save-users-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidateAll(cacheName = "user-cache")
    @CacheInvalidateAll(cacheName = "user-songs-cache")
    @CacheInvalidateAll(cacheName = "user-albums-cache")
    public Uni<List<UserDTO>> saveAllUsers(List<UserDTO> users) {
        users.forEach(this::validateUser);
        log.debug("Bulk saving {} users", users.size());
        return userRepository.saveAllUsers(users)
                .onItem().invoke(saved -> log.info("Successfully saved {} users", saved.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to save users batch", e));
    }

    @Operation(summary = "Get user by ID",
            description = "Retrieves complete user profile including collections")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User found"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-user-by-id-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "user-cache")
    public Uni<UserDTO> getUserById(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String id) {
        log.debug("Fetching user by ID: {}", id);
        return userRepository.findUserById(id)
                .onItem().ifNotNull().invoke(user -> log.debug("Found user: {}", user.getId()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("User not found with ID: " + id));
    }

    @Operation(summary = "Get users by song",
            description = "Lists users who have a specific song in their collection")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-users-by-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "users-by-song-cache")
    public Uni<List<UserDTO>> getUsersBySong(
            @Parameter(description = "Song ID", example = "song-456", required = true)
            String songId) {
        log.debug("Finding users for song: {}", songId);
        return userRepository.findUsersBySong(songId)
                .onItem().invoke(users -> log.debug("Found {} users for song {}", users.size(), songId))
                .onFailure().transform(e -> new RuntimeException("Failed to find users by song: " + songId, e));
    }

    @Operation(summary = "Get users by album",
            description = "Lists users who have a specific album in their collection")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-users-by-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "users-by-album-cache")
    public Uni<List<UserDTO>> getUsersByAlbum(
            @Parameter(description = "Album ID", example = "album-789", required = true)
            String albumId) {
        log.debug("Finding users for album: {}", albumId);
        return userRepository.findUsersByAlbum(albumId)
                .onItem().invoke(users -> log.debug("Found {} users for album {}", users.size(), albumId))
                .onFailure().transform(e -> new RuntimeException("Failed to find users by album: " + albumId, e));
    }

    @Operation(summary = "Add song to user's collection",
            description = "Idempotent operation to add song reference")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User updated"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-user-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    @CacheInvalidate(cacheName = "user-songs-cache")
    public Uni<UserDTO> addUserSong(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Song ID to add", example = "song-456", required = true)
            String songId) {
        log.debug("Adding song {} to user {}", songId, userId);
        return userRepository.addSongToUser(userId, songId)
                .onItem().invoke(user -> log.debug("Successfully added song {} to user {}", songId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to add song to user", e));
    }

    @Operation(summary = "Remove song from user's collection",
            description = "Idempotent operation to remove song reference")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User updated"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-user-song-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    @CacheInvalidate(cacheName = "user-songs-cache")
    public Uni<UserDTO> removeUserSong(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Song ID to remove", example = "song-456", required = true)
            String songId) {
        log.debug("Removing song {} from user {}", songId, userId);
        return userRepository.removeSongFromUser(userId, songId)
                .onItem().invoke(user -> log.debug("Successfully removed song {} from user {}", songId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to remove song from user", e));
    }

    @Operation(summary = "Add album to user's collection",
            description = "Idempotent operation to add album reference")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User updated"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-user-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    @CacheInvalidate(cacheName = "user-albums-cache")
    public Uni<UserDTO> addUserAlbum(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Album ID to add", example = "album-789", required = true)
            String albumId) {
        log.debug("Adding album {} to user {}", albumId, userId);
        return userRepository.addAlbumToUser(userId, albumId)
                .onItem().invoke(user -> log.debug("Successfully added album {} to user {}", albumId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to add album to user", e));
    }

    @Operation(summary = "Remove album from user's collection",
            description = "Idempotent operation to remove album reference")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User updated"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-user-album-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    @CacheInvalidate(cacheName = "user-albums-cache")
    public Uni<UserDTO> removeUserAlbum(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Album ID to remove", example = "album-789", required = true)
            String albumId) {
        log.debug("Removing album {} from user {}", albumId, userId);
        return userRepository.removeAlbumFromUser(userId, albumId)
                .onItem().invoke(user -> log.debug("Successfully removed album {} from user {}", albumId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to remove album from user", e));
    }

    private void validateUser(UserDTO user) {
        if (user.getId().isBlank()) {
            throw new IllegalArgumentException("User ID cannot be empty");
        }
    }

    @Operation(summary = "Create new user",
            description = "Validates and creates a new user profile")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User created"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("create-user-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidateAll(cacheName = "user-cache")
    public Uni<UserDTO> createUser(UserDTO user) {
        log.info("Creating new user with ID: {}", user.getId());
        validateUser(user);
        return saveUser(user);
    }

    @Operation(summary = "Get user's songs",
            description = "Lists song IDs in user's collection")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "List returned"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-user-songs-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "user-songs-cache")
    public Uni<List<String>> getUserSongs(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId) {
        return getUserById(userId)
                .onItem().transform(UserDTO::getSongListId);
    }

    @Operation(summary = "Get user's albums",
            description = "Lists album IDs in user's collection")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "List returned"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-user-albums-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "user-albums-cache")
    public Uni<List<String>> getUserAlbums(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId) {
        return getUserById(userId)
                .onItem().transform(UserDTO::getAlbumListId);
    }

    @Operation(summary = "Check song ownership",
            description = "Verifies if user has specific song in collection")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Ownership status returned"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("check-song-ownership-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<Boolean> userHasSong(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Song ID to check", example = "song-456", required = true)
            String songId) {
        return getUserById(userId)
                .onItem().transform(user -> user.getSongListId().contains(songId));
    }

    @Operation(summary = "Check album ownership",
            description = "Verifies if user has specific album in collection")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Ownership status returned"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("check-album-ownership-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    public Uni<Boolean> userHasAlbum(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Album ID to check", example = "album-789", required = true)
            String albumId) {
        return getUserById(userId)
                .onItem().transform(user -> user.getAlbumListId().contains(albumId));
    }

    @Operation(summary = "Remove the user",
            description = "Idempotent operation to remove a user")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User deleted"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-user-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache") // Invalidate this song's cache
    public Uni<Boolean> removeUser(String userId) {
        log.debug("Removing user {}", userId);
        return userRepository.deleteUserById(userId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.info("Successfully deleted user: {}", userId);
                    } else {
                        log.warn("User not found for deletion: {}", userId);
                    }
                });
    }

    @Operation(summary = "Add subscriber to user",
            description = "Adds a subscriber to a user's subscribers list if not already present")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Subscriber added to user's list"),
            @APIResponse(responseCode = "404", description = "User not found"),
            @APIResponse(responseCode = "400", description = "Bad request or operation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-subscriber-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    public Uni<UserDTO> addSubscriber(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Subscriber ID to add", example = "user-456", required = true)
            String subscriberId) {
        log.debug("Adding subscriber {} to user {}", subscriberId, userId);
        return userRepository.addSubscriberToUser(userId, subscriberId)
                .onItem().invoke(user -> log.debug("Successfully added subscriber {} to user {}", subscriberId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to add subscriber to user", e));
    }

    @Operation(summary = "Remove subscriber from user",
            description = "Removes a subscriber from a user's subscribers list")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Subscriber removed from user's list"),
            @APIResponse(responseCode = "404", description = "User not found"),
            @APIResponse(responseCode = "400", description = "Bad request or operation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-subscriber-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    public Uni<UserDTO> removeSubscriber(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Subscriber ID to remove", example = "user-456", required = true)
            String subscriberId) {
        log.debug("Removing subscriber {} from user {}", subscriberId, userId);
        return userRepository.removeSubscriberFromUser(userId, subscriberId)
                .onItem().invoke(user -> log.debug("Successfully removed subscriber {} from user {}", subscriberId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to remove subscriber from user", e));
    }

    @Operation(summary = "Add message to user",
            description = "Adds a message to a user's messages list")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message added to user's list"),
            @APIResponse(responseCode = "404", description = "User not found"),
            @APIResponse(responseCode = "400", description = "Bad request or operation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-message-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    public Uni<UserDTO> addMessage(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Message ID to add", example = "msg-789", required = true)
            String messageId) {
        log.debug("Adding message {} to user {}", messageId, userId);
        return userRepository.addMessageToUser(userId, messageId)
                .onItem().invoke(user -> log.debug("Successfully added message {} to user {}", messageId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to add message to user", e));
    }

    @Operation(summary = "Remove message from user",
            description = "Removes a message from a user's messages list")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message removed from user's list"),
            @APIResponse(responseCode = "404", description = "User not found"),
            @APIResponse(responseCode = "400", description = "Bad request or operation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-message-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-cache")
    public Uni<UserDTO> removeMessage(
            @Parameter(description = "User ID", example = "user-123", required = true)
            String userId,
            @Parameter(description = "Message ID to remove", example = "msg-789", required = true)
            String messageId) {
        log.debug("Removing message {} from user {}", messageId, userId);
        return userRepository.removeMessageFromUser(userId, messageId)
                .onItem().invoke(user -> log.debug("Successfully removed message {} from user {}", messageId, userId))
                .onFailure().transform(e -> new RuntimeException("Failed to remove message from user", e));
    }

    @Operation(summary = "Find users by subscriber",
            description = "Searches for users who have a specific subscriber in their subscribers list")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-users-by-subscriber-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "users-by-subscriber-cache")
    public Uni<List<UserDTO>> getUsersBySubscriber(
            @Parameter(description = "Subscriber ID", example = "user-456", required = true)
            String subscriberId) {
        log.debug("Finding users with subscriber: {}", subscriberId);
        return userRepository.findUsersBySubscriber(subscriberId)
                .onItem().invoke(users -> log.debug("Found {} users with subscriber {}", users.size(), subscriberId))
                .onFailure().transform(e -> new RuntimeException("Failed to find users by subscriber: " + subscriberId, e));
    }

    @Operation(summary = "Find users by message",
            description = "Searches for users who have a specific message in their messages list")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-users-by-message-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "users-by-message-cache")
    public Uni<List<UserDTO>> getUsersByMessage(
            @Parameter(description = "Message ID", example = "msg-789", required = true)
            String messageId) {
        log.debug("Finding users with message: {}", messageId);
        return userRepository.findUsersByMessage(messageId)
                .onItem().invoke(users -> log.debug("Found {} users with message {}", users.size(), messageId))
                .onFailure().transform(e -> new RuntimeException("Failed to find users by message: " + messageId, e));
    }
}