package by.losik.repository;

import by.losik.entity.GroupDTO;
import by.losik.entity.UserDTO;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Duration;
import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "User Repository", description = "Operations for managing users and their music collections in Elasticsearch")
public class UserRepository extends ReactiveElasticsearchRepository<UserDTO> {

    @Override
    protected String getIndexName() {
        return "users";
    }

    @Override
    protected String getId(UserDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<UserDTO> getEntityType() {
        return UserDTO.class;
    }

    @Operation(
            summary = "Save a single user",
            description = "Creates or updates a user in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "User successfully saved",
                    content = @Content(schema = @Schema(implementation = UserDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed after retries"
            )
    })
    public Uni<UserDTO> saveUser(UserDTO user) {
        return index(user)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to save user {}", user.getId(), e));
    }

    @Operation(
            summary = "Save multiple users",
            description = "Creates or updates multiple users in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Users successfully saved",
                    content = @Content(schema = @Schema(implementation = UserDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk save operation failed after retries"
            )
    })
    public Uni<List<UserDTO>> saveAllUsers(List<UserDTO> users) {
        return bulkIndex(users)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk save failed for {} users", users.size(), e));
    }

    @Operation(
            summary = "Find user by ID",
            description = "Retrieves a single user from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "User found and returned",
                    content = @Content(schema = @Schema(implementation = UserDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "User not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<UserDTO> findUserById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the user",
                    required = true,
                    example = "user-123"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch user {}", id, e));
    }

    @Operation(
            summary = "Find users by song",
            description = "Searches for users who have a specific song in their collection using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = UserDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<UserDTO>> findUsersBySong(
            @Parameter(
                    name = "songId",
                    description = "Exact song ID to search for in user collections",
                    required = true,
                    example = "song-456"
            ) String songId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("songListId.keyword", songId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by song failed for {}", songId, e));
    }

    @Operation(
            summary = "Find users by album",
            description = "Searches for users who have a specific album in their collection using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = UserDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<UserDTO>> findUsersByAlbum(
            @Parameter(
                    name = "albumId",
                    description = "Exact album ID to search for in user collections",
                    required = true,
                    example = "album-789"
            ) String albumId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("albumListId.keyword", albumId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by album failed for {}", albumId, e));
    }

    @Operation(
            summary = "Add song to user's collection",
            description = "Adds a song to a user's collection if not already present"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Song added to user's collection",
                    content = @Content(schema = @Schema(implementation = UserDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "User not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or operation failed"
            )
    })
    public Uni<UserDTO> addSongToUser(
            @Parameter(
                    name = "userId",
                    description = "ID of the user to update",
                    required = true,
                    example = "user-123"
            ) String userId,
            @Parameter(
                    name = "songId",
                    description = "ID of the song to add",
                    required = true,
                    example = "song-456"
            ) String songId) {
        return findUserById(userId)
                .onItem().transformToUni(user -> {
                    if (!user.getSongListId().contains(songId)) {
                        user.getSongListId().add(songId);
                        return saveUser(user);
                    }
                    return Uni.createFrom().item(user);
                });
    }

    @Operation(
            summary = "Remove song from user's collection",
            description = "Removes a song from a user's collection"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Song removed from user's collection",
                    content = @Content(schema = @Schema(implementation = UserDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "User not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or operation failed"
            )
    })
    public Uni<UserDTO> removeSongFromUser(
            @Parameter(
                    name = "userId",
                    description = "ID of the user to update",
                    required = true,
                    example = "user-123"
            ) String userId,
            @Parameter(
                    name = "songId",
                    description = "ID of the song to remove",
                    required = true,
                    example = "song-456"
            ) String songId) {
        return findUserById(userId)
                .onItem().transformToUni(user -> {
                    user.getSongListId().remove(songId);
                    return saveUser(user);
                });
    }

    @Operation(
            summary = "Add album to user's collection",
            description = "Adds an album to a user's collection if not already present"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Album added to user's collection",
                    content = @Content(schema = @Schema(implementation = UserDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "User not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or operation failed"
            )
    })
    public Uni<UserDTO> addAlbumToUser(
            @Parameter(
                    name = "userId",
                    description = "ID of the user to update",
                    required = true,
                    example = "user-123"
            ) String userId,
            @Parameter(
                    name = "albumId",
                    description = "ID of the album to add",
                    required = true,
                    example = "album-789"
            ) String albumId) {
        return findUserById(userId)
                .onItem().transformToUni(user -> {
                    if (!user.getAlbumListId().contains(albumId)) {
                        user.getAlbumListId().add(albumId);
                        return saveUser(user);
                    }
                    return Uni.createFrom().item(user);
                });
    }

    @Operation(
            summary = "Remove album from user's collection",
            description = "Removes an album from a user's collection"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Album removed from user's collection",
                    content = @Content(schema = @Schema(implementation = UserDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "User not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or operation failed"
            )
    })
    public Uni<UserDTO> removeAlbumFromUser(
            @Parameter(
                    name = "userId",
                    description = "ID of the user to update",
                    required = true,
                    example = "user-123"
            ) String userId,
            @Parameter(
                    name = "albumId",
                    description = "ID of the album to remove",
                    required = true,
                    example = "album-789"
            ) String albumId) {
        return findUserById(userId)
                .onItem().transformToUni(user -> {
                    user.getAlbumListId().remove(albumId);
                    return saveUser(user);
                });
    }

    @Operation(
            summary = "Delete users that have the given id",
            description = "Delete users by id"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Delete completed successfully",
                    content = @Content(schema = @Schema(implementation = UserDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or delete failed"
            )
    })
    public Uni<Boolean> deleteUserById(
            @Parameter(
                    name = "userId",
                    description = "Id of the user",
                    required = true,
                    example = "user-aj23jfq35jdt38q"
            ) String userId) {
        return deleteById(userId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted user: {}", userId);
                    } else {
                        log.debug("User not found for deletion: {}", userId);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete user {} after retries: {}", userId, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for user: {}", userId);
                    return false;
                });
    }
}