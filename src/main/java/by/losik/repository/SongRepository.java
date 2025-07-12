package by.losik.repository;

import by.losik.entity.SongDTO;
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
@Tag(name = "Song Repository", description = "Operations for managing songs in Elasticsearch")
public class SongRepository extends ReactiveElasticsearchRepository<SongDTO> {

    @Override
    protected String getIndexName() {
        return "songs";
    }

    @Override
    protected String getId(SongDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<SongDTO> getEntityType() {
        return SongDTO.class;
    }

    @Operation(
            summary = "Save a single song",
            description = "Creates or updates a song in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Song successfully saved",
                    content = @Content(schema = @Schema(implementation = SongDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed after retries"
            )
    })
    public Uni<SongDTO> saveSong(SongDTO song) {
        return index(song)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to save song {}", song.getId(), e));
    }

    @Operation(
            summary = "Save multiple songs",
            description = "Creates or updates multiple songs in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Songs successfully saved",
                    content = @Content(schema = @Schema(implementation = SongDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk save operation failed after retries"
            )
    })
    public Uni<List<SongDTO>> saveAllSongs(List<SongDTO> songs) {
        return bulkIndex(songs)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk save failed for {} songs", songs.size(), e));
    }

    @Operation(
            summary = "Find song by ID",
            description = "Retrieves a single song from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Song found and returned",
                    content = @Content(schema = @Schema(implementation = SongDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Song not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<SongDTO> findSongById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the song",
                    required = true,
                    example = "song-123"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch song {}", id, e));
    }

    @Operation(
            summary = "Find songs by user",
            description = "Searches for songs associated with a specific user using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = SongDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<SongDTO>> findSongsByUser(
            @Parameter(
                    name = "userId",
                    description = "Exact user ID to filter songs",
                    required = true,
                    example = "user-456"
            ) String userId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("userListId.keyword", userId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by user failed for {}", userId, e));
    }

    @Operation(
            summary = "Find songs by name",
            description = "Searches for songs using prefix phrase matching on song names"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = SongDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<SongDTO>> findSongsByName(
            @Parameter(
                    name = "name",
                    description = "Name or prefix of song name to search for",
                    required = true,
                    example = "Bohemian"
            ) String name) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match_phrase_prefix", new JsonObject()
                                .put("name", name)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by name failed for '{}'", name, e));
    }

    @Operation(
            summary = "Find songs with analysis",
            description = "Searches for songs containing a specific analysis using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = SongDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<SongDTO>> findSongsWithAnalysis(
            @Parameter(
                    name = "analysisId",
                    description = "Exact analysis ID to search for in songs",
                    required = true,
                    example = "analysis-789"
            ) String analysisId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("analysisListId.keyword", analysisId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by analysis failed for {}", analysisId, e));
    }

    @Operation(
            summary = "Find songs by contributors",
            description = "Searches for songs that have any of the specified users as contributors"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = SongDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<SongDTO>> findSongsByContributors(
            @Parameter(
                    name = "userIds",
                    description = "List of user IDs to search for as contributors",
                    required = true,
                    example = "[\"user-123\", \"user-456\"]"
            ) List<String> userIds) {
        JsonObject terms = new JsonObject()
                .put("userListId.keyword", new JsonObject()
                        .put("value", userIds));

        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("terms", terms));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by contributors failed for {} users", userIds.size(), e));
    }

    @Operation(
            summary = "Delete songs that have the given id",
            description = "Delete songs by id"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Delete completed successfully",
                    content = @Content(schema = @Schema(implementation = SongDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or delete failed"
            )
    })
    public Uni<Boolean> deleteSongById(
            @Parameter(
                    name = "songId",
                    description = "Id of the song",
                    required = true,
                    example = "song-aj23jfq35jdt38q"
            ) String songId) {
        return deleteById(songId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted song: {}", songId);
                    } else {
                        log.debug("Song not found for deletion: {}", songId);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete song {} after retries: {}", songId, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for song: {}", songId);
                    return false;
                });
    }
}