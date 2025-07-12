package by.losik.repository;

import by.losik.entity.AlbumDTO;
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
@Tag(name = "Album Repository", description = "Operations for managing music albums in Elasticsearch")
public class AlbumRepository extends ReactiveElasticsearchRepository<AlbumDTO> {

    @Override
    protected String getIndexName() {
        return "albums";
    }

    @Override
    protected String getId(AlbumDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<AlbumDTO> getEntityType() {
        return AlbumDTO.class;
    }

    @Operation(
            summary = "Save a single album",
            description = "Creates or updates an album in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Album successfully saved",
                    content = @Content(schema = @Schema(implementation = AlbumDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed after retries"
            )
    })
    public Uni<AlbumDTO> saveAlbum(AlbumDTO album) {
        return index(album)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to save album {}", album.getId(), e));
    }

    @Operation(
            summary = "Save multiple albums",
            description = "Creates or updates multiple albums in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Albums successfully saved",
                    content = @Content(schema = @Schema(implementation = AlbumDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk save operation failed after retries"
            )
    })
    public Uni<List<AlbumDTO>> saveAllAlbums(List<AlbumDTO> albums) {
        return bulkIndex(albums)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk save failed for {} albums", albums.size(), e));
    }

    @Operation(
            summary = "Find album by ID",
            description = "Retrieves a single album from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Album found and returned",
                    content = @Content(schema = @Schema(implementation = AlbumDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Album not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<AlbumDTO> findAlbumById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the album",
                    required = true,
                    example = "album-123"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch album {}", id, e));
    }

    @Operation(
            summary = "Find albums by name",
            description = "Searches for albums matching the given name using Elasticsearch's match query"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = AlbumDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<AlbumDTO>> findAlbumsByName(
            @Parameter(
                    name = "name",
                    description = "Name or partial name of the album to search for",
                    required = true,
                    example = "Thriller"
            ) String name) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match", new JsonObject()
                                .put("name", name)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by name failed for {}", name, e));
    }

    @Operation(
            summary = "Find albums by song ID",
            description = "Searches for albums containing a specific song using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = AlbumDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<AlbumDTO>> findAlbumsBySongId(
            @Parameter(
                    name = "songId",
                    description = "Exact song ID to search for in albums",
                    required = true,
                    example = "song-456"
            ) String songId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("songListId.keyword", songId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by songId failed for {}", songId, e));
    }

    @Operation(
            summary = "Find albums by type",
            description = "Filters albums by their type (album vs single/EP)"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = AlbumDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<AlbumDTO>> findAlbumsByType(
            @Parameter(
                    name = "isAlbum",
                    description = "Flag to filter by album type (true for full albums, false for singles/EPs)",
                    required = true,
                    example = "true"
            ) boolean isAlbum) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("isAlbum", isAlbum)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by type failed for isAlbum={}", isAlbum, e));
    }

    @Operation(
            summary = "Delete albums by id",
            description = "Filters albums by their id"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Delete completed successfully",
                    content = @Content(schema = @Schema(implementation = AlbumDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<Boolean> deleteAlbum(
            @Parameter(
                    name = "albumId",
                    description = "Identifier to acknowledge the album",
                    required = true,
                    example = "album-s9an5523wq47f76qfwq"
            ) String albumId) {

        return deleteById(albumId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted album: {}", albumId);
                    } else {
                        log.debug("album not found for deletion: {}", albumId);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete accord {} after retries: {}", albumId, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for accord: {}", albumId);
                    return false;
                });
    }
}