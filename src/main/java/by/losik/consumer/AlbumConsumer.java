package by.losik.consumer;

import by.losik.entity.AlbumDTO;
import by.losik.service.AlbumService;
import by.losik.service.AuthServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Album Consumer", description = "Message processing endpoints for album operations")
public class AlbumConsumer extends BaseConsumer {
    @Inject
    AlbumService albumService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "album";
    }

    @Operation(summary = "Process album indexing",
            description = "Handles incoming messages for indexing single album records. " +
                    "Requires 'index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid album data")
    })
    @Incoming("albums-index-in")
    @Outgoing("albums-index-out")
    public Uni<ProcessingResult> processAlbumIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("index", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "index")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "index", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        AlbumDTO album = parseMessage(message.getPayload(), AlbumDTO.class);
                        return albumService.saveAlbum(album)
                                .onFailure().retry()
                                .withBackOff(INITIAL_BACKOFF)
                                .atMost(MAX_RETRIES)
                                .onItem().transform(item -> createSuccessResult(
                                        album.getName(),
                                        "Album successfully indexed"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        album.getName(),
                                        error,
                                        "Indexing failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "index", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "index", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Process bulk album indexing",
            description = "Handles batch processing of multiple album records in a single operation. " +
                    "Requires 'bulk-index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Albums successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid album data in batch")
    })
    @Incoming("albums-bulk-index-in")
    @Outgoing("albums-bulk-index-out")
    public Uni<ProcessingResult> processBulkAlbumIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("bulk-index", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "bulk-index")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "bulk-index", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        List<AlbumDTO> albums = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return albumService.saveAllAlbums(albums)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully indexed " + items.size() + " albums"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "bulk-operation",
                                        error,
                                        "Bulk processing failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "bulk-index", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "bulk-index", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Retrieve album by ID",
            description = "Processes requests to fetch a specific album by its unique identifier. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Album not found")
    })
    @Incoming("albums-get-by-id-in")
    @Outgoing("albums-get-by-id-out")
    public Uni<ProcessingResult> processGetAlbumById(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-id", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-id", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String albumId = message.getPayload();
                        return albumService.getAlbumById(albumId)
                                .onItem().transform(album -> createSuccessResult(
                                        albumId,
                                        "Album retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        albumId,
                                        error,
                                        "Failed to retrieve album"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-id", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-id", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search albums by name",
            description = "Processes search requests for albums matching the provided name. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "name", description = "Album name or partial name to search for", required = true)
    @Incoming("albums-get-by-name-in")
    @Outgoing("albums-get-by-name-out")
    public Uni<ProcessingResult> processGetAlbumByName(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search-by-name", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search-by-name", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String albumName = message.getPayload();
                        return albumService.searchAlbumsByName(albumName)
                                .onItem().transform(album -> createSuccessResult(
                                        albumName,
                                        "Albums retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        albumName,
                                        error,
                                        "Failed to retrieve albums"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search-by-name", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "search-by-name", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Find albums containing song",
            description = "Processes requests to find all albums that contain a specific song. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "songId", description = "ID of the song to search for", required = true)
    @Incoming("albums-get-by-song-in")
    @Outgoing("albums-get-by-song-out")
    public Uni<ProcessingResult> processGetAlbumsThatContainSong(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("find-by-song", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "find-by-song", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String songId = message.getPayload();
                        return albumService.findAlbumsContainingSong(songId)
                                .onItem().transform(albums -> createSuccessResult(
                                        songId,
                                        "Found " + albums.size() + " albums containing song"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to find albums"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "find-by-song", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "find-by-song", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get albums by type",
            description = "Processes requests to filter albums by type (album/single). " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Filtering completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "isAlbum", description = "True for albums, false for singles", required = true)
    @Incoming("albums-get-by-type-in")
    @Outgoing("albums-get-by-type-out")
    public Uni<ProcessingResult> processGetAlbumsByType(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("filter-by-type", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "filter-by-type", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        boolean isAlbum = Boolean.parseBoolean(message.getPayload());
                        return albumService.getAlbumsByType(isAlbum)
                                .onItem().transform(albums -> createSuccessResult(
                                        String.valueOf(isAlbum),
                                        "Found " + albums.size() + " " + (isAlbum ? "albums" : "singles")
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        String.valueOf(isAlbum),
                                        error,
                                        "Failed to filter albums"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "filter-by-type", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process filter request"
                        );
                        recordMetrics(timer, "filter-by-type", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Create new album",
            description = "Handles album creation requests with validation. " +
                    "Requires 'create' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album successfully created"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid album data")
    })
    @Incoming("album-create-in")
    @Outgoing("album-create-out")
    public Uni<ProcessingResult> processCreateAlbum(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("create", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "create")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "create", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        AlbumDTO album = parseMessage(message.getPayload(), AlbumDTO.class);
                        return albumService.createAlbum(album)
                                .onItem().transform(item -> createSuccessResult(
                                        album.getName(),
                                        "Album created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        album.getName(),
                                        error,
                                        "Failed to create album"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "create", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (JsonProcessingException e) {
                        ProcessingResult result = handleParsingError(message, e);
                        recordMetrics(timer, "create", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Add song to album",
            description = "Processes requests to add a song to an album. " +
                    "Requires 'add-song' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully added to album"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Album or song not found")
    })
    @Parameters({
            @Parameter(name = "albumId", description = "ID of the album to modify", required = true),
            @Parameter(name = "songId", description = "ID of the song to remove", required = true)
    })
    @Incoming("album-add-song-in")
    @Outgoing("album-add-song-out")
    public Uni<ProcessingResult> processAddSongFromAlbum(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-song", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-song")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-song", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject albumAndSong = new JsonObject(message.getPayload());
                        String albumId = albumAndSong.getString("album");
                        String songId = albumAndSong.getString("song");
                        return albumService.addSongToAlbum(albumId, songId)
                                .onItem().transform(album ->
                                        createSuccessResult(album.getId(), "Song successfully added to album")
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        albumId,
                                        error,
                                        "Failed to add song to album"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-song", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during song addition"
                        );
                        recordMetrics(timer, "add-song", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete song from album",
            description = "Processes requests to remove a song from an album. " +
                    "Requires 'delete-song' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully removed from album"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Album or song not found")
    })
    @Parameters({
            @Parameter(name = "albumId", description = "ID of the album to modify", required = true),
            @Parameter(name = "songId", description = "ID of the song to remove", required = true)
    })
    @Incoming("album-delete-song-in")
    @Outgoing("album-delete-song-out")
    public Uni<ProcessingResult> processDeleteSongFromAlbum(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("delete-song", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "delete-song")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "delete-song", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject albumAndSong = new JsonObject(message.getPayload());
                        String albumId = albumAndSong.getString("album");
                        String songId = albumAndSong.getString("song");
                        return albumService.removeSongFromAlbum(albumId, songId)
                                .onItem().transform(album ->
                                        createSuccessResult(album.getId(), "Song successfully removed from album")
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        albumId,
                                        error,
                                        "Failed to remove song from album"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "delete-song", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during song removal"
                        );
                        recordMetrics(timer, "delete-song", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete album",
            description = "Processes requests to remove an album by ID. " +
                    "Requires 'delete' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album successfully deleted"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Album not found")
    })
    @Parameter(name = "albumId", description = "ID of the album to delete", required = true)
    @Incoming("album-delete-in")
    @Outgoing("album-delete-out")
    public Uni<ProcessingResult> processDeleteAlbum(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("delete", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "delete")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "delete", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        String albumId = message.getPayload();
                        return albumService.removeAlbum(albumId)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(albumId, "Album successfully deleted") :
                                        createNotFoundResult(albumId)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        albumId,
                                        error,
                                        "Delete failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "delete", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during delete"
                        );
                        recordMetrics(timer, "delete", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }
}