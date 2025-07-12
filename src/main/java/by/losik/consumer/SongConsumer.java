package by.losik.consumer;

import by.losik.entity.SongDTO;
import by.losik.service.SongService;
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
@Tag(name = "Song Consumer", description = "Message processing endpoints for song operations")
public class SongConsumer extends BaseConsumer {
    @Inject
    SongService songService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "song";
    }

    @Operation(summary = "Process song indexing",
            description = "Handles incoming messages for indexing single song records. " +
                    "Requires 'index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid song data")
    })
    @Incoming("song-index-in")
    @Outgoing("song-index-out")
    public Uni<ProcessingResult> processSongIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        SongDTO song = parseMessage(message.getPayload(), SongDTO.class);
                        return songService.saveSong(song)
                                .onFailure().retry()
                                .withBackOff(INITIAL_BACKOFF)
                                .atMost(MAX_RETRIES)
                                .onItem().transform(item -> createSuccessResult(
                                        song.getId(),
                                        "Song successfully indexed"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        song.getId(),
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

    @Operation(summary = "Process bulk song indexing",
            description = "Handles batch processing of multiple song records in a single operation. " +
                    "Requires 'bulk-index' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Songs successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid song data in batch")
    })
    @Incoming("song-bulk-index-in")
    @Outgoing("song-bulk-index-out")
    public Uni<ProcessingResult> processBulkSongIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        List<SongDTO> songs = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return songService.saveAllSongs(songs)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully indexed " + items.size() + " songs"
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

    @Operation(summary = "Retrieve song by ID",
            description = "Processes requests to fetch a specific song by its unique identifier. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Incoming("song-get-by-id-in")
    @Outgoing("song-get-by-id-out")
    public Uni<ProcessingResult> processGetSongById(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String songId = message.getPayload();
                        return songService.getSongById(songId)
                                .onItem().transform(song -> createSuccessResult(
                                        songId,
                                        "Song retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to retrieve song"
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

    @Operation(summary = "Get songs by user",
            description = "Processes requests to fetch all songs associated with a specific user. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Songs successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "userId", description = "ID of the user to fetch songs for", required = true)
    @Incoming("song-get-by-user-in")
    @Outgoing("song-get-by-user-out")
    public Uni<ProcessingResult> processGetSongsByUser(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-user", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-user", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String userId = message.getPayload();
                        return songService.getSongsByUser(userId)
                                .onItem().transform(songs -> createSuccessResult(
                                        userId,
                                        "Songs retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to retrieve songs"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-user", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-user", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Search songs by name",
            description = "Processes search requests for songs matching the provided name. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "name", description = "Song name or partial name to search for", required = true)
    @Incoming("song-search-by-name-in")
    @Outgoing("song-search-by-name-out")
    public Uni<ProcessingResult> processSearchSongsByName(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String name = message.getPayload();
                        return songService.searchSongsByName(name)
                                .onItem().transform(songs -> createSuccessResult(
                                        name,
                                        "Songs retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        name,
                                        error,
                                        "Failed to search songs"
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

    @Operation(summary = "Get songs with analysis",
            description = "Processes requests to find all songs that contain a specific analysis. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Songs successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "analysisId", description = "ID of the analysis to search for", required = true)
    @Incoming("song-get-with-analysis-in")
    @Outgoing("song-get-with-analysis-out")
    public Uni<ProcessingResult> processGetSongsWithAnalysis(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-with-analysis", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-with-analysis", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String analysisId = message.getPayload();
                        return songService.getSongsWithAnalysis(analysisId)
                                .onItem().transform(songs -> createSuccessResult(
                                        analysisId,
                                        "Songs retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        analysisId,
                                        error,
                                        "Failed to retrieve songs"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-with-analysis", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-with-analysis", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get songs by contributors",
            description = "Processes requests to find songs associated with specific contributors. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Songs successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "userIds", description = "List of contributor IDs to search for", required = true)
    @Incoming("song-get-by-contributors-in")
    @Outgoing("song-get-by-contributors-out")
    public Uni<ProcessingResult> processGetSongsByContributors(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-contributors", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-contributors", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        List<String> userIds = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return songService.getSongsByContributors(userIds)
                                .onItem().transform(songs -> createSuccessResult(
                                        "contributors-list",
                                        "Songs retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "contributors-list",
                                        error,
                                        "Failed to retrieve songs"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-contributors", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-contributors", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Create new song",
            description = "Handles song creation requests with validation. " +
                    "Requires 'create' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully created"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid song data")
    })
    @Incoming("song-create-in")
    @Outgoing("song-create-out")
    public Uni<ProcessingResult> processCreateSong(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        SongDTO song = parseMessage(message.getPayload(), SongDTO.class);
                        return songService.createSong(song)
                                .onItem().transform(item -> createSuccessResult(
                                        song.getId(),
                                        "Song created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        song.getId(),
                                        error,
                                        "Failed to create song"
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

    @Operation(summary = "Add contributor to song",
            description = "Processes requests to add a contributor to a song. " +
                    "Requires 'add-contributor' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Contributor successfully added"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Song or user not found")
    })
    @Parameters({
            @Parameter(name = "songId", description = "ID of the song to modify", required = true),
            @Parameter(name = "userId", description = "ID of the user to add as contributor", required = true)
    })
    @Incoming("song-add-contributor-in")
    @Outgoing("song-add-contributor-out")
    public Uni<ProcessingResult> processAddContributorToSong(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-contributor", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-contributor")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-contributor", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String songId = payload.getString("songId");
                        String userId = payload.getString("userId");

                        return songService.addContributorToSong(songId, userId)
                                .onItem().transform(updated -> createSuccessResult(
                                        songId,
                                        "Contributor added successfully to song"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to add contributor to song"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-contributor", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during contributor addition"
                        );
                        recordMetrics(timer, "add-contributor", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Remove contributor from song",
            description = "Processes requests to remove a contributor from a song. " +
                    "Requires 'remove-contributor' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Contributor successfully removed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Song or user not found")
    })
    @Parameters({
            @Parameter(name = "songId", description = "ID of the song to modify", required = true),
            @Parameter(name = "userId", description = "ID of the user to remove as contributor", required = true)
    })
    @Incoming("song-remove-contributor-in")
    @Outgoing("song-remove-contributor-out")
    public Uni<ProcessingResult> processRemoveContributorFromSong(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("remove-contributor", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "remove-contributor")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "remove-contributor", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String songId = payload.getString("songId");
                        String userId = payload.getString("userId");

                        return songService.removeContributorFromSong(songId, userId)
                                .onItem().transform(updated -> createSuccessResult(
                                        songId,
                                        "Contributor removed successfully from song"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to remove contributor from song"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "remove-contributor", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during contributor removal"
                        );
                        recordMetrics(timer, "remove-contributor", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Add analysis to song",
            description = "Processes requests to associate an analysis with a song. " +
                    "Requires 'add-analysis' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis successfully added"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Song or analysis not found")
    })
    @Parameters({
            @Parameter(name = "songId", description = "ID of the song to modify", required = true),
            @Parameter(name = "analysisId", description = "ID of the analysis to add", required = true)
    })
    @Incoming("song-add-analysis-in")
    @Outgoing("song-add-analysis-out")
    public Uni<ProcessingResult> processAddAnalysisToSong(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-analysis", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-analysis")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-analysis", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String songId = payload.getString("songId");
                        String analysisId = payload.getString("analysisId");

                        return songService.addAnalysisToSong(songId, analysisId)
                                .onItem().transform(updated -> createSuccessResult(
                                        songId,
                                        "Analysis added successfully to song"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to add analysis to song"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-analysis", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during analysis addition"
                        );
                        recordMetrics(timer, "add-analysis", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Remove analysis from song",
            description = "Processes requests to disassociate an analysis from a song. " +
                    "Requires 'remove-analysis' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Analysis successfully removed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Song or analysis not found")
    })
    @Parameters({
            @Parameter(name = "songId", description = "ID of the song to modify", required = true),
            @Parameter(name = "analysisId", description = "ID of the analysis to remove", required = true)
    })
    @Incoming("song-remove-analysis-in")
    @Outgoing("song-remove-analysis-out")
    public Uni<ProcessingResult> processRemoveAnalysisFromSong(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("remove-analysis", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "remove-analysis")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "remove-analysis", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String songId = payload.getString("songId");
                        String analysisId = payload.getString("analysisId");

                        return songService.removeAnalysisFromSong(songId, analysisId)
                                .onItem().transform(updated -> createSuccessResult(
                                        songId,
                                        "Analysis removed successfully from song"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to remove analysis from song"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "remove-analysis", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during analysis removal"
                        );
                        recordMetrics(timer, "remove-analysis", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Get song content",
            description = "Processes requests to retrieve binary content of a song. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song content successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Song not found or content unavailable")
    })
    @Parameter(name = "songId", description = "ID of the song to retrieve content for", required = true)
    @Incoming("song-get-content-in")
    @Outgoing("song-get-content-out")
    public Uni<ProcessingResult> processGetSongContent(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-content", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-content", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String songId = message.getPayload();
                        return songService.getSongContent(songId)
                                .onItem().transform(content -> createSuccessResult(
                                        songId,
                                        "Song content retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to retrieve song content"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-content", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process content request"
                        );
                        recordMetrics(timer, "get-content", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Update song content",
            description = "Processes requests to update binary content of a song. " +
                    "Requires 'update-content' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song content successfully updated"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid content data")
    })
    @Parameters({
            @Parameter(name = "songId", description = "ID of the song to update", required = true),
            @Parameter(name = "content", description = "Binary content data for the song", required = true)
    })
    @Incoming("song-update-content-in")
    @Outgoing("song-update-content-out")
    public Uni<ProcessingResult> processUpdateSongContent(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("update-content", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "update-content")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "update-content", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String songId = payload.getString("songId");
                        byte[] content = payload.getBinary("content");

                        return songService.updateSongContent(songId, content)
                                .onItem().transform(updated -> createSuccessResult(
                                        songId,
                                        "Song content updated successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to update song content"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "update-content", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during content update"
                        );
                        recordMetrics(timer, "update-content", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Delete song",
            description = "Processes requests to remove a song by ID. " +
                    "Requires 'delete' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully deleted"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Song not found")
    })
    @Parameter(name = "songId", description = "ID of the song to delete", required = true)
    @Incoming("song-delete-in")
    @Outgoing("song-delete-out")
    public Uni<ProcessingResult> processDeleteSong(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String songId = message.getPayload();
                        return songService.removeSong(songId)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(songId, "Song successfully deleted") :
                                        createNotFoundResult(songId)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Delete failed"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "delete", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during delete"
                        );
                        recordMetrics(timer, "delete", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }
}