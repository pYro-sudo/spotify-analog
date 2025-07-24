package by.losik.consumer;

import by.losik.entity.UserDTO;
import by.losik.service.AuthServiceImpl;
import by.losik.service.UserService;
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
@Tag(name = "User Consumer", description = "Message processing endpoints for user management operations")
public class UserConsumer extends BaseConsumer {
    @Inject
    UserService userService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "user";
    }

    @Operation(summary = "Index user",
            description = "Processes messages for indexing or updating a single user. " +
                    "Requires 'index' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid user data")
    })
    @Incoming("user-index-in")
    @Outgoing("user-index-out")
    public Uni<ProcessingResult> processUserIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        UserDTO user = parseMessage(message.getPayload(), UserDTO.class);
                        return userService.saveUser(user)
                                .onFailure().retry()
                                .withBackOff(INITIAL_BACKOFF)
                                .atMost(MAX_RETRIES)
                                .onItem().transform(item -> createSuccessResult(
                                        user.getId(),
                                        "User successfully indexed"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        user.getId(),
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

    @Operation(summary = "Bulk index users",
            description = "Processes batch indexing of multiple users in a single operation. " +
                    "Requires 'bulk-index' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid user data in batch")
    })
    @Incoming("user-bulk-index-in")
    @Outgoing("user-bulk-index-out")
    public Uni<ProcessingResult> processBulkUserIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        List<UserDTO> users = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return userService.saveAllUsers(users)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully indexed " + items.size() + " users"
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

    @Operation(summary = "Get user by ID",
            description = "Retrieves a specific user by their unique identifier. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameter(name = "userId", description = "ID of the user to retrieve", required = true)
    @Incoming("user-get-by-id-in")
    @Outgoing("user-get-by-id-out")
    public Uni<ProcessingResult> processGetUserById(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String userId = message.getPayload();
                        return userService.getUserById(userId)
                                .onItem().transform(user -> createSuccessResult(
                                        userId,
                                        "User retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to retrieve user"
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

    @Operation(summary = "Get users by song",
            description = "Retrieves users associated with a specific song. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "songId", description = "ID of the song to query", required = true)
    @Incoming("user-get-by-song-in")
    @Outgoing("user-get-by-song-out")
    public Uni<ProcessingResult> processGetUsersBySong(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-song", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-song", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String songId = message.getPayload();
                        return userService.getUsersBySong(songId)
                                .onItem().transform(users -> createSuccessResult(
                                        songId,
                                        "Users retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        songId,
                                        error,
                                        "Failed to retrieve users"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-song", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-song", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get users by album",
            description = "Retrieves users associated with a specific album. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "albumId", description = "ID of the album to query", required = true)
    @Incoming("user-get-by-album-in")
    @Outgoing("user-get-by-album-out")
    public Uni<ProcessingResult> processGetUsersByAlbum(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-album", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-album", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String albumId = message.getPayload();
                        return userService.getUsersByAlbum(albumId)
                                .onItem().transform(users -> createSuccessResult(
                                        albumId,
                                        "Users retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        albumId,
                                        error,
                                        "Failed to retrieve users"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-album", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-album", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Add song to user",
            description = "Associates a song with a user. " +
                    "Requires 'add-song' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully added to user"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User or song not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "songId", description = "ID of the song to add", required = true)
    })
    @Incoming("user-add-song-in")
    @Outgoing("user-add-song-out")
    public Uni<ProcessingResult> processAddSongToUser(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String songId = payload.getString("songId");

                        return userService.addUserSong(userId, songId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Song added successfully to user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to add song to user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-song", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during song addition"
                        );
                        recordMetrics(timer, "add-song", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Remove song from user",
            description = "Removes a song association from a user. " +
                    "Requires 'remove-song' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Song successfully removed from user"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User or song not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "songId", description = "ID of the song to remove", required = true)
    })
    @Incoming("user-remove-song-in")
    @Outgoing("user-remove-song-out")
    public Uni<ProcessingResult> processRemoveSongFromUser(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("remove-song", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "remove-song")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "remove-song", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String songId = payload.getString("songId");

                        return userService.removeUserSong(userId, songId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Song removed successfully from user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to remove song from user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "remove-song", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during song removal"
                        );
                        recordMetrics(timer, "remove-song", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Add album to user",
            description = "Associates an album with a user. " +
                    "Requires 'add-album' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album successfully added to user"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User or album not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "albumId", description = "ID of the album to add", required = true)
    })
    @Incoming("user-add-album-in")
    @Outgoing("user-add-album-out")
    public Uni<ProcessingResult> processAddAlbumToUser(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-album", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-album")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-album", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String albumId = payload.getString("albumId");

                        return userService.addUserAlbum(userId, albumId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Album added successfully to user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to add album to user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-album", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during album addition"
                        );
                        recordMetrics(timer, "add-album", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Remove album from user",
            description = "Removes an album association from a user. " +
                    "Requires 'remove-album' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Album successfully removed from user"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User or album not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "albumId", description = "ID of the album to remove", required = true)
    })
    @Incoming("user-remove-album-in")
    @Outgoing("user-remove-album-out")
    public Uni<ProcessingResult> processRemoveAlbumFromUser(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("remove-album", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "remove-album")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "remove-album", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String albumId = payload.getString("albumId");

                        return userService.removeUserAlbum(userId, albumId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Album removed successfully from user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to remove album from user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "remove-album", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during album removal"
                        );
                        recordMetrics(timer, "remove-album", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Create new user",
            description = "Handles user creation requests with validation. " +
                    "Requires 'create' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User successfully created"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid user data")
    })
    @Incoming("user-create-in")
    @Outgoing("user-create-out")
    public Uni<ProcessingResult> processCreateUser(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        UserDTO user = parseMessage(message.getPayload(), UserDTO.class);
                        return userService.createUser(user)
                                .onItem().transform(item -> createSuccessResult(
                                        user.getId(),
                                        "User created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        user.getId(),
                                        error,
                                        "Failed to create user"
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

    @Operation(summary = "Get user songs",
            description = "Retrieves all songs associated with a user. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User songs successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameter(name = "userId", description = "ID of the user to query", required = true)
    @Incoming("user-get-songs-in")
    @Outgoing("user-get-songs-out")
    public Uni<ProcessingResult> processGetUserSongs(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-songs", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-songs", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String userId = message.getPayload();
                        return userService.getUserSongs(userId)
                                .onItem().transform(songs -> createSuccessResult(
                                        userId,
                                        "User songs retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to retrieve user songs"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-songs", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-songs", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get user albums",
            description = "Retrieves all albums associated with a user. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User albums successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameter(name = "userId", description = "ID of the user to query", required = true)
    @Incoming("user-get-albums-in")
    @Outgoing("user-get-albums-out")
    public Uni<ProcessingResult> processGetUserAlbums(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-albums", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-albums", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String userId = message.getPayload();
                        return userService.getUserAlbums(userId)
                                .onItem().transform(albums -> createSuccessResult(
                                        userId,
                                        "User albums retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to retrieve user albums"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-albums", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-albums", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Check user song ownership",
            description = "Verifies if a user has a specific song. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Ownership check completed"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "User or song not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to check", required = true),
            @Parameter(name = "songId", description = "ID of the song to verify", required = true)
    })
    @Incoming("user-has-song-in")
    @Outgoing("user-has-song-out")
    public Uni<ProcessingResult> processCheckUserHasSong(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("has-song", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "has-song", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String songId = payload.getString("songId");

                        return userService.userHasSong(userId, songId)
                                .onItem().transform(hasSong -> createSuccessResult(
                                        userId,
                                        "User " + (hasSong ? "has" : "does not have") + " the song"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to check song ownership"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "has-song", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process ownership check"
                        );
                        recordMetrics(timer, "has-song", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Check user album ownership",
            description = "Verifies if a user has a specific album. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Ownership check completed"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "User or album not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to check", required = true),
            @Parameter(name = "albumId", description = "ID of the album to verify", required = true)
    })
    @Incoming("user-has-album-in")
    @Outgoing("user-has-album-out")
    public Uni<ProcessingResult> processCheckUserHasAlbum(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("has-album", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "has-album", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String albumId = payload.getString("albumId");

                        return userService.userHasAlbum(userId, albumId)
                                .onItem().transform(hasAlbum -> createSuccessResult(
                                        userId,
                                        "User " + (hasAlbum ? "has" : "does not have") + " the album"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to check album ownership"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "has-album", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process ownership check"
                        );
                        recordMetrics(timer, "has-album", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete user",
            description = "Permanently removes a user. " +
                    "Requires 'delete' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User successfully deleted"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameter(name = "userId", description = "ID of the user to delete", required = true)
    @Incoming("user-delete-in")
    @Outgoing("user-delete-out")
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
                        String userId = message.getPayload();
                        return userService.removeUser(userId)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(userId, "User successfully deleted") :
                                        createNotFoundResult(userId)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
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

    @Operation(summary = "Add subscriber to user",
            description = "Adds a subscriber to a user's subscribers list. " +
                    "Requires 'add-subscriber' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Subscriber successfully added"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "subscriberId", description = "ID of the subscriber to add", required = true)
    })
    @Incoming("user-add-subscriber-in")
    @Outgoing("user-add-subscriber-out")
    public Uni<ProcessingResult> processAddSubscriber(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-subscriber", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-subscriber")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-subscriber", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String subscriberId = payload.getString("subscriberId");

                        return userService.addSubscriber(userId, subscriberId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Subscriber added successfully to user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to add subscriber to user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-subscriber", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during subscriber addition"
                        );
                        recordMetrics(timer, "add-subscriber", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Remove subscriber from user",
            description = "Removes a subscriber from a user's subscribers list. " +
                    "Requires 'remove-subscriber' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Subscriber successfully removed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "subscriberId", description = "ID of the subscriber to remove", required = true)
    })
    @Incoming("user-remove-subscriber-in")
    @Outgoing("user-remove-subscriber-out")
    public Uni<ProcessingResult> processRemoveSubscriber(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("remove-subscriber", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "remove-subscriber")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "remove-subscriber", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String subscriberId = payload.getString("subscriberId");

                        return userService.removeSubscriber(userId, subscriberId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Subscriber removed successfully from user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to remove subscriber from user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "remove-subscriber", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during subscriber removal"
                        );
                        recordMetrics(timer, "remove-subscriber", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Add message to user",
            description = "Adds a message to a user's messages list. " +
                    "Requires 'add-message' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message successfully added"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "messageId", description = "ID of the message to add", required = true)
    })
    @Incoming("user-add-message-in")
    @Outgoing("user-add-message-out")
    public Uni<ProcessingResult> processAddMessage(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-message", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-message")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-message", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String messageId = payload.getString("messageId");

                        return userService.addMessage(userId, messageId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Message added successfully to user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to add message to user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-message", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during message addition"
                        );
                        recordMetrics(timer, "add-message", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Remove message from user",
            description = "Removes a message from a user's messages list. " +
                    "Requires 'remove-message' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Message successfully removed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
    @Parameters({
            @Parameter(name = "userId", description = "ID of the user to modify", required = true),
            @Parameter(name = "messageId", description = "ID of the message to remove", required = true)
    })
    @Incoming("user-remove-message-in")
    @Outgoing("user-remove-message-out")
    public Uni<ProcessingResult> processRemoveMessage(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("remove-message", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "remove-message")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "remove-message", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String userId = payload.getString("userId");
                        String messageId = payload.getString("messageId");

                        return userService.removeMessage(userId, messageId)
                                .onItem().transform(updated -> createSuccessResult(
                                        userId,
                                        "Message removed successfully from user"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to remove message from user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "remove-message", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult errorResult = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during message removal"
                        );
                        recordMetrics(timer, "remove-message", errorResult);
                        message.nack(e);
                        return Uni.createFrom().item(errorResult);
                    }
                });
    }

    @Operation(summary = "Find users by subscriber",
            description = "Retrieves users who have a specific subscriber in their subscribers list. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "subscriberId", description = "ID of the subscriber to query", required = true)
    @Incoming("user-get-by-subscriber-in")
    @Outgoing("user-get-by-subscriber-out")
    public Uni<ProcessingResult> processGetUsersBySubscriber(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-subscriber", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-subscriber", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String subscriberId = message.getPayload();
                        return userService.getUsersBySubscriber(subscriberId)
                                .onItem().transform(users -> createSuccessResult(
                                        subscriberId,
                                        "Users retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        subscriberId,
                                        error,
                                        "Failed to retrieve users"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-subscriber", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-subscriber", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Find users by message",
            description = "Retrieves users who have a specific message in their messages list. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "messageId", description = "ID of the message to query", required = true)
    @Incoming("user-get-by-message-in")
    @Outgoing("user-get-by-message-out")
    public Uni<ProcessingResult> processGetUsersByMessage(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-message", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-message", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String messageId = message.getPayload();
                        return userService.getUsersByMessage(messageId)
                                .onItem().transform(users -> createSuccessResult(
                                        messageId,
                                        "Users retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        messageId,
                                        error,
                                        "Failed to retrieve users"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-message", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-message", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }
}