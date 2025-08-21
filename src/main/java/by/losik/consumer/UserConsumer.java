package by.losik.consumer;

import by.losik.entity.Sex;
import by.losik.service.AuthServiceImpl;
import by.losik.service.UserService;
import io.micrometer.core.instrument.Timer;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

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

    @Operation(summary = "Create user",
            description = "Handles user creation messages. Requires 'user-create' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User created successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid user data")
    })
    @Incoming("user-create-in")
    @Outgoing("user-create-out")
    public Uni<ProcessingResult> processCreateUser(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("create", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "user-create")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "create", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String name = payload.getString("name");
                        int age = payload.getInteger("age");
                        Sex gender = Sex.valueOf(payload.getString("gender"));
                        String email = payload.getString("email");
                        String phone = payload.getString("phone");

                        return userService.createUser(name, age, gender, email, phone)
                                .onItem().transform(user -> createSuccessResult(
                                        user.getId().toString(),
                                        "User created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        UNKNOWN,
                                        error,
                                        "Failed to create user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "create", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process create user request"
                        );
                        recordMetrics(timer, "create", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get user by ID",
            description = "Retrieves user details by ID. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "User not found")
    })
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
                        Long userId = Long.parseLong(message.getPayload());
                        return userService.getUserById(userId)
                                .onItem().transform(user -> createSuccessResult(
                                        userId.toString(),
                                        "User retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId.toString(),
                                        error,
                                        "Failed to get user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-id", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get user request"
                        );
                        recordMetrics(timer, "get-by-id", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get all users",
            description = "Retrieves all users. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Incoming("user-get-all-in")
    @Outgoing("user-get-all-out")
    public Uni<ProcessingResult> processGetAllUsers(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-all", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-all", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        return userService.getAllUsers()
                                .onItem().transform(users -> createSuccessResult(
                                        "all-users",
                                        "Retrieved " + users.size() + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "all-users",
                                        error,
                                        "Failed to get all users"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-all", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get all users request"
                        );
                        recordMetrics(timer, "get-all", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get users by name",
            description = "Retrieves users filtered by name. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Incoming("user-get-by-name-in")
    @Outgoing("user-get-by-name-out")
    public Uni<ProcessingResult> processGetUsersByName(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-name", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-name", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String name = message.getPayload();
                        return userService.getUsersByName(name)
                                .onItem().transform(users -> createSuccessResult(
                                        name,
                                        "Found " + users.size() + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        name,
                                        error,
                                        "Failed to get users by name"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-name", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get users by name request"
                        );
                        recordMetrics(timer, "get-by-name", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get users by email",
            description = "Retrieves users filtered by email. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Incoming("user-get-by-email-in")
    @Outgoing("user-get-by-email-out")
    public Uni<ProcessingResult> processGetUsersByEmail(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-email", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-email", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String email = message.getPayload();
                        return userService.getUsersByEmail(email)
                                .onItem().transform(users -> createSuccessResult(
                                        email,
                                        "Found " + users.size() + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        email,
                                        error,
                                        "Failed to get users by email"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-email", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get users by email request"
                        );
                        recordMetrics(timer, "get-by-email", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get users by phone",
            description = "Retrieves users filtered by phone number. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Incoming("user-get-by-phone-in")
    @Outgoing("user-get-by-phone-out")
    public Uni<ProcessingResult> processGetUsersByPhone(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-phone", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-phone", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String phone = message.getPayload();
                        return userService.getUsersByPhone(phone)
                                .onItem().transform(users -> createSuccessResult(
                                        phone,
                                        "Found " + users.size() + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        phone,
                                        error,
                                        "Failed to get users by phone"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-phone", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get users by phone request"
                        );
                        recordMetrics(timer, "get-by-phone", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Update user email",
            description = "Updates user's email address. Requires 'user-update' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Email updated successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid email format")
    })
    @Incoming("user-update-email-in")
    @Outgoing("user-update-email-out")
    public Uni<ProcessingResult> processUpdateUserEmail(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("update-email", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "user-update")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "update-email", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        Long userId = payload.getLong("id");
                        String newEmail = payload.getString("email");

                        return userService.updateUserEmail(userId, newEmail)
                                .onItem().transform(user -> createSuccessResult(
                                        userId.toString(),
                                        "Email updated successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId.toString(),
                                        error,
                                        "Failed to update email"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "update-email", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process update email request"
                        );
                        recordMetrics(timer, "update-email", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Verify user",
            description = "Updates user verification status. Requires 'user-verify' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Verification status updated"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid user ID")
    })
    @Incoming("user-verify-in")
    @Outgoing("user-verify-out")
    public Uni<ProcessingResult> processVerifyUser(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("verify", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "user-verify")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "verify", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        Long userId = payload.getLong("id");
                        boolean verified = payload.getBoolean("verified");

                        return userService.verifyUser(userId, verified)
                                .onItem().transform(user -> createSuccessResult(
                                        userId.toString(),
                                        "User verification status updated"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId.toString(),
                                        error,
                                        "Failed to update verification status"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "verify", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process verify user request"
                        );
                        recordMetrics(timer, "verify", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete user",
            description = "Deletes a user by ID. Requires 'user-delete' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User deleted successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid user ID")
    })
    @Incoming("user-delete-in")
    @Outgoing("user-delete-out")
    public Uni<ProcessingResult> processDeleteUser(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("delete", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "user-delete")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "delete", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        Long userId = Long.parseLong(message.getPayload());
                        return userService.deleteUser(userId)
                                .onItem().transform(deleted -> createSuccessResult(
                                        userId.toString(),
                                        "User deleted successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId.toString(),
                                        error,
                                        "Failed to delete user"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "delete", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process delete user request"
                        );
                        recordMetrics(timer, "delete", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get users by gender",
            description = "Retrieves users filtered by gender. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Incoming("user-get-by-gender-in")
    @Outgoing("user-get-by-gender-out")
    public Uni<ProcessingResult> processGetUsersByGender(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-gender", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-gender", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        Sex gender = Sex.valueOf(message.getPayload());
                        return userService.getUsersByGender(gender)
                                .onItem().transform(users -> createSuccessResult(
                                        gender.name(),
                                        "Found " + users.size() + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        gender.name(),
                                        error,
                                        "Failed to get users by gender"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-gender", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get users by gender request"
                        );
                        recordMetrics(timer, "get-by-gender", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get users by age range",
            description = "Retrieves users filtered by age range. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid age range")
    })
    @Incoming("user-get-by-age-in")
    @Outgoing("user-get-by-age-out")
    public Uni<ProcessingResult> processGetUsersByAgeRange(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-age", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-age", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        int minAge = payload.getInteger("minAge");
                        int maxAge = payload.getInteger("maxAge");

                        return userService.getUsersByAgeRange(minAge, maxAge)
                                .onItem().transform(users -> createSuccessResult(
                                        "age-range-" + minAge + "-" + maxAge,
                                        "Found " + users.size() + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "age-range-" + minAge + "-" + maxAge,
                                        error,
                                        "Failed to get users by age range"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-age", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get users by age range request"
                        );
                        recordMetrics(timer, "get-by-age", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get users by verification status",
            description = "Retrieves users filtered by verification status. Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Incoming("user-get-by-verification-in")
    @Outgoing("user-get-by-verification-out")
    public Uni<ProcessingResult> processGetUsersByVerificationStatus(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-verification", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-verification", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        boolean verified = Boolean.parseBoolean(message.getPayload());
                        return userService.getUsersByVerificationStatus(verified)
                                .onItem().transform(users -> createSuccessResult(
                                        String.valueOf(verified),
                                        "Found " + users.size() + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        String.valueOf(verified),
                                        error,
                                        "Failed to get users by verification status"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-verification", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get users by verification status request"
                        );
                        recordMetrics(timer, "get-by-verification", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete users below age",
            description = "Deletes users below specified age. Requires 'user-delete' operation authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Users deleted successfully"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid age parameter")
    })
    @Incoming("user-delete-below-age-in")
    @Outgoing("user-delete-below-age-out")
    public Uni<ProcessingResult> processDeleteUsersBelowAge(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("delete-below-age", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "user-delete")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "delete-below-age", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        int maxAge = Integer.parseInt(message.getPayload());
                        return userService.deleteUsersBelowAge(maxAge)
                                .onItem().transform(count -> createSuccessResult(
                                        "age-" + maxAge,
                                        "Deleted " + count + " users"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        "age-" + maxAge,
                                        error,
                                        "Failed to delete users below age"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "delete-below-age", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process delete users below age request"
                        );
                        recordMetrics(timer, "delete-below-age", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }
}