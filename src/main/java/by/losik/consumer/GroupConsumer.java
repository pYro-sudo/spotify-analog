package by.losik.consumer;

import by.losik.entity.GroupDTO;
import by.losik.service.GroupService;
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
@Tag(name = "Group Consumer", description = "Message processing endpoints for group management operations")
public class GroupConsumer extends BaseConsumer {
    @Inject
    GroupService groupService;
    @Inject
    AuthServiceImpl authService;

    @Override
    protected String getMetricPrefix() {
        return "group";
    }

    @Operation(summary = "Index a group",
            description = "Processes messages for indexing or updating a single group. " +
                    "Requires 'index' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid group data")
    })
    @Incoming("group-index-in")
    @Outgoing("group-index-out")
    public Uni<ProcessingResult> processGroupIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        GroupDTO group = parseMessage(message.getPayload(), GroupDTO.class);
                        return groupService.saveGroup(group)
                                .onFailure().retry()
                                .withBackOff(INITIAL_BACKOFF)
                                .atMost(MAX_RETRIES)
                                .onItem().transform(item -> createSuccessResult(
                                        group.getId(),
                                        "Group successfully indexed"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        group.getId(),
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

    @Operation(summary = "Bulk index groups",
            description = "Processes batch indexing of multiple groups in a single operation. " +
                    "Requires 'bulk-index' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Groups successfully indexed"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid group data in batch")
    })
    @Incoming("group-bulk-index-in")
    @Outgoing("group-bulk-index-out")
    public Uni<ProcessingResult> processBulkGroupIndexing(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        List<GroupDTO> groups = objectMapper.readValue(
                                message.getPayload(),
                                new TypeReference<>() {}
                        );

                        return groupService.saveAllGroups(groups)
                                .onItem().transform(items -> createSuccessResult(
                                        "bulk-operation",
                                        "Successfully indexed " + items.size() + " groups"
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

    @Operation(summary = "Get group by ID",
            description = "Retrieves a specific group by its unique identifier. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Incoming("group-get-by-id-in")
    @Outgoing("group-get-by-id-out")
    public Uni<ProcessingResult> processGetGroupById(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String groupId = message.getPayload();
                        return groupService.getGroupById(groupId)
                                .onItem().transform(group -> createSuccessResult(
                                        groupId,
                                        "Group retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        groupId,
                                        error,
                                        "Failed to retrieve group"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get groups by user",
            description = "Retrieves all groups that a specific user belongs to. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Groups successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "userId", description = "ID of the user to search for", required = true)
    @Incoming("group-get-by-user-in")
    @Outgoing("group-get-by-user-out")
    public Uni<ProcessingResult> processGetGroupsByUser(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        return groupService.getGroupsByUser(userId)
                                .onItem().transform(groups -> createSuccessResult(
                                        userId,
                                        "Groups retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        userId,
                                        error,
                                        "Failed to retrieve groups"
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

    @Operation(summary = "Search groups by name",
            description = "Finds groups matching the provided name or partial name. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Search completed successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "name", description = "Group name or partial name to search for", required = true)
    @Incoming("group-search-by-name-in")
    @Outgoing("group-search-by-name-out")
    public Uni<ProcessingResult> processSearchGroupsByName(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("search", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "search", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String name = message.getPayload();
                        return groupService.searchGroupsByName(name)
                                .onItem().transform(groups -> createSuccessResult(
                                        name,
                                        "Groups retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        name,
                                        error,
                                        "Failed to search groups"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "search", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process search request"
                        );
                        recordMetrics(timer, "search", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get groups by member count",
            description = "Finds groups with at least the specified number of members. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Groups successfully retrieved"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials")
    })
    @Parameter(name = "minMembers", description = "Minimum number of members required", required = true)
    @Incoming("group-get-by-members-in")
    @Outgoing("group-get-by-members-out")
    public Uni<ProcessingResult> processGetGroupsByMemberCount(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("get-by-members", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "get-by-members", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        int minMembers = Integer.parseInt(message.getPayload());
                        return groupService.getGroupsWithMinimumMembers(minMembers)
                                .onItem().transform(groups -> createSuccessResult(
                                        String.valueOf(minMembers),
                                        "Groups retrieved successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        String.valueOf(minMembers),
                                        error,
                                        "Failed to retrieve groups"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "get-by-members", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process get request"
                        );
                        recordMetrics(timer, "get-by-members", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Create new group",
            description = "Handles group creation requests with validation. " +
                    "Requires 'create' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group successfully created"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "400", description = "Bad request - invalid group data")
    })
    @Incoming("group-create-in")
    @Outgoing("group-create-out")
    public Uni<ProcessingResult> processCreateGroup(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        GroupDTO group = parseMessage(message.getPayload(), GroupDTO.class);
                        return groupService.createGroup(group)
                                .onItem().transform(item -> createSuccessResult(
                                        group.getId(),
                                        "Group created successfully"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        group.getId(),
                                        error,
                                        "Failed to create group"
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

    @Operation(summary = "Add user to group",
            description = "Processes requests to add a user to a group. " +
                    "Requires 'add-user' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User successfully added to group"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Group or user not found")
    })
    @Parameters({
            @Parameter(name = "groupId", description = "ID of the group to modify", required = true),
            @Parameter(name = "userId", description = "ID of the user to add", required = true)
    })
    @Incoming("group-add-user-in")
    @Outgoing("group-add-user-out")
    public Uni<ProcessingResult> processAddUserToGroup(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("add-user", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "add-user")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "add-user", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String groupId = payload.getString("groupId");
                        String userId = payload.getString("userId");

                        return groupService.addUserToGroup(groupId, userId)
                                .onItem().transform(updated -> createSuccessResult(
                                        groupId,
                                        "User added successfully to group"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        groupId,
                                        error,
                                        "Failed to add user to group"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "add-user", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during user addition"
                        );
                        recordMetrics(timer, "add-user", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Remove user from group",
            description = "Processes requests to remove a user from a group. " +
                    "Requires 'remove-user' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "User successfully removed from group"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Group or user not found")
    })
    @Parameters({
            @Parameter(name = "groupId", description = "ID of the group to modify", required = true),
            @Parameter(name = "userId", description = "ID of the user to remove", required = true)
    })
    @Incoming("group-remove-user-in")
    @Outgoing("group-remove-user-out")
    public Uni<ProcessingResult> processRemoveUserFromGroup(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("remove-user", false, UNKNOWN);

        return authService.isAuthorizedForOperation(message, "remove-user")
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createForbiddenResult();
                        recordMetrics(timer, "remove-user", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Forbidden")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String groupId = payload.getString("groupId");
                        String userId = payload.getString("userId");

                        return groupService.removeUserFromGroup(groupId, userId)
                                .onItem().transform(updated -> createSuccessResult(
                                        groupId,
                                        "User removed successfully from group"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        groupId,
                                        error,
                                        "Failed to remove user from group"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "remove-user", result);
                                    acknowledgeMessage(message, result.status());
                                    logProcessingResult(result);
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Unexpected error during user removal"
                        );
                        recordMetrics(timer, "remove-user", result);
                        message.nack(e);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Get group member count",
            description = "Retrieves the number of members in a specific group. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Member count retrieved successfully"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Parameter(name = "groupId", description = "ID of the group to query", required = true)
    @Incoming("group-member-count-in")
    @Outgoing("group-member-count-out")
    public Uni<ProcessingResult> processGetGroupMemberCount(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("member-count", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "member-count", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        String groupId = message.getPayload();
                        return groupService.getGroupMemberCount(groupId)
                                .onItem().transform(count -> createSuccessResult(
                                        groupId,
                                        "Member count: " + count
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        groupId,
                                        error,
                                        "Failed to get member count"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "member-count", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process member count request"
                        );
                        recordMetrics(timer, "member-count", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Check user membership",
            description = "Verifies if a user is a member of a specific group. " +
                    "Requires general read authorization.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Membership check completed"),
            @APIResponse(responseCode = "401", description = "Unauthorized - missing valid credentials"),
            @APIResponse(responseCode = "404", description = "Group or user not found")
    })
    @Parameters({
            @Parameter(name = "groupId", description = "ID of the group to check", required = true),
            @Parameter(name = "userId", description = "ID of the user to verify", required = true)
    })
    @Incoming("group-check-membership-in")
    @Outgoing("group-check-membership-out")
    public Uni<ProcessingResult> processCheckUserMembership(@NotNull IncomingRabbitMQMessage<String> message) {
        Timer.Sample timer = startTimer();
        recordCount("check-membership", false, UNKNOWN);

        return authService.isAuthorized(message)
                .onItem().transformToUni(authorized -> {
                    if (!authorized) {
                        ProcessingResult result = createUnauthorizedResult();
                        recordMetrics(timer, "check-membership", result);
                        return Uni.createFrom().item(result)
                                .onItem().invoke(() -> message.nack(new SecurityException("Unauthorized")));
                    }

                    try {
                        JsonObject payload = new JsonObject(message.getPayload());
                        String groupId = payload.getString("groupId");
                        String userId = payload.getString("userId");

                        return groupService.isUserInGroup(groupId, userId)
                                .onItem().transform(isMember -> createSuccessResult(
                                        groupId,
                                        "User " + (isMember ? "is" : "is not") + " a member"
                                ))
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        groupId,
                                        error,
                                        "Failed to check membership"
                                ))
                                .onItem().invoke(result -> {
                                    recordMetrics(timer, "check-membership", result);
                                    acknowledgeMessage(message, result.status());
                                });
                    } catch (Exception e) {
                        ProcessingResult result = createErrorResult(
                                UNKNOWN,
                                e,
                                "Failed to process membership check"
                        );
                        recordMetrics(timer, "check-membership", result);
                        return Uni.createFrom().item(result);
                    }
                });
    }

    @Operation(summary = "Delete group",
            description = "Processes requests to permanently remove a group. " +
                    "Requires 'delete' operation permission.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group successfully deleted"),
            @APIResponse(responseCode = "403", description = "Forbidden - missing required permissions"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Parameter(name = "groupId", description = "ID of the group to delete", required = true)
    @Incoming("group-delete-in")
    @Outgoing("group-delete-out")
    public Uni<ProcessingResult> processDeleteGroup(@NotNull IncomingRabbitMQMessage<String> message) {
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
                        String groupId = message.getPayload();
                        return groupService.removeGroup(groupId)
                                .onItem().transform(deleted -> deleted ?
                                        createSuccessResult(groupId, "Group successfully deleted") :
                                        createNotFoundResult(groupId)
                                )
                                .onFailure().recoverWithItem(error -> createErrorResult(
                                        groupId,
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