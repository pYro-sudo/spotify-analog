package by.losik.service;

import by.losik.configuration.ServiceConfig;
import by.losik.entity.GroupDTO;
import by.losik.repository.GroupRepository;
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

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheKey;

import java.util.List;

@ApplicationScoped
@Slf4j
@Tag(name = "Group Service", description = "Operations for managing user groups and memberships")
public class GroupService extends ServiceConfig {
    @Inject
    GroupRepository groupRepository;

    @Operation(summary = "Save group",
            description = "Stores group with validation (requires non-empty name)")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group saved successfully"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("save-group-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "group-cache") // Invalidate single group cache
    @CacheInvalidate(cacheName = "user-groups-cache") // Invalidate all affected user caches
    public Uni<GroupDTO> saveGroup(GroupDTO group) {
        validateGroup(group);
        log.debug("Saving group with ID: {}", group.getId());
        return groupRepository.saveGroup(group)
                .onItem().invoke(saved -> log.info("Successfully saved group: {}", saved.getId()))
                .onFailure().transform(e -> new RuntimeException("Failed to save group: " + group.getId(), e));
    }

    @Operation(summary = "Bulk save groups",
            description = "Saves multiple groups with batch validation")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "All groups saved"),
            @APIResponse(responseCode = "400", description = "Batch validation failed")
    })
    @Retry(maxRetries = 2, delay = 1000)
    @Timeout(30000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("bulk-save-groups-cb")
    @Bulkhead(value = BULK_OPERATION_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "group-cache") // Clear entire group cache
    @CacheInvalidate(cacheName = "user-groups-cache") // Clear all user groups caches
    public Uni<List<GroupDTO>> saveAllGroups(List<GroupDTO> groups) {
        groups.forEach(this::validateGroup);
        log.debug("Bulk saving {} groups", groups.size());
        return groupRepository.saveAllGroups(groups)
                .onItem().invoke(saved -> log.info("Successfully saved {} groups", saved.size()))
                .onFailure().transform(e -> new RuntimeException("Failed to save groups batch", e));
    }

    @Operation(summary = "Get group by ID",
            description = "Retrieves complete group data including member list")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group found"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-group-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "group-cache") // Cache individual groups by ID
    public Uni<GroupDTO> getGroupById(
            @Parameter(description = "Group ID", example = "group-123", required = true)
            @CacheKey String id) {
        log.debug("Fetching group by ID: {}", id);
        return groupRepository.findGroupById(id)
                .onItem().ifNotNull().invoke(group -> log.debug("Found group: {}", group.getId()))
                .onItem().ifNull().failWith(() -> new IllegalArgumentException("Group not found with ID: " + id));
    }

    @Operation(summary = "Get user's groups",
            description = "Lists all groups containing the specified user")
    @APIResponse(responseCode = "200", description = "List returned (may be empty)")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-user-groups-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "user-groups-cache") // Cache user's groups
    public Uni<List<GroupDTO>> getGroupsByUser(
            @Parameter(description = "User ID", example = "user-456", required = true)
            @CacheKey String userId) {
        log.debug("Finding groups for user: {}", userId);
        return groupRepository.findGroupsByUser(userId)
                .onItem().invoke(groups -> log.debug("Found {} groups for user {}", groups.size(), userId))
                .onFailure().transform(e -> new RuntimeException("Failed to find groups for user: " + userId, e));
    }

    @Operation(summary = "Search groups by name",
            description = "Finds groups using prefix matching on names")
    @APIResponse(responseCode = "200", description = "Search results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("search-groups-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "group-search-cache") // Cache group search results
    public Uni<List<GroupDTO>> searchGroupsByName(
            @Parameter(description = "Name or partial name", example = "Developers", required = true)
            @CacheKey String name) {
        log.debug("Searching groups by name: {}", name);
        return groupRepository.findGroupsByName(name)
                .onItem().invoke(groups -> log.debug("Found {} groups matching name '{}'", groups.size(), name))
                .onFailure().transform(e -> new RuntimeException("Failed to search groups by name: " + name, e));
    }

    @Operation(summary = "Get groups by member count",
            description = "Filters groups by minimum number of members")
    @APIResponse(responseCode = "200", description = "Filtered results returned")
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-groups-by-members-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "groups-by-members-cache") // Cache groups by member count
    public Uni<List<GroupDTO>> getGroupsWithMinimumMembers(
            @Parameter(description = "Minimum member count", example = "5", required = true)
            @CacheKey int minMembers) {
        log.debug("Finding groups with at least {} members", minMembers);
        return groupRepository.findGroupsWithMinimumMembers(minMembers)
                .onItem().invoke(groups -> log.debug("Found {} groups with minimum {} members", groups.size(), minMembers))
                .onFailure().transform(e -> new RuntimeException("Failed to find groups by member count", e));
    }

    @Operation(summary = "Add user to group",
            description = "Idempotent operation to add user to a group")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group updated"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("add-user-to-group-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "group-cache") // Invalidate this group's cache
    @CacheInvalidate(cacheName = "user-groups-cache") // Invalidate user's groups cache
    public Uni<GroupDTO> addUserToGroup(
            @Parameter(description = "Group ID", example = "group-123", required = true)
            String groupId,
            @Parameter(description = "User ID to add", example = "user-456", required = true)
            String userId) {
        log.debug("Adding user {} to group {}", userId, groupId);
        return getGroupById(groupId)
                .onItem().transformToUni(group -> {
                    if (group.getUserListId().contains(userId)) {
                        log.warn("User {} already in group {}", userId, groupId);
                        return Uni.createFrom().item(group);
                    }
                    group.getUserListId().add(userId);
                    return saveGroup(group);
                });
    }

    @Operation(summary = "Remove user from group",
            description = "Idempotent operation to remove user from group")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group updated"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-user-from-group-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "group-cache") // Invalidate this group's cache
    @CacheInvalidate(cacheName = "user-groups-cache") // Invalidate user's groups cache
    public Uni<GroupDTO> removeUserFromGroup(
            @Parameter(description = "Group ID", example = "group-123", required = true)
            String groupId,
            @Parameter(description = "User ID to remove", example = "user-456", required = true)
            String userId) {
        log.debug("Removing user {} from group {}", userId, groupId);
        return getGroupById(groupId)
                .onItem().transformToUni(group -> {
                    if (!group.getUserListId().contains(userId)) {
                        log.warn("User {} not found in group {}", userId, groupId);
                        return Uni.createFrom().item(group);
                    }
                    group.getUserListId().remove(userId);
                    return saveGroup(group);
                });
    }

    @Operation(summary = "Create new group",
            description = "Validates and creates a new group")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group created"),
            @APIResponse(responseCode = "400", description = "Validation failed")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("create-group-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "user-groups-cache") // Invalidate all affected user caches
    public Uni<GroupDTO> createGroup(GroupDTO group) {
        log.info("Creating new group with ID: {}", group.getId());
        validateGroup(group);
        return saveGroup(group);
    }

    private void validateGroup(GroupDTO group) {
        if (group.getName().isBlank()) {
            throw new IllegalArgumentException("Group name cannot be empty");
        }
    }

    @Operation(summary = "Get group member count",
            description = "Returns the number of members in a group")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Count returned"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("get-member-count-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "group-member-count-cache") // Cache group member counts
    public Uni<Integer> getGroupMemberCount(
            @Parameter(description = "Group ID", example = "group-123", required = true)
            @CacheKey String groupId) {
        return getGroupById(groupId)
                .onItem().transform(group -> group.getUserListId().size());
    }

    @Operation(summary = "Check user membership",
            description = "Verifies if user is a member of the group")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Membership status returned"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Retry(maxRetries = 2, delay = 500)
    @Timeout(3000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("check-membership-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheResult(cacheName = "user-membership-cache") // Cache membership status
    public Uni<Boolean> isUserInGroup(
            @Parameter(description = "Group ID", example = "group-123", required = true)
            @CacheKey String groupId,
            @Parameter(description = "User ID to check", example = "user-456", required = true)
            @CacheKey String userId) {
        return getGroupById(groupId)
                .onItem().transform(group -> group.getUserListId().contains(userId));
    }

    @Operation(summary = "Remove the group",
            description = "Idempotent operation to remove a group")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Group deleted"),
            @APIResponse(responseCode = "404", description = "Group not found")
    })
    @Retry(maxRetries = 3, delay = 500)
    @Timeout(5000)
    @CircuitBreaker(
            requestVolumeThreshold = CIRCUIT_BREAKER_REQUEST_VOLUME,
            failureRatio = CIRCUIT_BREAKER_FAILURE_RATIO,
            delay = CIRCUIT_BREAKER_DELAY_MS
    )
    @CircuitBreakerName("remove-group-cb")
    @Bulkhead(value = DEFAULT_BULKHEAD_VALUE)
    @CacheInvalidate(cacheName = "group-cache") // Invalidate this group's cache
    public Uni<Boolean> removeGroup(String groupId) {
        log.debug("Removing analysis {}", groupId);
        return groupRepository.deleteGroupById(groupId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.info("Successfully deleted analysis: {}", groupId);
                    } else {
                        log.warn("Analysis not found for deletion: {}", groupId);
                    }
                });
    }
}