package by.losik.repository;

import by.losik.entity.AnalysisDTO;
import by.losik.entity.GroupDTO;
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
@Tag(name = "Group Repository", description = "Operations for managing user groups in Elasticsearch")
public class GroupRepository extends ReactiveElasticsearchRepository<GroupDTO> {

    @Override
    protected String getIndexName() {
        return "groups";
    }

    @Override
    protected String getId(GroupDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<GroupDTO> getEntityType() {
        return GroupDTO.class;
    }

    @Operation(
            summary = "Save a single group",
            description = "Creates or updates a user group in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Group successfully saved",
                    content = @Content(schema = @Schema(implementation = GroupDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed after retries"
            )
    })
    public Uni<GroupDTO> saveGroup(GroupDTO group) {
        return index(group)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to save group {}", group.getId(), e));
    }

    @Operation(
            summary = "Save multiple groups",
            description = "Creates or updates multiple user groups in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Groups successfully saved",
                    content = @Content(schema = @Schema(implementation = GroupDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk save operation failed after retries"
            )
    })
    public Uni<List<GroupDTO>> saveAllGroups(List<GroupDTO> groups) {
        return bulkIndex(groups)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk save failed for {} groups", groups.size(), e));
    }

    @Operation(
            summary = "Find group by ID",
            description = "Retrieves a single user group from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Group found and returned",
                    content = @Content(schema = @Schema(implementation = GroupDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Group not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<GroupDTO> findGroupById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the group",
                    required = true,
                    example = "group-123"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch group {}", id, e));
    }

    @Operation(
            summary = "Find groups by user",
            description = "Searches for groups containing a specific user using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = GroupDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<GroupDTO>> findGroupsByUser(
            @Parameter(
                    name = "userId",
                    description = "Exact user ID to search for in groups",
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
            summary = "Find groups by name",
            description = "Searches for groups using prefix phrase matching on group names"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = GroupDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<GroupDTO>> findGroupsByName(
            @Parameter(
                    name = "name",
                    description = "Name or prefix of group name to search for",
                    required = true,
                    example = "Developers"
            ) String name) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match_phrase_prefix", new JsonObject()
                                .put("name", name)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by name failed for {}", name, e));
    }

    @Operation(
            summary = "Find groups with minimum members",
            description = "Filters groups by minimum member count using Elasticsearch script queries"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = GroupDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<GroupDTO>> findGroupsWithMinimumMembers(
            @Parameter(
                    name = "minMembers",
                    description = "Minimum number of members required in groups",
                    required = true,
                    example = "5"
            ) int minMembers) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("script", new JsonObject()
                                .put("script", new JsonObject()
                                        .put("source", "doc['userListId.keyword'].length >= params.minMembers")
                                        .put("params", new JsonObject()
                                                .put("minMembers", minMembers)))));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by minimum members failed for {}", minMembers, e));
    }

    @Operation(
            summary = "Delete groups that have the given id",
            description = "Delete groups by id"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Delete completed successfully",
                    content = @Content(schema = @Schema(implementation = GroupDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or delete failed"
            )
    })
    public Uni<Boolean> deleteGroupById(
            @Parameter(
                    name = "groupId",
                    description = "Id of the group",
                    required = true,
                    example = "group-aj23jfq35jdt38q"
            ) String groupId) {
        return deleteById(groupId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted group: {}", groupId);
                    } else {
                        log.debug("Group not found for deletion: {}", groupId);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete group {} after retries: {}", groupId, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for group: {}", groupId);
                    return false;
                });
    }
}