package by.losik.repository;

import by.losik.entity.AccordDTO;
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
@Tag(name = "Accord Repository", description = "Operations for managing musical accords in Elasticsearch")
public class AccordRepository extends ReactiveElasticsearchRepository<AccordDTO> {
    @Override
    protected String getIndexName() {
        return "accords";
    }

    @Override
    protected String getId(AccordDTO entity) {
        return entity.getName();
    }

    @Override
    protected Class<AccordDTO> getEntityType() {
        return AccordDTO.class;
    }

    @Operation(
            summary = "Index a single accord",
            description = "Creates or updates a musical accord in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Accord successfully indexed",
                    content = @Content(schema = @Schema(implementation = AccordDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or indexing failed after retries"
            )
    })
    public Uni<AccordDTO> indexAccord(AccordDTO accord) {
        return index(accord)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to index accord {}", accord.getName(), e));
    }

    @Operation(
            summary = "Bulk index multiple accords",
            description = "Creates or updates multiple musical accords in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Accords successfully indexed",
                    content = @Content(schema = @Schema(implementation = AccordDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk indexing failed after retries"
            )
    })
    public Uni<List<AccordDTO>> bulkIndexAccords(List<AccordDTO> accords) {
        return bulkIndex(accords)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk index failed for {} accords", accords.size(), e));
    }

    @Operation(
            summary = "Get accord by name",
            description = "Retrieves a musical accord from Elasticsearch by its name"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Accord found and returned",
                    content = @Content(schema = @Schema(implementation = AccordDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Accord not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<AccordDTO> getAccord(
            @Parameter(
                    name = "name",
                    description = "Name of the accord to retrieve",
                    required = true,
                    example = "C-major"
            ) String name) {
        return getById(name)
                .onFailure().invoke(e -> log.error("Failed to fetch accord {}", name, e));
    }

    @Operation(
            summary = "Search accords by name",
            description = "Performs a search query to find accords matching the given term in their name"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = AccordDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<AccordDTO>> searchAccords(
            @Parameter(
                    name = "term",
                    description = "Search term to match against accord names",
                    required = true,
                    example = "major"
            ) String term) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match", new JsonObject()
                                .put("name", term)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search failed for term {}", term, e));
    }

    @Operation(
            summary = "Delete accord by name",
            description = "Removes a musical accord from Elasticsearch by its name. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Accord successfully deleted",
                    content = @Content(schema = @Schema(implementation = Boolean.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Accord not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or deletion failed"
            )
    })
    public Uni<Boolean> deleteAccord(
            @Parameter(
                    name = "name",
                    description = "Name of the accord to delete",
                    required = true,
                    example = "C-major"
            ) String name) {

        log.debug("Attempting to delete accord: {}", name);

        return deleteById(name)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted accord: {}", name);
                    } else {
                        log.debug("Accord not found for deletion: {}", name);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete accord {} after retries: {}", name, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for accord: {}", name);
                    return false;
                });
    }
}