package by.losik.repository;

import by.losik.entity.AnalysisDTO;
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
@Tag(name = "Analysis Repository", description = "Operations for managing text analyses in Elasticsearch")
public class AnalysisRepository extends ReactiveElasticsearchRepository<AnalysisDTO> {

    @Override
    protected String getIndexName() {
        return "analysis";
    }

    @Override
    protected String getId(AnalysisDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<AnalysisDTO> getEntityType() {
        return AnalysisDTO.class;
    }

    @Operation(
            summary = "Save a single analysis",
            description = "Creates or updates a text analysis in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Analysis successfully saved",
                    content = @Content(schema = @Schema(implementation = AnalysisDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed after retries"
            )
    })
    public Uni<AnalysisDTO> saveAnalysis(AnalysisDTO analysis) {
        return index(analysis)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to save analysis {}", analysis.getId(), e));
    }

    @Operation(
            summary = "Save multiple analyses",
            description = "Creates or updates multiple text analyses in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Analyses successfully saved",
                    content = @Content(schema = @Schema(implementation = AnalysisDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk save operation failed after retries"
            )
    })
    public Uni<List<AnalysisDTO>> saveAllAnalyses(List<AnalysisDTO> analyses) {
        return bulkIndex(analyses)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk save failed for {} analyses", analyses.size(), e));
    }

    @Operation(
            summary = "Find analysis by ID",
            description = "Retrieves a single text analysis from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Analysis found and returned",
                    content = @Content(schema = @Schema(implementation = AnalysisDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Analysis not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<AnalysisDTO> findAnalysisById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the analysis",
                    required = true,
                    example = "analysis-789"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch analysis {}", id, e));
    }

    @Operation(
            summary = "Find analyses by user ID",
            description = "Searches for analyses belonging to a specific user using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = AnalysisDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<AnalysisDTO>> findAnalysesByUserId(
            @Parameter(
                    name = "userId",
                    description = "Exact user ID to filter analyses",
                    required = true,
                    example = "user-123"
            ) String userId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("userId.keyword", userId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by userId failed for {}", userId, e));
    }

    @Operation(
            summary = "Find analyses containing lexeme type",
            description = "Searches for analyses containing specific lexeme types in their text using nested queries"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = AnalysisDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<AnalysisDTO>> findAnalysesContainingLexeme(
            @Parameter(
                    name = "lexemeType",
                    description = "Type of lexeme to search for in analysis text",
                    required = true,
                    example = "NOUN"
            ) String lexemeType) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("nested", new JsonObject()
                                .put("path", "text")
                                .put("query", new JsonObject()
                                        .put("term", new JsonObject()
                                                .put("text.value.type.keyword", lexemeType)))));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by lexeme type failed for {}", lexemeType, e));
    }

    @Operation(
            summary = "Find analyses with text length greater than",
            description = "Filters analyses by text length using Elasticsearch script queries"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = AnalysisDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<AnalysisDTO>> findAnalysesWithTextLengthGreaterThan(
            @Parameter(
                    name = "minLength",
                    description = "Minimum text length threshold (in characters)",
                    required = true,
                    example = "1000"
            ) int minLength) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("script", new JsonObject()
                                .put("script", new JsonObject()
                                        .put("source", "params.text.size() > params.minLength")
                                        .put("params", new JsonObject()
                                                .put("minLength", minLength)))));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by text length failed for minLength={}", minLength, e));
    }

    @Operation(
            summary = "Delete analyses that have the given id",
            description = "Delete analyses by id"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Delete completed successfully",
                    content = @Content(schema = @Schema(implementation = AnalysisDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or delete failed"
            )
    })
    public Uni<Boolean> deleteAnalysisById(
            @Parameter(
                    name = "analysisId",
                    description = "Id of the analysis",
                    required = true,
                    example = "analysis-aj23jfq35jdn38q"
            ) String analysisId) {
        return deleteById(analysisId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted analysis: {}", analysisId);
                    } else {
                        log.debug("Analysis not found for deletion: {}", analysisId);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete analysis {} after retries: {}", analysisId, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for analysis: {}", analysisId);
                    return false;
                });
    }
}