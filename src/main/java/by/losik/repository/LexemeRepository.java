package by.losik.repository;

import by.losik.entity.LexemeDTO;
import by.losik.entity.SongDTO;
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
@Tag(name = "Lexeme Repository", description = "Operations for managing linguistic lexemes in Elasticsearch")
public class LexemeRepository extends ReactiveElasticsearchRepository<LexemeDTO> {

    @Override
    protected String getIndexName() {
        return "lexemes";
    }

    @Override
    protected String getId(LexemeDTO entity) {
        return entity.getId();
    }

    @Override
    protected Class<LexemeDTO> getEntityType() {
        return LexemeDTO.class;
    }

    @Operation(
            summary = "Save a single lexeme",
            description = "Creates or updates a linguistic lexeme in Elasticsearch. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Lexeme successfully saved",
                    content = @Content(schema = @Schema(implementation = LexemeDTO.class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or save operation failed after retries"
            )
    })
    public Uni<LexemeDTO> saveLexeme(LexemeDTO lexeme) {
        return index(lexeme)
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1)).atMost(3)
                .onFailure().invoke(e -> log.error("Failed to save lexeme {}", lexeme.getId(), e));
    }

    @Operation(
            summary = "Save multiple lexemes",
            description = "Creates or updates multiple linguistic lexemes in Elasticsearch in a single request. Retries up to 3 times on failure."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Lexemes successfully saved",
                    content = @Content(schema = @Schema(implementation = LexemeDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or bulk save operation failed after retries"
            )
    })
    public Uni<List<LexemeDTO>> saveAllLexemes(List<LexemeDTO> lexemes) {
        return bulkIndex(lexemes)
                .onFailure().retry().withBackOff(Duration.ofMillis(200), Duration.ofSeconds(2)).atMost(3)
                .onFailure().invoke(e -> log.error("Bulk save failed for {} lexemes", lexemes.size(), e));
    }

    @Operation(
            summary = "Find lexeme by ID",
            description = "Retrieves a single linguistic lexeme from Elasticsearch by its unique identifier"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Lexeme found and returned",
                    content = @Content(schema = @Schema(implementation = LexemeDTO.class))
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Lexeme not found"
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or retrieval failed"
            )
    })
    public Uni<LexemeDTO> findLexemeById(
            @Parameter(
                    name = "id",
                    description = "Unique identifier of the lexeme",
                    required = true,
                    example = "lex-123"
            ) String id) {
        return getById(id)
                .onFailure().invoke(e -> log.error("Failed to fetch lexeme {}", id, e));
    }

    @Operation(
            summary = "Find lexemes by exact substring match",
            description = "Searches for lexemes containing an exact substring using match_phrase query"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = LexemeDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<LexemeDTO>> findLexemesBySubstring(
            @Parameter(
                    name = "substring",
                    description = "Exact substring to match in lexemes",
                    required = true,
                    example = "hello"
            ) String substring) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match_phrase", new JsonObject()
                                .put("substring", substring)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by substring failed for '{}'", substring, e));
    }

    @Operation(
            summary = "Find lexemes by accord ID",
            description = "Searches for lexemes associated with a specific accord using exact term matching"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = LexemeDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<LexemeDTO>> findLexemesByAccord(
            @Parameter(
                    name = "accordId",
                    description = "Exact accord ID to filter lexemes",
                    required = true,
                    example = "accord-456"
            ) String accordId) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("term", new JsonObject()
                                .put("accordId.keyword", accordId)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by accordId failed for {}", accordId, e));
    }

    @Operation(
            summary = "Find lexemes by substring prefix",
            description = "Searches for lexemes starting with the given prefix using match_phrase_prefix query"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Search completed successfully",
                    content = @Content(schema = @Schema(implementation = LexemeDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or search failed"
            )
    })
    public Uni<List<LexemeDTO>> findLexemesBySubstringPrefix(
            @Parameter(
                    name = "prefix",
                    description = "Prefix to match at the start of lexeme substrings",
                    required = true,
                    example = "hel"
            ) String prefix) {
        JsonObject query = new JsonObject()
                .put("query", new JsonObject()
                        .put("match_phrase_prefix", new JsonObject()
                                .put("substring", prefix)));

        return search(query)
                .onFailure().invoke(e -> log.error("Search by substring prefix failed for '{}'", prefix, e));
    }

    @Operation(
            summary = "Delete lexemes that have the given id",
            description = "Delete lexemes by id"
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Delete completed successfully",
                    content = @Content(schema = @Schema(implementation = LexemeDTO[].class))
            ),
            @APIResponse(
                    responseCode = "400",
                    description = "Bad request or delete failed"
            )
    })
    public Uni<Boolean> deleteLexemeById(
            @Parameter(
                    name = "lexemeId",
                    description = "Id of the lexeme",
                    required = true,
                    example = "lexeme-aj23jfq35jdt38q"
            ) String lexemeId) {
        return deleteById(lexemeId)
                .onItem().invoke(deleted -> {
                    if (deleted) {
                        log.debug("Successfully deleted lexeme: {}", lexemeId);
                    } else {
                        log.debug("Lexeme not found for deletion: {}", lexemeId);
                    }
                })
                .onFailure().retry()
                .withBackOff(Duration.ofMillis(100), Duration.ofSeconds(1))
                .atMost(3)
                .onFailure().invoke(e ->
                        log.error("Failed to delete lexeme {} after retries: {}", lexemeId, e.getMessage()))
                .onFailure().recoverWithItem(e -> {
                    log.warn("Returning false after delete failure for lexeme: {}", lexemeId);
                    return false;
                });
    }
}