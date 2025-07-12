package by.losik.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.NonNull;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.LinkedHashMap;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true, allowGetters = true, allowSetters = true)
@JsonPropertyOrder({"id", "userId", "text"})
@Schema(
        name = "Analysis",
        description = "Represents a linguistic analysis of song lyrics"
)
public class AnalysisDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique analysis identifier",
            required = true
    )
    private String id;

    @JsonProperty("userId")
    @NonNull
    @NotEmpty
    @Size(min = 1)
    @Schema(
            description = "List of user IDs who contributed to this analysis",
            minItems = 1,
            required = true
    )
    private List<String> userId;

    @JsonProperty("text")
    @NonNull
    @Schema(
            description = "Mapping of text positions to lexical units",
            implementation = Object.class,
            additionalProperties = LexemeDTO.class,
            required = true
    )
    private LinkedHashMap<String, LexemeDTO> text;

    @Operation(
            summary = "Adds a lexeme to the analysis",
            description = "Updates the text by adding a lexeme at the specified position"
    )
    public void addLexeme(
            @NonNull
            @Schema(description = "Position in the text", required = true)
            Integer position,

            @NonNull
            @Schema(description = "Lexical unit to add", required = true)
            LexemeDTO lexeme
    ) {
        this.text.put(position.toString(), lexeme);
    }
}