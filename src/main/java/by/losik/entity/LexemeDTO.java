package by.losik.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.NonNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Data
@JsonIgnoreProperties(ignoreUnknown = true, allowSetters = true, allowGetters = true)
@JsonPropertyOrder({"id", "substring", "accordId"})
@Schema(
        name = "Lexeme",
        description = "Represents a lexical unit from analyzed text"
)
public class LexemeDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique lexeme identifier",
            required = true
    )
    private String id;

    @JsonProperty("substring")
    @NonNull
    @Schema(
            description = "The actual text fragment",
            required = true
    )
    private String substring;

    @JsonProperty("accordId")
    @NonNull
    @NotBlank
    @Schema(
            description = "Reference to musical accord if applicable",
            required = true
    )
    private String accordId;
}
