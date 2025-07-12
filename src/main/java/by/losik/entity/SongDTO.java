package by.losik.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.NonNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true, allowGetters = true, allowSetters = true)
@JsonPropertyOrder({"id", "name", "userListId", "songContent", "analysisListId"})
@Schema(
        name = "Song",
        description = "Represents a musical composition"
)
public class SongDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique song identifier",
            required = true
    )
    private String id;

    @JsonProperty("name")
    @NonNull
    @NotBlank
    @Schema(
            description = "Song title",
            required = true
    )
    private String name;

    @JsonProperty("userListId")
    @NonNull
    @NotEmpty
    @Schema(
            description = "List of contributing user IDs",
            minItems = 1,
            required = true
    )
    private List<String> userListId;

    @JsonProperty("songContent")
    @NonNull
    @Schema(
            description = "Binary audio content",
            required = true
    )
    private byte[] songContent;

    @JsonProperty("analysisListId")
    @NonNull
    @Schema(
            description = "List of lyric analysis IDs",
            required = true
    )
    private List<String> analysisListId;
}