package by.losik.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.NonNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true, allowGetters = true, allowSetters = true)
@JsonPropertyOrder({"id", "songListId", "albumListId"})
@Schema(
        name = "User",
        description = "Represents a system user"
)
public class UserDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique user identifier",
            required = true
    )
    private String id;

    @JsonProperty("songListId")
    @NonNull
    @Schema(
            description = "List of song IDs created by user",
            required = true
    )
    private List<String> songListId;

    @JsonProperty("albumListId")
    @NonNull
    @Schema(
            description = "List of album IDs owned by user",
            required = true
    )
    private List<String> albumListId;
}
