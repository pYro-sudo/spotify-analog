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
@JsonPropertyOrder({"id", "name", "userListId"})
@Schema(
        name = "Group",
        description = "Represents a user group/collective"
)
public class GroupDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique group identifier",
            required = true
    )
    private String id;

    @JsonProperty("name")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Display name of the group",
            required = true
    )
    private String name;

    @JsonProperty("userListId")
    @NonNull
    @Size(min = 1)
    @Schema(
            description = "List of member user IDs",
            required = true
    )
    private List<String> userListId;
}
