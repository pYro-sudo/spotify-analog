package by.losik.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Data
@JsonIgnoreProperties(ignoreUnknown = true, allowGetters = true, allowSetters = true)
@JsonPropertyOrder({"id", "userId", "payload"})
@Schema(
        name = "Log",
        description = "Represents an application log entry"
)
@NoArgsConstructor
public class MessageDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique message identifier",
            required = true
    )
    private String id;

    @JsonProperty("userId")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique user identifier",
            required = true
    )
    private String userId;

    @JsonProperty("payload")
    @NonNull
    @NotBlank
    @Size(min = 1, max = 20)
    @Schema(
            description = "Payload of the message",
            required = true
    )
    private String payload;
}
