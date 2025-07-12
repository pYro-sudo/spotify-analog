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

import java.time.Instant;
import java.time.LocalDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true, allowGetters = true, allowSetters = true)
@JsonPropertyOrder({"id", "log", "date"})
@Schema(
        name = "Log",
        description = "Represents an application log entry"
)
@NoArgsConstructor
public class LogDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Log entry identifier",
            required = true
    )
    private String id;

    @JsonProperty("log")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "The log message content",
            example = "User login failed: invalid credentials",
            required = true
    )
    private String log;

    @JsonProperty("date")
    @NonNull
    @Schema(
            description = "The log message date",
            required = true
    )
    private LocalDateTime date = LocalDateTime.from(Instant.now());
}
