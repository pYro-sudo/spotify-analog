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
@JsonPropertyOrder({"name", "picList"})
@Schema(
        name = "Accord",
        description = "Represents a musical accord/chord with its visual representations"
)
public class AccordDTO {
    @JsonProperty("name")
    @NonNull
    @NotBlank
    @Size(max = 20, min = 5)
    @Schema(
            description = "Unique identifier of the accord",
            example = "A#",
            required = true
    )
    private String name;

    @JsonProperty("picList")
    @NonNull
    @Size(min = 1)
    @Schema(
            description = "List of binary images showing how to play the chord",
            example = "[\"JVBERi0xLjQK...\", \"iVBORw0KGgoA...\"]"
    )
    private List<byte[]> picList;
}
