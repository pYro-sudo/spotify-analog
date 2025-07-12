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
@JsonPropertyOrder({"id", "name", "isAlbum", "songListId"})
@Schema(
        name = "Album",
        description = "Represents a music album or playlist container"
)
public class AlbumDTO {
    @JsonProperty("id")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Unique identifier of the album",
            required = true
    )
    private String id;

    @JsonProperty("name")
    @NonNull
    @NotBlank
    @Size(min = 5, max = 20)
    @Schema(
            description = "Display name of the album",
            required = true
    )
    private String name;

    @JsonProperty("isAlbum")
    @Schema(
            description = "True for albums, false for playlists",
            required = true
    )
    private boolean isAlbum;

    @JsonProperty("songListId")
    @NonNull
    @Size(min = 1)
    @Schema(
            description = "List of song IDs contained in this album",
            example = "[\"song-123\", \"song-456\"]",
            required = true
    )
    private List<String> songListId;
}
