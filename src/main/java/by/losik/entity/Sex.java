package by.losik.entity;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Enumeration representing user gender")
public enum Sex {

    @Schema(description = "Female gender", example = "FEMALE")
    FEMALE,

    @Schema(description = "Male gender", example = "MALE")
    MALE
}