package by.losik.configuration;

import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Tag(name = "Status", description = "Includes all the possible states of the message handle result")
public enum Status {
    SUCCESS,
    ERROR
}
