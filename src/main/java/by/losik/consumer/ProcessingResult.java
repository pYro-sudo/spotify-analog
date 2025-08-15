package by.losik.consumer;

import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Tag(name = "Processing result",
        description = "Represents the entity that holds the result of the of the message handle")
public record ProcessingResult(
        String name,
        Status status,
        String message
) {}
