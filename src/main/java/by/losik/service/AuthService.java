package by.losik.service;

import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.reactive.messaging.Message;

@Tag(
        name = "Auth Service interface",
        description = "Interface for decorating the methods being used in the class that is used for validating, " +
                "checking the role, ensuring, that authorisation might occur")
public interface AuthService {
    Uni<Boolean> validateToken(String token);
    Uni<Boolean> hasRequiredRole(String token, String requiredRole);
    @Operation(summary = "Authorize message")
    Uni<Boolean> isAuthorized(Message<?> message);

    @Operation(summary = "Authorize operation")
    Uni<Boolean> isAuthorizedForOperation(Message<?> message, String operation);
}