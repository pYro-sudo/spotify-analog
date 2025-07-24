package by.losik.service;

import io.smallrye.mutiny.Uni;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Tag(name = "Auth Service", description = "JWT authentication and authorization service")
public interface AuthService {
    @Operation(summary = "Validate JWT token",
            description = "Validates JWT token format, expiration, issuer and subject")
    Uni<Boolean> validateToken(@NotNull String token);

    @Operation(summary = "Check user role",
            description = "Verifies if the token has the required role")
    Uni<Boolean> hasRequiredRole(@NotNull String token, @NotNull String requiredRole);

    @Operation(summary = "Authorize HTTP request",
            description = "Validates token from HTTP Authorization header")
    Uni<Boolean> isAuthorized(@NotNull String authHeader);

    @Operation(summary = "Authorize operation",
            description = "Checks if token has required role for specific operation")
    Uni<Boolean> isAuthorizedForOperation(
            @NotNull String authHeader,
            @NotNull @NotBlank String operation);
}