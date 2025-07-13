package by.losik.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheResult;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.smallrye.jwt.auth.principal.JWTParser;
import io.smallrye.jwt.auth.principal.ParseException;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.util.Optional;
import java.util.Set;

@ApplicationScoped
@Slf4j
@Tag(name = "Auth Service", description = "JWT authentication and authorization service")
public class AuthServiceImpl implements AuthService {
    private static final String TOKEN_CACHE_NAME = "token-cache";
    private static final String METRIC_PREFIX = "auth.service.";

    @ConfigProperty(name = "security.jwt.issuer")
    String issuer;

    @ConfigProperty(name = "security.jwt.required-role", defaultValue = "internal")
    String defaultRequiredRole;

    @ConfigProperty(name = "auth.token.field", defaultValue = "authToken")
    String AUTH_TOKEN_FIELD;

    @Inject
    JWTParser jwtParser;

    @Inject
    JWTAuthContextInfo authContextInfo;

    @Inject
    PrometheusMeterRegistry registry;

    @Override
    @Operation(summary = "Validate JWT token",
            description = "Validates JWT token format, expiration, issuer and subject")
    @Retry(maxRetries = 2, delay = 100)
    @CircuitBreaker(
            failOn = {RuntimeException.class},
            delay = 10000,
            requestVolumeThreshold = 5,
            failureRatio = 0.6
    )
    @CacheResult(cacheName = TOKEN_CACHE_NAME)
    public Uni<Boolean> validateToken(@NotNull String token) {
        Timer.Sample sample = Timer.start(registry);
        return Uni.createFrom().item(() -> {
            try {
                if (token == null || !token.startsWith("Bearer ")) {
                    log.debug("Invalid token format");
                    registry.counter(METRIC_PREFIX + "validate.token", "result", "invalid_format").increment();
                    return false;
                }

                var jsonWebToken = jwtParser.parse(token.substring(7), authContextInfo);

                boolean isValid = jsonWebToken.getExpirationTime() != 0 &&
                        jsonWebToken.getExpirationTime() >= System.currentTimeMillis() / 1000 &&
                        jsonWebToken.getIssuer().equals(issuer) &&
                        jsonWebToken.getSubject() != null;

                if (isValid) {
                    registry.counter(METRIC_PREFIX + "validate.token", "result", "success").increment();
                } else {
                    registry.counter(METRIC_PREFIX + "validate.token", "result", "invalid").increment();
                }

                return isValid;
            } catch (ParseException e) {
                log.warn("JWT parsing failed: {}", e.getMessage());
                registry.counter(METRIC_PREFIX + "validate.token", "result", "parse_error").increment();
                return false;
            } catch (Exception e) {
                log.error("Token validation error", e);
                registry.counter(METRIC_PREFIX + "validate.token", "result", "error").increment();
                return false;
            } finally {
                sample.stop(registry.timer(METRIC_PREFIX + "validate.token.time"));
            }
        });
    }

    @Override
    @Operation(summary = "Check user role",
            description = "Verifies if the token has the required role")
    @Timeout(2000)
    public Uni<Boolean> hasRequiredRole(@NotNull String token, @NotNull String requiredRole) {
        Timer.Sample sample = Timer.start(registry);
        return validateToken(token).onItem().transformToUni(valid -> {
            try {
                if (!valid) {
                    registry.counter(METRIC_PREFIX + "check.role", "result", "invalid_token").increment();
                    return Uni.createFrom().item(false);
                }

                var jsonWebToken = jwtParser.parse(token.substring(7), authContextInfo);
                Set<String> groups = jsonWebToken.getGroups();
                boolean hasRole = groups != null && groups.contains(requiredRole);

                if (hasRole) {
                    registry.counter(METRIC_PREFIX + "check.role", "result", "success", "role", requiredRole).increment();
                } else {
                    registry.counter(METRIC_PREFIX + "check.role", "result", "missing", "role", requiredRole).increment();
                    log.debug("Missing required role: {}", requiredRole);
                }

                return Uni.createFrom().item(hasRole);
            } catch (Exception e) {
                log.error("Role validation failed", e);
                registry.counter(METRIC_PREFIX + "check.role", "result", "error").increment();
                return Uni.createFrom().item(false);
            } finally {
                sample.stop(registry.timer(METRIC_PREFIX + "check.role.time"));
            }
        });
    }

    @Override
    @Operation(summary = "Authorize message",
            description = "Validates token from RabbitMQ message headers")
    public Uni<Boolean> isAuthorized(@NotNull IncomingRabbitMQMessage<?> message) {
        Timer.Sample sample = Timer.start(registry);
        String token = extractTokenFromMessage(message);

        if (token == null) {
            registry.counter(METRIC_PREFIX + "authorize", "result", "no_token").increment();
            sample.stop(registry.timer(METRIC_PREFIX + "authorize.time"));
            return Uni.createFrom().item(false);
        }

        return validateToken(token)
                .onItem().invoke(valid -> {
                    if (valid) {
                        registry.counter(METRIC_PREFIX + "authorize", "result", "success").increment();
                    } else {
                        registry.counter(METRIC_PREFIX + "authorize", "result", "unauthorized").increment();
                    }
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.time"));
                })
                .onFailure().invoke(e -> {
                    log.error("Authorization check failed", e);
                    registry.counter(METRIC_PREFIX + "authorize", "result", "error").increment();
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.time"));
                })
                .onFailure().recoverWithItem(false);
    }

    @Override
    @Operation(summary = "Authorize operation",
            description = "Checks if token has required role for specific operation")
    public Uni<Boolean> isAuthorizedForOperation(
            @NotNull IncomingRabbitMQMessage<?> message,
            @NotNull @NotBlank String operation) {
        Timer.Sample sample = Timer.start(registry);
        String token = extractTokenFromMessage(message);

        if (token == null) {
            registry.counter(METRIC_PREFIX + "authorize.operation", "result", "no_token", "operation", operation).increment();
            sample.stop(registry.timer(METRIC_PREFIX + "authorize.operation.time", "operation", operation));
            return Uni.createFrom().item(false);
        }

        String requiredRole = getRequiredRoleForOperation(operation);
        return hasRequiredRole(token, requiredRole)
                .onItem().invoke(hasRole -> {
                    if (hasRole) {
                        registry.counter(METRIC_PREFIX + "authorize.operation", "result", "success", "operation", operation).increment();
                    } else {
                        registry.counter(METRIC_PREFIX + "authorize.operation", "result", "unauthorized", "operation", operation).increment();
                    }
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.operation.time", "operation", operation));
                })
                .onFailure().invoke(e -> {
                    log.error("Operation authorization failed for {}", operation, e);
                    registry.counter(METRIC_PREFIX + "authorize.operation", "result", "error", "operation", operation).increment();
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.operation.time", "operation", operation));
                })
                .onFailure().recoverWithItem(false);
    }

    @Scheduled(every = "30m")
    @Operation(summary = "Invalidate token cache",
            description = "Scheduled task to clear JWT token validation cache")
    @CacheInvalidate(cacheName = TOKEN_CACHE_NAME)
    public void invalidateTokenCacheEntry() {
        log.debug("Invalidating token from cache");
        registry.counter(METRIC_PREFIX + "cache.invalidation").increment();
    }

    @Operation(hidden = true)
    private String extractTokenFromMessage(@NotNull IncomingRabbitMQMessage<?> message) {
        Optional<String> headerToken = Optional.ofNullable(message.getHeaders())
                .map(headers -> headers.get("Authorization"))
                .map(Object::toString);

        if (headerToken.isPresent()) {
            return headerToken.get();
        }

        try {
            if (message.getPayload() instanceof String) {
                JsonObject payload = new JsonObject((String) message.getPayload());
                return payload.getString(AUTH_TOKEN_FIELD);
            } else if (message.getPayload() instanceof JsonObject) {
                return ((JsonObject) message.getPayload()).getString(AUTH_TOKEN_FIELD);
            }
        } catch (Exception e) {
            log.debug("Failed to extract token from message payload", e);
        }

        return null;
    }

    @Operation(hidden = true)
    private String getRequiredRoleForOperation(@NotNull String operation) {
        return switch (operation.toLowerCase()) {
            case "delete", "create", "index", "bulk-index" -> "internal";
            default -> defaultRequiredRole;
        };
    }
}