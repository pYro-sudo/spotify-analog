package by.losik.service;

import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheResult;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.smallrye.jwt.auth.principal.JWTParser;
import io.smallrye.jwt.auth.principal.ParseException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ExecutorService;

@ApplicationScoped
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

    @Inject
    @Named("authServiceExecutor")
    ExecutorService authServiceExecutor;

    @Override
    @Operation(summary = "Validate JWT token")
    @CacheResult(cacheName = TOKEN_CACHE_NAME)
    public Uni<Boolean> validateToken(String token) {
        return Uni.createFrom().item(() -> {
            Timer.Sample sample = Timer.start(registry);
            try {
                if (token == null || !token.startsWith("Bearer ")) {
                    registry.counter(METRIC_PREFIX + "validate.token", "result", "invalid_format").increment();
                    return false;
                }

                var jsonWebToken = jwtParser.parse(token.substring(7), authContextInfo);
                boolean isValid = jsonWebToken.getExpirationTime() != 0 &&
                        jsonWebToken.getExpirationTime() >= System.currentTimeMillis() / 1000 &&
                        jsonWebToken.getIssuer().equals(issuer) &&
                        jsonWebToken.getSubject() != null;

                String result = isValid ? "success" : "invalid";
                registry.counter(METRIC_PREFIX + "validate.token", "result", result).increment();
                return isValid;
            } catch (ParseException e) {
                registry.counter(METRIC_PREFIX + "validate.token", "result", "parse_error").increment();
                return false;
            } finally {
                sample.stop(registry.timer(METRIC_PREFIX + "validate.token.time"));
            }
        }).runSubscriptionOn(authServiceExecutor);
    }

    @Override
    @Operation(summary = "Check user role")
    public Uni<Boolean> hasRequiredRole(String token, String requiredRole) {
        return validateToken(token)
                .onItem().transformToUni(valid -> {
                    if (!valid) {
                        registry.counter(METRIC_PREFIX + "check.role", "result", "invalid_token").increment();
                        return Uni.createFrom().item(false);
                    }

                    return Uni.createFrom().item(Unchecked.supplier(() -> {
                        Timer.Sample sample = Timer.start(registry);
                        try {
                            var jsonWebToken = jwtParser.parse(token.substring(7), authContextInfo);
                            Set<String> groups = jsonWebToken.getGroups();
                            boolean hasRole = groups != null && groups.contains(requiredRole);

                            String result = hasRole ? "success" : "missing";
                            registry.counter(METRIC_PREFIX + "check.role", "result", result, "role", requiredRole).increment();
                            return hasRole;
                        } catch (Exception e) {
                            registry.counter(METRIC_PREFIX + "check.role", "result", "error").increment();
                            throw new RuntimeException("Role validation failed", e);
                        } finally {
                            sample.stop(registry.timer(METRIC_PREFIX + "check.role.time"));
                        }
                    })).runSubscriptionOn(authServiceExecutor);
                });
    }

    @Override
    @Operation(summary = "Authorize message")
    public Uni<Boolean> isAuthorized(Message<?> message) {
        String token = extractTokenFromMessage(message);
        if (token == null) {
            registry.counter(METRIC_PREFIX + "authorize", "result", "no_token").increment();
            return Uni.createFrom().item(false);
        }

        Timer.Sample sample = Timer.start(registry);
        return validateToken(token)
                .onItem().invoke(valid -> {
                    String result = valid ? "success" : "unauthorized";
                    registry.counter(METRIC_PREFIX + "authorize", "result", result).increment();
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.time"));
                })
                .onFailure().invoke(e -> {
                    registry.counter(METRIC_PREFIX + "authorize", "result", "error").increment();
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.time"));
                })
                .onFailure().recoverWithItem(false);
    }

    @Override
    @Operation(summary = "Authorize operation")
    public Uni<Boolean> isAuthorizedForOperation(Message<?> message, String operation) {
        String token = extractTokenFromMessage(message);
        if (token == null) {
            registry.counter(METRIC_PREFIX + "authorize.operation", "result", "no_token", "operation", operation).increment();
            return Uni.createFrom().item(false);
        }

        String requiredRole = getRequiredRoleForOperation(operation);
        Timer.Sample sample = Timer.start(registry);
        return hasRequiredRole(token, requiredRole)
                .onItem().invoke(hasRole -> {
                    String result = hasRole ? "success" : "unauthorized";
                    registry.counter(METRIC_PREFIX + "authorize.operation", "result", result, "operation", operation).increment();
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.operation.time", "operation", operation));
                })
                .onFailure().invoke(e -> {
                    registry.counter(METRIC_PREFIX + "authorize.operation", "result", "error", "operation", operation).increment();
                    sample.stop(registry.timer(METRIC_PREFIX + "authorize.operation.time", "operation", operation));
                })
                .onFailure().recoverWithItem(false);
    }

    private String extractTokenFromMessage(Message<?> message) {
        Headers headers = message.getMetadata(org.apache.kafka.common.header.Headers.class)
                .orElse(null);

        if (headers != null) {
            Header authHeader = headers.lastHeader("Authorization");
            if (authHeader != null) {
                return new String(authHeader.value(), StandardCharsets.UTF_8);
            }
        }

        try {
            Object payload = message.getPayload();
            if (payload instanceof JsonObject) {
                return ((JsonObject) payload).getString(AUTH_TOKEN_FIELD);
            } else if (payload instanceof String) {
                JsonObject jsonPayload = new JsonObject((String) payload);
                return jsonPayload.getString(AUTH_TOKEN_FIELD);
            }
        } catch (Exception e) {
            return null;
        }
        return null;
    }

    @Scheduled(every = "30m")
    @Operation(summary = "Invalidate token cache")
    @CacheInvalidate(cacheName = TOKEN_CACHE_NAME)
    public void invalidateTokenCacheEntry() {
        registry.counter(METRIC_PREFIX + "cache.invalidation").increment();
    }

    private String getRequiredRoleForOperation(String operation) {
        return switch (operation.toLowerCase()) {
            case "delete", "create", "index", "bulk-index" -> "internal";
            default -> defaultRequiredRole;
        };
    }
}