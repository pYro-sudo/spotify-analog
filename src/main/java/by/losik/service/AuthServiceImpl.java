package by.losik.service;

import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.quarkus.cache.CacheInvalidate;
import io.quarkus.cache.CacheResult;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.jwt.auth.principal.JWTAuthContextInfo;
import io.smallrye.jwt.auth.principal.JWTParser;
import io.smallrye.jwt.auth.principal.ParseException;
import io.smallrye.jwt.build.Jwt;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.jwt.Claims;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.time.Duration;
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

    @ConfigProperty(name = "expiration", defaultValue = "3600")
    Integer expiration;

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
    @Operation(summary = "Authorize HTTP request",
            description = "Validates token from HTTP Authorization header")
    public Uni<Boolean> isAuthorized(@NotNull String authHeader) {
        Timer.Sample sample = Timer.start(registry);

        if (authHeader == null || authHeader.isBlank()) {
            registry.counter(METRIC_PREFIX + "authorize", "result", "no_token").increment();
            sample.stop(registry.timer(METRIC_PREFIX + "authorize.time"));
            return Uni.createFrom().item(false);
        }

        return validateToken(authHeader)
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
            @NotNull String authHeader,
            @NotNull @NotBlank String operation) {
        Timer.Sample sample = Timer.start(registry);

        if (authHeader == null || authHeader.isBlank()) {
            registry.counter(METRIC_PREFIX + "authorize.operation", "result", "no_token", "operation", operation).increment();
            sample.stop(registry.timer(METRIC_PREFIX + "authorize.operation.time", "operation", operation));
            return Uni.createFrom().item(false);
        }

        String requiredRole = getRequiredRoleForOperation(operation);
        return hasRequiredRole(authHeader, requiredRole)
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
    private String getRequiredRoleForOperation(@NotNull String operation) {
        return switch (operation.toLowerCase()) {
            case "delete", "create", "index", "bulk-index" -> "internal";
            default -> defaultRequiredRole;
        };
    }

//    public Uni<String> login(String username, String password) {
//        log.info("Attempting login for user: {}", username);
//        return userService.findByUsername(username)
//                .onItem().transformToUni(users -> {
//                    log.info("Found users: {}", users.size());
//                    if (users.isEmpty()) {
//                        log.error("No user found with username: {}", username);
//                        return Uni.createFrom().failure(new SecurityException("Invalid credentials"));
//                    }
//                    UserDTO user = users.get(0);
//                    log.info("Comparing passwords - input: {}, stored: {}", password, user.getPassword());
//                    if (!user.getPassword().equals(password)) {
//                        log.error("Password mismatch for user: {}", username);
//                        return Uni.createFrom().failure(new SecurityException("Invalid credentials"));
//                    }
//                    return generateToken(user);
//                });
//    }

//    public Uni<UserDTO> registerUser(String username, String password) {
//        UserDTO newUser = new UserDTO();
//        newUser.setUsername(username);
//        newUser.setPassword(password);
//        newUser.setWins(0L);
//        newUser.setLosses(0L);
//        newUser.setRole("USER");
//        return userService.insert(List.of(newUser))
//                .onItem().transform(ignore -> newUser);
//    }

//    public Uni<UserDTO> registerAdmin(String username, String password) {
//        UserDTO newUser = new UserDTO();
//        newUser.setUsername(username);
//        newUser.setPassword(password);
//        newUser.setWins(0L);
//        newUser.setLosses(0L);
//        newUser.setRole("ADMIN");
//
//        return userService.insert(List.of(newUser))
//                .onItem().transform(ignore -> newUser);
//    }

    private Uni<String> generateToken(Set<String> roles, String username, String userId) {

        return Uni.createFrom().item(Jwt.issuer(issuer)
                .upn(username)
                .groups(roles)
                .claim(Claims.sub, userId)
                .expiresIn(Duration.ofSeconds(expiration))
                .sign());
    }
}