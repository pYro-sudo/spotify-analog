package by.losik.configuration;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.vault.VaultKVSecretEngine;
import io.smallrye.jwt.config.JWTAuthContextInfoProvider;
import io.smallrye.jwt.util.KeyUtils;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

import java.security.PublicKey;
import java.util.Optional;

@ApplicationScoped
@Slf4j
@Tag(name = "JWT configuration", description = "Manages JWT keys retrieval from Vault or configuration")
public class JWTConfiguration {

    @ConfigProperty(name = "mp.jwt.verify.publickey")
    Optional<String> publicKey;

    @ConfigProperty(name = "mp.jwt.verify.issuer")
    Optional<String> issuer;

    @ConfigProperty(name = "quarkus.vault.secret-config-kv-path")
    Optional<String> vaultPath;

    @Inject
    VaultKVSecretEngine kvSecretEngine;

    private String resolvedPublicKey;

    void onStart(@Observes StartupEvent ev) {
        initializePublicKey();
    }

    @Scheduled(every = "{jwt.key.refresh.interval}")
    void refreshKey() {
        if (vaultPath.isPresent()) {
            loadKeyFromVault();
        }
    }

    private void initializePublicKey() {
        try {
            if (publicKey.isEmpty()) {
                loadKeyFromVault();
            } else {
                useConfiguredKey();
            }
            validateKey();
        } catch (Exception e) {
            log.error("Failed to initialize JWT public key", e);
            throw new IllegalStateException("JWT public key initialization failed", e);
        }
    }

    private void loadKeyFromVault() {
        try {
            String path = vaultPath.orElseThrow(() ->
                    new IllegalStateException("Vault path not configured"));
            String newKey = kvSecretEngine.readSecret(path).toString();

            if (newKey == null || newKey.isBlank()) {
                throw new IllegalStateException("Public key loaded from Vault is empty");
            }

            resolvedPublicKey = newKey;
            log.debug("Successfully refreshed public key from Vault at path: {}", path);
        } catch (Exception e) {
            log.error("Failed to load public key from Vault at path: {}", vaultPath, e);
            if (resolvedPublicKey == null) {
                throw new IllegalStateException("No valid public key available", e);
            }
        }
    }

    private void useConfiguredKey() {
        resolvedPublicKey = publicKey.orElseThrow();
        log.info("Using configured public key from mp.jwt.verify.publickey");
    }

    private void validateKey() {
        try {
            KeyUtils.decodePublicKey(resolvedPublicKey);
        } catch (Exception e) {
            throw new IllegalStateException("Invalid public key format", e);
        }
    }

    @Produces
    JWTAuthContextInfoProvider getAuthContextInfo() {
        if (resolvedPublicKey == null) {
            throw new IllegalStateException("JWT public key not initialized");
        }

        JWTAuthContextInfoProvider provider = new JWTAuthContextInfoProvider();
        provider.getContextInfo().setPublicKeyContent(resolvedPublicKey);
        provider.getContextInfo().setIssuedBy(issuer.orElse("default-issuer"));
        return provider;
    }

    @Produces
    PublicKey getPublicKey() {
        if (resolvedPublicKey == null) {
            throw new IllegalStateException("JWT public key not initialized");
        }

        try {
            return KeyUtils.decodePublicKey(resolvedPublicKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to decode public key. Key content: " +
                    resolvedPublicKey.substring(0, Math.min(20, resolvedPublicKey.length())) + "...", e);
        }
    }
}