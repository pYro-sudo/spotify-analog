package by.losik.configuration;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@ApplicationScoped
@Tag(name = "Service configuration", description = "Manages services' configurations")
public class ServiceConfig {
    protected static final int CIRCUIT_BREAKER_REQUEST_VOLUME = 4;
    protected static final double CIRCUIT_BREAKER_FAILURE_RATIO = 0.5;
    protected static final long CIRCUIT_BREAKER_DELAY_MS = 10000;
    protected static final int DEFAULT_BULKHEAD_VALUE = 10;
    protected static final int BULK_OPERATION_BULKHEAD_VALUE = 5;
}
