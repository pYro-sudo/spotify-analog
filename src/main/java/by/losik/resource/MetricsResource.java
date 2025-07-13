package by.losik.resource;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/metrics")
public class MetricsResource {

    @Inject
    PrometheusMeterRegistry registry;

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> getMetrics() {
        log.debug("Scraping metrics");
        return Uni.createFrom().item(() -> registry.scrape())
                .onFailure().recoverWithItem(e -> {
                    log.error("Failed to scrape metrics", e);
                    return "# ERROR: Failed to scrape metrics\n";
                });
    }

    @GET
    @Path("/prometheus")
    @Produces(MediaType.TEXT_PLAIN)
    public Uni<String> getPrometheusMetrics() {
        log.debug("Scraping Prometheus metrics");
        return getMetrics();
    }
}