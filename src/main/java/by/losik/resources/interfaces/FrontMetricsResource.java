package by.losik.resources.interfaces;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

@RegisterRestClient(configKey = "front-metrics-api")
public interface FrontMetricsResource {
    @GET
    @Path("/front-metrics")
    @Produces(MediaType.TEXT_PLAIN)
    Uni<String> getMetrics();
}