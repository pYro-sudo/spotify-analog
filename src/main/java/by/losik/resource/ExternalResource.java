package by.losik.resource;

import by.losik.configuration.Status;
import by.losik.entity.Message;
import by.losik.service.AuthServiceImpl;
import by.losik.service.MessageService;
import io.quarkus.qute.Location;
import io.smallrye.faulttolerance.api.RateLimit;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.faulttolerance.*;

import java.sql.Timestamp;
import java.util.UUID;

@Path("/spotify-ripoff")
@ApplicationScoped
public class ExternalResource {
    @ConfigProperty(name = "inbound.address", defaultValue = "resource.proxy")
    String INBOUND_ADDRESS;
    @Inject
    EventBus eventBus;
    @Inject
    AuthServiceImpl authService;
    @Inject
    MessageService messageService;
    @Inject
    @Location("message-form.html")
    io.quarkus.qute.Template messageForm;

    @GET
    @Path("/messages/form")
    @Produces(MediaType.TEXT_HTML)
    @Timeout(5000)
    @Retry(maxRetries = 2)
    @RateLimit(value = 10)
    public Uni<String> showMessageForm() {
        return Uni.createFrom().item(messageForm.render());
    }

    @POST
    @Path("/messages")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.APPLICATION_JSON)
    @Timeout(10000)
    @Retry(maxRetries = 3)
    @Bulkhead(50)
    @RateLimit(value = 100)
    public Uni<Response> handleFormMessage(
            @HeaderParam("Authorization") String authHeader,
            @FormParam("aggregateId") String aggregateId,
            @FormParam("eventType") String eventType,
            @FormParam("payload") String payload) {

        JsonObject requestJson = new JsonObject()
                .put("aggregateId", aggregateId != null && !aggregateId.isEmpty() ? aggregateId : UUID.randomUUID().toString())
                .put("eventType", eventType)
                .put("payload", payload);

        return handleJsonMessage(authHeader, requestJson.encode());
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Timeout(10000)
    @Retry(maxRetries = 3)
    @Bulkhead(50)
    @RateLimit(value = 100)
    @CircuitBreaker(
            requestVolumeThreshold = 20,
            failureRatio = 0.5,
            delay = 10000,
            successThreshold = 5
    )
    public Uni<Response> handleJsonMessage(
            @HeaderParam("Authorization") String authHeader,
            String requestBody) {

        return authService.validateToken(authHeader)
                .onItem().transformToUni(isValid -> {
                    if (!isValid) {
                        return Uni.createFrom().item(
                                Response.status(401).entity("Invalid or missing token").build()
                        );
                    }

                    JsonObject requestJson = new JsonObject(requestBody);
                    Message message = new Message();
                    message.setAggregateId(requestJson.getString("aggregateId", UUID.randomUUID().toString()));
                    message.setEventType(requestJson.getString("eventType", "default"));
                    message.setPayload(requestJson.getString("payload", "{}"));
                    message.setStatus(Status.PENDING);
                    message.setCreatedAt(new Timestamp(System.currentTimeMillis()));

                    return messageService.createMessage(
                                    message.getAggregateId(),
                                    message.getEventType(),
                                    message.getPayload()
                            )
                            .onItem().transformToUni(savedMessage -> {
                                JsonObject messageToSend = new JsonObject()
                                        .put("id", savedMessage.getId())
                                        .put("aggregateId", savedMessage.getAggregateId())
                                        .put("eventType", savedMessage.getEventType())
                                        .put("payload", savedMessage.getPayload())
                                        .put("status", savedMessage.getStatus().name())
                                        .put("createdAt", savedMessage.getCreatedAt().toString());

                                return eventBus.<JsonObject>request(INBOUND_ADDRESS, messageToSend)
                                        .onItem().transform(reply ->
                                                Response.ok(reply.body()).build()
                                        );
                            });
                })
                .onFailure().recoverWithItem(failure ->
                        Response.status(500).entity(failure.getMessage()).build()
                );
    }
}