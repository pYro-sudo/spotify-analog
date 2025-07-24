package by.losik.resource;

import by.losik.entity.Status;
import by.losik.service.AuthService;
import by.losik.service.MessageService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;

import java.sql.Timestamp;

@Path("/messages")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@ApplicationScoped
@SecurityRequirement(name = "Bearer Authentication")
public class MessageResource {

    @Inject
    MessageService messageService;

    @Inject
    AuthService authService;

    @POST
    public Uni<Response> createMessage(
            @HeaderParam("Authorization") String authHeader,
            @QueryParam("aggregateId") String aggregateId,
            @QueryParam("eventType") String eventType,
            @QueryParam("payload") String payload) {
        return authService.hasRequiredRole(authHeader, "internal")
                .onItem().transformToUni(hasRole -> {
                    if (!hasRole) {
                        return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
                    }
                    return messageService.createMessage(aggregateId, eventType, payload)
                            .map(message -> Response.status(Response.Status.CREATED).entity(message).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @GET
    @Path("/{id}")
    public Uni<Response> getMessageById(
            @HeaderParam("Authorization") String authHeader,
            @PathParam("id") Long id) {
        return authService.validateToken(authHeader)
                .onItem().transformToUni(valid -> {
                    if (!valid) {
                        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
                    }
                    return messageService.getMessageById(id)
                            .map(message -> message != null
                                    ? Response.ok(message).build()
                                    : Response.status(Response.Status.NOT_FOUND).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @GET
    public Uni<Response> getAllMessages(
            @HeaderParam("Authorization") String authHeader) {
        return authService.validateToken(authHeader)
                .onItem().transformToUni(valid -> {
                    if (!valid) {
                        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
                    }
                    return messageService.getAllMessages()
                            .map(messages -> Response.ok(messages).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @GET
    @Path("/status/{status}")
    public Uni<Response> getMessagesByStatus(
            @HeaderParam("Authorization") String authHeader,
            @PathParam("status") Status status) {
        return authService.validateToken(authHeader)
                .onItem().transformToUni(valid -> {
                    if (!valid) {
                        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
                    }
                    return messageService.getMessagesByStatus(status)
                            .map(messages -> Response.ok(messages).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @GET
    @Path("/event/{eventType}")
    public Uni<Response> getMessagesByEventType(
            @HeaderParam("Authorization") String authHeader,
            @PathParam("eventType") String eventType) {
        return authService.validateToken(authHeader)
                .onItem().transformToUni(valid -> {
                    if (!valid) {
                        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
                    }
                    return messageService.getMessagesByEventType(eventType)
                            .map(messages -> Response.ok(messages).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @PUT
    @Path("/{id}/status")
    public Uni<Response> updateMessageStatus(
            @HeaderParam("Authorization") String authHeader,
            @PathParam("id") Long id,
            @QueryParam("status") Status newStatus) {
        return authService.hasRequiredRole(authHeader, "internal")
                .onItem().transformToUni(hasRole -> {
                    if (!hasRole) {
                        return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
                    }
                    return messageService.updateMessageStatus(id, newStatus)
                            .map(message -> message != null
                                    ? Response.ok(message).build()
                                    : Response.status(Response.Status.NOT_FOUND).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @DELETE
    @Path("/{id}")
    public Uni<Response> deleteMessage(
            @HeaderParam("Authorization") String authHeader,
            @PathParam("id") Long id) {
        return authService.hasRequiredRole(authHeader, "internal")
                .onItem().transformToUni(hasRole -> {
                    if (!hasRole) {
                        return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
                    }
                    return messageService.deleteMessage(id)
                            .map(deleted -> deleted
                                    ? Response.noContent().build()
                                    : Response.status(Response.Status.NOT_FOUND).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @GET
    @Path("/aggregate/{aggregateId}")
    public Uni<Response> getMessagesByAggregateId(
            @HeaderParam("Authorization") String authHeader,
            @PathParam("aggregateId") String aggregateId) {
        return authService.validateToken(authHeader)
                .onItem().transformToUni(valid -> {
                    if (!valid) {
                        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
                    }
                    return messageService.getMessagesByAggregateId(aggregateId)
                            .map(messages -> Response.ok(messages).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @GET
    @Path("/payload")
    public Uni<Response> getMessagesByPayloadField(
            @HeaderParam("Authorization") String authHeader,
            @QueryParam("jsonPath") String jsonPath,
            @QueryParam("value") String value) {
        return authService.validateToken(authHeader)
                .onItem().transformToUni(valid -> {
                    if (!valid) {
                        return Uni.createFrom().item(Response.status(Response.Status.UNAUTHORIZED).build());
                    }
                    if (jsonPath == null || value == null) {
                        return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                                .entity("Both jsonPath and value parameters are required").build());
                    }
                    return messageService.getMessagesByPayloadField(jsonPath, value)
                            .map(messages -> Response.ok(messages).build())
                            .onFailure().recoverWithItem(throwable ->
                                    Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                            .entity(throwable.getMessage()).build());
                });
    }

    @DELETE
    @Path("/old")
    public Uni<Response> deleteMessagesOlderThan(
            @HeaderParam("Authorization") String authHeader,
            @QueryParam("cutoffDate") String cutoffDateStr) {
        return authService.hasRequiredRole(authHeader, "internal")
                .onItem().transformToUni(hasRole -> {
                    if (!hasRole) {
                        return Uni.createFrom().item(Response.status(Response.Status.FORBIDDEN).build());
                    }
                    try {
                        Timestamp cutoffDate = Timestamp.valueOf(cutoffDateStr);
                        return messageService.deleteMessagesOlderThan(cutoffDate)
                                .map(count -> Response.ok()
                                        .entity(String.format("Deleted %d messages", count))
                                        .build())
                                .onFailure().recoverWithItem(throwable ->
                                        Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                .entity(throwable.getMessage()).build());
                    } catch (IllegalArgumentException e) {
                        return Uni.createFrom().item(Response.status(Response.Status.BAD_REQUEST)
                                .entity("Invalid cutoffDate format. Use yyyy-MM-dd HH:mm:ss.SSS").build());
                    }
                });
    }
}