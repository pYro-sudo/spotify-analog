package by.losik.service;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@Tag(
        name = "Auth Service interface",
        description = "Interface for decorating the methods being used in the class that is used for validating, " +
                "checking the role, ensuring, that authorisation might occur")
public interface AuthService {
    Uni<Boolean> validateToken(String token);
    Uni<Boolean> hasRequiredRole(String token, String requiredRole);
    Uni<Boolean> isAuthorized(IncomingRabbitMQMessage<?> message);
    Uni<Boolean> isAuthorizedForOperation(IncomingRabbitMQMessage<?> message, String operation);
}