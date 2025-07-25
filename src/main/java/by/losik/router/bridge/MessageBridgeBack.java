package by.losik.router.bridge;

import by.losik.configuration.ProcessingResult;
import by.losik.router.producer.DynamicEmitterFactory;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@Slf4j
@Dependent
public class MessageBridgeBack {

    @Inject
    DynamicEmitterFactory emitterFactory;

    @Incoming("message-save-out")
    public Uni<Void> handleSaveResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "save", "message-save-result");
    }

    @Incoming("message-delete-out")
    public Uni<Void> handleDeleteResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "delete", "message-delete-result");
    }

    @Incoming("message-get-by-id-out")
    public Uni<Void> handleGetResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "get", "message-get-result");
    }

    @Incoming("message-bulk-save-out")
    public Uni<Void> handleBulkSaveResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "bulk-save", "message-bulk-save-result");
    }

    @Incoming("message-get-by-user-out")
    public Uni<Void> handleGetByUserResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "get-by-user", "message-get-by-user-result");
    }

    @Incoming("message-search-out")
    public Uni<Void> handleSearchResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "search", "message-search-result");
    }

    private Uni<Void> processResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg,
                                    String operationType,
                                    String resultChannel) {
        ProcessingResult result = rabbitMsg.getPayload();

        JsonObject kafkaPayload = new JsonObject()
                .put("operation", operationType)
                .put("status", result.status())
                .put("data", result)
                .put("message", result.message())
                .put("timestamp", System.currentTimeMillis());

        return Uni.createFrom().completionStage(
                        emitterFactory.getEmitter(resultChannel).send(kafkaPayload)
                )
                .onItem().invoke(() -> {
                    log.debug("Successfully forwarded {} result to channel {}", operationType, resultChannel);
                    rabbitMsg.ack();
                })
                .onFailure().recoverWithUni(e -> {
                    log.error("Failed to forward {} result to channel {}", operationType, resultChannel, e);
                    return Uni.createFrom().completionStage(rabbitMsg.nack(e));
                });
    }
}