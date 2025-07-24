package by.losik.router.bridge;

import by.losik.configuration.ProcessingResult;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.rabbitmq.IncomingRabbitMQMessage;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@Slf4j
@Dependent
public class MessageBridgeBack {

    @Inject
    @Channel("kafka-in-1")
    Emitter<JsonObject> kafkaEmitter;

    @Incoming("message-save-out")
    public Uni<Void> handleSaveResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "save", kafkaEmitter);
    }

    @Incoming("message-delete-out")
    public Uni<Void> handleDeleteResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "delete", kafkaEmitter);
    }

    @Incoming("message-get-by-id-out")
    public Uni<Void> handleGetResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg) {
        return processResult(rabbitMsg, "get", kafkaEmitter);
    }

    private Uni<Void> processResult(IncomingRabbitMQMessage<ProcessingResult> rabbitMsg,
                                    String operationType,
                                    Emitter<JsonObject> kafkaEmitter) {
        ProcessingResult result = rabbitMsg.getPayload();

        JsonObject kafkaPayload = new JsonObject()
                .put("operation", operationType)
                .put("status", result.status())
                .put("data", result)
                .put("message", result.message())
                .put("timestamp", System.currentTimeMillis());

        return Uni.createFrom().completionStage(kafkaEmitter.send(kafkaPayload))
                .onItem().invoke(() -> rabbitMsg.ack())
                .onFailure().recoverWithUni(e -> {
                    log.error("Failed to forward {} result to Kafka", operationType, e);
                    return Uni.createFrom().completionStage(rabbitMsg.nack(e));
                });
    }
}