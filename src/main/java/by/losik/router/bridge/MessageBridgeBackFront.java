package by.losik.router.bridge;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletionStage;

@Dependent
@Slf4j
public class MessageBridgeBackFront {
    @ConfigProperty(name = "proxy.response.address", defaultValue = "proxy.responses")
    String proxyResponseAddress;

    @Inject
    EventBus eventBus;

    @Incoming("outer-out")
    @Incoming("another-outer-out")
    @Incoming("message-save-result")
    @Incoming("message-delete-result")
    @Incoming("message-get-result")
    @Incoming("message-bulk-save-result")
    @Incoming("message-get-by-user-result")
    @Incoming("message-search-result")
    public CompletionStage<Void> processKafkaMessage(Message<JsonObject> kafkaMessage) {
        JsonObject payload = kafkaMessage.getPayload();
        String originalReplyAddress = payload.getString("originalReplyAddress");

        if (originalReplyAddress == null) {
            log.error("Missing originalReplyAddress in message: {}", payload);
            return kafkaMessage.nack(new IllegalArgumentException("Missing originalReplyAddress"));
        }

        JsonObject response = new JsonObject()
                .put("originalReplyAddress", originalReplyAddress)
                .put("aggregateId", payload.getString("aggregateId"))
                .put("operation", payload.getString("operation"))
                .put("status", payload.getString("status"))
                .put("data", payload.getJsonObject("data"))
                .put("processedAt", System.currentTimeMillis())
                .put("originalMessageId", payload.getLong("id"));

        return eventBus.<JsonObject>request(proxyResponseAddress, response)
                .onItem().invoke(() -> log.debug("Response forwarded to proxy"))
                .onFailure().invoke(e -> log.error("Failed to forward response to proxy", e))
                .onItem().transformToUni(unused -> Uni.createFrom().completionStage(kafkaMessage.ack()))
                .onFailure().recoverWithUni(e -> Uni.createFrom().completionStage(kafkaMessage.nack(e)))
                .subscribeAsCompletionStage();
    }
}