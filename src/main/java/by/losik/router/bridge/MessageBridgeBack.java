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
public class MessageBridgeBack {
    @ConfigProperty(name = "proxy.reply.address", defaultValue = "proxy.reply.address")
    String proxyReplyAddressBack;
    @Inject
    EventBus eventBus;

    @Incoming("outer-out")
    public CompletionStage<Void> processKafkaMessage(Message<JsonObject> kafkaMessage) {
        return Uni.createFrom().item(kafkaMessage.getPayload())
                .onItem().transformToUni(payload -> {
                    String proxyReplyAddress = payload.getString(proxyReplyAddressBack);

                    if (proxyReplyAddress == null) {
                        log.warn("Message missing proxyReplyAddress: {}", payload);
                        return Uni.createFrom().completionStage(kafkaMessage.nack(
                                new IllegalArgumentException("Missing proxyReplyAddress")));
                    }

                    JsonObject response = buildResponse(payload);

                    return eventBus.<JsonObject>request(proxyReplyAddress, response)
                            .onItem().transformToUni(reply -> {
                                log.debug("Successfully delivered to Proxy at {}", proxyReplyAddress);
                                return Uni.createFrom().completionStage(kafkaMessage.ack());
                            })
                            .onFailure().recoverWithUni(e -> {
                                log.error("Failed to deliver to Proxy at {}", proxyReplyAddress, e);
                                return Uni.createFrom().completionStage(kafkaMessage.nack(e));
                            });
                })
                .onFailure().recoverWithUni(e -> {
                    log.error("Message processing failed", e);
                    return Uni.createFrom().completionStage(kafkaMessage.nack(e));
                })
                .subscribeAsCompletionStage();
    }

    private JsonObject buildResponse(JsonObject payload) {
        return new JsonObject()
                .put("aggregateId", payload.getString("aggregateId"))
                .put("operation", payload.getString("operation"))
                .put("status", payload.getString("status"))
                .put("data", payload.getJsonObject("data"))
                .put("processedAt", System.currentTimeMillis())
                .put("originalMessageId", payload.getLong("id"));
    }
}