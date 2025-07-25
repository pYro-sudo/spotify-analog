package by.losik.router.router;

import by.losik.router.producer.DynamicEmitterFactory;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.Map;

@Dependent
@Slf4j
@Retry(maxRetries = 4, delay = 1000)
@Timeout(value = 30000)
@CircuitBreaker(
        requestVolumeThreshold = 4,
        failureRatio = 0.4,
        delay = 10
)
public class PostgresRouter extends BaseRouter {

    @Inject
    DynamicEmitterFactory emitterFactory;

    private static final Map<String, String> CHANNEL_MAPPINGS = Map.of(
            "user-create-in", "user-create-out",
            "user-get-by-id-in", "user-get-by-id-out",
            "user-get-all-in", "user-get-all-out",
            "user-update-email-in", "user-update-email-out",
            "user-verify-in", "user-verify-out",
            "user-delete-in", "user-delete-out",
            "user-get-by-gender-in", "user-get-by-gender-out",
            "user-get-by-age-in", "user-get-by-age-out",
            "user-get-by-verification-in", "user-get-by-verification-out"
    );

    @Override
    public void start() throws Exception {
        super.start();

        CHANNEL_MAPPINGS.forEach((in, out) -> {
            addChannelMapping(in, out);
            registerEmitter(out, emitterFactory.getEmitter(out));
            log.info("Registered channel mapping: {} -> {}", in, out);
        });
    }

    @Incoming("user-create-in")
    public Uni<Void> processUserCreate(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-create-in");
    }

    @Incoming("user-get-by-id-in")
    public Uni<Void> processUserGetById(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-get-by-id-in");
    }

    @Incoming("user-get-all-in")
    public Uni<Void> processUserGetAll(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-get-all-in");
    }

    @Incoming("user-update-email-in")
    public Uni<Void> processUserUpdateEmail(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-update-email-in");
    }

    @Incoming("user-verify-in")
    public Uni<Void> processUserVerify(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-verify-in");
    }

    @Incoming("user-delete-in")
    public Uni<Void> processUserDelete(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-delete-in");
    }

    @Incoming("user-get-by-gender-in")
    public Uni<Void> processUserGetByGender(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-get-by-gender-in");
    }

    @Incoming("user-get-by-age-in")
    public Uni<Void> processUserGetByAge(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-get-by-age-in");
    }

    @Incoming("user-get-by-verification-in")
    public Uni<Void> processUserGetByVerification(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "user-get-by-verification-in");
    }

    @Override
    public void stop() {
        emitterFactory.cleanUp();
        super.stop();
    }
}
