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
public class ElasticsearchRouter extends BaseRouter {

    @Inject
    DynamicEmitterFactory emitterFactory;

    private static final Map<String, String> CHANNEL_MAPPINGS = Map.of(
            "elasticsearch-in-1", "elasticsearch-out-1",
            "elasticsearch-in-2", "elasticsearch-out-2",
            "logs-in", "logs-out",
            "metrics-in", "metrics-out"
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

    @Incoming("elasticsearch-in-1")
    public Uni<Void> processElasticsearch1(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "elasticsearch-in-1");
    }

    @Incoming("elasticsearch-in-2")
    public Uni<Void> processElasticsearch2(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "elasticsearch-in-2");
    }

    @Incoming("logs-in")
    public Uni<Void> processLogs(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "logs-in");
    }

    @Incoming("metrics-in")
    public Uni<Void> processMetrics(Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "metrics-in");
    }

    @Override
    public void stop() {
        emitterFactory.cleanUp();
        super.stop();
    }
}