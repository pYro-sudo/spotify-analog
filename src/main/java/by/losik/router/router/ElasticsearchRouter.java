package by.losik.router.router;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import org.eclipse.microprofile.faulttolerance.Timeout;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;

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
    @Channel("elasticsearch-out-1")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    Emitter<JsonObject> elasticsearchEmitter1;

    @Inject
    @Channel("elasticsearch-out-2")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 500)
    Emitter<JsonObject> elasticsearchEmitter2;

    @Override
    public void start() throws Exception {
        super.start();
        registerEmitter("elasticsearch-out-1", elasticsearchEmitter1);
        registerEmitter("elasticsearch-out-2", elasticsearchEmitter2);

        addChannelMapping("elasticsearch-in-1", "elasticsearch-out-1");
        addChannelMapping("elasticsearch-in-2", "elasticsearch-out-2");
    }

    @Incoming("elasticsearch-in-1")
    public Uni<Void> processElasticsearchMessage1(org.eclipse.microprofile.reactive.messaging.Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "elasticsearch-in-1");
    }

    @Incoming("elasticsearch-in-2")
    public Uni<Void> processElasticsearchMessage2(org.eclipse.microprofile.reactive.messaging.Message<ConsumerRecords<String, JsonObject>> batch) {
        return processMessage(batch, "elasticsearch-in-2");
    }
}