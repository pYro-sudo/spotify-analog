package by.losik.router.producer;

import io.smallrye.reactive.messaging.ChannelRegistry;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
@Slf4j
public class DynamicEmitterFactory {

    @Inject
    ChannelRegistry channelRegistry;

    private final Map<String, Emitter<JsonObject>> emitters = new ConcurrentHashMap<>();

    public Emitter<JsonObject> getEmitter(String channelName) {
        return emitters.computeIfAbsent(channelName, name -> channelRegistry.getEmitter(name, JsonObject.class));
    }

    public void cleanUp() {
        emitters.forEach((name, emitter) -> {
            if (emitter.isCancelled()) {
                emitters.remove(name);
                log.debug("Removed cancelled emitter for channel: {}", name);
            }
        });
    }
}