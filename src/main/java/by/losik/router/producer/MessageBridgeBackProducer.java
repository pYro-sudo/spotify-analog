package by.losik.router.producer;

import by.losik.router.bridge.MessageBridgeBack;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.DeploymentOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class MessageBridgeBackProducer extends BaseProducer<MessageBridgeBack> {
    @Inject
    DeploymentOptions deploymentOptions;

    @Override
    public void init(@ObservesAsync StartupEvent event) {
        log.info("Initializing Elasticsearch Router Producer");
        deployRouter(clazz, deploymentOptions);
    }
}
