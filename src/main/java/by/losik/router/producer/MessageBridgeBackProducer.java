package by.losik.router.producer;

import by.losik.router.bridge.MessageBridgeBack;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class MessageBridgeBackProducer extends BaseProducer<MessageBridgeBack> {
    @Inject
    DeploymentOptions deploymentOptions;

    @Override
    public Uni<Void> init(StartupEvent event) {
        log.info("Deploying {} archive scheduler instances", deploymentOptions.getInstances());
        return deployRouter(clazz, deploymentOptions).replaceWithVoid();
    }
}
