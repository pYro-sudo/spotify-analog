package by.losik.router.producer;

import by.losik.router.router.Router;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class RouterProducer extends BaseProducer<Router> {

    @Inject
    DeploymentOptions deploymentOptions;

    @Override
    public Uni<Void> init(StartupEvent event) {
        log.info("Deploying {} router instances", deploymentOptions.getInstances());
        return deployRouter(clazz, deploymentOptions).replaceWithVoid();
    }
}