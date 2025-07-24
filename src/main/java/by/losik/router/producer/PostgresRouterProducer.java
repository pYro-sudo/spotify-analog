package by.losik.router.producer;

import by.losik.router.router.PostgresRouter;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.DeploymentOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class PostgresRouterProducer extends BaseProducer<PostgresRouter> {
    @Inject
    DeploymentOptions deploymentOptions;

    @Override
    public void init(@ObservesAsync StartupEvent event) {
        log.info("Initializing Postgres Router Producer");
        deployRouter(clazz, deploymentOptions);
    }
}