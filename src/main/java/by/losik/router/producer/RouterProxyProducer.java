package by.losik.router.producer;

import by.losik.router.proxy.RouterProxy;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.DeploymentOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class RouterProxyProducer extends BaseProducer<RouterProxy> {
    @Inject
    DeploymentOptions deploymentOptions;

    @Override
    public void init(StartupEvent event) {
        log.info("Deploying {} router proxy instances", deploymentOptions.getInstances());
        deployRouter(clazz, deploymentOptions);
    }
}