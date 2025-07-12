package by.losik.router.producer;

import by.losik.router.scheduler.RetryScheduler;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.DeploymentOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class RetrySchedulerProducer extends BaseProducer<RetryScheduler> {
    @Inject
    DeploymentOptions deploymentOptions;

    @Override
    public void init(StartupEvent event) {
        log.info("Deploying {} retry scheduler instances", deploymentOptions.getInstances());
        deployRouter(clazz, deploymentOptions);
    }
}
