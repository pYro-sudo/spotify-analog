package by.losik.router.producer;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.ThreadingModel;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import jakarta.ws.rs.Produces;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.lang.reflect.ParameterizedType;

@Slf4j
public abstract class BaseProducer<T> {
    @ConfigProperty(name = "verticle.instances")
    protected Integer instances;
    @ConfigProperty(name = "verticle.worker", defaultValue = "false")
    protected Boolean worker;
    protected Class<T> clazz;
    @Inject
    protected Vertx vertx;
    @SuppressWarnings("unchecked")
    protected BaseProducer() {
        this.clazz = (Class<T>) ((ParameterizedType) getClass()
                .getGenericSuperclass())
                .getActualTypeArguments()[0];
    }

    @Produces
    @ApplicationScoped
    public DeploymentOptions deploymentOptions() {
        return new DeploymentOptions()
                .setInstances(instances)
                .setWorker(worker)
                .setThreadingModel(ThreadingModel.EVENT_LOOP);
    }
    @PostConstruct
    public abstract void init(@ObservesAsync StartupEvent event);

    protected void deployRouter(Class<T> clazz, DeploymentOptions deploymentOptions) {
        vertx.deployVerticle(clazz.getName(), deploymentOptions)
                .subscribe()
                .with(
                        deploymentId -> log.info("Successfully deployed {} with deploymentId: {}",
                                clazz.getSimpleName(), deploymentId),
                        failure -> log.error("Failed to deploy {}",
                                clazz.getSimpleName(), failure)
                );
    }
}
