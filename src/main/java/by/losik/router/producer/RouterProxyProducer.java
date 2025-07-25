package by.losik.router.producer;

import by.losik.router.proxy.RouterProxy;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
@Slf4j
public class RouterProxyProducer extends BaseProducer<RouterProxy> {
    @ConfigProperty(name = "router.instances", defaultValue = "4")
    Integer routerInstances;

    @Override
    public Uni<Void> init(@Observes StartupEvent ev) {
        log.info("Deploying {} sharded router proxies", routerInstances);

        DeploymentOptions options = deploymentOptions()
                .setInstances(routerInstances)
                .setConfig(new JsonObject()
                        .put("router.instances", routerInstances));

        return deployRouter(clazz, options)
                .onItem().invoke(id -> log.info("Deployed sharded proxy {}", id))
            .replaceWithVoid();
    }
}