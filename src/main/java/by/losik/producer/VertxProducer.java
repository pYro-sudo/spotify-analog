package by.losik.producer;

import by.losik.configuration.MutinyVertx;
import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import lombok.extern.slf4j.Slf4j;

@ApplicationScoped
@Slf4j
public class VertxProducer {
    @Produces
    @ApplicationScoped
    @MutinyVertx
    public Vertx vertx() {
        return Vertx.vertx();
    }
}
