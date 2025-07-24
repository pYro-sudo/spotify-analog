package by.losik.producer;

import io.vertx.mutiny.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class VertxProducer {

    @Produces
    @ApplicationScoped
    @MutinyVertx
    public Vertx vertx() {
        return Vertx.vertx();
    }
}
