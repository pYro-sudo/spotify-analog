package by.losik.producer;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@ApplicationScoped
public class ExecutorServiceProducer {
    @ConfigProperty(name = "auth.service.thread.pool.size", defaultValue = "10")
    int threadPoolSize;

    @Produces
    @ApplicationScoped
    @Named("authServiceExecutor")
    public ExecutorService produceAuthServiceExecutor() {
        return Executors.newFixedThreadPool(threadPoolSize, r -> {
            Thread t = new Thread(r);
            t.setName("auth-service-executor-" + t.getId());
            return t;
        });
    }
}
