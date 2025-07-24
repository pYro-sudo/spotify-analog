package by.losik.producer;

import io.smallrye.common.annotation.Identifier;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.rabbitmq.RabbitMQOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class RabbitMQOptionsProducer {
    @Produces
    @Identifier("my-named-options") //maybe i will need that, because there is security in need, god, fuck, i hate security
    public RabbitMQOptions getNamedOptions() {
        PemKeyCertOptions keycert = new PemKeyCertOptions()
                .addCertPath("./tls/tls.crt")
                .addKeyPath("./tls/tls.key");
        PemTrustOptions trust = new PemTrustOptions().addCertPath("./tlc/ca.crt");
        return new RabbitMQOptions()
                .setSsl(true)
                .setPemKeyCertOptions(keycert)
                .setPemTrustOptions(trust)
                .setUser("user1")
                .setPassword("password1")
                .setHost("localhost")
                .setPort(5672)
                .setVirtualHost("vhost1")
                .setConnectionTimeout(6000)
                .setRequestedHeartbeat(60)
                .setHandshakeTimeout(6000)
                .setRequestedChannelMax(5)
                .setNetworkRecoveryInterval(500)
                .setAutomaticRecoveryEnabled(true);
    }
}
