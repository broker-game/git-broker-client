package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

@Slf4j
public class ConsumerTests {

    @Test
    public void given_Consumer_when_consume_then_Ok() {

        BrokerClientConfig defaultConfig = new BrokerClientConfig("brokerclient.e2e.properties");
        BrokerClient client = new BrokerClient(defaultConfig);

        /*
        //In the future
        client = BrokerClient.builder()
            .serviceUrl("https://github.com/broker-game/broker-dev-environment")
            .build();
        */

        Consumer consumer = client.newConsumer();

        IntStream.rangeClosed(1, 5).boxed()
            .forEach(x -> {
                // Wait for a message
                Message msg = consumer.receive("PING");
            });
    }
}
