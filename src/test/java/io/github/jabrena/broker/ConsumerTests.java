package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

public class ConsumerTests extends BaseTestContainersTest {

    @Test
    public void given_Consumer_when_consume_then_Ok() {

        //TODO Review how to add dynamic fields in the Config Object
        BrokerClient client = new BrokerClient(
            BROKER_TEST_ADDRESS,
            "demo",
            "node",
            "jab",
            "bren@juantonio.info",
            "xxx",
            "zzz");

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
