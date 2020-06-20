package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

public class ProducerTests extends BaseTestContainersTest {

    @Test
    public void given_Producer_when_send_then_Ok() {

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

        Producer<String> producer = client.newProducer()
            .topic("PING")
            .create();

        IntStream.rangeClosed(1, 2)
            .forEach(x -> {
                producer.send("Hello World");
            });

        client.close();
    }
}
