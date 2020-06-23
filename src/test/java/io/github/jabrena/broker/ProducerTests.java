package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

public class ProducerTests extends BaseTestContainersTest {

    @Test
    public void given_Producer_when_send_then_Ok() {

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
            .node("PING-NODE")
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PING")
            .event("PING-EVENT")
            .create();

        IntStream.rangeClosed(1, 2).boxed()
            .forEach(x -> producer.send("Hello World"));

        client.close();
    }
}
