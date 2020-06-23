package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

public class ConsumerTests extends BaseTestContainersTest {

    @Test
    public void given_Consumer_when_consume_then_Ok() {

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
            .node("PING-NODE")
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PING")
            .event("PING-EVENT")
            .create();
        producer.send("Hello World");

        Consumer consumer = client.newConsumer()
            //.topic("PING")
            .subscribe();

        consumer.receive("PING-EVENT");
    }
}
