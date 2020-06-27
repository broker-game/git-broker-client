package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import java.util.stream.StreamSupport;

import static org.assertj.core.api.BDDAssertions.then;

public class ConsumerTests extends BaseTestContainersTest {

    @Test
    public void given_Consumer_when_consume_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PINGPONG")
            .node("PING-NODE")
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Consumer<String> consumer = client.newConsumer()
            .topic("PINGPONG")
            .node("PING-NODE")
            .subscribe();

        Messages<String> response = consumer.batchReceive();
        then(StreamSupport.stream(response.spliterator(), false)
            .count())
            .isEqualTo(1);
        then(StreamSupport.stream(response.spliterator(), false)
            .map(x -> x.getValue())
            .findFirst().get())
            .isEqualTo(expectedMessage);

        client.close();
    }
}
