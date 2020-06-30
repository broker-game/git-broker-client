package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.stream.StreamSupport;

import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class ConsumerTests extends BaseTestContainersTest {

    @Test
    public void given_Consumer_when_consume_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
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

    @Test
    public void given_Consumer_when_no_consume_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Consumer<String> consumer = client.newConsumer()
            .topic("PINGPONG")
            .node("PING-NODE")
            .subscribe();

        Messages<String> response = consumer.batchReceive();
        then(StreamSupport.stream(response.spliterator(), false)
            .count())
            .isEqualTo(0);

        client.close();
    }

    @Test
    public void given_Consumer_when_consume_multiple_times_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
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

        Messages<String> response2 = consumer.batchReceive();
        then(response2.iterator().hasNext()).isFalse();
        Messages<String> response3 = consumer.batchReceive();
        then(response3.iterator().hasNext()).isFalse();

        producer.send(expectedMessage);
        producer.send(expectedMessage);

        Messages<String> response4 = consumer.batchReceive();
        then(StreamSupport.stream(response4.spliterator(), false)
            .count())
            .isEqualTo(2);

        Messages<String> response5 = consumer.batchReceive();
        then(response5.iterator().hasNext()).isFalse();

        client.close();
    }
}
