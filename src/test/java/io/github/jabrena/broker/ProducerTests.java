package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class ProducerTests extends TestContainersBaseTest {

    @Test
    public void given_Producer_when_send_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PINGPONG")
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Consumer<String> consumer = client.newConsumer()
            .topic("PINGPONG")
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
    public void given_Producer_when_sendAsync_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PINGPONG")
            .create();

        var future = producer.sendAsync("Hello World");
        future
            .thenApply(s -> s.toUpperCase())
            .thenAccept(LOGGER::info)
            .join();

        client.close();
    }
}
