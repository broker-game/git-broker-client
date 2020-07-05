package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toUnmodifiableList;
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

    @Disabled("The library doesn´t support multiple messages in parallel for the same topic")
    @Test
    public void given_Producer_when_sendAsyncInParallel_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        final String topic = "PINGPONG";
        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        final String message = "Hello World";
        var futures = IntStream.rangeClosed(1, 2).boxed()
            .map(i -> producer.sendAsync(message))
            .collect(toUnmodifiableList());

        var list = futures.stream()
            .map(CompletableFuture::join)
            .peek(s -> LOGGER.info(s))
            .collect(toUnmodifiableList());

        then(verifyAllElementsAreDifferent(list)).isTrue();

        client.close();
    }

    public boolean verifyAllElementsAreDifferent(List<String> list) {
        return new HashSet<>(list).size() == list.size();
    }

    @Test
    public void given_MultipleProducers_when_sendAsyncInParallel_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        final String topic = "PINGPONG";
        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        Producer<String> producer2 = client.newProducer()
            .topic(topic)
            .create();

        final String message = "Hello World";
        var futures = List.of(
            producer.sendAsync(message),
            producer2.sendAsync(message));
        var list = futures.stream()
            .map(CompletableFuture::join)
            .peek(s -> LOGGER.info(s))
            .collect(toUnmodifiableList());

        then(verifyAllElementsAreDifferent(list)).isTrue();

        client.close();
    }
}
