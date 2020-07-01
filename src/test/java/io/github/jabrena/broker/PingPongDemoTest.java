package io.github.jabrena.broker;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class PingPongDemoTest extends BaseTestContainersTest {

    @Tag("complex")
    @Test
    public void given_PingPong_when_execute_then_Ok() {

        final String topic1 = "PING";
        final String topic2 = "PONG";
        final int iterations = 5;
        final int timeout = 60;

        var playersList = List.of(
            new Player(topic1, topic2, iterations, timeout),
            new Player(topic2, topic1, iterations, timeout));
        var futureRequests = playersList.stream()
            .map(Player::runAsync)
            .collect(toList());
        var results = futureRequests.stream()
            .map(CompletableFuture::join)
            .reduce(0L, (x1, x2) -> x1 + x2);

        then(results).isEqualTo((iterations - 1) * 2);
        verify(topic1, iterations);
        verify(topic2, iterations);
    }

    private void verify(String TOPIC, int iterations) {

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .build();

        Reader<String> reader = client.newReader()
            .topic(TOPIC)
            .create();

        int counter = 0;
        while (true) {
            if (reader.hasReachedEndOfTopic()) {
                break;
            }
            Message<String> value = reader.readNext();
            LOGGER.info(value.getValue());
            counter++;
        }
        LOGGER.info("{}", counter);
        then(counter).isEqualTo(iterations);

        client.close();
    }

    private interface Client<T> {

        T run();

        CompletableFuture<T> runAsync();
    }

    private static class Player implements Client<Long> {

        private final String TOPIC_PRODUCE;
        private final String TOPIC_CONSUME;
        private final int iterations;

        private final GitBrokerClient client;
        private final Producer<String> producer;
        private final Consumer<String> consumer;
        private final int timeout;

        private final Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Player(
            @NonNull String topicProduce,
            @NonNull String topicConsume,
            @NonNull int iterations,
            @NonNull int timeout) {

            this.TOPIC_PRODUCE = topicProduce;
            this.TOPIC_CONSUME = topicConsume;
            this.iterations = iterations;
            this.timeout = timeout;

            client = GitBrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(authentication)
                .build();

            producer = client.newProducer()
                .topic(TOPIC_PRODUCE)
                .create();

            consumer = client.newConsumer()
                .topic(TOPIC_CONSUME)
                .subscribe();
        }

        @Override
        public Long run() {
            LOGGER.info(TOPIC_PRODUCE);
            return IntStream.rangeClosed(1, iterations).boxed()
                .mapToLong(x -> {
                    LOGGER.info("Iteration {}: {}", TOPIC_PRODUCE, x);
                    Messages<String> messages = consumer.batchReceive();
                    producer.send(TOPIC_PRODUCE);
                    return StreamSupport.stream(messages.spliterator(), false).count();
                })
                .reduce(0L, (x, y) -> x + y);
        }

        @Override
        public CompletableFuture<Long> runAsync() {

            LOGGER.info("Thread: {}", Thread.currentThread().getName());
            CompletableFuture<Long> future = CompletableFuture
                .supplyAsync(() -> run())
                .orTimeout(this.timeout, TimeUnit.SECONDS)
                .handle((response, ex) -> {
                    if (!Objects.isNull(ex)) {
                        LOGGER.error(ex.getLocalizedMessage(), ex);
                    }
                    return response;
                });
            return future;
        }
    }
}
