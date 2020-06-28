package io.github.jabrena.broker;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class PingPongDemoTest extends BaseTestContainersTest {

    @Disabled
    @Tag("complex")
    @Test
    public void given_PingPongGame_when_execute_then_Ok() {

        final String TOPIC1 = "PING";
        final String TOPIC2 = "PONG";
        final String NODE = TOPIC1 + "-NODE";
        final String NODE2 = TOPIC2 + "-NODE";
        final int iterations = 5;

        var playerList = List.of(
            new Player(TOPIC1,TOPIC2, NODE, iterations),
            new Player(TOPIC2, TOPIC1, NODE2, iterations));
        var futureRequests = playerList.stream()
            .map(Client::runAsync)
            .collect(toList());

        var results = futureRequests.stream()
            .map(CompletableFuture::join)
            .collect(toList());

        then(results.stream().count()).isEqualTo(2);

        verify(TOPIC1, iterations);
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
        int expectedMessages = iterations * 2;
        then(counter).isBetween(expectedMessages -1, expectedMessages);
    }

    private interface Client {
        Integer run();
        CompletableFuture<Integer> runAsync();
    }

    private static class Player implements Client {

        private final String TOPIC_PRODUCE;
        private final String TOPIC_CONSUME;
        private final String NODE;
        private final int iterations;

        private final GitBrokerClient client;
        private final Producer<String> producer;
        private final Consumer<String> consumer;

        private final Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Player(
            @NonNull String topicProduce,
            @NonNull String topicConsume,
            @NonNull String node,
            @NonNull int iterations) {

            this.TOPIC_PRODUCE = topicProduce;
            this.TOPIC_CONSUME = topicConsume;
            this.NODE = node;
            this.iterations = iterations;

            client = GitBrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(authentication)
                .build();

            producer = client.newProducer()
                .topic(TOPIC_PRODUCE)
                .node(NODE)
                .create();

            consumer = client.newConsumer()
                .topic(TOPIC_CONSUME)
                .node(NODE)
                .subscribe();
        }

        @Override
        public Integer run() {
            LOGGER.info(TOPIC_PRODUCE);

            IntStream.rangeClosed(1, iterations)
                .forEach(x -> {
                    LOGGER.info("Iteration {}: {}", TOPIC_PRODUCE, x);
                    consumer.batchReceive();
                    producer.send(TOPIC_PRODUCE);
                });

            return 1;
        }

        @Override
        public CompletableFuture<Integer> runAsync() {

            LOGGER.info("Thread: {}", Thread.currentThread().getName());
            CompletableFuture<Integer> future = CompletableFuture
                .supplyAsync(() -> run())
                .orTimeout(60, TimeUnit.SECONDS)
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
