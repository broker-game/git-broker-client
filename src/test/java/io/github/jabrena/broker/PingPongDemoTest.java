package io.github.jabrena.broker;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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

        var futureRequests = List.of(new Game(), new Ping(), new Pong()).stream()
            .map(Client::runAsync)
            .collect(toList());

        var results = futureRequests.stream()
            .map(CompletableFuture::join)
            .collect(toList());

        then(results.stream().count()).isEqualTo(3);

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .build();

        Reader<String> reader = client.newReader()
            .topic("PING")
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
        then(counter).isBetween(9, 11);
    }

    private interface Client {

        Logger LOGGER = LoggerFactory.getLogger(Client.class);

        Integer run();

        default CompletableFuture<Integer> runAsync() {

            LOGGER.info("Thread: {}", Thread.currentThread().getName());
            CompletableFuture<Integer> future = CompletableFuture
                .supplyAsync(() -> run())
                .exceptionally(ex -> {
                    LOGGER.error(ex.getLocalizedMessage(), ex);
                    return 0;
                })
                .completeOnTimeout(0, 60, TimeUnit.SECONDS);

            return future;
        }
    }

    private static class Ping implements Client {

        private final String TOPIC_PRODUCE = "PING";
        private final String TOPIC_CONSUME = "PONG";
        private final String NODE = "PING-NODE";

        private GitBrokerClient client;
        private Producer<String> producer;
        private Consumer<String> consumer;

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Ping() {

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
            LOGGER.info("Ping");

            IntStream.rangeClosed(1, 5)
                .forEach(x -> {
                    LOGGER.info("Iteration Ping: {}", x);
                    consumer.batchReceive();
                    producer.send("Ping");
                });

            return 1;
        }

    }

    private static class Pong implements Client {

        private final String TOPIC_PRODUCE = "PONG";
        private final String TOPIC_CONSUME = "PING";
        private final String NODE = "PONG-NODE";

        private GitBrokerClient client;
        private Producer<String> producer;
        private Consumer<String> consumer;

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Pong() {

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
            LOGGER.info("Pong");

            IntStream.rangeClosed(1, 5)
                .forEach(x -> {
                    LOGGER.info("Iteration Pong: {}", x);
                    consumer.batchReceive();
                    producer.send("Pong");
                });

            return 1;
        }

    }

    private static class Game implements Client {

        private final String TOPIC_PRODUCE = "PING";
        private final String NODE = "GAME-NODE";

        private GitBrokerClient client;
        private Producer<String> producer;

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Game() {

            client = GitBrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(authentication)
                .build();

            producer = client.newProducer()
                .topic(TOPIC_PRODUCE)
                .node(NODE)
                .create();
        }

        @Override
        public Integer run() {
            LOGGER.info("Game");

            sleep(10);
            producer.send("Game");

            return 1;
        }

        @SneakyThrows
        private void sleep(int seconds) {
            Thread.sleep(seconds * 1000);
        }

    }


}
