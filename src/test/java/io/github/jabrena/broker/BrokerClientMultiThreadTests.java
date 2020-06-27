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
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.BDDAssertions.then;

public class BrokerClientMultiThreadTests extends BaseTestContainersTest {

    @Disabled
    @Tag("complex")
    @Test
    public void given_Client_when_produceAndConsumeInParallelForEvent_then_Ok() {

        var futureRequests = List.of(new Client1(), new Client2()).stream()
            .map(Client::runAsync)
            .collect(toList());

        var results = futureRequests.stream()
            .map(CompletableFuture::join)
            .collect(toList());

        then(results.stream().count()).isEqualTo(2);

        //Check that the last commits is from Client 2 (The consumer)
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
                .completeOnTimeout(0, 50, TimeUnit.SECONDS);

            return future;
        }
    }

    private static class Client1 implements Client {

        private BrokerClient client;
        private Producer<String> producer;
        private String TOPIC = "PING";
        private String NODE = "PING-NODE";

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Client1() {

            client = BrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(authentication)
                .build();

            producer = client.newProducer()
                .topic(TOPIC)
                .node(NODE)
                .create();
        }

        public Integer run() {
            LOGGER.info("CLIENT 1");

            IntStream.rangeClosed(1, 4).boxed()
                .forEach(x -> {
                    sleep(3);
                    producer.send("Hello World " + x);
                });
            return 1;
        }

        @SneakyThrows
        private void sleep(int seconds) {
            Thread.sleep(seconds * 1000);
        }

    }

    @Slf4j
    private static class Client2 implements Client {

        private BrokerClient client;
        private Consumer<String> consumer;
        private String TOPIC = "PING";
        private String NODE = "PING-NODE";

        public Client2() {

            client = BrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
                .build();

            consumer = client.newConsumer()
                .topic(TOPIC)
                .node(NODE)
                .subscribe();
        }

        public Integer run() {
            LOGGER.info("CLIENT 2");

            IntStream.rangeClosed(1, 5).boxed()
                .map(x -> sleep.apply(x, 5))
                .flatMap(x -> StreamSupport.stream(consumer.batchReceive().spliterator(), false))
                .map(Message::getValue)
                .forEach(LOGGER::info);

            return 1;
        }

        private BiFunction<Integer, Integer, Integer> sleep = (x, seconds) -> {
            try {
                Thread.sleep(seconds * 1000);
            } catch (InterruptedException e) {
                //Empty
            }
            return x;
        };
    }

}
