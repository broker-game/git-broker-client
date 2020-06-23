package io.github.jabrena.broker;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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

public class BrokerClientMultiThreadTests extends BaseTestContainersTest {

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
        private String EVENT = "PING";

        public Client1() {

            client = BrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
                .node("PING-NODE")
                .build();

            //TODO Something is not necessary... Topic vs Event
            producer = client.newProducer()
                .topic(EVENT)
                .event(EVENT)
                .create();
        }

        public Integer run() {
            LOGGER.info("CLIENT 1");

            sleep(2);
            IntStream.rangeClosed(1, 3).boxed()
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
        private Consumer consumer;
        private String EVENT = "PING";

        public Client2() {

            client = BrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
                .node("PONG-NODE")
                .build();

            consumer = client.newConsumer()
                .topic(EVENT)
                .subscribe();
        }

        public Integer run() {
            LOGGER.info("CLIENT 2");
            IntStream.rangeClosed(1, 10)
                .forEach(x -> {
                    LOGGER.info("{}", x);
                    consumer.receive("PING");
                    sleep(1);
                });
            return 1;
        }

        @SneakyThrows
        private void sleep(int seconds) {
            Thread.sleep(seconds * 1000);
        }
    }

}
