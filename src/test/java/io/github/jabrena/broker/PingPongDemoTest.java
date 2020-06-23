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

@Slf4j
public class PingPongDemoTest extends BaseTestContainersTest {

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

        private final int poolingPeriod = 1;
        private final String EVENT = "PING";
        private final String WAITING_EVENT = "PONG";

        private final BrokerClient defaultBrokerClient;

        public Ping() {

            //TODO Review how to add dynamic fields in the Config Object
            defaultBrokerClient = new BrokerClient(
                BROKER_TEST_ADDRESS,
                "PINGPONG",
                "PING-NODE",
                "Full Name",
                "email@gmail.com",
                "XXX",
                "YYY");
        }

        @Override
        public Integer run() {
            LOGGER.info("Ping");

            IntStream.rangeClosed(1, 2)
                .forEach(x -> {
                    LOGGER.info("Iteration Ping: {}", x);
                    //var result = defaultBrokerClient.consume(WAITING_EVENT, poolingPeriod);
                    //defaultBrokerClient.produce(EVENT, "Ping");
                });

            return 1;
        }

    }

    private static class Pong implements Client {

        private final int poolingPeriod = 1;
        private final String EVENT = "PONG";
        private final String WAITING_EVENT = "PING";

        private final BrokerClient defaultBrokerClient;

        public Pong() {

            //TODO Review how to add dynamic fields in the Config Object
            defaultBrokerClient = new BrokerClient(
                BROKER_TEST_ADDRESS,
                "PINGPONG",
                "PONG-NODE",
                "Full Name",
                "email@gmail.com",
                "XXX",
                "YYY");
        }

        @Override
        public Integer run() {
            LOGGER.info("Pong");

            IntStream.rangeClosed(1, 2)
                .forEach(x -> {
                    LOGGER.info("Iteration Ping: {}", x);
                    //var result = defaultBrokerClient.consume(WAITING_EVENT, poolingPeriod);
                    //defaultBrokerClient.produce(EVENT, "Pong");
                });

            return 1;
        }

    }

    private static class Game implements Client {

        private final int poolingPeriod = 1;
        private final String EVENT = "PONG";

        private final BrokerClient defaultBrokerClient;

        public Game() {

            //TODO Review how to add dynamic fields in the Config Object
            defaultBrokerClient = new BrokerClient(
                BROKER_TEST_ADDRESS,
                "PINGPONG",
                "GAME-NODE",
                "Full Name",
                "email@gmail.com",
                "XXX",
                "YYY");
        }

        @Override
        public Integer run() {
            LOGGER.info("Game");

            sleep(10);
            //defaultBrokerClient.produce(EVENT, "Game Event");

            return 1;
        }

        @SneakyThrows
        private void sleep(int seconds) {
            Thread.sleep(seconds * 1000);
        }

    }


}
