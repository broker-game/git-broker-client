package com.github.broker;

import lombok.extern.slf4j.Slf4j;
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
public class BrokerClientMultiThreadTests {

    @Test
    public void given_Client_when_produceAndConsumeInParallelForEvent_then_Ok() {

        var futureRequests = List.of(new Client1(), new Client2()).stream()
            .map(Client::runAsync)
            .collect(toList());

        var results = futureRequests.stream()
            .map(CompletableFuture::join)
            .collect(toList());

        then(results.stream().count()).isEqualTo(2);
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

    @Slf4j
    private static class Client1 implements Client {

        private BrokerClientConfig defaultConfig;
        private BrokerClient defaultBrokerClient;
        private String EVENT = "PING";

        public Client1() {
            defaultConfig = new BrokerClientConfig("config_ping.properties");
            defaultBrokerClient = new BrokerClient(defaultConfig);
            defaultBrokerClient.connect();
        }

        public Integer run() {
            IntStream.rangeClosed(1, 5)
                .forEach(x -> defaultBrokerClient.produce(EVENT, ""));
            return 1;
        }

    }

    @Slf4j
    private static class Client2 implements Client {

        final int poolingPeriod = 1;

        private BrokerClientConfig defaultConfig;
        private BrokerClient defaultBrokerClient;
        private String EVENT = "PING";

        public Client2() {
            defaultConfig = new BrokerClientConfig("config_pong.properties");
            defaultBrokerClient = new BrokerClient(defaultConfig);
            defaultBrokerClient.connect();
        }

        public Integer run() {
            defaultBrokerClient.consume(EVENT, poolingPeriod);

            return 1;
        }

    }
}
