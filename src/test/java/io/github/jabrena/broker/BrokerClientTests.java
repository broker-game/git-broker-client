package io.github.jabrena.broker;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class BrokerClientTests {

    private BrokerClientConfig defaultConfig;
    private BrokerClient defaultBrokerClient;

    @BeforeEach
    public void setUp() {
        defaultConfig = new BrokerClientConfig("brokerclient.e2e.properties");
        defaultBrokerClient = new BrokerClient(defaultConfig);
    }

    @AfterEach
    public void close() {
        defaultBrokerClient.close();
    }

    @Data
    private static class Message {
        private final String value = "OK";
    }

    @Disabled
    @Test
    public void given_Client_when_produceEvent_then_Ok() {

        final String EVENT = "PING-EVENT";
        final Message MESSAGE = new Message();

        then(defaultBrokerClient.produce(EVENT, MESSAGE)).isEqualTo(true);
    }

    @Disabled
    @Test
    public void given_Client_when_consumeForEvent_then_Ok() {

        final String EVENT = "PING-EVENT";
        final int poolingPeriod = 1;

        defaultBrokerClient.produce(EVENT, new Message());
        BrokerResponse response = defaultBrokerClient.consume(EVENT, poolingPeriod);
        then(response).isNotNull();
    }

}
