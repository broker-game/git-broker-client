package com.github.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class BrokerClientTests {

    private BrokerClientConfig defaultConfig;
    private BrokerClient defaultBrokerClient;

    @BeforeEach
    public void setUp(){
        defaultConfig = new BrokerClientConfig();
        defaultBrokerClient = new BrokerClient(defaultConfig);
    }

    @AfterEach
    public void close() {
        defaultBrokerClient.close();
    }

    @Test
    public void given_Client_when_connectWithBroker_then_Ok() {

        var result =  defaultBrokerClient.connect();

        then(result).isEqualTo(true);
    }

    private static class Message {
        private final String value = "OK";
    }

    @Test
    public void given_Client_when_produceEvent_then_Ok() {

        defaultBrokerClient.connect();

        final String EVENT = "PING";
        final Message MESSAGE = new Message();

        then(defaultBrokerClient.produce(EVENT, MESSAGE)).isEqualTo(true);
    }

    @Test
    public void given_Client_when_consumeForEvent_then_Ok() {

        defaultBrokerClient.connect();
        final String EVENT = "PING";
        final int poolingPeriod = 1;

        BrokerResponse response = defaultBrokerClient.consume(EVENT, poolingPeriod);
        then(response).isNotNull();
    }

}
