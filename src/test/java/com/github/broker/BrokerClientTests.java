package com.github.broker;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class BrokerClientTests {

    @Test
    public void given_Client_when_connectWithBroker_then_Ok() {

        final String USER = "demo";
        final String PASSWORD = "demo";

        BrokerClient brokerClient = new BrokerClient(USER, PASSWORD);

        then(brokerClient.connect()).isEqualTo(true);
    }

    @Test
    public void given_Client_when_triggerEvent_then_Ok() {

        final String USER = "demo";
        final String PASSWORD = "demo";
        final String APPLICATION = "demo";
        final String EVENT = "ping";
        final Message MESSAGE = new Message();

        BrokerClient brokerClient = new BrokerClient(USER, PASSWORD);

        then(brokerClient.send(APPLICATION, EVENT, MESSAGE)).isEqualTo(true);
    }

    private static class Message {
        private final String value = "OK";
    }

    @Test
    public void given_Client_when_waitForEvent_then_Ok() {

        final String USER = "demo";
        final String PASSWORD = "demo";
        final String APPLICATION = "demo";
        final String EVENT = "ping";

        BrokerClient brokerClient = new BrokerClient(USER, PASSWORD);

        BrokerResponse response = brokerClient.wait(APPLICATION, EVENT);
        then(response).isNotNull();
    }
}
