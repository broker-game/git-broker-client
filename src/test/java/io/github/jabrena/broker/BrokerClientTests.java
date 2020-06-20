package io.github.jabrena.broker;

import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BrokerClientTests  extends BaseTestContainersTest {

    private BrokerClient client;

    @BeforeEach
    public void setUp() {

        //TODO Review how to add dynamic fields in the Config Object
        client = new BrokerClient(
            BROKER_TEST_ADDRESS,
            "demo",
            "node",
            "jab",
            "bren@juantonio.info",
            "xxx",
            "zzz");
    }

    @Data
    private static class Message {
        private final String value = "OK";
    }

    @Test
    public void given_Client_when_produceEvent_then_Ok() {

        final String EVENT = "PING-EVENT";
        final Message MESSAGE = new Message();

        then(client.produce(EVENT, MESSAGE)).isEqualTo(true);
    }

    @Test
    public void given_Client_when_consumeForEvent_then_Ok() {

        final String EVENT = "PING-EVENT";
        final int poolingPeriod = 1;

        client.produce(EVENT, new Message());
        BrokerResponse response = client.consume(EVENT, poolingPeriod);
        then(response).isNotNull();
    }

    @Test
    public void given_ClientBuilder_when_BuildWithMinimumParameters_then_CreateBrokerClient() {

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .topic("demo")
            .build();
    }

    @Test
    public void given_ClientBuilder_when_BuildWithNonMinimumParameters_then_Ko() {

        assertThrows(IllegalArgumentException.class, () -> {
            BrokerClient client = BrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .build();
        });
    }

    @Test
    public void given_Client_when_useProducerBuilder_then_createProducer() {

        //TODO, it is necessary to use this way in the future
        /*
        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .topic("demo")
            .build();
         */

        BrokerClient client = new BrokerClient(
            BROKER_TEST_ADDRESS,
            "demo",
            "node",
            "jab",
            "bren@juantonio.info",
            "xxx",
            "zzz");

        Producer producer = client.newProducer()
            .topic("demo")
            .create();

        producer.send("Hello World");
        producer.send("Hello World 2");
    }

    @Test
    public void given_Client_when_useConsumerBuilder_then_createConsumer() {

        //TODO, it is necessary to use this way in the future
        /*
        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .topic("demo")
            .build();
         */

        BrokerClient client = new BrokerClient(
            BROKER_TEST_ADDRESS,
            "demo",
            "node",
            "jab",
            "bren@juantonio.info",
            "xxx",
            "zzz");

        Producer producer = client.newProducer()
            .topic("PING")
            .create();

        producer.send("Hello World");

        Consumer consumer = client.newConsumer()
            //.topic("PING")
            .subscribe();
        consumer.receive("PING");
    }

}
