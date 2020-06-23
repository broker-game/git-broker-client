package io.github.jabrena.broker;

import lombok.Data;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.BDDAssertions.then;

public class BrokerClientTests  extends BaseTestContainersTest {

    @Data
    private static class Message {
        private final String value = "OK";
    }

    @Test
    public void given_Client_when_useProducerBuilder_then_createProducer() {

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
            .node("PING-NODE")
            .topic("demo")
            .build();

        Producer producer = client.newProducer()
            .topic("demo")
            .event("PING-EVENT")
            .create();

        producer.send("Hello World");
        producer.send("Hello World 2");

        client.close();
    }

    @Test
    public void given_ClientBuilder_when_useProducerBuilder_then_createProducer() {

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            //.topic("demo")
            .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
            .node("PING-NODE")
            .build();

        Producer producer = client.newProducer()
            .topic("PINGPONG")
            .event("PING-EVENT")
            .create();

        producer.send("Hello World");
        producer.send("Hello World 2");

        client.close();
    }

    @Test
    public void given_Client_when_useConsumerBuilder_then_createConsumer() {

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            //.topic("demo")
            .authentication(new Authentication("user", "user@my-email.com", "xxx", "yyy"))
            .node("PING-NODE")
            .topic("PING")
            .build();

        Producer producer = client.newProducer()
            .topic("PING")
            .event("PING-EVENT")
            .create();

        producer.send("Hello World");

        Consumer consumer = client.newConsumer()
            //.topic("PING")
            .subscribe();
        consumer.receive("PING-EVENT");

        client.close();
    }

}
