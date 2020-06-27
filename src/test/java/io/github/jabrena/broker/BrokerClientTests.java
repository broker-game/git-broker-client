package io.github.jabrena.broker;

import org.junit.jupiter.api.Test;

import java.util.stream.StreamSupport;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BrokerClientTests  extends BaseTestContainersTest {

    @Test
    public void given_ClientBuilder_when_buildWithServiceUrl_then_createClient() {

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .build();

        client.close();
    }

    @Test
    public void given_ClientBuilder_when_use_serviceUrl_and_build_then_createClient() {

        Exception exception = assertThrows(IllegalArgumentException.class, () -> BrokerClient.builder().build());

        String expectedMessage = "broker is marked non-null but is null";
        then(exception.getMessage()).isEqualTo(expectedMessage);
    }

    @Test
    public void given_ClientBuilder_when_use_serviceUrl_and_authentication_and_build_then_createBuilder() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        client.close();
    }

    @Test
    public void given_Client_when_call_newProducer_using_topic_and_node_and_create_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer producer = client.newProducer()
            .topic("PINGPONG")
            .node("PING-NODE")
            .create();

        producer.send("Hello World");
        producer.send("Hello World 2");

        client.close();
    }

    @Test
    public void given_Client_when_call_newProducer_using_topic_without_node_and_create_then_Ko() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            client.newProducer()
                .topic("PINGPONG")
                .create();
        });

        String expectedMessage = "node is marked non-null but is null";
        then(exception.getMessage()).isEqualTo(expectedMessage);

        client.close();
    }

    @Test
    public void given_Client_when_call_newConsumer_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Consumer consumer = client.newConsumer()
            .topic("PINGPONG")
            .node("PING-NODE")
            .subscribe();

        client.close();
    }

    @Test
    public void given_Client_when_call_newConsumer_node_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            client.newConsumer()
                .topic("PINGPONG")
                //.node("PING-NODE")
                .subscribe();
        });

        String expectedMessage = "node is marked non-null but is null";
        then(exception.getMessage()).isEqualTo(expectedMessage);

        client.close();
    }

    @Test
    public void given_Client_when_useConsumerBuilder_then_createConsumer() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        BrokerClient client = BrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PINGPONG")
            .node("PING-NODE")
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Consumer<String> consumer = client.newConsumer()
            .topic("PINGPONG")
            .node("PING-NODE")
            .subscribe();

        Messages<String> response = consumer.batchReceive();
        then(StreamSupport.stream(response.spliterator(), false)
            .count())
            .isEqualTo(1);
        then(StreamSupport.stream(response.spliterator(), false)
            .map(x -> x.getValue())
            .findFirst().get())
            .isEqualTo(expectedMessage);

        client.close();
    }

}
