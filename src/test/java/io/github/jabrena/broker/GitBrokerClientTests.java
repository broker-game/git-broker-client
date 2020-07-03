package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.stream.StreamSupport;

import static org.assertj.core.api.BDDAssertions.then;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public class GitBrokerClientTests extends TestContainersBaseTest {

    @Test
    public void given_ClientBuilder_when_buildWithServiceUrl_then_createClient() {

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .build();

        client.close();
    }

    @Test
    public void given_ClientBuilder_when_use_serviceUrl_and_build_then_createClient() {

        Exception exception = assertThrows(IllegalArgumentException.class, () -> GitBrokerClient.builder().build());

        String expectedMessage = "broker is marked non-null but is null";
        then(exception.getMessage()).isEqualTo(expectedMessage);
    }

    @Test
    public void given_ClientBuilder_when_use_serviceUrl_and_authentication_and_build_then_createBuilder() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        client.close();
    }

    @Test
    public void given_Client_when_call_newProducer_using_topic_and_node_and_create_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
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
    public void given_Client_when_call_newConsumer_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Consumer consumer = client.newConsumer()
            .topic("PINGPONG")
            .subscribe();

        client.close();
    }

    @Test
    public void given_Client_when_useConsumerBuilder_then_createConsumer() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PINGPONG")
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Consumer<String> consumer = client.newConsumer()
            .topic("PINGPONG")
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

    @Test
    public void given_Client_when_call_newReader_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic("PINGPONG")
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Reader<String> reader = client.newReader()
            .topic("PINGPONG")
            .create();

        Message<String> message = reader.readNext();
        then(message.getValue()).isEqualTo(expectedMessage);
        then(reader.hasReachedEndOfTopic()).isTrue();

        client.close();
    }

}
