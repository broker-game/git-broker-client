package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class ReaderTests extends TestContainersBaseTest {

    @Test
    public void given_Reader_when_iterate_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Reader<String> reader = client.newReader()
            .topic("PINGPONG")
            .create();

        then(reader.hasReachedEndOfTopic()).isTrue();

        client.close();
    }

    @Test
    public void given_Reader_when_iterate_with_elements_then_Ok() {

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
        int expectedIterations = 3;
        IntStream.rangeClosed(1, expectedIterations).boxed()
            .forEach(x -> producer.send(expectedMessage));

        Reader<String> reader = client.newReader()
            .topic("PINGPONG")
            .create();

        int counter = 0;
        while (true) {
            if (reader.hasReachedEndOfTopic()) {
                break;
            }
            Message<String> value = reader.readNext();
            then(value.getValue()).isEqualTo(expectedMessage);
            counter++;
        }

        then(counter).isEqualTo(expectedIterations);

        client.close();
    }
}
