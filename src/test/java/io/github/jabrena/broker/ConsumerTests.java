package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class ConsumerTests extends TestContainersBaseTest {

    @Test
    public void given_Consumer_when_consume_then_Ok() {

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
    public void given_Consumer_when_no_consume_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Consumer<String> consumer = client.newConsumer()
            .topic("PINGPONG")
            .subscribe();

        Messages<String> response = consumer.batchReceive();
        then(StreamSupport.stream(response.spliterator(), false)
            .count())
            .isEqualTo(0);

        client.close();
    }

    @Test
    public void given_Consumer_when_consume_multiple_times_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        final String topic = "PINGPONG";
        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Consumer<String> consumer = client.newConsumer()
            .topic(topic)
            .subscribe();

        Messages<String> response = consumer.batchReceive();
        then(StreamSupport.stream(response.spliterator(), false)
            .count())
            .isEqualTo(1);
        then(StreamSupport.stream(response.spliterator(), false)
            .map(x -> x.getValue())
            .findFirst().get())
            .isEqualTo(expectedMessage);

        Messages<String> response2 = consumer.batchReceive();
        then(response2.iterator().hasNext()).isFalse();
        Messages<String> response3 = consumer.batchReceive();
        then(response3.iterator().hasNext()).isFalse();

        producer.send(expectedMessage);
        producer.send(expectedMessage);

        Messages<String> response4 = consumer.batchReceive();
        then(StreamSupport.stream(response4.spliterator(), false)
            .count())
            .isEqualTo(2);

        Messages<String> response5 = consumer.batchReceive();
        then(response5.iterator().hasNext()).isFalse();

        client.close();
    }

    @Test
    public void given_Consumer_when_consume_multiple_times_and_iterate_then_Ok() {

        final String topic = "PINGPONG";
        final Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        String expectedMessage = "Hello World";
        var idList = IntStream.rangeClosed(1, 5).boxed()
            .map(i -> producer.send(expectedMessage))
            .collect(toUnmodifiableList());

        Consumer<String> consumer = client.newConsumer()
            .topic(topic)
            .subscribe();

        Messages<String> response = consumer.batchReceive();
        then(StreamSupport.stream(response.spliterator(), false)
            .count())
            .isEqualTo(5);

        var returnedMessages = StreamSupport.stream(response.spliterator(), false)
            .map(message -> message.getPublishTime())
            .map(s -> new StringBuilder()
                .append(s)
                .append(".json")
                .toString())
            .collect(toUnmodifiableList());

        then(idList).isEqualTo(returnedMessages);

        List<Message<String>> actualList = StreamSupport
            .stream(response.spliterator(), false)
            .collect(toUnmodifiableList());

        client.close();
    }

    @Test
    public void given_Consumer_when_consume_and_acknowledge_then_Ok() {

        final String topic = "PINGPONG";

        final Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Consumer<String> consumer = client.newConsumer()
            .topic(topic)
            .subscribe();

        Messages<String> response = consumer.batchReceive();
        then(StreamSupport.stream(response.spliterator(), false)
            .count())
            .isEqualTo(1);

        List<Message<String>> actualList = StreamSupport
            .stream(response.spliterator(), false)
            .collect(toUnmodifiableList());

        Message<String> msg = actualList.get(0);
        LOGGER.info("{}", msg.getPublishTime());
        consumer.acknowledge(msg);

        Reader<String> reader = client.newReader()
            .topic(topic)
            .create();

        then(reader.hasReachedEndOfTopic()).isTrue();

        client.close();
    }

    @Test
    public void given_Consumer_when_batchReceiveAsync_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        final String topic = "PINGPONG";
        final String node = "PING-NODE";

        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        Consumer<String> consumer = client.newConsumer()
            .topic(topic)
            .node(node)
            .subscribe();

        var future = consumer.batchReceiveAsync();
        future
            .thenApply(response -> {
                var count = StreamSupport.stream(response.spliterator(), false).count();
                then(count).isEqualTo(1);
                return count;
            })
            .join();

        client.close();
    }

    @Test
    public void given_MultipleConsumers_when_batchReceive_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        final String topic = "PINGPONG";
        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        final String node1 = "PING-NODE1";
        final String node2 = "PING-NODE2";
        Consumer<String> consumer1 = client.newConsumer()
            .topic(topic)
            .node(node1)
            .subscribe();

        var messages1 = consumer1.batchReceive();
        var count1 = StreamSupport.stream(messages1.spliterator(), false).count();
        then(count1).isEqualTo(1);

        Consumer<String> consumer2 = client.newConsumer()
            .topic(topic)
            .node(node2)
            .subscribe();

        var messages2 = consumer2.batchReceive();
        var count2 = StreamSupport.stream(messages2.spliterator(), false).count();
        then(count2).isEqualTo(1);

        Reader<String> reader = client.newReader()
            .topic(topic)
            .create();

        int counter = 0;
        while (true) {
            if (reader.hasReachedEndOfTopic()) {
                break;
            }
            Message<String> value = reader.readNext();
            LOGGER.info("D: {}", value.getValue());
            counter++;
        }

        then(counter).isEqualTo(1);

        client.close();
    }

    @Test
    public void given_MultipleConsumers_when_batchReceiveAsync_then_Ok() {

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .authentication(authentication)
            .build();

        final String topic = "PINGPONG";
        Producer<String> producer = client.newProducer()
            .topic(topic)
            .create();

        String expectedMessage = "Hello World";
        producer.send(expectedMessage);

        final String node1 = "PING-NODE1";
        final String node2 = "PING-NODE2";
        Consumer<String> consumer1 = client.newConsumer()
            .topic(topic)
            .node(node1)
            .subscribe();
        Consumer<String> consumer2 = client.newConsumer()
            .topic(topic)
            .node(node2)
            .subscribe();

        var futures = List.of(
            consumer1.batchReceiveAsync(),
            consumer2.batchReceiveAsync());
        var list = futures.stream()
            .map(CompletableFuture::join)
            .flatMap(messages ->
                StreamSupport.stream(messages.spliterator(), false)
                    .map(m -> m.getValue()))
            .peek(s -> LOGGER.info(s))
            .collect(toUnmodifiableList());

        then(list.size()).isEqualTo(2);

        Reader<String> reader = client.newReader()
            .topic(topic)
            .create();

        int counter = 0;
        while (true) {
            if (reader.hasReachedEndOfTopic()) {
                break;
            }
            Message<String> value = reader.readNext();
            LOGGER.info("D: {}", value.getValue());
            counter++;
        }

        then(counter).isEqualTo(1);

        client.close();
    }
}
