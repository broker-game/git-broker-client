# Broker client

A Slow Event Streaming client for Git

![Java CI](https://github.com/broker-game/broker-client/workflows/Java%20CI/badge.svg?branch=master)

## Motivation

`Lego Mindstorms EV3` is the first programmable brick which it is able to run a `Linux OS` inside, and
it has `Internet connection capabilities`. Using the Brick, you can develop educational robots which
run software to manage `data` from Sensors & Actuators. In the ecosystem you could find many awesome examples
but it is not common to use the bricks in a remote way, and it is hard to find Bricks communicating each others outside
of your local network.

In real world, large distributed systems implements Event-Driven architectures (EDA)
and Streaming architectures to manage a huge amount of data to solve problems.

Why not explore with the help of Bricks the development of some examples to `"imitate"` some architectural patterns
with Bricks?

## Why Git?

`Git` is a free and open source distributed version control system. On Internet exist some companies
which offer `free` git accounts to store the code.

**Why not use a Git repository as a persistant event storage for your events?**

## What components are common in a Streaming platform?

- The publisher
- The consumer
- The partitions
- The events
- The topics

![](docs/kafka-example.png)

## Features included with the client:

- Publish Events
- Consume Events
- Initial partition support

## Limitations:

- It is not possible to create a Node in High Availability

## Examples

``` java
@Slf4j
public class PingPongDemoTest extends BaseTestContainersTest {

    @Test
    public void given_PingPongGame_when_execute_then_Ok() {

        var futureRequests = List.of(new Game(), new Ping(), new Pong()).stream()
            .map(Client::runAsync)
            .collect(toList());

        var results = futureRequests.stream()
            .map(CompletableFuture::join)
            .collect(toList());

        then(results.stream().count()).isEqualTo(3);

        GitBrokerClient client = GitBrokerClient.builder()
            .serviceUrl(BROKER_TEST_ADDRESS)
            .build();

        Reader<String> reader = client.newReader()
            .topic("PING")
            .create();

        int counter = 0;
        while (true) {
            if (reader.hasReachedEndOfTopic()) {
                break;
            }
            Message<String> value = reader.readNext();
            LOGGER.info(value.getValue());
            counter++;
        }
        LOGGER.info("{}", counter);
        then(counter).isBetween(9, 11);
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
                .completeOnTimeout(0, 60, TimeUnit.SECONDS);

            return future;
        }
    }

    private static class Ping implements Client {

        private final String TOPIC_PRODUCE = "PING";
        private final String TOPIC_CONSUME = "PONG";
        private final String NODE = "PING-NODE";

        private GitBrokerClient client;
        private Producer<String> producer;
        private Consumer<String> consumer;

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Ping() {

            client = GitBrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(authentication)
                .build();

            producer = client.newProducer()
                .topic(TOPIC_PRODUCE)
                .node(NODE)
                .create();

            consumer = client.newConsumer()
                .topic(TOPIC_CONSUME)
                .node(NODE)
                .subscribe();
        }

        @Override
        public Integer run() {
            LOGGER.info("Ping");

            IntStream.rangeClosed(1, 5)
                .forEach(x -> {
                    LOGGER.info("Iteration Ping: {}", x);
                    consumer.batchReceive();
                    producer.send("Ping");
                });

            return 1;
        }

    }

    private static class Pong implements Client {

        private final String TOPIC_PRODUCE = "PONG";
        private final String TOPIC_CONSUME = "PING";
        private final String NODE = "PONG-NODE";

        private GitBrokerClient client;
        private Producer<String> producer;
        private Consumer<String> consumer;

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Pong() {

            client = GitBrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(authentication)
                .build();

            producer = client.newProducer()
                .topic(TOPIC_PRODUCE)
                .node(NODE)
                .create();

            consumer = client.newConsumer()
                .topic(TOPIC_CONSUME)
                .node(NODE)
                .subscribe();
        }

        @Override
        public Integer run() {
            LOGGER.info("Pong");

            IntStream.rangeClosed(1, 5)
                .forEach(x -> {
                    LOGGER.info("Iteration Pong: {}", x);
                    consumer.batchReceive();
                    producer.send("Pong");
                });

            return 1;
        }

    }

    private static class Game implements Client {

        private final String TOPIC_PRODUCE = "PING";
        private final String NODE = "GAME-NODE";

        private GitBrokerClient client;
        private Producer<String> producer;

        Authentication authentication =
            new Authentication("user", "user@my-email.com", "xxx", "yyy");

        public Game() {

            client = GitBrokerClient.builder()
                .serviceUrl(BROKER_TEST_ADDRESS)
                .authentication(authentication)
                .build();

            producer = client.newProducer()
                .topic(TOPIC_PRODUCE)
                .node(NODE)
                .create();
        }

        @Override
        public Integer run() {
            LOGGER.info("Game");

            sleep(10);
            producer.send("Game");

            return 1;
        }

        @SneakyThrows
        private void sleep(int seconds) {
            Thread.sleep(seconds * 1000);
        }

    }


}

```

## JDK Requeriments in EV3

Current default JVM has an issue with `CA Certificates` and it is necessary to
install a complete JDK. Create a ssh session with your EV3 Brick and execute
the following steps:

```
wget https://ci.adoptopenjdk.net/view/ev3dev/job/eljbuild/job/stretch-14/lastSuccessfulBuild/artifact/build/jdk-ev3.tar.gz
sudo tar -zxvf jdk-ev3.tar.gz -C /opt
sudo mv /opt/jdk/ /opt/jdk-14
sudo update-alternatives --install /usr/bin/java java /opt/jdk-14/bin/java 2014
java -version
```

You should see:

```
openjdk version "14" 2020-03-17
OpenJDK Runtime Environment (build 14+36-ev3-unreleased)
OpenJDK Client VM (build 14+36-ev3-unreleased, mixed mode, sharing)
```

## Dependency

In order to use this library, you need to add the following
dependency:

```
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

```
<dependency>
    <groupId>com.github.broker-game</groupId>
    <artifactId>broker-client</artifactId>
    <version>0.3.0</version>
</dependency>
```

- https://jitpack.io/#broker-game/broker-client/0.3.0

## How to build the project?

```
mvn clean test -DexcludedGroups=complex


# Generate Checkstyle report
mvn clean site -DskipTests
mvn clean test site -DexcludedGroups=complex
```
