package io.github.jabrena.broker.impl;

import io.github.jabrena.broker.Authentication;
import io.github.jabrena.broker.GitBrokerClientException;
import io.github.jabrena.broker.GitBrokerFileParser;
import io.github.jabrena.broker.Consumer;
import io.github.jabrena.broker.GitClientWrapper;
import io.github.jabrena.broker.LocalDirectoryWrapper;
import io.github.jabrena.broker.Message;
import io.github.jabrena.broker.Messages;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toUnmodifiableList;

@Slf4j
@AllArgsConstructor
public class ConsumerImpl<T> implements Consumer<T> {

    private final GitBrokerClientImpl client;
    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;

    private final String broker;
    private final String topic;
    private final String node;

    /**
     * Constructor
     *
     * @param authentication authentication
     * @param topic application
     * @param node event
     */
    public ConsumerImpl(@NonNull GitBrokerClientImpl client,
                        @NonNull String broker,
                        @NonNull Authentication authentication,
                        @NonNull String topic,
                        String node) {

        this.client = client;
        this.localRepositoryWrapper = new LocalDirectoryWrapper();
        this.gitWrapper = new GitClientWrapper();

        this.broker = broker;
        this.topic = topic;
        this.node = node;

        this.localRepositoryWrapper.createLocalRepository();
        this.gitWrapper.cloneRepository(localRepositoryWrapper.getLocalFS(), this.broker);
        this.gitWrapper.setAuthentication(authentication);
        this.gitWrapper.checkout(this.topic);

        client.addConsumer(this);
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public Message<T> receive() throws GitBrokerClientException {
        return null;
    }

    private void writeCheckpoint() {

        //Write checkpoint
        final String fileName = this.getFilename("OK");
        LOGGER.info(fileName);
        gitWrapper.addFile(this.localRepositoryWrapper.getLocalFS(), fileName, "PROCESSED");
        gitWrapper.push();
    }

    private String getFilename(String event) {
        if (Objects.isNull(this.node)) {
            return getEpoch() + "_" + event + ".json";
        }
        return getEpoch() + "_" + this.node + "_" + event + ".json";
    }

    private long getEpoch() {
        return System.currentTimeMillis();
    }

    @Override
    public CompletableFuture<Message<T>> receiveAsync(String event) {
        return null;
    }

    @Override
    public Message<T> receive(int timeout, TimeUnit unit) throws GitBrokerClientException {
        return null;
    }

    @Override
    public Messages<T> batchReceive() {

        gitWrapper.upgradeRepository(this.topic);

        var localDirectory = this.localRepositoryWrapper.getLocalFS();
        var counter = Arrays.stream(localDirectory.list())
            .filter(y -> y.indexOf(".json") != -1)
            .count();

        //Wait
        if (counter == 0) {
            return emptyMessages();
        } else {

            //Detect last checkpoints
            var checkPointList = Arrays.stream(localDirectory.list())
                .filter(y -> y.indexOf("OK.json") != -1)
                .sorted()
                .collect(toList());

            if (checkPointList.size() > 0) {

                var lastCheckpoint = checkPointList.get(checkPointList.size() - 1);
                var list = Arrays.stream(localDirectory.list())
                    .filter(y -> y.indexOf(".json") != -1)
                    .sorted()
                    .dropWhile(z -> !z.equals(lastCheckpoint))
                    .filter(y -> y.indexOf("OK.json") == -1)
                    .map(GitBrokerFileParser::new)
                    //.peek(System.out::println)
                    .collect(toList());

                if (list.size() > 0) {
                    LOGGER.info("Processing messages from last checkpoint: {}", lastCheckpoint);

                    writeCheckpoint();

                    return new Messages<T>() {

                        @Override
                        public int size() {
                            return list.size();
                        }

                        @Override
                        public Iterator<Message<T>> iterator() {
                            return list.stream()
                                .map(x -> (Message<T>) new MessageImpl<T>(x, localRepositoryWrapper))
                                .collect(toUnmodifiableList())
                                .iterator();
                        }

                    };
                } else {
                    LOGGER.info("Without new messages from last checkpoint: {}", lastCheckpoint);
                    return emptyMessages();
                }

            } else if (checkPointList.size() == 0) {

                var count = Arrays.stream(localDirectory.list())
                    .filter(y -> y.indexOf(".json") != -1)
                    .map(GitBrokerFileParser::new)
                    //.peek(System.out::println)
                    .count();

                if (count > 0) {
                    LOGGER.info("Processing messages");

                    var list = Arrays.stream(localDirectory.list())
                        .filter(y -> y.indexOf(".json") != -1)
                        .map(GitBrokerFileParser::new)
                        .collect(toUnmodifiableList());

                    writeCheckpoint();

                    //Break stream
                    return new Messages<T>() {

                        @Override
                        public int size() {
                            return Long.valueOf(list.stream().count()).intValue();
                        }

                        @Override
                        public Iterator<Message<T>> iterator() {
                            return list.stream()
                                .map(x -> (Message<T>) new MessageImpl<T>(x, localRepositoryWrapper))
                                .collect(toUnmodifiableList())
                                .iterator();
                        }
                    };
                }
                LOGGER.info("Without new messages from last checkpoint: {}", checkPointList);
            }
        }

        return emptyMessages();
    }

    private Messages<T> emptyMessages() {

        return new Messages<T>() {

            @Override
            public int size() {
                return 0;
            }

            @Override
            public Iterator<Message<T>> iterator() {
                return List.of().stream()
                    .map(String::valueOf)
                    .map(GitBrokerFileParser::new)
                    .map(x -> (Message<T>) new MessageImpl<T>(x, localRepositoryWrapper))
                    .collect(toUnmodifiableList())
                    .iterator();
            }
        };
    }

    @Override
    public void close() throws GitBrokerClientException {

        LOGGER.info("Closing Consumer resources");

    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return false;
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public String getConsumerName() {
        return null;
    }
}
