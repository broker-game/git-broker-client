package io.github.jabrena.broker;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Slf4j
@AllArgsConstructor
public class ConsumerImpl<T> implements Consumer<T> {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final BrokerClientConfig config;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public Message<T> receive(String event) throws BrokerClientException {
        this.consume(event);

        return null;
    }

    public BrokerResponse consume(String event) {

        var result = getFiniteStream()
            .map(x -> {
                gitWrapper.upgradeRepository(this.config.getApplication());
                return x;
            })
            .map(x -> {

                var localDirectory = this.localRepositoryWrapper.getLocalFS();
                var counter = Arrays.stream(localDirectory.list())
                    .filter(y -> y.indexOf(".json") != -1)
                    .count();

                //Wait
                if (counter == 0) {
                    return x;
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
                            .map(BrokerFileParser::new)
                            .filter(b -> b.getEvent().equals(event))
                            .peek(System.out::println)
                            .collect(toList());

                        if (list.size() > 0) {
                            LOGGER.info("Processing events: {} from last checkpoint: {}", event, lastCheckpoint);
                            list.stream()
                                .forEach(file -> LOGGER.info(file.toString()));

                            writeCheckpoint();

                            return null;
                        } else {
                            LOGGER.info("Without new events for: {} from last checkpoint: {}", event, lastCheckpoint);
                        }

                    } else if (checkPointList.size() == 0) {
                        var count = Arrays.stream(localDirectory.list())
                            .filter(y -> y.indexOf(".json") != -1)
                            .map(BrokerFileParser::new)
                            .filter(b -> b.getEvent().equals(event))
                            .peek(System.out::println)
                            .count();

                        if (count > 0) {
                            LOGGER.info("Processing events: {}", event);

                            writeCheckpoint();

                            //Break stream
                            return null;
                        }
                        LOGGER.info("Without new events for: {} from last checkpoint: {}", event);
                    }
                }
                return x;
            })
            //.peek(System.out::println)
            .takeWhile(Objects::nonNull)
            .count();

        return new BrokerResponse();
    }

    private void writeCheckpoint() {

        //Write checkpoint
        final String fileName = this.getFilename("OK");
        gitWrapper.addFile(this.localRepositoryWrapper.getLocalFS(), fileName, "PROCESSED", this.config.getFullName(), this.config.getEmail());
        gitWrapper.push(this.config.getUser(), this.config.getPassword());
    }

    private Stream<Long> getFiniteStream() {
        return IntStream.rangeClosed(1,1).boxed().map(Long::valueOf);
    }

    private String getFilename(String event) {
        return getEpoch() + "_" + this.config.getNode() + "_" + event + ".json";
    }

    private long getEpoch() {
        return System.currentTimeMillis();
    }

    @Override
    public CompletableFuture<Message<T>> receiveAsync(String event) {
        return null;
    }

    @Override
    public Message<T> receive(int timeout, TimeUnit unit) throws BrokerClientException {
        return null;
    }

    @Override
    public void close() throws BrokerClientException {

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
