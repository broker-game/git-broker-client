package io.github.jabrena.broker;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Slf4j
public class BrokerClient {

    LocalDirectoryWrapper localRepositoryWrapper;
    GitClientWrapper gitWrapper;
    final BrokerClientConfig config;

    //Branch
    private final String application;

    //Node
    private final String node;

    //Credentials
    private final String broker;
    private final String user;
    private final String password;
    private final String fullName;
    private final String email;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructor
     *
     * @param broker      broker
     * @param application application
     * @param node        node
     * @param fullName    fullName
     * @param email       email
     * @param user        user
     * @param password    password
     */
    public BrokerClient(String broker, String application, String node,
                        String fullName, String email, String user, String password) {

        LOGGER.info("Creating an instance of BrokerClient");

        this.broker = broker;
        this.application = application;
        this.node = node;
        this.fullName = fullName;
        this.email = email;
        this.user = user;
        this.password = password;

        this.localRepositoryWrapper = new LocalDirectoryWrapper();
        this.gitWrapper = new GitClientWrapper();
        this.config = new BrokerClientConfig(broker, application, node, fullName, email, user, password);

        //Connect
        this.connect();
    }

    /**
     * Constructor
     *
     * @param config ConfigFile
     */
    public BrokerClient(BrokerClientConfig config) {
        this(
            config.getBroker(),
            config.getApplication(),
            config.getNode(),
            config.getFullName(),
            config.getEmail(),
            config.getUser(),
            config.getPassword()
        );
    }

    private void connect() {

        localRepositoryWrapper.createLocalRepository(this.node);
        gitWrapper.cloneRepository(localRepositoryWrapper.getLocalFS(), this.broker, this.application);
    }

    /**
     * Produce
     *
     * @param event   event
     * @param message message
     * @return
     */
    @Deprecated
    public boolean produce(String event, Object message) {

        final String fileName = this.getFilename(event);

        LOGGER.info("Producing event: {}", fileName);

        final String fileContent = getFileContent(message);

        gitWrapper.upgradeRepository(this.application);
        gitWrapper.addFile(this.localRepositoryWrapper.getLocalFS(), fileName, fileContent, this.fullName, this.email);
        gitWrapper.push(user, password);

        return true;
    }

    @SneakyThrows
    private String getFileContent(Object message) {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        return objectMapper.writeValueAsString(message);
    }

    private String getFilename(String event) {
        return getEpoch() + "_" + this.node + "_" + event + ".json";
    }

    private long getEpoch() {
        return System.currentTimeMillis();
    }

    /**
     * Consume
     *
     * @param event         event
     * @param poolingPeriod Pooling period
     * @return response
     */
    @Deprecated
    public BrokerResponse consume(String event, int poolingPeriod) {

        var result = getInfiniteStream()
            .map(x -> {
                sleep(poolingPeriod);
                gitWrapper.upgradeRepository(this.application);
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
        gitWrapper.addFile(this.localRepositoryWrapper.getLocalFS(), fileName, "PROCESSED", fullName, email);
        gitWrapper.push(user, password);
    }

    private Stream<Long> getInfiniteStream() {
        return Stream.iterate(0L, i -> i + 1L);
    }

    @SneakyThrows
    private void sleep(int seconds) {
        Thread.sleep(seconds * 1000);
    }

    /**
     * Close
     */
    public void close() {

        if (Objects.nonNull(this.localRepositoryWrapper.getLocalFS())) {
            try {
                Files.walk(this.localRepositoryWrapper.getLocalFS().toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

                assert (!this.localRepositoryWrapper.getLocalFS().exists());
            } catch (IOException e) {
                LOGGER.warn(e.getLocalizedMessage(), e);
            }
        }
    }

    @VisibleForTesting
    public void setLocalRepository(LocalDirectoryWrapper localRepository) {
        this.localRepositoryWrapper = localRepository;
    }

    @VisibleForTesting
    public void setGitWrapper(GitClientWrapper gitWrapper) {
        this.gitWrapper = gitWrapper;
    }

    //TODO Refactor
    //Create a builder
    public Producer newProducer(String event) {

        return new ProducerImpl(localRepositoryWrapper, gitWrapper, config, event);
    }

    public Consumer newConsumer() {

        return new ConsumerImpl(localRepositoryWrapper, gitWrapper, config);
    }
}
