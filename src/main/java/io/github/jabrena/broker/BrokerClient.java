package io.github.jabrena.broker;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    LocalDirectoryWrapper localRepository;
    GitClientWrapper gitWrapper;

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

        this.localRepository = new LocalDirectoryWrapper();
        this.gitWrapper = new GitClientWrapper();

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

        localRepository.createLocalRepository(this.node);
        gitWrapper.cloneRepository(localRepository.getLocalFS(), this.broker, this.application);
    }

    /**
     * Produce
     *
     * @param event event
     * @param message message
     * @return
     */
    public boolean produce(String event, Object message) {

        final String fileName = this.getFilename(event);

        LOGGER.info("Producing event: {}", fileName);

        final String fileContent = getFileContent(message);

        gitWrapper.upgradeRepository(this.application);
        gitWrapper.addFile(this.localRepository.getLocalFS(), fileName, fileContent, this.fullName, this.email);
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
     * @param event event
     * @param poolingPeriod Pooling period
     * @return response
     */
    public BrokerResponse consume(String event, int poolingPeriod) {

        getInfiniteStream()
            .map(x -> {
                sleep(poolingPeriod);
                gitWrapper.upgradeRepository(this.application);
                return x;
            })
            .map(x -> {

                var localDirectory = this.localRepository.getLocalFS();
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
                            //.peek(System.out::println)
                            .collect(toList());

                        if (list.size() > 0) {
                            LOGGER.info("Processing events: {} from last checkpoint: {}", event, lastCheckpoint);
                            list.stream()
                                .forEach(file -> LOGGER.info(file.toString()));

                            return null;
                        } else {
                            LOGGER.info("Without new events for: {} from last checkpoint: {}", event, lastCheckpoint);
                        }

                    } else if (checkPointList.size() == 0) {
                        LOGGER.info("Processing events: {}", event);
                        Arrays.stream(localDirectory.list())
                            .filter(y -> y.indexOf(".json") != -1)
                            //.peek(System.out::println)
                            .map(BrokerFileParser::new)
                            .filter(b -> b.getEvent().equals(event))
                            .forEach(System.out::println);

                        //Break stream
                        return null;
                    }
                }
                return x;
            })
            //.peek(System.out::println)
            .takeWhile(Objects::nonNull)
            .count();

        //Write checkpoint
        final String fileName = this.getFilename("OK");
        gitWrapper.addFile(this.localRepository.getLocalFS(), fileName, "PROCESSED", fullName, email);
        gitWrapper.push(user, password);

        return new BrokerResponse();
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

        if (Objects.nonNull(this.localRepository.getLocalFS())) {
            try {
                Files.walk(this.localRepository.getLocalFS().toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

                assert (!this.localRepository.getLocalFS().exists());
            } catch (IOException e) {
                LOGGER.warn(e.getLocalizedMessage(), e);
            }
        }
    }

    //Testing purposes
    public void setLocalRepository(LocalDirectoryWrapper localRepository) {
        this.localRepository = localRepository;
    }

    public void setGitWrapper(GitClientWrapper gitWrapper) {
        this.gitWrapper = gitWrapper;
    }
}
