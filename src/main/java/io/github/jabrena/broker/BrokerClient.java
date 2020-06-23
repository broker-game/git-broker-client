package io.github.jabrena.broker;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NonNull;
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
    public BrokerClient(@NonNull String broker, String application,
                        String node, String fullName, String email, String user, String password) {

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

    public ProducerBuilder newProducer() {

        return new ProducerBuilder(localRepositoryWrapper, gitWrapper, config);
    }

    public ConsumerBuilder newConsumer() {

        return new ConsumerBuilder(localRepositoryWrapper, gitWrapper, config);
    }

    static ClientBuilder builder() {
        return new ClientBuilder();
    }
}
