package io.github.jabrena.broker;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;

@Slf4j
public class GitBrokerClient {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final Authentication authentication;
    private final String broker;

    /**
     * Constructor
     * @param broker broker
     * @param authentication authentication
     */
    public GitBrokerClient(@NonNull String broker, Authentication authentication) {

        LOGGER.info("Creating an instance of GitBrokerClient");

        this.broker = broker;
        this.authentication = authentication;
        this.localRepositoryWrapper = new LocalDirectoryWrapper();
        this.gitWrapper = new GitClientWrapper();

        //Connect
        this.connect();
    }

    private void connect() {

        localRepositoryWrapper.createLocalRepository();
        gitWrapper.cloneRepository(localRepositoryWrapper.getLocalFS(), this.broker);
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
                assert(!this.localRepositoryWrapper.getLocalFS().exists());
            } catch (IOException e) {
                LOGGER.warn(e.getLocalizedMessage(), e);
            }
        }
    }

    public ProducerBuilder newProducer() {

        return new ProducerBuilder(localRepositoryWrapper, gitWrapper, authentication);
    }

    public ConsumerBuilder newConsumer() {

        return new ConsumerBuilder(localRepositoryWrapper, gitWrapper, authentication);
    }

    public ReaderBuilder newReader() {

        return new ReaderBuilder(localRepositoryWrapper, gitWrapper);
    }

    static ClientBuilder builder() {
        return new ClientBuilder();
    }
}
