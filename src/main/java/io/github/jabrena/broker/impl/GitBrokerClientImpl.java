package io.github.jabrena.broker.impl;

import io.github.jabrena.broker.Authentication;
import io.github.jabrena.broker.ConsumerBuilder;
import io.github.jabrena.broker.GitBrokerClient;
import io.github.jabrena.broker.GitClientWrapper;
import io.github.jabrena.broker.LocalDirectoryWrapper;
import io.github.jabrena.broker.ProducerBuilder;
import io.github.jabrena.broker.ReaderBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;

@Slf4j
public class GitBrokerClientImpl implements GitBrokerClient {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final Authentication authentication;
    private final String broker;

    /**
     * Constructor
     * @param broker broker
     * @param authentication authentication
     */
    public GitBrokerClientImpl(@NonNull String broker, Authentication authentication) {

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
    @Override
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

    @Override
    public ProducerBuilder newProducer() {

        return new ProducerBuilder(localRepositoryWrapper, gitWrapper, authentication);
    }

    @Override
    public ConsumerBuilder newConsumer() {

        return new ConsumerBuilder(localRepositoryWrapper, gitWrapper, authentication);
    }

    @Override
    public ReaderBuilder newReader() {

        return new ReaderBuilder(localRepositoryWrapper, gitWrapper);
    }

}
