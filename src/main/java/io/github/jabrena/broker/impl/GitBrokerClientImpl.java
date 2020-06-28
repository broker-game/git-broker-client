package io.github.jabrena.broker.impl;

import io.github.jabrena.broker.Authentication;
import io.github.jabrena.broker.ConsumerBuilder;
import io.github.jabrena.broker.GitBrokerClient;
import io.github.jabrena.broker.ProducerBuilder;
import io.github.jabrena.broker.ReaderBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GitBrokerClientImpl implements GitBrokerClient {

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
    }

    /**
     * Close
     */
    @Override
    public void close() {

        //TODO find a solution to close all objects provided
        /*
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
         */
    }

    @Override
    public ProducerBuilder newProducer() {

        return new ProducerBuilder(broker, authentication);
    }

    @Override
    public ConsumerBuilder newConsumer() {

        return new ConsumerBuilder(broker, authentication);
    }

    @Override
    public ReaderBuilder newReader() {

        return new ReaderBuilder(broker);
    }

}
