package io.github.jabrena.broker.impl;

import io.github.jabrena.broker.Authentication;
import io.github.jabrena.broker.Consumer;
import io.github.jabrena.broker.ConsumerBuilder;
import io.github.jabrena.broker.GitBrokerClient;
import io.github.jabrena.broker.Producer;
import io.github.jabrena.broker.ProducerBuilder;
import io.github.jabrena.broker.Reader;
import io.github.jabrena.broker.ReaderBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class GitBrokerClientImpl implements GitBrokerClient {

    private final Authentication authentication;
    private final String broker;

    private List<Producer> producerList;
    private List<Consumer> consumerList;
    private List<Reader> readerList;

    /**
     * Constructor
     * @param broker broker
     * @param authentication authentication
     */
    public GitBrokerClientImpl(@NonNull String broker, Authentication authentication) {

        LOGGER.info("Creating an instance of GitBrokerClient");

        this.broker = broker;
        this.authentication = authentication;

        producerList = new ArrayList<>();
        consumerList = new ArrayList<>();
        readerList = new ArrayList<>();

    }

    /**
     * Close
     */
    @Override
    public void close() {

        producerList.stream().forEach(p -> p.close());
        consumerList.stream().forEach(c -> c.close());
        readerList.stream().forEach(r -> r.close());
    }

    @Override
    public ProducerBuilder newProducer() {

        return new ProducerBuilder(this, broker, authentication);
    }

    @Override
    public ConsumerBuilder newConsumer() {

        return new ConsumerBuilder(this, broker, authentication);
    }

    @Override
    public ReaderBuilder newReader() {

        return new ReaderBuilder(this, broker);
    }

    void addProducer(Producer producer) {
        producerList.add(producer);
    }

    void addConsumer(Consumer consumer) {
        consumerList.add(consumer);
    }

    void addReader(Reader reader) {
        readerList.add(reader);
    }
}
