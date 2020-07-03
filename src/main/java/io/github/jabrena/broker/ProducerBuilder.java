package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.GitBrokerClientImpl;
import io.github.jabrena.broker.impl.ProducerImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProducerBuilder {

    private final GitBrokerClientImpl client;
    private final String broker;
    private final Authentication authentication;

    private String topic;
    private String node;

    public ProducerBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public ProducerBuilder node(String node) {
        this.node = node;
        return this;
    }

    /**
     * Create
     *
     * @return Producer
     */
    public Producer create() {
        return new ProducerImpl(
            client,
            broker,
            authentication,
            topic,
            node);
    }


}
