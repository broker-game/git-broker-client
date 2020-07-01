package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.ConsumerImpl;
import io.github.jabrena.broker.impl.GitBrokerClientImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConsumerBuilder {

    private final GitBrokerClientImpl client;
    private final String broker;
    private final Authentication authentication;

    private String topic;
    private String node;

    public ConsumerBuilder topic(String topic) {
        this.topic = topic;
        return this;
    }

    public ConsumerBuilder node(String node) {
        this.node = node;
        return this;
    }

    /**
     * Subscribe
     * @return ConsumerImpl
     */
    public Consumer subscribe() {
        return new ConsumerImpl(
            client,
            broker,
            authentication,
            topic,
            node);
    }
}
