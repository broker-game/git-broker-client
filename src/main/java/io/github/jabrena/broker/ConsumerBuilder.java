package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.ConsumerImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConsumerBuilder {

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

    public Consumer subscribe() {
        return new ConsumerImpl(broker, authentication, topic, node);
    }
}
