package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.ConsumerImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ConsumerBuilder {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final BrokerClientConfig config;

    private String application;

    public ConsumerBuilder topic(String topic) {
        this.application = topic;
        return this;
    }

    public Consumer subscribe() {
        return new ConsumerImpl(localRepositoryWrapper, gitWrapper, config);
    }
}
