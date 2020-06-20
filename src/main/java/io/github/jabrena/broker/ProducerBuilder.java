package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.ProducerImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProducerBuilder {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final BrokerClientConfig config;

    private String application;

    public ProducerBuilder topic(String topic) {
        this.application = topic;
        return this;
    }

    public Producer create() {
        return new ProducerImpl(localRepositoryWrapper, gitWrapper, config, application);
    }
}
