package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.ProducerImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProducerBuilder {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final BrokerClientConfig config;

    private String application;
    private String event;

    public ProducerBuilder topic(String topic) {
        this.application = topic;
        return this;
    }

    public ProducerBuilder event(String event) {
        this.event = event;
        return this;
    }

    public Producer create() {
        return new ProducerImpl(localRepositoryWrapper, gitWrapper, config, application, event);
    }


}
