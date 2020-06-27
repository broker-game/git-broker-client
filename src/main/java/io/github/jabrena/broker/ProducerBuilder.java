package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.ProducerImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ProducerBuilder {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
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
            localRepositoryWrapper,
            gitWrapper,
            authentication,
            topic,
            node);
    }


}
