package io.github.jabrena.broker.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jabrena.broker.BrokerFileParser;
import io.github.jabrena.broker.LocalDirectoryWrapper;
import io.github.jabrena.broker.Message;
import lombok.SneakyThrows;

import java.io.File;

public class MessageImpl<T> implements Message<T> {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final long publishTime;
    private final String raw;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructor
     *
     * @param brokerFileParser BrokerFileParser
     * @param localRepositoryWrapper localRepositoryWrapper
     */
    public MessageImpl(BrokerFileParser brokerFileParser, LocalDirectoryWrapper localRepositoryWrapper) {
        this.localRepositoryWrapper = localRepositoryWrapper;
        this.publishTime = brokerFileParser.getEpoch();
        this.raw = brokerFileParser.getRaw();
    }

    @SneakyThrows
    private Object getFileContent(File file) {
        return objectMapper.readValue(file, Object.class);
    }

    @Override
    public T getValue() {
        return (T) getFileContent(localRepositoryWrapper.getLocalFS().toPath().resolve(this.raw).toFile());
    }

    @Override
    public long getPublishTime() {
        return this.publishTime;
    }

    @Override
    public long getEventTime() {
        return System.currentTimeMillis();
    }

    @Override
    public String getTopicName() {
        return null;
    }
}
