package io.github.jabrena.broker.impl;

import io.github.jabrena.broker.BrokerClientException;
import io.github.jabrena.broker.BrokerFileParser;
import io.github.jabrena.broker.GitClientWrapper;
import io.github.jabrena.broker.LocalDirectoryWrapper;
import io.github.jabrena.broker.Message;
import io.github.jabrena.broker.Reader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static java.util.stream.Collectors.toUnmodifiableList;

public class ReaderImpl<T> implements Reader<T> {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final String topic;

    private Iterator<BrokerFileParser> list;

    /**
     * Constructor
     * @param localRepositoryWrapper localRepositoryWrapper
     * @param gitWrapper gitWrapper
     * @param topic topic
     */
    public ReaderImpl(LocalDirectoryWrapper localRepositoryWrapper, GitClientWrapper gitWrapper, String topic) {

        this.localRepositoryWrapper = localRepositoryWrapper;
        this.gitWrapper = gitWrapper;
        this.topic = topic;

        init();
    }

    private void init() {
        this.gitWrapper.checkout(this.topic);
        this.gitWrapper.upgradeRepository(this.topic);

        var localDirectory = this.localRepositoryWrapper.getLocalFS();
        list = Arrays.stream(localDirectory.list())
            .filter(y -> y.indexOf(".json") != -1)
            .sorted()
            .map(BrokerFileParser::new)
            .collect(toUnmodifiableList())
            .iterator();
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public Message<T> readNext() throws BrokerClientException {
        try {
            return new MessageImpl<T>(list.next(), localRepositoryWrapper);
        } catch (NoSuchElementException e) {
            throw new BrokerClientException(e);
        }
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws BrokerClientException {
        return null;
    }

    @Override
    public CompletableFuture<Message<T>> readNextAsync() {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }

    @Override
    public boolean hasReachedEndOfTopic() {
        return !list.hasNext();
    }

    @Override
    public boolean hasMessageAvailable() throws BrokerClientException {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> hasMessageAvailableAsync() {
        return null;
    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public void seek(long timestamp) throws BrokerClientException {

    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
