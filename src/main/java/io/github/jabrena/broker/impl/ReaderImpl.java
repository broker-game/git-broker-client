package io.github.jabrena.broker.impl;

import io.github.jabrena.broker.GitBrokerClientException;
import io.github.jabrena.broker.GitBrokerFileParser;
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

    private Iterator<GitBrokerFileParser> list;

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
            .filter(y -> y.indexOf("OK.json") == -1)
            .sorted()
            .map(GitBrokerFileParser::new)
            .collect(toUnmodifiableList())
            .iterator();
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public Message<T> readNext() throws GitBrokerClientException {
        try {
            return new MessageImpl<T>(list.next(), localRepositoryWrapper);
        } catch (NoSuchElementException e) {
            throw new GitBrokerClientException(e);
        }
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws GitBrokerClientException {
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
    public boolean hasMessageAvailable() throws GitBrokerClientException {
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
    public void seek(long timestamp) throws GitBrokerClientException {

    }

    @Override
    public CompletableFuture<Void> seekAsync(long timestamp) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
