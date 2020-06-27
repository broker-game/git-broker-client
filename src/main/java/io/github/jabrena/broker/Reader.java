package io.github.jabrena.broker;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Reader can be used to scan through all the messages currently available in a topic.
 */
public interface Reader<T> extends Closeable {

    String getTopic();

    Message<T> readNext() throws BrokerClientException;

    Message<T> readNext(int timeout, TimeUnit unit) throws BrokerClientException;

    CompletableFuture<Message<T>> readNextAsync();

    CompletableFuture<Void> closeAsync();

    boolean hasReachedEndOfTopic();

    boolean hasMessageAvailable() throws BrokerClientException;

    CompletableFuture<Boolean> hasMessageAvailableAsync();

    boolean isConnected();

    void seek(long timestamp) throws BrokerClientException;

    CompletableFuture<Void> seekAsync(long timestamp);
}
