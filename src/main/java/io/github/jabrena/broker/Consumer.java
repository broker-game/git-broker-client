package io.github.jabrena.broker;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface Consumer<T> extends Closeable {

    String getTopic();

    Message<T> receive(String event) throws BrokerClientException;

    CompletableFuture<Message<T>> receiveAsync(String event);

    Message<T> receive(int timeout, TimeUnit unit) throws BrokerClientException;

    @Override
    void close() throws BrokerClientException;

    CompletableFuture<Void> closeAsync();

    boolean hasReachedEndOfTopic();
    boolean isConnected();

    /**
     * Get the name of consumer.
     * @return consumer name.
     */
    String getConsumerName();

}
