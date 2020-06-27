package io.github.jabrena.broker;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

public interface Producer<T> extends Closeable {

    String getTopic();

    String getProducerName();

    String send(T message) throws GitBrokerClientException;

    CompletableFuture<String> sendAsync(T message);

    long getLastSequenceId();

    @Override
    void close() throws GitBrokerClientException;

    CompletableFuture<Void> closeAsync();

    boolean isConnected();
}
