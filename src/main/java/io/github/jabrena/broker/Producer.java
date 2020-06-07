package io.github.jabrena.broker;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

public interface Producer<T> extends Closeable {

    String getTopic();

    String getProducerName();

    String send(T message) throws BrokerClientException;

    CompletableFuture<String> sendAsync(T message);

    long getLastSequenceId();

    @Override
    void close() throws BrokerClientException;

    CompletableFuture<Void> closeAsync();

    boolean isConnected();
}
