package io.github.jabrena.broker;

public interface Messages<T> extends Iterable<Message<T>> {

    /**
     * Get number of messages.
     */
    int size();
}
