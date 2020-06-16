package io.github.jabrena.broker;

public interface Message<T> {

    T getValue();

    long getPublishTime();

    long getEventTime();

    String getTopicName();
}
