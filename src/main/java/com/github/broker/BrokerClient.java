package com.github.broker;

public class BrokerClient {

    public BrokerClient(String user, String password) {

    }

    public boolean connect() {
        return false;
    }

    public boolean send(String application, String event, Object message) {
        return false;
    }

    public BrokerResponse wait(String application, String event) {

        return null;
    }
}
