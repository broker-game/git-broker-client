package io.github.jabrena.broker;

import io.github.jabrena.broker.impl.GitBrokerClientImpl;

public class GitClientBuilder {

    private String broker;
    private Authentication authentication;

    public GitClientBuilder serviceUrl(String broker) {
        this.broker = broker;
        return this;
    }

    /**
     * Add authentication data
     * @param authentication Authentication object
     * @return ClientBuilder
     */
    public GitClientBuilder authentication(Authentication authentication) {
        this.authentication = authentication;
        return this;
    }

    /**
     * Method to build a BrokerClient
     * @return BrokerClient
     */
    public GitBrokerClient build() {

        return new GitBrokerClientImpl(
            this.broker,
            this.authentication
        );
    }

}
