package io.github.jabrena.broker;

public class ClientBuilder {

    //Branch
    private String application;

    //Node
    private String node;

    //Credentials
    private String broker;
    private String user;
    private String password;
    private String fullName;
    private String email;
    private Authentication authentication;

    public ClientBuilder serviceUrl(String broker) {
        this.broker = broker;
        return this;
    }

    /**
     * Add authentication data
     * @param authentication Authentication object
     * @return ClientBuilder
     */
    public ClientBuilder authentication(Authentication authentication) {
        this.authentication = authentication;
        return this;
    }

    /**
     * Method to build a BrokerClient
     * @return BrokerClient
     */
    public BrokerClient build() {

        return new BrokerClient(
            this.broker,
            this.authentication
        );
    }

    @Deprecated
    public ClientBuilder node(String node) {
        this.node = node;
        return this;
    }

    //ClientBuilder loadConf(Map<String, Object> config);
    //ClientBuilder connectionTimeout(int duration, TimeUnit unit);

}
