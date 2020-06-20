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

    public ClientBuilder serviceUrl(String broker) {
        this.broker = broker;
        return this;
    }

    public ClientBuilder topic(String topic) {
        this.application = topic;
        return this;
    }

    /**
     * Method to build a BrokerClient
     * @return BrokerClient
     */
    public BrokerClient build() {

        return new BrokerClient(
            this.broker,
            this.application,
            this.node,
            this.fullName,
            this.email,
            this.user,
            this.password
        );
    }

    //ClientBuilder authentication(Authentication authentication);
    //ClientBuilder loadConf(Map<String, Object> config);
    //ClientBuilder connectionTimeout(int duration, TimeUnit unit);

}
