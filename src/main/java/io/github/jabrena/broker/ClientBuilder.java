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

    public ClientBuilder authentication(Authentication authentication) {
        this.fullName = authentication.getFullName();
        this.email = authentication.getEmail();
        this.user = authentication.getUser();
        this.password = authentication.getPassword();
        return this;
    };

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

    public ClientBuilder node(String node) {
        this.node = node;
        return this;
    }

    //ClientBuilder loadConf(Map<String, Object> config);
    //ClientBuilder connectionTimeout(int duration, TimeUnit unit);

}
