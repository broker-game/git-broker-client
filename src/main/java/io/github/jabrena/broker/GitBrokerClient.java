package io.github.jabrena.broker;

public interface GitBrokerClient {

    /**
     * Close
     */
    void close();

    ProducerBuilder newProducer();

    ConsumerBuilder newConsumer();

    ReaderBuilder newReader();

    static GitClientBuilder builder() {
        return new GitClientBuilder();
    }

    //ClientBuilder loadConf(Map<String, Object> config);
    //ClientBuilder connectionTimeout(int duration, TimeUnit unit);
}
