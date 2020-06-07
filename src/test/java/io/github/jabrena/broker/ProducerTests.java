package io.github.jabrena.broker;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

public class ProducerTests {

    @Test
    public void given_Producer_when_send_then_Ok() {

        BrokerClientConfig defaultConfig = new BrokerClientConfig("brokerclient.e2e.properties");
        BrokerClient client = new BrokerClient(defaultConfig);

        /*
        //In the future
        client = BrokerClient.builder()
            .serviceUrl("https://github.com/broker-game/broker-dev-environment")
            .build();
        */

        Producer<String> producer = client.newProducer("PING");

        IntStream.rangeClosed(1, 2)
            .forEach(x -> {
                producer.send("Hello World");
            });

        producer.close();
    }
}
