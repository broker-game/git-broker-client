package io.github.jabrena.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@Slf4j
public class BrokerClientTests2 {

    private BrokerClientConfig defaultConfig;

    @Mock
    private LocalDirectoryWrapper mockLocalRepository;

    @Mock
    private GitClientWrapper mockGitWrapper;

    private BrokerClient defaultBrokerClient;

    /**
     * Setup
     */
    @BeforeEach
    public void setUp() {
        initMocks(this);

        defaultConfig = new BrokerClientConfig();
        defaultBrokerClient = new BrokerClient(defaultConfig);

        //Inject Mocks
        defaultBrokerClient.setLocalRepository(mockLocalRepository);
        defaultBrokerClient.setGitWrapper(mockGitWrapper);
    }

    @AfterEach
    public void close() {
        //defaultBrokerClient.close();
    }

    private static class Message {
        private final String value = "OK";
    }

    @Test
    public void given_Client_when_produceEvent_then_Ok() {

        doNothing().when(mockLocalRepository).createLocalRepository(ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper)
            .cloneRepository(ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        doNothing().when(mockGitWrapper).upgradeRepository(ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).addFile(
            ArgumentMatchers.any(), ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).push(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        final String EVENT = "PING-EVENT";
        final Message MESSAGE = new Message();

        then(defaultBrokerClient.produce(EVENT, MESSAGE)).isEqualTo(true);
    }

    @Disabled
    @Test
    public void given_Client_when_consumeForEvent_then_Ok() throws IOException {

        doNothing().when(mockLocalRepository).createLocalRepository(ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).cloneRepository(
            ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).upgradeRepository(ArgumentMatchers.anyString());

        String[] fileArray = {
            "1589573422620_PING_PING.json",
            "1589573437533_PING_PING.json",
            "1589582622634_PING-NODE_OK.json"
        };
        //Path tempDirectory = Files.createTempDirectory("PING");
        Path tempFile2 = Files.createTempFile("PING", "");

        System.out.println(tempFile2.toString());

        String data = "Test data";

        Files.write(tempFile2, data.getBytes());
        File tempFile = tempFile2.toFile();

        when(mockLocalRepository.getLocalFS()).thenReturn(tempFile);
        doNothing().when(mockGitWrapper).addFile(
            ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
            ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).push(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        final String EVENT = "PING";
        final int poolingPeriod = 1;

        BrokerResponse response = defaultBrokerClient.consume(EVENT, poolingPeriod);
        then(response).isNotNull();
    }
}
