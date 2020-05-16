package com.github.broker;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.BDDAssertions.then;
import static org.mockito.Mockito.doNothing;

@Slf4j
public class BrokerClientTests2 {

    private BrokerClientConfig defaultConfig;

    @Mock
    private LocalRepository mockLocalRepository;

    @Mock
    private GitWrapper mockGitWrapper;

    private BrokerClient defaultBrokerClient;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        defaultConfig = new BrokerClientConfig();
        defaultBrokerClient = new BrokerClient(defaultConfig);

        //Inject Mocks
        defaultBrokerClient.setLocalRepository(mockLocalRepository);
        defaultBrokerClient.setGitWrapper(mockGitWrapper);
    }

    @AfterEach
    public void close() {
        defaultBrokerClient.close();
    }

    @Test
    public void given_Client_when_connectWithBroker_then_Ok() {

        doNothing().when(mockLocalRepository).createLocalRepository(ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).cloneRepository(ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        var result =  defaultBrokerClient.connect();

        then(result).isEqualTo(true);
    }

    private static class Message {
        private final String value = "OK";
    }

    @Test
    public void given_Client_when_produceEvent_then_Ok() {

        doNothing().when(mockLocalRepository).createLocalRepository(ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).cloneRepository(ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        doNothing().when(mockGitWrapper).upgradeRepository(ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).addFile(ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).push(ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        defaultBrokerClient.connect();

        final String EVENT = "PING";
        final Message MESSAGE = new Message();

        then(defaultBrokerClient.produce(EVENT, MESSAGE)).isEqualTo(true);
    }

    @Disabled
    @Test
    public void given_Client_when_consumeForEvent_then_Ok() {

        doNothing().when(mockLocalRepository).createLocalRepository(ArgumentMatchers.anyString());
        doNothing().when(mockGitWrapper).cloneRepository(ArgumentMatchers.any(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());

        defaultBrokerClient.connect();
        final String EVENT = "PING";
        final int poolingPeriod = 1;

        BrokerResponse response = defaultBrokerClient.consume(EVENT, poolingPeriod);
        then(response).isNotNull();
    }

}
