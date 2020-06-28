package io.github.jabrena.broker.impl;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jabrena.broker.Authentication;
import io.github.jabrena.broker.GitBrokerClientException;
import io.github.jabrena.broker.GitClientWrapper;
import io.github.jabrena.broker.LocalDirectoryWrapper;
import io.github.jabrena.broker.Producer;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Slf4j
public final class ProducerImpl<T> implements Producer<T> {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;

    private final String broker;
    private final String topic;
    private final String node;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Constructor
     *
     * @param authentication authentication
     * @param topic application
     * @param node event
     */
    public ProducerImpl(@NonNull String broker,
                        @NonNull Authentication authentication,
                        @NonNull String topic,
                        @NonNull String node) {

        this.localRepositoryWrapper = new LocalDirectoryWrapper();
        this.gitWrapper = new GitClientWrapper();

        this.broker = broker;
        this.topic = topic;
        this.node = node;

        this.localRepositoryWrapper.createLocalRepository();
        this.gitWrapper.cloneRepository(localRepositoryWrapper.getLocalFS(), this.broker);
        this.gitWrapper.setAuthentication(authentication);
        this.gitWrapper.checkout(this.topic);
    }

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public String getProducerName() {
        return null;
    }

    @Override
    public String send(T message) throws GitBrokerClientException {

        final String fileName = this.getFilename(this.node);

        LOGGER.info("Producing event: {}", fileName);

        final String fileContent = getFileContent(message);

        gitWrapper.upgradeRepository(this.topic);
        gitWrapper.addFile(this.localRepositoryWrapper.getLocalFS(), fileName, fileContent);
        gitWrapper.push();

        return fileName;
    }

    @SneakyThrows
    private String getFileContent(Object message) {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        return objectMapper.writeValueAsString(message);
    }

    private String getFilename(String node) {
        return getEpoch() + "_" + node + ".json";
    }

    private long getEpoch() {
        return System.currentTimeMillis();
    }


    @Override
    public CompletableFuture<String> sendAsync(T message) {
        return CompletableFuture.supplyAsync(() -> this.send(message));
    }

    @Override
    public long getLastSequenceId() {
        return 0;
    }

    @Override
    public void close() throws GitBrokerClientException {

        if (Objects.nonNull(this.localRepositoryWrapper.getLocalFS())) {
            try {
                Files.walk(this.localRepositoryWrapper.getLocalFS().toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);

                assert (!this.localRepositoryWrapper.getLocalFS().exists());
            } catch (IOException e) {
                LOGGER.warn(e.getLocalizedMessage(), e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }

    @Override
    public boolean isConnected() {
        return false;
    }
}
