package io.github.jabrena.broker;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
final class ProducerImpl<T> implements Producer<T> {

    private final LocalDirectoryWrapper localRepositoryWrapper;
    private final GitClientWrapper gitWrapper;
    private final BrokerClientConfig config;
    private final String event;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public String getProducerName() {
        return null;
    }

    @Override
    public String send(T message) throws BrokerClientException {

        final String fileName = this.getFilename(this.event);

        LOGGER.info("Producing event: {}", fileName);

        final String fileContent = getFileContent(message);

        gitWrapper.upgradeRepository(this.config.getApplication());
        gitWrapper.addFile(this.localRepositoryWrapper.getLocalFS(), fileName, fileContent,
            this.config.getFullName(), this.config.getEmail());
        gitWrapper.push(this.config.getUser(), this.config.getPassword());

        return fileName;
    }

    @SneakyThrows
    private String getFileContent(Object message) {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        return objectMapper.writeValueAsString(message);
    }

    private String getFilename(String event) {
        return getEpoch() + "_" + this.config.getNode() + "_" + event + ".json";
    }

    private long getEpoch() {
        return System.currentTimeMillis();
    }


    @Override
    public CompletableFuture<String> sendAsync(T message) {
        return CompletableFuture
            .supplyAsync(() -> this.send(message));
    }

    @Override
    public long getLastSequenceId() {
        return 0;
    }

    @Override
    public void close() throws BrokerClientException {

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
