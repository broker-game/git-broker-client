package io.github.jabrena.broker;

import lombok.Data;

import java.util.Optional;

@Data
public class GitBrokerFileParser {

    private final String raw;
    private final long epoch;
    private Optional<String> node = Optional.empty();

    /**
     * Constructor
     *
     * @param fileName fileName
     */
    public GitBrokerFileParser(String fileName) {

        this.raw = fileName;

        String[] parts = fileName.split("\\.");
        var fileNameParts = parts[0].split("_");
        this.epoch = Long.valueOf(fileNameParts[0]);
        if (parts.length > 2) {
            this.node = Optional.of(fileNameParts[1]);
        }

    }

}
