package io.github.jabrena.broker;

import lombok.Data;

@Data
public class GitBrokerFileParser {

    private final String raw;
    private final long epoch;
    private final String node;

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
        this.node = fileNameParts[1];
    }

}
