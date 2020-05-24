package io.github.jabrena.broker;

import lombok.Data;

@Data
public class BrokerFileParser {

    private final long epoch;
    private final String node;
    private final String event;

    /**
     * Constructor
     *
     * @param fileName fileName
     */
    public BrokerFileParser(String fileName) {
        String[] parts = fileName.split("\\.");
        var fileNameParts = parts[0].split("_");
        this.epoch = Long.valueOf(fileNameParts[0]);
        this.node = fileNameParts[1];
        this.event = fileNameParts[2];
    }

}
