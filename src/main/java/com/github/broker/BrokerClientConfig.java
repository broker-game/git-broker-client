package com.github.broker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

@Slf4j
@Data
@AllArgsConstructor
public class BrokerClientConfig {

    private static String DEFAULT_BROKER_CLIENT_CONFIG_FILE = "brokerclient.properties";

    private static final String PREFIX = "brokerclient";
    private static final String DOT = ".";
    private final String KEY_NODE = "node";
    private final String KEY_BROKER = "broker";
    private final String KEY_USER = "user";
    private final String KEY_PASSWORD = "password";
    private final String KEY_FULLNAME = "fullname";
    private final String KEY_EMAIL = "email";
    private final String KEY_APPLICATION = "application";

    private final String broker;
    private final String application;
    private final String node;
    private final String fullName;
    private final String email;
    private final String user;
    private final String password;

    /**
     * Constructor
     *
     * @param propFileName Property file name
     */
    public BrokerClientConfig(String propFileName) {

        var properties = loadConfigurationFromProperties(propFileName);
        this.broker =       properties.getProperty(getKeyPath(KEY_BROKER));
        this.application =  properties.getProperty(getKeyPath(KEY_APPLICATION));
        this.node =         properties.getProperty(getKeyPath(KEY_NODE));
        this.fullName =     properties.getProperty(getKeyPath(KEY_FULLNAME));
        this.email =        properties.getProperty(getKeyPath(KEY_EMAIL));
        this.user =         properties.getProperty(getKeyPath(KEY_USER));
        this.password =     properties.getProperty(getKeyPath(KEY_PASSWORD));
    }

    public BrokerClientConfig(String propFileName, String instance) {

        var properties = loadConfigurationFromProperties(propFileName);
        this.broker =       properties.getProperty(getKeyPath(instance, KEY_BROKER));
        this.application =  properties.getProperty(getKeyPath(instance, KEY_APPLICATION));
        this.node =         properties.getProperty(getKeyPath(instance, KEY_NODE));
        this.fullName =     properties.getProperty(getKeyPath(instance, KEY_FULLNAME));
        this.email =        properties.getProperty(getKeyPath(instance, KEY_EMAIL));
        this.user =         properties.getProperty(getKeyPath(instance, KEY_USER));
        this.password =     properties.getProperty(getKeyPath(instance, KEY_PASSWORD));
    }

    public BrokerClientConfig() {
        this(DEFAULT_BROKER_CLIENT_CONFIG_FILE);
    }

    private String getKeyPath(String key) {
        return PREFIX + DOT + key;
    }

    private String getKeyPath(String instance, String key) {
        return PREFIX + DOT + instance + DOT + key;
    }

    private Properties loadConfigurationFromProperties(String propFileName) {

        LOGGER.debug("Loading configuration from: {}", propFileName);

        Properties prop = new Properties();
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName)) {

            if (Objects.nonNull(inputStream)) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

        } catch (IOException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
        }

        return prop;
    }
}
