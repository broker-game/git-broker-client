package io.github.jabrena.broker;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

@Slf4j
@Data
@AllArgsConstructor
public class BrokerClientConfig {

    private static String DEFAULT_BROKER_CLIENT_CONFIG_FILE = "brokerclient.properties";

    private static final String PREFIX = "brokerclient";
    private static final String GENERAL_INSTANCE = "general";
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
     * @param propFileName Property filename
     */
    public BrokerClientConfig(String propFileName) {

        var properties = loadConfigurationFromProperties(propFileName);
        this.broker =       getKeyPath(properties, KEY_BROKER);
        this.application =  getKeyPath(properties, KEY_APPLICATION);
        this.node =         getKeyPath(properties, KEY_NODE);
        this.fullName =     getKeyPath(properties, KEY_FULLNAME);
        this.email =        getKeyPath(properties, KEY_EMAIL);
        this.user =         getKeyPath(properties, KEY_USER);
        this.password =     getKeyPath(properties, KEY_PASSWORD);
    }

    /**
     * Constructor
     *
     * @param propFileName Property filename
     * @param instance Instance
     */
    public BrokerClientConfig(String propFileName, String instance) {

        var properties = loadConfigurationFromProperties(propFileName);
        this.broker =       getKeyPathWithInstance(properties, instance, KEY_BROKER);
        this.application =  getKeyPathWithInstance(properties, instance, KEY_APPLICATION);
        this.node =         getKeyPathWithInstance(properties, instance, KEY_NODE);
        this.fullName =     getKeyPathWithInstance(properties, instance, KEY_FULLNAME);
        this.email =        getKeyPathWithInstance(properties, instance, KEY_EMAIL);
        this.user =         getKeyPathWithInstance(properties, instance, KEY_USER);
        this.password =     getKeyPathWithInstance(properties, instance, KEY_PASSWORD);
    }

    public BrokerClientConfig() {
        this(DEFAULT_BROKER_CLIENT_CONFIG_FILE);
    }

    private String getKeyPath(Properties properties, String key) {
        return getPropertyValue(properties, PREFIX + DOT + key)
            .orElseThrow(() -> new RuntimeException("Not found key: " + key));
    }

    private String getKeyPathWithInstance(Properties properties, String instance, String key) {
        var value = getPropertyValue(properties, PREFIX + DOT + instance + DOT + key);
        if (value.isPresent()) {
            return value.get();
        }
        return getPropertyValue(properties, PREFIX + DOT + GENERAL_INSTANCE + DOT + key)
            .orElseThrow(() -> new RuntimeException("Not found key: " + key));
    }

    private Optional<String> getPropertyValue(Properties properties, String key) {
        return Optional.ofNullable(properties.getProperty(key));
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
