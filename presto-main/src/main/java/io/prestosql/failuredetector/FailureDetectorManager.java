package io.prestosql.failuredetector;

import io.airlift.log.Logger;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.failuredetector.FailureRetryFactory;
import io.prestosql.spi.failuredetector.FailureRetryPolicy;


import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class FailureDetectorManager {
    private static final Logger LOG = Logger.get(FailureDetectorManager.class);
    private static final String MAIN_CONFIG_DIR = "etc/";
    private static final String MAIN_CONFIG_FILE = "config.properties";
    private static final String FD_RETRY_CONFIG_DIR = "etc/failure-retry-policy/";
    private static final String DEFAULT_CONFIG_NAME = "default";

    private String failureRetryPolicyConfig;

    private static final List<FailureDetector> failureDetectors = new ArrayList<>();

    private static final Map<String, FailureRetryFactory> failureRetryFactories = new ConcurrentHashMap<>();
    private static final Map<String, Properties> availableFrConfigs = new ConcurrentHashMap<>();

    @Inject
    public FailureDetectorManager(FailureDetector failureDetector) {

        requireNonNull(failureDetector, "failureDetector is null");
        failureDetectors.add(failureDetector);
        Properties defaultProfile = new Properties();
        defaultProfile.setProperty(FailureRetryPolicy.FD_RETRY_TYPE, FailureRetryPolicy.TIMEOUT);
        defaultProfile.setProperty(FailureRetryPolicy.MAX_TIMEOUT_DURATION, FailureRetryPolicy.DEFAULT_TIMEOUT_DURATION);
        availableFrConfigs.put(DEFAULT_CONFIG_NAME, defaultProfile);
        this.failureRetryPolicyConfig = DEFAULT_CONFIG_NAME;
    }

    public static FailureDetector getDefaultFailureDetector()
    {
        return failureDetectors.get(0);
    }
    public static void addFailureRetryFactory(FailureRetryFactory factory) {
        failureRetryFactories.putIfAbsent(factory.getName(), factory);
    }

    public String getFailureRetryPolicyUserProfile() {
        File etcDir = new File(MAIN_CONFIG_DIR);
        if (!etcDir.exists() || !etcDir.isDirectory()) {
            LOG.error("--etc directory not found. Skipped loading --");
            return null;
        }
        String[] files = requireNonNull(etcDir.list(),
                "Error reading directory: " + MAIN_CONFIG_DIR);
        String configName = DEFAULT_CONFIG_NAME;
        for (String configFile : files) {
            if (MAIN_CONFIG_FILE.equals(configFile)) {
                File config = new File(MAIN_CONFIG_FILE);
                try {
                    Properties properties = loadProperties(config);
                    configName = properties.getProperty(FailureRetryPolicy.FD_RETRY_PROFILE);

                } catch (IOException e) {
                    LOG.warn("retry policy config not set. Going for default.");
                }
                finally {
                    return configName;
                }
            }
        }
        return DEFAULT_CONFIG_NAME;
    }

    public void loadFactoryConfigs()
            throws IOException {

        LOG.info(String.format("-- Available failure retry policy factories: %s --", failureRetryFactories.keySet().toString()));
        LOG.info("-- failure retry policy --");

        File configDir = new File(FD_RETRY_CONFIG_DIR);
        if (!configDir.exists() || !configDir.isDirectory()) {
            LOG.info("-- failure retry policy configs not found. Skipped loading --");
            return;
        }

        String[] failureRetries = configDir.list();

        if (failureRetries == null || failureRetries.length == 0) {
            LOG.info("-- no retry policy set. Default failure retry policy will be used. --");
            return;
        }

        for (String fileName : failureRetries) {
            if (fileName.endsWith(".properties")) {

                String configName = fileName.replaceAll("\\.properties", "");
                File configFile = new File(FD_RETRY_CONFIG_DIR + fileName);
                Properties properties = loadProperties(configFile);

                String configType = properties.getProperty(FailureRetryPolicy.FD_RETRY_TYPE);
                checkState(configType != null, "%s must be specified in %s",
                        FailureRetryPolicy.FD_RETRY_TYPE, configFile.getCanonicalPath());
                checkState(failureRetryFactories.containsKey(configType),
                        "Factory for failure retry policy type %s not found", configType);

                availableFrConfigs.put(configName, properties);
                LOG.info(String.format("Loaded '%s' failure retry policy config '%s'", configType, configName));

                LOG.info(String.format("-- Loaded file system profiles: %s --",
                        availableFrConfigs.keySet().toString()));
            }
        }
    }

    private Properties loadProperties(File configFile)
            throws IOException
    {
        Properties properties = new Properties();
        try (InputStream in = new FileInputStream(configFile)) {
            properties.load(in);
        }
        return properties;
    }

    public FailureRetryPolicy getFailureRetryPolicy(String name)
    {
        String profileName;
        if (!availableFrConfigs.containsKey(name)) {
            LOG.error(String.format("Profile %s is not available. using default profile.", name));
            profileName = DEFAULT_CONFIG_NAME;
        }
        else {
            profileName = name;
        }
        Properties frConfig = availableFrConfigs.get(profileName);
        return getFailureRetryPolicy(frConfig);
    }

    private String checkProperty(Properties properties, String key)
    {
        String val = properties.getProperty(key);
        if (val == null) {
            throw new IllegalArgumentException(String.format("Configuration entry '%s' must be specified", key));
        }
        return val;
    }

    public FailureRetryPolicy getFailureRetryPolicy(Properties properties)
    {
        String type = checkProperty(properties, FailureRetryPolicy.FD_RETRY_TYPE);
        checkState(failureRetryFactories.containsKey(type),
                "Factory for failure retry policy type %s not found", type);
        FailureRetryFactory factory = failureRetryFactories.get(type);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            return factory.getFailureRetryPolicy(properties);
        }
    }

}
