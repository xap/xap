package com.gigaspaces.grid.zookeeper;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.internal.utils.LazySingleton;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.start.SystemLocations;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class ZookeeperConfig {

    private static final String DEFAULT_CLIENT_PORT = "2181";
    private static final String ZK_CLIENT_PORT_PROPERTY = "clientPort";

    private static final LazySingleton<ZookeeperConfig> instance = new LazySingleton<>(() -> new ZookeeperConfig(findZookeeperConfigFile()));
    private final Properties properties;

    public static ZookeeperConfig getDefaultConfig() {
        return instance.getOrCreate();
    }

    private ZookeeperConfig(String path) {
        if (path == null) {
            throw new IllegalArgumentException("Zookeeper client file path is not defined in system property nor environment variable.");
        }

        File configFile = new File(path);
        if (!configFile.exists()) {
            throw new IllegalArgumentException("File not found: " + path);
        }

        properties = loadProperties(configFile);
    }

    public static String getDefaultClientPort() {
        String clientProperty = GsEnv.property(CommonSystemProperties.ZOOKEEPER_CLIENT_PORT).get();
        if (clientProperty != null) {
            return clientProperty;
        }

        File file = new File(findZookeeperConfigFile());
        if (!file.exists()) {
            return DEFAULT_CLIENT_PORT;
        }
        return loadProperties(file).getProperty(ZK_CLIENT_PORT_PROPERTY, DEFAULT_CLIENT_PORT);
    }

    private static Properties loadProperties(File file) {
        Properties properties = new Properties();
        try {
            try (FileInputStream in = new FileInputStream(file)) {
                properties.load(in);
                String clientPort = GsEnv.property(CommonSystemProperties.ZOOKEEPER_CLIENT_PORT).get();
                if (clientPort != null) {
                    properties.setProperty(ZK_CLIENT_PORT_PROPERTY, clientPort);
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read properties from " + file.getAbsolutePath());
        }

        StringUtils.resolvePlaceholders(properties);

        return properties;
    }

    public String getClientPort() {
        return properties.getProperty(ZK_CLIENT_PORT_PROPERTY, DEFAULT_CLIENT_PORT);
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public void saveToFile(File target) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(target);
        try {
            properties.store(fileOut, "");
        } finally {
            fileOut.close();
        }
    }

    private static String findZookeeperConfigFile() {
        String result = System.getProperty(CommonSystemProperties.ZOOKEEPER_CONFIG_FILE);
        if (result == null)
            result = GsEnv.getOrElse("ZOOKEEPER_SERVER_CONFIG_FILE", () -> SystemLocations.singleton().config("zookeeper").resolve("zoo.cfg").toString());
        return result;
    }
}
