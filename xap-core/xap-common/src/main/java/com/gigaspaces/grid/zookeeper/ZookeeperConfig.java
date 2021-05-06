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

    private static final GsEnv.GsEnvProperty<String> clientPortProperty = GsEnv.property(CommonSystemProperties.ZOOKEEPER_CLIENT_PORT);
    private static final LazySingleton<ZookeeperConfig> instance = new LazySingleton<>(() -> new ZookeeperConfig(findZookeeperConfigFile()));
    private final Properties properties = new Properties();

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

        try {
            FileInputStream in = new FileInputStream(configFile);
            try {
                properties.load(in);
                String clientPort = GsEnv.property(CommonSystemProperties.ZOOKEEPER_CLIENT_PORT).get();
                if (clientPort != null) {
                    properties.setProperty(ZK_CLIENT_PORT_PROPERTY, clientPort);
                }
            } finally {
                in.close();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read properties from " + path);
        }

        StringUtils.resolvePlaceholders(properties);
    }

    public static String getDefaultClientPort() {
        return clientPortProperty.get(DEFAULT_CLIENT_PORT);
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
