package com.gigaspaces.internal.zookeeper;

import com.gigaspaces.api.InternalApi;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
@InternalApi
public class ZNodePathFactory {
    private static final String XAP_PREFIX = "/xap/";
    private static final String PUS = "pus";
    private static final String LOCKS = "locks";
    private static final String SPACES = "spaces";

    public static String path(String ... elements) {
        return XAP_PREFIX + String.join("/", elements);
    }


    private static String path(String category, String categoryId, String component, String partitionId, String ... elements) {
        String suffix = elements != null && elements.length != 0 ? "/" + String.join("/", elements) : "";
        return XAP_PREFIX + String.join("/", category, categoryId, component, partitionId) + suffix;
    }

    public static String processingUnit(String puName) {
        return path(PUS, puName);
    }

    public static String lockPuName(String name) {
        return path(LOCKS,PUS, name);
    }

    public static String lockPersistentName(String puName) {
        return path(LOCKS,PUS, puName+"/persistent");
    }

    public static String processingUnit(String puName, String component) {
        return path(PUS, puName, component);
    }

    public static String processingUnit(String puName, String component, int partitionId, String ... elements) {
        return path(PUS, puName, component, String.valueOf(partitionId), elements);
    }

    public static String space(String spaceName) {
        return path(SPACES, spaceName);
    }

    public static String space(String spaceName, String component, int partitionId, String ... elements) {
        return path(SPACES, spaceName, component, String.valueOf(partitionId), elements);
    }
    public static String space(String spaceName, String component, String partitionId, String ... elements) {
        return path(SPACES, spaceName, component, partitionId, elements);
    }

    public static String space(String spaceName, String componenet) {
        return path(SPACES, spaceName+"/"+componenet);
    }
}
