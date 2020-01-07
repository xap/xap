package com.gigaspaces.internal.utils.yaml;

import com.gigaspaces.api.InternalApi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/**
 * Yaml utilities
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class YamlUtils {

    public static Map<String, Object> parse(Path path) throws IOException {
        return YamlParserFactory.create().parse(path);
    }

    public static Properties toProperties(Map<String, Object> yaml) {
        Properties result = new Properties();
        toProperties(result, yaml, "");
        return result;
    }

    public static Map<String, String> toMap(Map<String, Object> yaml) {
        Map<String, String> result = new HashMap<>();
        toProperties(yaml).forEach((k, v) -> result.put((String)k, (String)v));
        return result;
    }

    private static void toProperties(Properties properties, Map<String, Object> yaml, String prefix) {
        yaml.forEach((key, value) -> {
            if (value instanceof Map) {
                toProperties(properties, (Map) value, prefix + key + ".");
            } else if (value instanceof List) {
                List list = (List) value;
                for (int i = 0; i < list.size(); i++) {
                    Object listItem = list.get(i);
                    if (listItem instanceof Map) {
                        toProperties(properties, (Map) listItem, prefix + key + "[" + i + "].");
                    } else {
                        properties.setProperty(prefix + key + "[" + i + "]", listItem.toString());
                    }
                }
            } else if (value != null) {
                properties.setProperty(prefix + key,  value.toString());
            }
        });
    }

}
