package com.gigaspaces.internal.utils.yaml;

import com.gigaspaces.api.InternalApi;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Yaml utilities
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class YamlUtils {
    public static Properties toProperties(Map<String, Object> yaml) {
        Properties result = new Properties();
        toProperties(result, yaml, "");
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
