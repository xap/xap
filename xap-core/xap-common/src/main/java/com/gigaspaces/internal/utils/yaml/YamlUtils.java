/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        toMap(yaml).forEach(result::setProperty);
        return result;
    }

    public static Map<String, String> toMap(Map<String, Object> yaml) {
        Map<String, String> result = new LinkedHashMap<>();
        toMap(result, yaml, "");
        return result;
    }

    private static void toMap(Map<String, String> properties, Map<String, Object> yaml, String prefix) {
        yaml.forEach((key, value) -> {
            if (value instanceof Map) {
                toMap(properties, (Map) value, prefix + key + ".");
            } else if (value instanceof List) {
                List list = (List) value;
                for (int i = 0; i < list.size(); i++) {
                    Object listItem = list.get(i);
                    if (listItem instanceof Map) {
                        toMap(properties, (Map) listItem, prefix + key + "[" + i + "].");
                    } else {
                        properties.put(prefix + key + "[" + i + "]", listItem.toString());
                    }
                }
            } else if (value != null) {
                properties.put(prefix + key,  value.toString());
            }
        });
    }
}
