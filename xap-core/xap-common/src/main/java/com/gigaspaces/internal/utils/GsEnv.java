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
package com.gigaspaces.internal.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class GsEnv {

    private static final Map<String, String> env = System.getenv();
    private static final String GS_ENV_PREFIX = "GS_";
    private static final String XAP_ENV_PREFIX = "XAP_";

    public static String key(String suffix) {
        return key(suffix, env);
    }

    public static String keyOrDefault(String suffix) {
        String key = key(suffix);
        return key != null ? key : GS_ENV_PREFIX + suffix;
    }

    public static String key(String suffix, Map<String, String> env) {
        String gsKey = GS_ENV_PREFIX + suffix;
        if (env.containsKey(gsKey))
            return gsKey;
        String xapKey = XAP_ENV_PREFIX + suffix;
        if (env.containsKey(xapKey))
            return xapKey;
        return null;
    }

    public static String get(String suffix) {
        return get(suffix, null, env);
    }

    public static String get(String suffix, Map<String, String> env) {
        return get(suffix, null, env);
    }

    public static String get(String suffix, String defaultValue) {
        return get(suffix, defaultValue, env);
    }

    public static String get(String suffix, String defaultValue, Map<String, String> env) {
        String key = key(suffix, env);
        return key != null ? env.get(key) : defaultValue;
    }

    public static Optional<String> getOptional(String suffix) {
        return Optional.ofNullable(get(suffix, null, env));
    }

    public static String getOrElse(String suffix, Supplier<String> defaultSupplier) {
        String key = key(suffix);
        return key != null ? env.get(key) : defaultSupplier.get();
    }

    public static Map<String, String> filterByPrefix(Map<String, String> env) {
        final Map<String, String> result = new HashMap<>(env.size());
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(GS_ENV_PREFIX)
                    || key.startsWith(XAP_ENV_PREFIX)) {
                String value = entry.getValue();
                result.put(key, value);
            }
        }
        return result;
    }

    public static Map<String, String> getPropertiesWithPrefix(String sysPropPrefix) {
        return getPropertiesWithPrefix(sysPropPrefix, System.getProperties(), env);
    }

    public static Map<String, String> getPropertiesWithPrefix(String sysPropPrefix, Properties sysProps, Map<String, String> env) {
        Map<String, String> result = new HashMap<>();
        // Search for environment variables with prefix and append:
        String envPrefix = toEnvKey(sysPropPrefix);
        String gsPrefix= GS_ENV_PREFIX + envPrefix;
        String xapPrefix = XAP_ENV_PREFIX + envPrefix;
        env.forEach((k, v) -> {
            if (k.startsWith(gsPrefix)) {
                result.put(k.substring(gsPrefix.length()).toLowerCase(), v);
            } else if (k.startsWith(xapPrefix)) {
                result.put(k.substring(xapPrefix.length()).toLowerCase(), v);
            }
        });
        // Search for system properties with prefix and append (overrides env var if exists):
        sysProps.forEach((k, v) -> {
            String key = (String) k;
            if (key.startsWith(sysPropPrefix))
                result.put(key.substring(sysPropPrefix.length()), (String) v);
        });
        return result;
    }

    public static GsEnvProperty<String> property(String systemProperty) {
        return property(systemProperty, toEnvKey(systemProperty));
    }

    public static GsEnvProperty<String> property(String systemProperty, String envKey) {
        return new GsEnvProperty<>(systemProperty, envKey, s -> s);
    }

    public static GsEnvProperty<Integer> propertyInt(String systemProperty) {
        return propertyInt(systemProperty, toEnvKey(systemProperty));
    }

    public static GsEnvProperty<Integer> propertyInt(String systemProperty, String envKey) {
        return new GsEnvProperty<>(systemProperty, envKey, Integer::parseInt);
    }

    public static GsEnvProperty<Long> propertyLong(String systemProperty) {
        return propertyLong(systemProperty, toEnvKey(systemProperty));
    }

    public static GsEnvProperty<Long> propertyLong(String systemProperty, String envKey) {
        return new GsEnvProperty<>(systemProperty, envKey, Long::parseLong);
    }

    public static GsEnvProperty<Boolean> propertyBoolean(String systemProperty) {
        return propertyBoolean(systemProperty, toEnvKey(systemProperty));
    }

    public static GsEnvProperty<Boolean> propertyBoolean(String systemProperty, String envKey) {
        return new GsEnvProperty<>(systemProperty, envKey, Boolean::parseBoolean);
    }

    public static GsEnvProperty<Path> propertyPath(String systemProperty) {
        return propertyPath(systemProperty, toEnvKey(systemProperty));
    }

    public static GsEnvProperty<Path> propertyPath(String systemProperty, String envKey) {
        return new GsEnvProperty<>(systemProperty, envKey, Paths::get);
    }

    private static String toEnvKey(String key) {
        final String[] prefixes = new String[] {"com.gs.", "com.gigaspaces."};
        for (String prefix : prefixes) {
            if (key.startsWith(prefix)) {
                key = key.substring(prefix.length());
                break;
            }
        }
        return key.toUpperCase().replace('.', '_').replace('-', '_');
    }

    public static class GsEnvProperty<T> {
        private final String sysProp;
        private final String envKey;
        private final Function<String, T> parser;
        private T defaultValue;

        private GsEnvProperty(String sysProp, String envKey, Function<String, T> parser) {
            this.sysProp = sysProp;
            this.envKey = envKey;
            this.parser = parser;
        }

        public GsEnvProperty<T> withDefault(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public String name() {
            return sysProp;
        }

        public T get() {
            return get(defaultValue);
        }

        public T get(T defaultValue) {
            String result = System.getProperty(sysProp);
            if (result == null)
                result = GsEnv.get(envKey);
            return result != null ? parser.apply(result) : defaultValue;
        }

        public T getAndInit(T defaultValue) {
            T result = get(defaultValue);
            set(result);
            return result;
        }

        public void set(T value) {
            if (value != null)
                System.setProperty(sysProp, value.toString());
            else
                clear();
        }

        public void clear() {
            System.clearProperty(sysProp);
        }
    }
}
