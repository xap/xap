package com.gigaspaces.internal.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class GsEnv {

    private static final Map<String, String> env = System.getenv();
    private static final String GS_ENV_PREFIX = "GS_";
    private static final String XAP_ENV_PREFIX = "XAP_";

    public static String key(String suffix) {
        return key(suffix, env);
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

    public static String keyOrElse(String suffix, String defaultKey) {
        String key = key(suffix);
        return key != null ? key : defaultKey;
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

    public static String keyOrSystemProperty(String key, String propertyKey, String defaultValue){
        return getOrElse(key,() -> System.getProperty(propertyKey, defaultValue));

    }

    public static int keyOrSystemProperty(String key, String propertyKey, int defaultValue){
        String result = keyOrSystemProperty(key, propertyKey, null);

        return result != null ?
                Integer.parseInt(result) :
                defaultValue;
    }

}
