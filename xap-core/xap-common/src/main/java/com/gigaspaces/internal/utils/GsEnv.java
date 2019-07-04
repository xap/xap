package com.gigaspaces.internal.utils;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class GsEnv {

    private static final Map<String, String> env = System.getenv();

    public static String key(String suffix) {
        return key(suffix, env);
    }

    public static String key(String suffix, Map<String, String> env) {
        String gsKey = "GS_" + suffix;
        if (env.containsKey(gsKey))
            return gsKey;
        String xapKey = "XAP_" + suffix;
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
}
