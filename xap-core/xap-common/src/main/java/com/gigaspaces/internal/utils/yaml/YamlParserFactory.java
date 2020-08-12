package com.gigaspaces.internal.utils.yaml;

import com.gigaspaces.api.InternalApi;

import java.util.function.Supplier;

/**
 * Factory for obtaining a yaml parser. Returns a full yaml parser if available, otherwise a lightweight parser.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class YamlParserFactory {
    private static final Supplier<YamlParser> factory = initFactory();

    private static Supplier<YamlParser> initFactory() {
        try {
            createSnakeYamlParser();
            return YamlParserFactory::createSnakeYamlParser;
        } catch (Throwable e) {
            return YamlParserFactory::createInternalParser;
        }
    }

    public static YamlParser create() {
        return factory.get();
    }

    private static YamlParser createInternalParser() {
        return new GsLightYamlParser();
    }

    private static YamlParser createSnakeYamlParser() {
        return new GsSnakeYamlWrapper();
    }
}
