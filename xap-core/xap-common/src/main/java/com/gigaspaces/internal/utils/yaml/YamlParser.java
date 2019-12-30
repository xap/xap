package com.gigaspaces.internal.utils.yaml;

import com.gigaspaces.api.InternalApi;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Abstraction for Yaml parsing
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public interface YamlParser {
    Map<String, Object> parse(Path path) throws IOException;
}
