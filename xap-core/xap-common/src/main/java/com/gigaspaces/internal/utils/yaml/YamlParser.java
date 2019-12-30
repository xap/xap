package com.gigaspaces.internal.utils.yaml;

import com.gigaspaces.api.InternalApi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
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
    default Map<String, Object> parse(Path path) throws IOException {
        try (InputStream inputStream = Files.newInputStream(path)) {
            return parse(inputStream);
        }
    }

    Map<String, Object> parse(InputStream stream) throws IOException;
}
