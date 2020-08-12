package com.gigaspaces.internal.utils.yaml;

import com.gigaspaces.api.InternalApi;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * A yaml parser based on wrapping the SnakeYaml library.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class GsSnakeYamlWrapper implements YamlParser {
    private final LoadSettings settings = LoadSettings.builder().build();

    @Override
    public Map<String, Object> parse(InputStream inputStream) throws IOException {
        return (Map<String, Object>) new Load(settings).loadFromInputStream(inputStream);
    }
}
