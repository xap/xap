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
