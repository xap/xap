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
