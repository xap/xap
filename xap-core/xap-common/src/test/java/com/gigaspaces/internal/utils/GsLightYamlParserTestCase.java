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

import com.gigaspaces.internal.utils.yaml.GsSnakeYamlWrapper;
import com.gigaspaces.internal.utils.yaml.GsLightYamlParser;
import com.gigaspaces.internal.utils.yaml.YamlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;


public class GsLightYamlParserTestCase {
    @Test
    public void testCommonFunctionality() throws IOException {
        Path path = getResourcePath("com/gigaspaces/internal/utils/test.yaml");
        Map<String, Object> expected = new GsSnakeYamlWrapper().parse(path);
        Map<String, Object> actual = new GsLightYamlParser().parse(path);
        Assert.assertEquals(expected, actual);

        Properties properties = YamlUtils.toProperties(actual);
        // Common values
        testProperty(properties, "val-bool", "true");
        testProperty(properties, "val-int", "42");
        testProperty(properties, "val-float", "3.14");
        testProperty(properties, "val-string", "foo");
        // quotes
        testProperty(properties, "string-empty", "");
        testProperty(properties, "string-empty-single", "");
        testProperty(properties, "string-space", " ");
        testProperty(properties, "string-bool", "true");
        testProperty(properties, "string-int", "42");
        testProperty(properties, "string-float", "3.14");
        // comments
        testProperty(properties, "val-1", "foo");
        testProperty(properties, "val-2", "foo # bar");
        testProperty(properties, "val-3", "foo # bar");
        testProperty(properties, "val-4", "foo '# bar");
        testProperty(properties, "val-5", "foo \"# bar");
        testProperty(properties, "val-6", "foo \\# bar");
        // list
        testProperty(properties, "val-array[0]", "one");
        testProperty(properties, "val-array[1]", "two");
        testProperty(properties, "val-array[2]", "three");
        // nested
        testProperty(properties, "val-nested.enabled", "false");
        testProperty(properties, "val-nested.level", "1");
        testProperty(properties, "val-nested.value", "foo");
        testProperty(properties, "val-nested.nested-2.foo", "bar");
        testProperty(properties, "val-nested.nested-2.level", "2");
        testProperty(properties, "val-nested.properties[0]", "key1=value1");
        testProperty(properties, "val-nested.properties[1]", "key2");
        testProperty(properties, "val-nested.properties[2]", "key3=value3");
        testProperty(properties, "val-nested.map[0].key", "one");
        testProperty(properties, "val-nested.map[0].value", "1");
        testProperty(properties, "val-nested.map[1].key", "two");
        testProperty(properties, "val-nested.map[1].value", "2");
        testProperty(properties, "val-nested.map[2].key", "three");
        testProperty(properties, "val-nested.map[2].value[0]", "1");
        testProperty(properties, "val-nested.map[2].value[1]", "2");
        testProperty(properties, "val-nested.map[2].value[2]", "3");
        testProperty(properties, "val-nested.port", "1234");

        Assert.assertEquals("unexpected properties",0, properties.size());
    }

    private void testProperty(Properties properties, String key, String expectedValue) {
        Assert.assertEquals(expectedValue, properties.getProperty(key));
        properties.remove(key);
    }

    private Path getResourcePath(String resource) {
        try {
            return Paths.get(getClass().getResource("/" + resource).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
