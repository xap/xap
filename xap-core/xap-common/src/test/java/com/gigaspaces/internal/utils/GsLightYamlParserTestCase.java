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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


public class GsLightYamlParserTestCase {
    @Test
    public void testCommonFunctionality() throws IOException {
        Path path = getResourcePath("com/gigaspaces/internal/utils/test.yaml");
        Map<String, Object> expected = new GsSnakeYamlWrapper().parse(path);
        Map<String, Object> actual = new GsLightYamlParser().parse(path);

        Assert.assertEquals(expected, actual);
    }

    private Path getResourcePath(String resource) {
        try {
            return Paths.get(getClass().getResource("/" + resource).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
