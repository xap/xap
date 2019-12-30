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
