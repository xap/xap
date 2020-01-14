package org.gigaspaces.blueprints;

import com.gigaspaces.internal.io.BootIOUtils;
import org.gigaspaces.blueprints.java.PojoInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PojoInfoTests {
    @Test
    public void testFoo() throws IOException {
        String expected = BootIOUtils.readAsString(BootIOUtils.getResourcePath("samples/Person.java"));
        String actual = new PojoInfo("Person", "com.gigaspaces.demo")
                .addProperty("id", int.class)
                .addProperty("name", String.class)
                .addPropertyWithAutoGenerate("auto-generate", long.class)
                .generate();
        System.out.println(actual);
        Assert.assertEquals(expected, actual);
    }

}
