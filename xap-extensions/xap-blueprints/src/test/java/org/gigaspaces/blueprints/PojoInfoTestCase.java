package org.gigaspaces.blueprints;

import com.gigaspaces.internal.io.BootIOUtils;
import org.gigaspaces.blueprints.java.PojoInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PojoInfoTestCase {
    @Test
    public void basicTestWithInitialLoad() throws IOException {
        String expected = BootIOUtils.readAsString(BootIOUtils.getResourcePath("samples/Person.java"));
        PojoInfo personPojoInfo = new PojoInfo("Person", "com.gigaspaces.demo");
        personPojoInfo.addInitialLoadQuery("rowNum < 1000");
        personPojoInfo.addProperty("id", int.class);
        personPojoInfo.addProperty("name", String.class);
        personPojoInfo.addPropertyWithAutoGenerate("auto-generate", long.class);

        String actual = personPojoInfo.generate();
        System.out.println(actual);
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testEmptyInitialLoad() throws IOException {
        String expected = BootIOUtils.readAsString(BootIOUtils.getResourcePath("samples/Person.java"));
        PojoInfo personPojoInfo = new PojoInfo("Person", "com.gigaspaces.demo");
        personPojoInfo.addProperty("id", int.class);
        personPojoInfo.addProperty("name", String.class);
        personPojoInfo.addPropertyWithAutoGenerate("auto-generate", long.class);
        String actual = personPojoInfo.generate();
        Assert.assertTrue(!actual.contains("@SpaceInitialLoadQuery"));
    }
}
