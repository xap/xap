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
        personPojoInfo.addProperty("id", int.class);
        personPojoInfo.addProperty("name", String.class);
        personPojoInfo.addPropertyWithAutoGenerate("auto-generate", long.class);

        String actual = personPojoInfo.generate();
        System.out.println("actual=" +actual);
        System.out.println("expected=" +expected);
        Assert.assertEquals(expected, actual);
    }

//    @Test
//    public void testEmptyInitialLoad() throws IOException {
//        String expected = BootIOUtils.readAsString(BootIOUtils.getResourcePath("samples/Person.java"));
//        PojoInfo personPojoInfo = new PojoInfo("Person", "com.gigaspaces.demo");
//        personPojoInfo.addProperty("id", int.class);
//        personPojoInfo.addProperty("name", String.class);
//        personPojoInfo.addPropertyWithAutoGenerate("auto-generate", long.class);
//    }

    @Test
    public void basicCompoundId() throws IOException {
        String expected = BootIOUtils.readAsString(BootIOUtils.getResourcePath("samples/SimpleCompoundIdPojo.java"));
        PojoInfo simpleCompoundIdPojo = new PojoInfo("SimpleCompoundIdPojo", "com.gigaspaces.demo",true);
        simpleCompoundIdPojo.annotate("@Entity(name = \"SimpleCompoundIdPojo\")")
                .annotate("@Table(name = \"SimpleCompoundIdPojo\")")
                .annotate("@SpaceClass");

        simpleCompoundIdPojo.addPropertyWithAnnotation("Field1", Integer.class,"@Column(name = \"FIELD1\")");
        simpleCompoundIdPojo.addPropertyWithAnnotation("Field2", Integer.class,"@Column(name = \"FIELD2\")");

        simpleCompoundIdPojo.getCompoundKeyClass().addPropertyWithAnnotation("FieldKey1", String.class,"@Column(name = \"FIELDKEY1\")");
        simpleCompoundIdPojo.getCompoundKeyClass().addPropertyWithAnnotation("FieldKey2", String.class,"@Column(name = \"FIELDKEY2\")");

        simpleCompoundIdPojo.addImport("com.gigaspaces.annotation.pojo.*");
        simpleCompoundIdPojo.addImport("javax.persistence.*");
        String actual = simpleCompoundIdPojo.generate();
        System.out.println("actual=" +actual);
        System.out.println("expected=" +expected);
        Assert.assertEquals(expected, actual);
    }


}
