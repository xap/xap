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
