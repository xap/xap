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

package org.openspaces.itest.persistency.cassandra;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexType;

import org.junit.Assert;
import org.junit.Test;
import org.openspaces.itest.persistency.cassandra.data.TestDocumentFactory;
import org.openspaces.test.common.data.TestPojo1;
import org.openspaces.test.common.data.TestPojo3;
import org.openspaces.test.common.data.TestPojo4;
import org.openspaces.test.common.mock.MockAddIndexData;
import org.openspaces.test.common.mock.MockDataSourceQuery;
import org.openspaces.test.common.mock.MockDataSourceSqlQuery;
import org.openspaces.test.common.mock.MockIntroduceTypeData;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;
import org.openspaces.test.common.mock.MockSpaceIndex;

import java.util.Date;

public class MultiTypeNestedPropertiesCassandraTest extends AbstractCassandraTest {

    private SpaceDocument _topLevelSpaceDocument;

    private final TestPojo3 _pojoInsidePojo = createPojoInsidePojo();

    private final SpaceDocument _pojoInsideDocument = createPojoInsideDocument();

    private final TestPojo1 _documentInsidePojo = createDocumentInsidePojo();

    private final SpaceDocument _documentInsideDocument = createDocumentInsideDocument();

    private final SpaceDocument _pojoInsidePojoInsideDocument = createPojoInsidePojoInsideDocument(_pojoInsidePojo);

    private SpaceTypeDescriptor _typeDescriptor;

    private MockIntroduceTypeData _typeData;

    /**
     * we test two different things in this test: 1) adding index in different stages: on type
     * introduction, after type introduction but before writing the actual columns after writing the
     * columns to cassandra 2) queries different nested properties (document, pojo)
     */
    @Test
    public void test() {
        // test indexing

        _topLevelSpaceDocument = createTopLevelDocument();

        // not really document only as means of builder style set
        SpaceDocument indexes = new SpaceDocument("indexes")
                .setProperty("pojoInsidePojo.name", null)
                .setProperty("pojoInsidePojo.pojo4_1.longProperty", null)
                .setProperty("pojoInsidePojo.pojo4_2.longProperty", null)
                .setProperty("pojoInsideDocument.intProperty", null)
                .setProperty("documentInsidePojo.spaceDocument.firstName", null)
                .setProperty("documentInsideDocument.intProperty", null)
                .setProperty("documentInsideDocument.spaceDocument.intProperty", null)
                .setProperty("pojoInsidePojoInsideDocument.testPojo3.name", null);

        _typeData = createIntroduceTypeDataFromSpaceDocument(_topLevelSpaceDocument, "key", indexes.getProperties().keySet());
        _typeDescriptor = _typeData.getTypeDescriptor();

        _syncInterceptor.onIntroduceType(_typeData);

        addDynamicIndex("pojoInsidePojo.pojo4_2.dateProperty");
        addDynamicIndex("pojoInsideDocument.testPojo4.longProperty");
        addDynamicIndex("documentInsidePojo.str");
        addDynamicIndex("documentInsideDocument.spaceDocument.longProperty");

        _topLevelSpaceDocument.setProperty("pojoInsidePojoInsideDocument", _pojoInsidePojoInsideDocument);

        addDynamicIndex("pojoInsidePojoInsideDocument.testPojo3.age");
        addDynamicIndex("pojoInsidePojoInsideDocument.testPojo3.pojo4_1.longProperty");

        MockOperationsBatchDataBuilder builder = new MockOperationsBatchDataBuilder();
        builder.write(_topLevelSpaceDocument, "key");
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        _dataSource.initialMetadataLoad();

        addDynamicIndex("pojoInsidePojo.age");
        addDynamicIndex("pojoInsidePojo.pojo4_1.dateProperty");
        addDynamicIndex("pojoInsideDocument.testPojo4.dateProperty");
        addDynamicIndex("documentInsidePojo.spaceDocument.lastName");
        addDynamicIndex("documentInsideDocument.spaceDocument.booleanProperty");
        addDynamicIndex("pojoInsidePojoInsideDocument.testPojo3.pojo4_1.dateProperty");

        // Test queries
        assertValidQuery("pojoInsidePojo.name = ?", _pojoInsidePojo.getName());
        assertValidQuery("pojoInsidePojo.age = ?", _pojoInsidePojo.getAge());
        assertValidQuery("pojoInsidePojo.pojo4_1.dateProperty = ?", _pojoInsidePojo.getPojo4_1().getDateProperty());

        assertValidQuery("pojoInsideDocument.intProperty = ?", _pojoInsideDocument.getProperty("intProperty"));
        assertValidQuery("pojoInsideDocument.testPojo4.longProperty = ?",
                ((TestPojo4) _pojoInsideDocument.getProperty("testPojo4")).getLongProperty());
        assertValidQuery("pojoInsideDocument.testPojo4.dateProperty = ?",
                ((TestPojo4) _pojoInsideDocument.getProperty("testPojo4")).getDateProperty());

        assertValidQuery("documentInsidePojo.str = ?", _documentInsidePojo.getStr());
        assertValidQuery("documentInsidePojo.spaceDocument.firstName = ?", _documentInsidePojo.getSpaceDocument().getProperty("firstName"));
        assertValidQuery("documentInsidePojo.spaceDocument.lastName = ?", _documentInsidePojo.getSpaceDocument().getProperty("lastName"));

        assertValidQuery("documentInsideDocument.intProperty = ?", _documentInsideDocument.getProperty("intProperty"));
        assertValidQuery("documentInsideDocument.spaceDocument.intProperty = ?",
                ((SpaceDocument) _documentInsideDocument.getProperty("spaceDocument")).getProperty("intProperty"));
        assertValidQuery("documentInsideDocument.spaceDocument.longProperty = ?",
                ((SpaceDocument) _documentInsideDocument.getProperty("spaceDocument")).getProperty("longProperty"));
        assertValidQuery("documentInsideDocument.spaceDocument.booleanProperty = ?",
                ((SpaceDocument) _documentInsideDocument.getProperty("spaceDocument")).getProperty("booleanProperty"));

        assertValidQuery("pojoInsidePojoInsideDocument.testPojo3.name = ?", _pojoInsidePojo.getName());
        assertValidQuery("pojoInsidePojoInsideDocument.testPojo3.age = ?", _pojoInsidePojo.getAge());
        assertValidQuery("pojoInsidePojoInsideDocument.testPojo3.pojo4_1.dateProperty = ?", _pojoInsidePojo.getPojo4_1().getDateProperty());

    }

    private void assertValidQuery(String query, Object... params) {
        MockDataSourceSqlQuery sqlQuery = new MockDataSourceSqlQuery(query, params);
        MockDataSourceQuery sourceQuery = new MockDataSourceQuery(_typeDescriptor, sqlQuery, Integer.MAX_VALUE);
        DataIterator<Object> iterator = _dataSource.getDataIterator(sourceQuery);
        Assert.assertTrue("Missing result", iterator.hasNext());
        SpaceDocument result = (SpaceDocument) iterator.next();
        iterator.close();

        Assert.assertEquals(_pojoInsidePojo, result.getProperty("pojoInsidePojo"));
        Assert.assertEquals(_documentInsidePojo, result.getProperty("documentInsidePojo"));
        Assert.assertEquals(_documentInsideDocument, result.getProperty("documentInsideDocument"));
        Assert.assertEquals(_pojoInsideDocument, result.getProperty("pojoInsideDocument"));
        Assert.assertEquals(_pojoInsidePojoInsideDocument, result.getProperty("pojoInsidePojoInsideDocument"));

        // uncomment if we decide pojo will be restored as documents no matter what
//        SpaceDocument cassandraPojoInsideDocument = result.getProperty("pojoInsideDocument");
//        Assert.assertEquals(_pojoInsideDocument.getProperty("intProperty"), cassandraPojoInsideDocument.getProperty("intProperty"));
//        testPojo4 originaltestPojo4 = _pojoInsideDocument.getProperty("testPojo4");
//        SpaceDocument testPojo4AsDocument = cassandraPojoInsideDocument.getProperty("testPojo4");
//        Assert.assertEquals(originaltestPojo4.getDateProperty(), testPojo4AsDocument.getProperty("dateProperty"));
//        Assert.assertEquals(originaltestPojo4.getLongProperty(), testPojo4AsDocument.getProperty("longProperty"));
//        
//        SpaceDocument cassnadraPojoInsidePojoInsideDocument = result.getProperty("pojoInsidePojoInsideDocument");
//        SpaceDocument testPojo3AsDocument = cassnadraPojoInsidePojoInsideDocument.getProperty("testPojo3");
//        SpaceDocument pojo4_1AsDocument = testPojo3AsDocument.getProperty("pojo4_1");
//        Assert.assertEquals(_pojoInsidePojo.getName(), testPojo3AsDocument.getProperty("name"));
//        Assert.assertEquals(_pojoInsidePojo.getAge(), testPojo3AsDocument.getProperty("age"));
//        Assert.assertEquals(_pojoInsidePojo.getPojo4_1().getDateProperty(),
//                            pojo4_1AsDocument.getProperty("dateProperty"));

    }

    private void addDynamicIndex(String name) {
        _syncInterceptor.onAddIndex(new MockAddIndexData(_topLevelSpaceDocument.getTypeName(),
                new SpaceIndex[]{new MockSpaceIndex(name,
                        SpaceIndexType.EQUAL)}));
    }

    private SpaceDocument createTopLevelDocument() {
        return new SpaceDocument("TopLevelDocument")
                .setProperty("key", random.nextLong())
                .setProperty("pojoInsidePojo", _pojoInsidePojo)
                .setProperty("pojoInsideDocument", _pojoInsideDocument)
                .setProperty("documentInsidePojo", _documentInsidePojo)
                .setProperty("documentInsideDocument", _documentInsideDocument);
    }

    private TestPojo3 createPojoInsidePojo() {
        TestPojo3 pojo3 = new TestPojo3();
        pojo3.setAge(15);
        pojo3.setName("dank");
        TestPojo4 pojo4_1 = new TestPojo4();
        pojo4_1.setDateProperty(new Date(123));
        pojo4_1.setLongProperty(null);
        pojo3.setPojo4_1(pojo4_1);
        pojo3.setPojo4_2(null);
        return pojo3;
    }

    private SpaceDocument createDocumentInsideDocument() {
        return TestDocumentFactory.getTestDocument5(15,
                TestDocumentFactory.getTestDocument3(3, 5l, false));
    }

    private TestPojo1 createDocumentInsidePojo() {
        TestPojo1 pojo1 = new TestPojo1();
        pojo1.setStr("this is a string");
        pojo1.setSpaceDocument(TestDocumentFactory.getTestDocument1("dan dan", "kilman kilman"));
        return pojo1;
    }

    private SpaceDocument createPojoInsideDocument() {
        TestPojo4 pojo4 = new TestPojo4();
        pojo4.setDateProperty(new Date(123123));
        pojo4.setLongProperty(1555l);
        return TestDocumentFactory.getTestDocument4(89, pojo4);

    }

    private SpaceDocument createPojoInsidePojoInsideDocument(
            TestPojo3 pojoInsidePojo) {
        return TestDocumentFactory.getTestDocument6(pojoInsidePojo);
    }

}
