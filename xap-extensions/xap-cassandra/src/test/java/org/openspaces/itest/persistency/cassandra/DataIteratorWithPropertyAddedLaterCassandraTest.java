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
import com.gigaspaces.sync.IntroduceTypeData;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.openspaces.test.common.data.TestPojo1;
import org.openspaces.test.common.mock.MockDataSourceQuery;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

public class DataIteratorWithPropertyAddedLaterCassandraTest extends AbstractCassandraTest {
    private final String keyName = "key";
    private final String typeName = "TypeName";
    private final TestPojo1 newType = new TestPojo1("123");
    private IntroduceTypeData introduceDataType;

    @Before
    public void before() {
        introduceDataType = createIntroduceTypeDataFromSpaceDocument(createSpaceDocument(),
                keyName);
        _syncInterceptor.onIntroduceType(introduceDataType);
        _dataSource.initialMetadataLoad();
    }

    @Test
    public void test() {
        MockOperationsBatchDataBuilder builder = new MockOperationsBatchDataBuilder();
        builder.write(createSpaceDocument(), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        DataIterator<Object> iterator;
        SpaceDocument doc;

        iterator = _dataSource.getDataIterator(new MockDataSourceQuery(introduceDataType.getTypeDescriptor(),
                new SpaceDocument(typeName),
                Integer.MAX_VALUE));
        Assert.assertTrue("No object found", iterator.hasNext());
        doc = (SpaceDocument) iterator.next();
        Assert.assertEquals("Wrong type name", typeName, doc.getTypeName());
        Assert.assertEquals("Wrong value", 1, (int) doc.getProperty(keyName));
        Assert.assertEquals("Wrong value", true, (boolean) doc.getProperty("some_prop"));

        builder = new MockOperationsBatchDataBuilder();
        builder.write(createSpaceDocument()
                .setProperty("new_prop", newType)
                .setProperty("new_prop2", 2), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        iterator.close();
        iterator = _dataSource.getDataIterator(new MockDataSourceQuery(introduceDataType.getTypeDescriptor(),
                new SpaceDocument(typeName),
                Integer.MAX_VALUE));
        Assert.assertTrue("No object found", iterator.hasNext());
        doc = (SpaceDocument) iterator.next();
        Assert.assertEquals("Wrong type name", typeName, doc.getTypeName());
        Assert.assertEquals("Wrong value", 1, (int) doc.getProperty(keyName));
        Assert.assertEquals("Wrong value", true, (boolean) doc.getProperty("some_prop"));

        // uncomment if we decide pojo will be restored as documents no matter what
        Assert.assertEquals("Wrong value", newType, doc.getProperty("new_prop"));
//        Assert.assertEquals("Wrong value", newType.getStr(), ((SpaceDocument)doc.getProperty("new_prop")).getProperty("str"));

        Assert.assertEquals("Wrong value", 2, (int) doc.getProperty("new_prop2"));

        iterator.close();
    }

    private SpaceDocument createSpaceDocument() {
        return new SpaceDocument(typeName)
                .setProperty(keyName, 1)
                .setProperty("some_prop", true);
    }

}
