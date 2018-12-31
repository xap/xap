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

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.sync.IntroduceTypeData;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.openspaces.test.common.data.TestPojo2;
import org.openspaces.test.common.mock.MockDataSourceIdQuery;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

public class ReadByIdWithPropertyAddedLaterCassandraTest extends AbstractCassandraTest {
    private final String keyName = "key";
    private final TestPojo2 keyValue = new TestPojo2("dank",
            13);
    private final String someProp = "some_prop";
    private final boolean somePropValue = true;
    private final String newProp = "new_prop";
    private final int newPropValue = 2;
    private final String typeName = "TypeName";
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

        SpaceDocument doc = (SpaceDocument) _dataSource.getById(new MockDataSourceIdQuery(introduceDataType.getTypeDescriptor(),
                keyValue));

        Assert.assertNotNull("No object found", doc);
        Assert.assertEquals("Wrong type name", typeName, doc.getTypeName());
        Assert.assertEquals("Wrong value", keyValue, doc.getProperty(keyName));
        Assert.assertEquals("Wrong value", somePropValue, (boolean) doc.getProperty(someProp));

        builder.clear().write(createSpaceDocument().setProperty(newProp, newPropValue), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        doc = (SpaceDocument) _dataSource.getById(new MockDataSourceIdQuery(introduceDataType.getTypeDescriptor(),
                keyValue));

        Assert.assertNotNull("No object found", doc);
        Assert.assertEquals("Wrong type name", typeName, doc.getTypeName());
        Assert.assertEquals("Wrong value", keyValue, doc.getProperty(keyName));
        Assert.assertEquals("Wrong value", somePropValue, (boolean) doc.getProperty(someProp));
        Assert.assertEquals("Wrong value", newPropValue, (int) doc.getProperty(newProp));
    }

    private SpaceDocument createSpaceDocument() {
        return new SpaceDocument(typeName)
                .setProperty(keyName, keyValue)
                .setProperty(someProp, somePropValue);
    }

}
