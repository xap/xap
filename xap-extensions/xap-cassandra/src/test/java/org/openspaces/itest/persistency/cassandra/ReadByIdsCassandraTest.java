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
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.IntroduceTypeData;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadByIdsCassandraTest extends AbstractCassandraTest {
    private final String keyName = "key";
    private final AtomicInteger keyValues = new AtomicInteger(0);
    private final String someProp = "some_prop";
    private final boolean somePropValue = true;
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
        builder.write(createSpaceDocument(), keyName); // key == 1
        builder.write(createSpaceDocument(), keyName); // key == 2
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        DataIterator<Object> resultsIterator = _dataSource.getDataIteratorByIds(new DataSourceIdsQuery() {
            public SpaceTypeDescriptor getTypeDescriptor() {
                return introduceDataType.getTypeDescriptor();
            }

            public Object[] getIds() {
                return new Object[]{0, 1, 2}; // 0 doesn't exist, 1 and 2 exist
            }

            @Override
            public boolean supportsAsSQLQuery() {
                return false;
            }

            @Override
            public DataSourceSQLQuery getAsSQLQuery() {
                return null;
            }
        });

        List<Object> results = new ArrayList<Object>();
        while (resultsIterator.hasNext())
            results.add(resultsIterator.next());

        Assert.assertEquals("wrong results", 2, results.size());
        SpaceDocument doc1 = (SpaceDocument) results.get(0);
        SpaceDocument doc2 = (SpaceDocument) results.get(1);

        Assert.assertNotNull("got: " + results, doc1);
        Assert.assertNotNull("got: " + results, doc2);

        Assert.assertEquals("bad document", 1, (int) doc1.getProperty(keyName));
        Assert.assertEquals("bad document", 2, (int) doc2.getProperty(keyName));
        Assert.assertEquals("bad document", somePropValue, (boolean) doc1.getProperty(someProp));
        Assert.assertEquals("bad document", somePropValue, (boolean) doc2.getProperty(someProp));


    }

    private SpaceDocument createSpaceDocument() {
        return new SpaceDocument(typeName)
                .setProperty(keyName, keyValues.getAndIncrement())
                .setProperty(someProp, somePropValue);
    }

}
