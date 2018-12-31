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

import org.apache.cassandra.cql.jdbc.CassandraDataSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openspaces.persistency.cassandra.CassandraConsistencyLevel;
import org.openspaces.persistency.cassandra.CassandraSpaceDataSource;
import org.openspaces.persistency.cassandra.CassandraSpaceDataSourceConfigurer;
import org.openspaces.persistency.cassandra.HectorCassandraClient;
import org.openspaces.persistency.cassandra.HectorCassandraClientConfigurer;
import org.openspaces.test.common.mock.MockDataSourceIdQuery;
import org.openspaces.test.common.mock.MockDataSourceQuery;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class DifferentConsistencyLevelsCassandraTest extends AbstractCassandraTest {

    private final String keyName = "key";
    private final String keyValue = "keyValue";
    private final String someProp = "some_prop";
    private final boolean somePropValue = true;
    private final String typeName = "TypeName";
    private IntroduceTypeData introduceDataType;

    CassandraConsistencyLevel hectorClientReadConsistencyLevel;
    CassandraConsistencyLevel hectorClientWriteConsistencyLevel;

    public DifferentConsistencyLevelsCassandraTest(
            CassandraConsistencyLevel hectorClientReadConsistencyLevel,
            CassandraConsistencyLevel hectorClientWriteConsistencyLevel) {
        this.hectorClientReadConsistencyLevel = hectorClientReadConsistencyLevel;
        this.hectorClientWriteConsistencyLevel = hectorClientWriteConsistencyLevel;
    }

    @SuppressWarnings("unchecked")
    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                // test big objects
                new Object[]{CassandraConsistencyLevel.ALL, CassandraConsistencyLevel.ALL},
                new Object[]{CassandraConsistencyLevel.ONE, CassandraConsistencyLevel.ONE},
                new Object[]{CassandraConsistencyLevel.QUORUM, CassandraConsistencyLevel.QUORUM},
                new Object[]{CassandraConsistencyLevel.ONE, CassandraConsistencyLevel.ANY},
        });
    }

    @Before
    public void before() {
        introduceDataType = createIntroduceTypeDataFromSpaceDocument(createSpaceDocument(), keyName);
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

        DataIterator<Object> iterator = _dataSource.getDataIterator(new MockDataSourceQuery(introduceDataType.getTypeDescriptor(),
                new SpaceDocument(typeName), Integer.MAX_VALUE));

        Assert.assertTrue("No object found", iterator.hasNext());
        doc = (SpaceDocument) iterator.next();
        Assert.assertEquals("Wrong type name", typeName, doc.getTypeName());
        Assert.assertEquals("Wrong value", keyValue, doc.getProperty(keyName));
        Assert.assertEquals("Wrong value", somePropValue, (boolean) doc.getProperty(someProp));

    }

    private SpaceDocument createSpaceDocument() {
        return new SpaceDocument(typeName)
                .setProperty(keyName, keyValue)
                .setProperty(someProp, somePropValue);
    }

    @Override
    protected CassandraSpaceDataSource createCassandraSpaceDataSource(
            HectorCassandraClient hectorClient) {
        CassandraDataSource ds = createCassandraDataSource();
        return new CassandraSpaceDataSourceConfigurer()
                .cassandraDataSource(ds)
                .hectorClient(hectorClient)
                .create();
    }

    @Override
    protected HectorCassandraClient createCassandraHectorClient(String clusterName) {
        return new HectorCassandraClientConfigurer()
                .clusterName(clusterName)
                .hosts(server.getHost())
                .port(server.getPort())
                .keyspaceName(server.getKeySpaceName())
                .readConsistencyLevel(hectorClientReadConsistencyLevel)
                .writeConsistencyLevel(hectorClientWriteConsistencyLevel)
                .create();
    }

}
