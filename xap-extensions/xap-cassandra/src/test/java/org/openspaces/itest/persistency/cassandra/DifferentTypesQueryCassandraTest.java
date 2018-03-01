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
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.sync.OperationsBatchData;

import org.junit.Before;
import org.junit.Test;
import org.openspaces.test.common.mock.MockDataSourceQuery;
import org.openspaces.test.common.mock.MockIntroduceTypeData;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DifferentTypesQueryCassandraTest extends AbstractCassandraTest {
    private static final String TEST_CF = "TestColumnFamily";
    private static final String KEY_NAME = "keyColumn";
    private static final UUID KEY_VAL = UUID.randomUUID();
    private static final String LONG_COL = "longColumn";
    private static final Long LONG_COL_VAL = new Long(888L);
    private static final String STRING_COL = "stringColumn";
    private static final String STRING_COL_VAL = "testString";
    private static final String BIGINT_COL = "bigintColumn";
    private static final BigInteger BIGINT_COL_VAL = new BigInteger("12323");

    // not really dynamic, i simply didn't bother changing these when i
    // copied them from BasicCassandraTest
    private static final String DYNAMIC_COL_1 = "dynamicCol1";
    private static final Integer DYNAMIC_COL_1_VAL = new Integer(3333);
    private static final String DYNAMIC_COL_2 = "dynamicCol2";
    private static final Date DYNAMIC_COL_2_VAL = new Date(1234);
    private static final String DYNAMIC_COL_3 = "dynamicCol3";
    private static final Float DYNAMIC_COL_3_VAL = new Float(3434.6f);
    private static final String DYNAMIC_COL_4 = "dynamicCol4";
    private static final Double DYNAMIC_COL_4_VAL = new Double(123.5);
    private static final String DYNAMIC_COL_5 = "dynamicCol5";
    private static final Boolean DYNAMIC_COL_5_VAL = Boolean.TRUE;
    private static final String DYNAMIC_COL_6 = "dynamicCol6";
    private static final Byte DYNAMIC_COL_6_VAL = Byte.valueOf((byte) 1);
    private static final String DYNAMIC_COL_7 = "dynamicCol7";
    private static final Character DYNAMIC_COL_7_VAL = Character.valueOf('a');
    private static final String DYNAMIC_COL_8 = "dynamicCol8";
    private static final byte[] DYNAMIC_COL_8_VAL = {(byte) 123};

    @Before
    public void before() {
        _syncInterceptor.onIntroduceType(createMockIntroduceTypeData());
        _syncInterceptor.onOperationsBatchSynchronization(createMockOperationBatchData());
        _dataSource.initialMetadataLoad();
    }

    @Test
    public void test() {
        // test some cql queries
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(LONG_COL, LONG_COL_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(STRING_COL, STRING_COL_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(BIGINT_COL, BIGINT_COL_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_1, DYNAMIC_COL_1_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_2, DYNAMIC_COL_2_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_3, DYNAMIC_COL_3_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_4, DYNAMIC_COL_4_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_5, DYNAMIC_COL_5_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_6, DYNAMIC_COL_6_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_7, DYNAMIC_COL_7_VAL));
        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_8, DYNAMIC_COL_8_VAL));

        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(LONG_COL, LONG_COL_VAL)
                .setProperty(STRING_COL, STRING_COL_VAL));

        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(LONG_COL, LONG_COL_VAL)
                .setProperty(DYNAMIC_COL_1, DYNAMIC_COL_1_VAL));

        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(LONG_COL, LONG_COL_VAL)
                .setProperty(BIGINT_COL, BIGINT_COL_VAL)
                .setProperty(DYNAMIC_COL_1, DYNAMIC_COL_1_VAL));

        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_1, DYNAMIC_COL_1_VAL)
                .setProperty(DYNAMIC_COL_2, DYNAMIC_COL_2_VAL)
                .setProperty(DYNAMIC_COL_3, DYNAMIC_COL_3_VAL));

        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_4, DYNAMIC_COL_4_VAL)
                .setProperty(DYNAMIC_COL_5, DYNAMIC_COL_5_VAL)
                .setProperty(DYNAMIC_COL_6, DYNAMIC_COL_6_VAL));

        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_7, DYNAMIC_COL_7_VAL)
                .setProperty(DYNAMIC_COL_8, DYNAMIC_COL_8_VAL)
                .setProperty(DYNAMIC_COL_2, DYNAMIC_COL_2_VAL));

        testDataIterator(new SpaceDocument(TEST_CF)
                .setProperty(DYNAMIC_COL_1, DYNAMIC_COL_1_VAL)
                .setProperty(DYNAMIC_COL_5, DYNAMIC_COL_5_VAL)
                .setProperty(DYNAMIC_COL_8, DYNAMIC_COL_8_VAL));
    }

    private void testDataIterator(SpaceDocument spaceDocument) {
        createMockSpaceTypeDescriptor();
        DataSourceQuery dataSourceQuery = new MockDataSourceQuery(createMockSpaceTypeDescriptor(),
                spaceDocument,
                Integer.MAX_VALUE);

        DataIterator<Object> iterator = _dataSource.getDataIterator(dataSourceQuery);

        assertTrue("No result", iterator.hasNext());
        SpaceDocument result = (SpaceDocument) iterator.next();
        assertValidDocument(result);
        assertTrue("No result expected", !iterator.hasNext());

        iterator.close();
    }

    private void assertValidDocument(SpaceDocument result) {
        assertEquals("type wrong", TEST_CF, result.getTypeName());
        assertEquals("ID wrong", KEY_VAL, result.getProperty(KEY_NAME));
        assertEquals("stringCol wrong", STRING_COL_VAL, result.getProperty(STRING_COL));
        assertEquals("longCol wrong", LONG_COL_VAL, result.getProperty(LONG_COL));
        assertEquals("bigintCol wrong", BIGINT_COL_VAL, result.getProperty(BIGINT_COL));
        assertEquals("dynamicCol1 wrong", DYNAMIC_COL_1_VAL, result.getProperty(DYNAMIC_COL_1));
        assertEquals("dynamicCol2 wrong", DYNAMIC_COL_2_VAL, result.getProperty(DYNAMIC_COL_2));
        assertEquals("dynamicCol3 wrong", DYNAMIC_COL_3_VAL, result.getProperty(DYNAMIC_COL_3));
        assertEquals("dynamicCol4 wrong", DYNAMIC_COL_4_VAL, result.getProperty(DYNAMIC_COL_4));
        assertEquals("dynamicCol5 wrong", DYNAMIC_COL_5_VAL, result.getProperty(DYNAMIC_COL_5));
        assertEquals("dynamicCol6 wrong", DYNAMIC_COL_6_VAL, result.getProperty(DYNAMIC_COL_6));
        assertEquals("dynamicCol7 wrong", DYNAMIC_COL_7_VAL, result.getProperty(DYNAMIC_COL_7));
        assertEquals("dynamicCol8 wrong", DYNAMIC_COL_8_VAL[0], ((byte[]) result.getProperty(DYNAMIC_COL_8))[0]);
    }

    private MockIntroduceTypeData createMockIntroduceTypeData() {
        SpaceTypeDescriptor typeDescriptor = createMockSpaceTypeDescriptor();
        return new MockIntroduceTypeData(typeDescriptor);
    }

    private SpaceTypeDescriptor createMockSpaceTypeDescriptor() {
        return new SpaceTypeDescriptorBuilder(TEST_CF)
                .addFixedProperty(KEY_NAME, UUID.class)
                .addFixedProperty(LONG_COL, Long.class)
                .addFixedProperty(STRING_COL, String.class)
                .addFixedProperty(BIGINT_COL, BigInteger.class)
                .addFixedProperty(DYNAMIC_COL_1, DYNAMIC_COL_1_VAL.getClass())
                .addFixedProperty(DYNAMIC_COL_2, DYNAMIC_COL_2_VAL.getClass())
                .addFixedProperty(DYNAMIC_COL_3, DYNAMIC_COL_3_VAL.getClass())
                .addFixedProperty(DYNAMIC_COL_4, DYNAMIC_COL_4_VAL.getClass())
                .addFixedProperty(DYNAMIC_COL_5, DYNAMIC_COL_5_VAL.getClass())
                .addFixedProperty(DYNAMIC_COL_6, DYNAMIC_COL_6_VAL.getClass())
                .addFixedProperty(DYNAMIC_COL_7, DYNAMIC_COL_7_VAL.getClass())
                .addFixedProperty(DYNAMIC_COL_8, DYNAMIC_COL_8_VAL.getClass())
                .addPropertyIndex(LONG_COL, SpaceIndexType.EQUAL)
                .addPropertyIndex(STRING_COL, SpaceIndexType.EQUAL)
                .addPropertyIndex(BIGINT_COL, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_1, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_2, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_3, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_4, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_5, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_6, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_7, SpaceIndexType.EQUAL)
                .addPropertyIndex(DYNAMIC_COL_8, SpaceIndexType.EQUAL)
                .idProperty(KEY_NAME)
                .create();
    }

    private OperationsBatchData createMockOperationBatchData() {
        SpaceDocument spaceDoc = new SpaceDocument(TEST_CF)
                .setProperty(KEY_NAME, KEY_VAL)
                .setProperty(STRING_COL, STRING_COL_VAL)
                .setProperty(LONG_COL, LONG_COL_VAL)
                .setProperty(BIGINT_COL, BIGINT_COL_VAL)
                .setProperty(DYNAMIC_COL_1, DYNAMIC_COL_1_VAL)
                .setProperty(DYNAMIC_COL_2, DYNAMIC_COL_2_VAL)
                .setProperty(DYNAMIC_COL_3, DYNAMIC_COL_3_VAL)
                .setProperty(DYNAMIC_COL_4, DYNAMIC_COL_4_VAL)
                .setProperty(DYNAMIC_COL_5, DYNAMIC_COL_5_VAL)
                .setProperty(DYNAMIC_COL_6, DYNAMIC_COL_6_VAL)
                .setProperty(DYNAMIC_COL_7, DYNAMIC_COL_7_VAL)
                .setProperty(DYNAMIC_COL_8, DYNAMIC_COL_8_VAL);

        return new MockOperationsBatchDataBuilder()
                .write(spaceDoc, KEY_NAME).build();
    }

}
