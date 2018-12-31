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

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

public class VeryLongTypeNameCassandraTest extends AbstractCassandraTest {
    private final String keyName = "key";

    // a simple name longer than 48 chars would suffice just as well 
    private final String typeName = "com.gigaspaces.ridiculously.long.type.name.that.would.cassandra." +
            "to.complain.on.a.windows.machine." +
            "if.we.were.using.naive.name.conversion.IAmHereForTheSolePurposeOfBeingRidiculouslyLong";

    @Before
    public void before() {
        _syncInterceptor.onIntroduceType(createIntroduceTypeDataFromSpaceDocument(createSpaceDocument(),
                keyName));
        _dataSource.initialMetadataLoad();
    }

    @Test
    public void test() {
        MockOperationsBatchDataBuilder builder = new MockOperationsBatchDataBuilder();
        builder.write(createSpaceDocument(), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());
        DataIterator<Object> iterator = _dataSource.initialDataLoad();
        Assert.assertTrue("Missing document", iterator.hasNext());
        SpaceDocument doc = (SpaceDocument) iterator.next();
        iterator.close();
        Assert.assertEquals("Wrong type name", typeName, doc.getTypeName());
        Assert.assertEquals("Wrong value", 1, (int) doc.getProperty(keyName));
        Assert.assertEquals("Wrong value", true, (boolean) doc.getProperty("some_prop"));
    }

    private SpaceDocument createSpaceDocument() {
        return new SpaceDocument(typeName)
                .setProperty(keyName, 1)
                .setProperty("some_prop", true);
    }

}
