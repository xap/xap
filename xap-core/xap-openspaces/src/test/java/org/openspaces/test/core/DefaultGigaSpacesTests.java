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


package org.openspaces.test.core;

import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ClearModifiers;
import com.gigaspaces.client.CountModifiers;
import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.client.ReadModifiers;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;
import org.jmock.core.Constraint;
import org.openspaces.core.DefaultGigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.exception.ExceptionTranslator;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.transaction.TransactionDefinition;

/**
 * A set of mock tests verifies that the correct {@link com.j_spaces.core.IJSpace} API is called as
 * a result of {@link org.openspaces.core.DefaultGigaSpace} execution.
 *
 * @author kimchy
 */
public class DefaultGigaSpacesTests extends MockObjectTestCase {

    private DefaultGigaSpace gs;

    private Mock mockIJSpace;
    private Mock mockTxProvider;

    protected void setUp() throws Exception {
        mockIJSpace = mock(ISpaceProxy.class);
        mockTxProvider = mock(TransactionProvider.class);

        mockIJSpace.expects(once()).method("getName").will(returnValue("mockSpaceName"));
        mockIJSpace.expects(once()).method("getReadModifiers").will(returnValue(0));
        IJSpace space = (IJSpace) mockIJSpace.proxy();
        GigaSpaceConfigurer configurer = new GigaSpaceConfigurer(space)
                .clustered(true)
                .txProvider((TransactionProvider) mockTxProvider.proxy());
        gs = new DefaultGigaSpace(configurer);
    }

    public void testReadOperation() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once()).method("read").with(same(template), NULL, eq(0l), eq(ReadModifiers.READ_COMMITTED)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        Object actualRetVal = gs.read(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadOperationWithDefaultReadModifiers() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once()).method("read").with(same(template), NULL, eq(0l), eq(ReadModifiers.READ_COMMITTED | com.gigaspaces.client.ReadModifiers.FIFO.getCode()))
                .will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        gs.setDefaultReadModifiers(com.gigaspaces.client.ReadModifiers.FIFO);
        Object actualRetVal = gs.read(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadOperationWithDefaultReadModifiersAndIsolationLevelOverride() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once()).method("read").with(same(template), NULL, eq(0l), eq(ReadModifiers.READ_COMMITTED)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        gs.setDefaultReadModifiers(com.gigaspaces.client.ReadModifiers.DIRTY_READ);
        Object actualRetVal = gs.read(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadOperationWithDefaultTimeout() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once())
                .method("read")
                .with(same(template), NULL, eq(10l), eq(ReadModifiers.READ_COMMITTED))
                .will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        gs.setDefaultReadTimeout(10l);
        Object actualRetVal = gs.read(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadOperationWithTimeoutParameter() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once())
                .method("read")
                .with(same(template), NULL, eq(11l), eq(ReadModifiers.READ_COMMITTED))
                .will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        Object actualRetVal = gs.read(template, 11l);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadIfExistsOperation() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once()).method("readIfExists").with(same(template), NULL, eq(0l),
                eq(ReadModifiers.READ_COMMITTED)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        Object actualRetVal = gs.readIfExists(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadIfExistsOperationWithDefaultTimeout() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once()).method("readIfExists").with(same(template), NULL, eq(10l),
                eq(ReadModifiers.READ_COMMITTED)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        gs.setDefaultReadTimeout(10l);
        Object actualRetVal = gs.readIfExists(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadIfExistsOperationWithTimeoutParameter() {
        Object template = new Object();
        Object retVal = new Object();

        mockIJSpace.expects(once()).method("readIfExists").with(same(template), NULL, eq(11l),
                eq(ReadModifiers.READ_COMMITTED)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        Object actualRetVal = gs.readIfExists(template, 11l);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadMultipleOperation() {
        Object template = new Object();
        Object[] retVal = new Object[]{new Object(), new Object()};

        mockIJSpace.expects(once()).method("readMultiple").with(same(template), NULL, eq(2),
                eq(ReadModifiers.READ_COMMITTED)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        Object actualRetVal = gs.readMultiple(template, 2);

        assertEquals(retVal, actualRetVal);
    }

    public void testReadMultipleNoLimitOperation() {
        Object template = new Object();
        Object[] retVal = new Object[]{new Object(), new Object(), new Object(), new Object()};

        mockIJSpace.expects(once()).method("readMultiple").with(same(template), NULL, eq(Integer.MAX_VALUE),
                eq(ReadModifiers.READ_COMMITTED)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(
                returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        Object actualRetVal = gs.readMultiple(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeOperation() {
        Object template = new Object();
        Object retVal = new Object();

        Constraint[] constraints = new Constraint[]{
                same(template),
                NULL,
                eq(0l),
                eq(0),
                same(false)
        };
        mockIJSpace.expects(once()).method("take").with(constraints).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        Object actualRetVal = gs.take(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeOperationWithDefaultTimeout() {
        Object template = new Object();
        Object retVal = new Object();

        Constraint[] constraints = new Constraint[]{
                same(template),
                NULL,
                eq(10l),
                eq(0),
                same(false)};
        mockIJSpace.expects(once()).method("take").with(constraints).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultTakeTimeout(10l);
        Object actualRetVal = gs.take(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeOperationWithTimeoutParameter() {
        Object template = new Object();
        Object retVal = new Object();

        Constraint[] constraints = new Constraint[]{
                same(template),
                NULL,
                eq(11l),
                eq(0),
                same(false)
        };
        mockIJSpace.expects(once()).method("take").with(constraints).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        Object actualRetVal = gs.take(template, 11l);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeIfExistsOperation() {
        Object template = new Object();
        Object retVal = new Object();

        Constraint[] constraints = new Constraint[]{
                same(template),
                NULL,
                eq(0l),
                eq(0),
                eq(Boolean.TRUE)
        };
        mockIJSpace.expects(once()).method("take").with(constraints).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        Object actualRetVal = gs.takeIfExists(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeIfExistsOperationWithDefaultTimeout() {
        Object template = new Object();
        Object retVal = new Object();

        Constraint[] constraints = new Constraint[]{
                same(template),
                NULL,
                eq(10l),
                eq(0),
                eq(Boolean.TRUE)
        };
        mockIJSpace.expects(once()).method("take").with(constraints).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultTakeTimeout(10l);
        Object actualRetVal = gs.takeIfExists(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeIfExistsOperationWithTimeoutParameter() {
        Object template = new Object();
        Object retVal = new Object();

        Constraint[] constraints = new Constraint[]{
                same(template),
                NULL,
                eq(11l),
                eq(0),
                eq(Boolean.TRUE)
        };

        mockIJSpace.expects(once()).method("take").with(constraints).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        Object actualRetVal = gs.takeIfExists(template, 11l);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeWithDefaultTakeModifiers() {
        Object template = new Object();
        Object retVal = new Object();

        Constraint[] constraints = new Constraint[]{
                same(template),
                NULL,
                eq(11l),
                eq(TakeModifiers.MEMORY_ONLY_SEARCH.getCode()),
                eq(Boolean.TRUE)
        };

        mockIJSpace.expects(once()).method("take").with(constraints).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultTakeModifiers(TakeModifiers.MEMORY_ONLY_SEARCH);
        Object actualRetVal = gs.takeIfExists(template, 11l);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeMultipleNoLimit() {
        Object template = new Object();
        Object[] retVal = new Object[]{new Object(), new Object(), new Object(), new Object()};

        mockIJSpace.expects(once()).method("takeMultiple").with(same(template), NULL, eq(Integer.MAX_VALUE), eq(0)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        Object actualRetVal = gs.takeMultiple(template);

        assertEquals(retVal, actualRetVal);
    }

    public void testTakeMultiple() {
        Object template = new Object();
        Object[] retVal = new Object[]{new Object(), new Object()};

        mockIJSpace.expects(once()).method("takeMultiple").with(same(template), NULL, eq(2), eq(0)).will(returnValue(retVal));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        Object actualRetVal = gs.takeMultiple(template, 2);

        assertEquals(retVal, actualRetVal);
    }

    @SuppressWarnings("unchecked")
    public void testWriteOperation() {
        Object entry = new Object();
        Mock mockLeaseContext = mock(LeaseContext.class);
        LeaseContext<Object> leaseContext = (LeaseContext<Object>) mockLeaseContext.proxy();

        Constraint[] constraints = new Constraint[]{
                same(entry),
                NULL,
                eq(Long.MAX_VALUE),
                eq(0l),
                eq(WriteModifiers.NONE.getCode())
        };

        mockIJSpace.expects(once()).method("write").with(constraints).will(returnValue(leaseContext));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        LeaseContext<Object> actualLeaseContext = gs.write(entry);
        assertEquals(leaseContext, actualLeaseContext);
    }

    @SuppressWarnings("unchecked")
    public void testWriteOperationWithDefaultLease() {
        Object entry = new Object();
        Mock mockLeaseContext = mock(LeaseContext.class);
        LeaseContext<Object> leaseContext = (LeaseContext<Object>) mockLeaseContext.proxy();

        Constraint[] constraints = new Constraint[]{
                same(entry),
                NULL,
                eq(10l),
                eq(0l),
                eq(WriteModifiers.NONE.getCode())
        };

        mockIJSpace.expects(once()).method("write").with(constraints).will(returnValue(leaseContext));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultWriteLease(10l);
        LeaseContext actualLeaseContext = gs.write(entry);

        assertEquals(leaseContext, actualLeaseContext);
    }

    @SuppressWarnings("unchecked")
    public void testWriteOperationWithLeaseParameter() {
        Object entry = new Object();
        Mock mockLeaseContext = mock(LeaseContext.class);
        LeaseContext<Object> leaseContext = (LeaseContext<Object>) mockLeaseContext.proxy();

        Constraint[] constraints = new Constraint[]{
                same(entry),
                NULL,
                eq(10l),
                eq(0l),
                eq(WriteModifiers.NONE.getCode())
        };

        mockIJSpace.expects(once()).method("write").with(constraints).will(returnValue(leaseContext));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        LeaseContext<Object> actualLeaseContext = gs.write(entry, 10l);

        assertEquals(leaseContext, actualLeaseContext);
    }

    @SuppressWarnings("unchecked")
    public void testWriteOperationWithLeaseTimeoutModifiersParameters() {
        Object entry = new Object();
        Mock mockLeaseContext = mock(LeaseContext.class);
        LeaseContext<Object> leaseContext = (LeaseContext<Object>) mockLeaseContext.proxy();

        mockIJSpace.expects(once())
                .method("write")
                .with(new Constraint[]{same(entry), NULL, eq(10l), eq(2l), eq(3)})
                .will(returnValue(leaseContext));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        LeaseContext actualLeaseContext = gs.write(entry, 10l, 2l, 3);

        assertEquals(leaseContext, actualLeaseContext);
    }

    @SuppressWarnings("unchecked")
    public void testWriteOperationWithDefaultWriteModifiers() {
        Object entry = new Object();
        Mock mockLeaseContext = mock(LeaseContext.class);
        LeaseContext<Object> leaseContext = (LeaseContext<Object>) mockLeaseContext.proxy();

        Constraint[] constraints = new Constraint[]{
                same(entry),
                NULL,
                eq(Long.MAX_VALUE),
                eq(0l),
                eq(WriteModifiers.ONE_WAY.getCode())
        };

        mockIJSpace.expects(once()).method("write").with(constraints).will(returnValue(leaseContext));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultWriteModifiers(WriteModifiers.ONE_WAY);
        LeaseContext actualLeaseContext = gs.write(entry);

        assertEquals(leaseContext, actualLeaseContext);
    }

    @SuppressWarnings("unchecked")
    public void testWriteMultipleOperationWithWriteModifiersParam() {
        Object entry1 = new Object();
        Object entry2 = new Object();
        Object[] entries = new Object[]{entry1, entry2};
        Mock mockLeaseContext = mock(LeaseContext.class);

        LeaseContext<Object> leaseContext = (LeaseContext<Object>) mockLeaseContext.proxy();

        LeaseContext<Object>[] leaseContexts = new LeaseContext[]{leaseContext, leaseContext};

        Constraint[] constraints = new Constraint[]{
                same(entries),
                NULL,
                eq(Long.MAX_VALUE),
                eq(null),
                eq(0L),
                eq(WriteModifiers.ONE_WAY.getCode())
        };

        mockIJSpace.expects(once()).method("writeMultiple").with(constraints).will(returnValue(leaseContexts));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultWriteModifiers(WriteModifiers.MEMORY_ONLY_SEARCH);
        LeaseContext<Object>[] actualLeaseContexts = gs.writeMultiple(entries, WriteModifiers.ONE_WAY);

        assertEquals(leaseContexts, actualLeaseContexts);
    }

    @SuppressWarnings("unchecked")
    public void testWriteMultipleOperationWithDefaultWriteModifiers() {
        Object entry1 = new Object();
        Object entry2 = new Object();
        Object[] entries = new Object[]{entry1, entry2};
        Mock mockLeaseContext = mock(LeaseContext.class);

        LeaseContext<Object> leaseContext = (LeaseContext<Object>) mockLeaseContext.proxy();

        LeaseContext<Object>[] leaseContexts = new LeaseContext[]{leaseContext, leaseContext};

        Constraint[] constraints = new Constraint[]{
                same(entries),
                NULL,
                eq(Long.MAX_VALUE),
                eq(null),
                eq(0L),
                eq(WriteModifiers.ONE_WAY.getCode())
        };

        mockIJSpace.expects(once()).method("writeMultiple").with(constraints).will(returnValue(leaseContexts));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultWriteModifiers(WriteModifiers.ONE_WAY);
        LeaseContext<Object>[] actualLeaseContexts = gs.writeMultiple(entries);

        assertEquals(leaseContexts, actualLeaseContexts);
    }

    public void testClearWithDefaultModifiers() {
        Object entry1 = new Object();

        Constraint[] constraints = new Constraint[]{
                same(entry1),
                NULL,
                eq(ClearModifiers.MEMORY_ONLY_SEARCH.getCode())
        };

        mockIJSpace.expects(once()).method("clear").with(constraints).will(returnValue(0));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultClearModifiers(ClearModifiers.MEMORY_ONLY_SEARCH);
        gs.clear(entry1);

    }

    public void testCountWithDefaultModifiers() {
        int expectedCount = 2;
        Object entry1 = new Object();

        Constraint[] constraints = new Constraint[]{
                same(entry1),
                NULL,
                eq(CountModifiers.MEMORY_ONLY_SEARCH.add(CountModifiers.READ_COMMITTED).getCode())
        };

        mockIJSpace.expects(once()).method("count").with(constraints).will(returnValue(expectedCount));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        gs.setDefaultCountModifiers(CountModifiers.MEMORY_ONLY_SEARCH);
        int count = gs.count(entry1);

        assertEquals(expectedCount, count);
    }

    public void testCountWithDefaultModifiersIsolationLevelOverride() {
        int expectedCount = 2;
        Object entry1 = new Object();

        Constraint[] constraints = new Constraint[]{
                same(entry1),
                NULL,
                eq(CountModifiers.READ_COMMITTED.getCode())
        };

        mockIJSpace.expects(once()).method("count").with(constraints).will(returnValue(expectedCount));
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));
        mockTxProvider.expects(once()).method("getCurrentTransactionIsolationLevel").with(eq(gs)).will(returnValue(TransactionDefinition.ISOLATION_READ_COMMITTED));

        gs.setDefaultCountModifiers(CountModifiers.DIRTY_READ);
        int count = gs.count(entry1);

        assertEquals(expectedCount, count);
    }

    public void testChangeWithDefaultModifiers() {
        Object entry1 = new Object();

        Constraint[] constraints = new Constraint[]{
                same(entry1),
                NULL,
                NULL,
                eq(0l),
                same(ChangeModifiers.RETURN_DETAILED_RESULTS)
        };

        mockIJSpace.expects(once()).method("change").with(constraints);
        mockTxProvider.expects(once()).method("getCurrentTransaction").with(eq(gs), eq(gs.getSpace()));

        gs.setDefaultChangeModifiers(ChangeModifiers.RETURN_DETAILED_RESULTS);
        gs.change(entry1, null);

    }
}
