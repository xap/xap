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

package org.openspaces.esb.mule.transaction;

import com.gigaspaces.client.transaction.DistributedTransactionManagerProvider;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.XAResourceImpl;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import org.mule.transaction.TransactionCoordination;
import org.mule.transaction.XaTransaction;
import org.openspaces.core.TransactionDataAccessException;
import org.openspaces.core.transaction.TransactionProvider;
import org.springframework.transaction.TransactionDefinition;

import java.io.IOException;
import java.rmi.RemoteException;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * @author kimchy (Shay Banon)
 */
public class MuleXATransactionProvider implements TransactionProvider {

    private DistributedTransactionManagerProvider distributedTransactionManagerProvider;

    private final Object distributedTransactionManagerProviderLock = new Object();

    public Transaction.Created getCurrentTransaction(Object transactionalContext, IJSpace space) {
        org.mule.api.transaction.Transaction tx = TransactionCoordination.getInstance().getTransaction();
        if (!(tx instanceof XaTransaction)) {
            return null;
        }
        XaTransaction xaTransaction = (XaTransaction) tx;
        if (xaTransaction.hasResource(space)) {
            // already bound the space, return
            return ((CustomXaResource) xaTransaction.getResource(space)).transaction;
        }
        if (distributedTransactionManagerProvider == null) {
            synchronized (distributedTransactionManagerProviderLock) {
                if (distributedTransactionManagerProvider == null) {
                    try {
                        distributedTransactionManagerProvider = new DistributedTransactionManagerProvider();
                    } catch (TransactionException e) {
                        throw new TransactionDataAccessException("Failed to get local transaction manager for space [" + space + "]", e);
                    }
                }
            }
        }
        CustomXaResource xaResourceSpace = new CustomXaResource(new XAResourceImpl(distributedTransactionManagerProvider.getTransactionManager(), space));

        // enlist the Space xa resource with the current JTA transaction
        // we rely on the fact that this call will start the XA transaction
        try {
            xaTransaction.bindResource(space, xaResourceSpace);
        } catch (Exception e) {
            throw new TransactionDataAccessException("Failed to enlist xa resource [" + xaResourceSpace + "] with space [" + space + "]", e);
        }

        // get the context transaction from the Space and nullify it. We will handle
        // the declarative transaction nature using Spring sync
        Transaction.Created transaction = ((ISpaceProxy) space).getContextTransaction();
        ((ISpaceProxy) space).replaceContextTransaction(null);

        xaResourceSpace.transaction = transaction;

        return transaction;
    }

    public int getCurrentTransactionIsolationLevel(Object transactionalContext) {
        return TransactionDefinition.ISOLATION_DEFAULT;
    }

    public boolean isEnabled() {
        return true;
    }

    public void destroy() throws RemoteException {
        synchronized (distributedTransactionManagerProviderLock) {
            if (distributedTransactionManagerProvider != null)
                distributedTransactionManagerProvider.destroy();
        }
    }

    @Override
    public void close() throws IOException {
        destroy();
    }

    private static class CustomXaResource implements XAResource {

        public final XAResource actual;

        public Transaction.Created transaction;

        private CustomXaResource(XAResource actual) {
            this.actual = actual;
        }

        public void commit(Xid xid, boolean b) throws XAException {
            actual.commit(xid, b);
        }

        public void end(Xid xid, int i) throws XAException {
            actual.end(xid, i);
        }

        public void forget(Xid xid) throws XAException {
            actual.forget(xid);
        }

        public int getTransactionTimeout() throws XAException {
            return actual.getTransactionTimeout();
        }

        public boolean isSameRM(XAResource xaResource) throws XAException {
            return actual.isSameRM(xaResource);
        }

        public int prepare(Xid xid) throws XAException {
            return actual.prepare(xid);
        }

        public Xid[] recover(int i) throws XAException {
            return actual.recover(i);
        }

        public void rollback(Xid xid) throws XAException {
            actual.rollback(xid);
        }

        public boolean setTransactionTimeout(int i) throws XAException {
            return actual.setTransactionTimeout(i);
        }

        public void start(Xid xid, int i) throws XAException {
            actual.start(xid, i);
        }
    }
}
