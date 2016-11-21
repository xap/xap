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


package org.openspaces.core.transaction;

import com.gigaspaces.client.transaction.DistributedTransactionManagerProvider;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.XAResourceImpl;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import org.openspaces.core.TransactionDataAccessException;
import org.openspaces.core.transaction.manager.ExistingJiniTransactionManager;
import org.openspaces.core.transaction.manager.JiniPlatformTransactionManager;
import org.openspaces.core.transaction.manager.JiniTransactionHolder;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * Default transaction provider works in conjunction with {@link org.openspaces.core.transaction.manager.JiniPlatformTransactionManager
 * JiniPlatformTransactionManager} and one of its derived classes. Uses Spring support for
 * transactional resource binding (using thread local) in order to get the current transaction. If
 * no transaction is active, will return <code>null</code> (which means the operation will be
 * executed under no transaction).
 *
 * <p>Also supports for Spring JTA transaction by automatically creating and binding GigaSpaces
 * support for XA.
 *
 * <p> As a transaction context it uses the one passed to its constructor, and not the runtime
 * transactional context provided to {@link #getCurrentTransaction(Object,
 * com.j_spaces.core.IJSpace)}
 *
 * @author kimchy
 * @see org.openspaces.core.transaction.manager.AbstractJiniTransactionManager
 * @see org.openspaces.core.GigaSpaceFactoryBean
 * @see org.openspaces.core.transaction.manager.JiniTransactionHolder
 * @see org.springframework.transaction.support.TransactionSynchronizationManager
 */
public class DefaultTransactionProvider implements TransactionProvider {

    private final Object actualTransactionalContext;

    private final PlatformTransactionManager transactionManager;

    private final boolean isJta;

    private DistributedTransactionManagerProvider distributedTransactionManagerProvider;

    private final Object distributedTransactionManagerProviderLock = new Object();

    public DefaultTransactionProvider(PlatformTransactionManager transactionManager) {
        this(transactionManager instanceof JiniPlatformTransactionManager ? ((JiniPlatformTransactionManager) transactionManager).getTransactionalContext() : null, transactionManager);
    }

    /**
     * Creates a new transaction provider. Will use the provided transactional context in order to
     * fetch the current running transaction.
     *
     * @param actualTransactionalContext The transactional context to fetch the transaction by
     */
    public DefaultTransactionProvider(Object actualTransactionalContext, PlatformTransactionManager transactionManager) {
        this.actualTransactionalContext = actualTransactionalContext;
        this.transactionManager = transactionManager;
        this.isJta = transactionManager != null && transactionManager instanceof JtaTransactionManager;
    }

    /**
     * Returns the current running transaction based on the constructor provided transactional
     * context (Note that the passed transactional context is not used).
     *
     * <p> Uses Spring support for transactional resource registration in order to fetch the current
     * running transaction (or the {@link JiniTransactionHolder}. An example of Spring platform
     * transaction managers that register it are ones derived form {@link
     * org.openspaces.core.transaction.manager.AbstractJiniTransactionManager}.
     *
     * <p> If no transaction is found bound the the transactional context (provided in the
     * constructor), <code>null</code> is returned. This means that operations will execute without
     * a transaction.
     *
     * @param transactionalContext Not Used. The transactional context used is the one provided in
     *                             the constructor.
     * @return The current running transaction or <code>null</code> if no transaction is running
     */
    public Transaction.Created getCurrentTransaction(Object transactionalContext, IJSpace space) {
        JiniTransactionHolder txObject = (JiniTransactionHolder) TransactionSynchronizationManager.getResource(ExistingJiniTransactionManager.CONTEXT);
        if (txObject != null && txObject.hasTransaction()) {
            return txObject.getTxCreated();
        }

        // try and perform early exit when we should not support declarative transactions for better performance
        if (actualTransactionalContext == null && !isJta) {
            return null;
        }

        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return null;
        }

        if (isJta) {
            List<TransactionSynchronization> txSynchronizations = TransactionSynchronizationManager.getSynchronizations();
            for (TransactionSynchronization txSynchronization : txSynchronizations) {
                if (txSynchronization instanceof SpaceAndTransactionSync) {
                    SpaceAndTransactionSync spaceSync = (SpaceAndTransactionSync) txSynchronization;
                    if (spaceSync.getSpace().equals(space)) {
                        // we already registered this space on this JTA transaction, simply return the transaction
                        return spaceSync.getTransaction();
                    }
                }
            }

            // Register and enlist a new XA resource with the transaction manager
            JtaTransactionManager jtaTransactionManager = (JtaTransactionManager) transactionManager;
            javax.transaction.Transaction jtaTransaction = null;
            try {
                jtaTransaction = jtaTransactionManager.getTransactionManager().getTransaction();
            } catch (Exception e) {
                throw new TransactionDataAccessException("Failed to get JTA transaction", e);
            }
            if (jtaTransaction == null)
                return null;

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
            XAResource xaResourceSpace = new XAResourceImpl(distributedTransactionManagerProvider.getTransactionManager(), space, true/*delegatedXa*/, false/*resourcePerSingleTxn*/);
            // set the default timeout to be the one specified on the JTA transaction manager
            if (jtaTransactionManager.getDefaultTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
                try {
                    xaResourceSpace.setTransactionTimeout(jtaTransactionManager.getDefaultTimeout() * 1000);
                } catch (XAException e) {
                    throw new TransactionDataAccessException("Failed to set default timeout of [" + (jtaTransactionManager.getDefaultTimeout() * 1000) + "] on xa resource", e);
                }
            }

            // enlist the Space xa resource with the current JTA transaction
            // we rely on the fact that this call will start the XA transaction
            try {
                jtaTransaction.enlistResource(xaResourceSpace);
            } catch (Exception e) {
                throw new TransactionDataAccessException("Failed to enlist xa resource [" + xaResourceSpace + "] with space [" + space + "]", e);
            }

            // get the context transaction from the Space, dont nullify it since the proxy thread local contains the XAResoureceImpl instance
            //   Transaction.Created transaction = ((ISpaceProxy) space).replaceContextTransaction(null);
            Transaction.Created transaction = ((ISpaceProxy) space).getContextTransaction();

            // register a marker sync object that acts as a placeholder for both the Space and the transaction
            TransactionSynchronizationManager.registerSynchronization(new SpaceAndTransactionSync(space, transaction));

            return transaction;
        }

        if (actualTransactionalContext == null) {
            return null;
        }

        txObject = (JiniTransactionHolder) TransactionSynchronizationManager.getResource(actualTransactionalContext);
        if (txObject != null && txObject.hasTransaction()) {
            return txObject.getTxCreated();
        }
        return null;
    }

    public PlatformTransactionManager getTransactionManager() {
        return this.transactionManager;
    }

    public JiniTransactionHolder getHolder() {
        if (isJta) {
            return null;
        }
        // try and perform early exit when we should not support declarative transactions for better performance
        if (actualTransactionalContext == null) {
            return null;
        }

        JiniTransactionHolder txObject = (JiniTransactionHolder) TransactionSynchronizationManager.getResource(ExistingJiniTransactionManager.CONTEXT);
        if (txObject != null && txObject.hasTransaction()) {
            return txObject;
        }

        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return null;
        }

        txObject = (JiniTransactionHolder) TransactionSynchronizationManager.getResource(actualTransactionalContext);
        if (txObject != null && txObject.hasTransaction()) {
            return txObject;
        }
        return null;
    }

    public int getCurrentTransactionIsolationLevel(Object transactionalContext) {
        if (actualTransactionalContext == null) {
            return TransactionDefinition.ISOLATION_DEFAULT;
        }
        Integer currentIsoaltionLevel = TransactionSynchronizationManager.getCurrentTransactionIsolationLevel();
        if (currentIsoaltionLevel != null) {
            return currentIsoaltionLevel;
        }
        return TransactionDefinition.ISOLATION_DEFAULT;
    }

    public boolean isEnabled() {
        if (actualTransactionalContext != null) {
            return true;
        }
        if (isJta) {
            return true;
        }
        return false;
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

    /**
     * A Spring synchronization that acts as a place-holder for the Space associated with the
     * current Spring transaction.
     */
    private static class SpaceAndTransactionSync implements TransactionSynchronization {

        private IJSpace space;

        private Transaction.Created transaction;

        public SpaceAndTransactionSync(IJSpace space, Transaction.Created transaction) {
            this.space = space;
            this.transaction = transaction;
        }

        public IJSpace getSpace() {
            return space;
        }

        public Transaction.Created getTransaction() {
            return transaction;
        }

        public void suspend() {
        }

        public void resume() {
        }

        public void beforeCommit(boolean readOnly) {
        }

        public void beforeCompletion() {
        }

        public void afterCommit() {
        }

        public void afterCompletion(int status) {
        }

        public void flush() {
        }
    }
}
