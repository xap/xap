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

package com.j_spaces.core.transaction;

import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.XtnStatus;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.LocalTransactionManager;
import com.j_spaces.kernel.SystemProperties;
import com.sun.jini.mahalo.TxnMgrProxy;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.ServerTransaction;
import org.slf4j.Logger;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_UNUSED_TXN_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_UNUSED_TXN_PROP;

/**
 * Handle engine transactions
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class TransactionHandler {
    private final ConcurrentHashMap<ServerTransaction, XtnEntry> m_XtnTable = new ConcurrentHashMap<ServerTransaction, XtnEntry>();

    final static private boolean IS_FAIR_LOCK = Boolean.getBoolean("com.gs.transaction.lock.fair");
    final static public long XTN_ADDITIONAL_TIMEOUT = Long.getLong("com.gs.transaction.leaseAddition", 10000).longValue();
    final static private long GLOBAL_XTN_PHANTOM_TIMEOUT = 1000 * 60 * 3;


    //the following constants relate to stuck prepared xtn
    final static private boolean SUPPORT_HANDLING_STUCK_2PC_XTNS = true;
    final static private long BASIC_TIME_FOR_PREPARED_XTN =
            Long.getLong(SystemProperties.CACHE_STUCK_2PC_BASIC_TIME, SystemProperties.CACHE_STUCK_2PC_BASIC_TIME_DEFAULT);
    final static private boolean SUPPORTS_PREPARED_XTNS_EXTENTION_TIME = true;
    final static private long EXTENTION_TIME_FOR_PREPARED_XTN =
            Long.getLong(SystemProperties.CACHE_STUCK_2PC_EXTENTION_TIME, SystemProperties.CACHE_STUCK_2PC_EXTENTION_TIME_DEFAULT);
    final static private int MAX_NUMBER_OF_PREAPARED_XTN_EXTENTIONS = 2;
    final static private long TIME_TO_WAIT_FOR_TM = 1000 * 10;


    // the lock has to be fair to avoid starvation of write operations
    private final ReadWriteLock _txLock = new ReentrantReadWriteLock(IS_FAIR_LOCK);

    /**
     * @SBGen Collection of com.j_spaces.core.XtnEntry
     */
    private final ConcurrentHashMap<ServerTransaction, Long> m_TimedXtns = new ConcurrentHashMap<ServerTransaction, Long>();
    //contains global xtns which abort reached the space before join
    private final Map<ServerTransaction, Long> _phantomGlobalXtns = new ConcurrentHashMap<ServerTransaction, Long>();
    private final SpaceEngine _engine;

    //after this time we clean unused xtns
    private int clean_unused_xtns_interval = 0;

    //prepared 2PC xtns
    private final Map<ServerTransaction, Prepared2PCXtnInfo> _prepared2PCXtns = new ConcurrentHashMap<ServerTransaction, Prepared2PCXtnInfo>();
    private static final int TXN_SETTLE_DURATION = GsEnv.propertyInt("com.gs.drain.transactions-settle-duration").get(7500);

    public TransactionHandler(SpaceConfigReader configReader, SpaceEngine engine) {
        _engine = engine;
        clean_unused_xtns_interval = configReader.getIntSpaceProperty(LM_EXPIRATION_TIME_UNUSED_TXN_PROP, String.valueOf(LM_EXPIRATION_TIME_UNUSED_TXN_DEFAULT));
        Logger logger = CacheManager.getCacheLogger();
        if (logger.isDebugEnabled())
            logger.debug("TransactionHandler BASIC_TIME_FOR_PREPARED_XTN=" + BASIC_TIME_FOR_PREPARED_XTN + " EXTENTION_TIME_FOR_PREPARED_XTN-" + EXTENTION_TIME_FOR_PREPARED_XTN);
    }

    /**
     * @return the xtnTable
     */
    public Map<ServerTransaction, XtnEntry> getXtnTable() {
        return m_XtnTable;
    }


    /**
     * @return the timedLocalXtns
     */
    public Map<ServerTransaction, Long> getTimedXtns() {
        return m_TimedXtns;
    }

    public Map<ServerTransaction, Long> getPhantomGlobalXtns() {
        return _phantomGlobalXtns;
    }

    public Lock getTxWriteLock() {
        return _txLock.writeLock();
    }

    public Lock getTxReadLock() {
        return _txLock.readLock();
    }

    public void addTransaction(XtnEntry xtnEntry) {
        m_XtnTable.put(xtnEntry.m_Transaction, xtnEntry);
    }

    public XtnEntry addTransactionIfAbsent(XtnEntry xtnEntry) {
        XtnEntry prev;
        if ((prev = m_XtnTable.putIfAbsent(xtnEntry.m_Transaction, xtnEntry)) != null)
            return prev;
        return xtnEntry;
    }

    /**
     * Remove given transaction from handled transactions
     */
    public void removeTransactionAfterEnd(XtnEntry xtnEntry) {
        try {
            xtnEntry.lock();

            ((ConcurrentHashMap<ServerTransaction, XtnEntry>) getXtnTable()).remove(xtnEntry.m_Transaction, xtnEntry);
            m_TimedXtns.remove(xtnEntry.m_Transaction);
        } finally {
            xtnEntry.unlock();
        }
    }

    /**
     * Remove an unused transaction from handled transactions
     */
    public void removeUnusedTransaction(XtnEntry xtnEntry, boolean needLock) {

        try {
            if (needLock)
                xtnEntry.lock();

            if (((ConcurrentHashMap<ServerTransaction, XtnEntry>) getXtnTable()).remove(xtnEntry.m_Transaction, xtnEntry))
                m_TimedXtns.remove(xtnEntry.m_Transaction);
        } finally {
            if (needLock)
                xtnEntry.unlock();
        }
    }

    /**
     * Create new TransactionEntry and add it to the list of managed transactions
     *
     * @return XtnEntry
     */
    public XtnEntry createTransactionEntry(ServerTransaction txn) {

        //is it a local transaction with lease
        if (isTransactionWithLease(txn))
            addTransactionWithLease(txn);

        // if we got here, Xtn does not exist in table
        XtnEntry xtnEntry = new XtnEntry(txn, _engine.getSpaceImpl().isBackup());

        addTransaction(xtnEntry);
        return xtnEntry;
    }

    /**
     * Create new TransactionEntry and add it to the list of managed transactions if such xtn does
     * not exist. if it exists=return it
     *
     * @param newXtnEntry - to be inserted if absent
     * @return XtnEntry
     */
    public XtnEntry createTransactionEntryIfAbsent(ServerTransaction txn, XtnEntry newXtnEntry) {

        XtnEntry cur;

        cur = addTransactionIfAbsent(newXtnEntry);
        if (cur != newXtnEntry)
            return cur;
        //is it a local transaction with lease
        if (isTransactionWithLease(txn))
            addTransactionWithLease(txn);

        return newXtnEntry;
    }

    private void addTransactionWithLease(ServerTransaction txn) {
        //insert a record to hash table
        renewTransactionLease(txn, txn.getLease());
    }

    public void renewTransactionLease(ServerTransaction txn, long time) {
        //insert a record to hash table,
        //if lease record already exists- keep the max of them
        Long limit = time + SystemTime.timeMillis() + XTN_ADDITIONAL_TIMEOUT;
        while (true) {
            Long prev;
            if ((prev = m_TimedXtns.putIfAbsent(txn, limit)) == null)
                break;
            if (prev.longValue() >= limit.longValue())
                break;
            if (m_TimedXtns.replace(txn, prev, limit))
                break;
        }
    }

    /**
     * Returns true if given transaction is a  timed transaction
     *
     * @return true if transaction lease is different from java.lang.{@link Long} and 0
     */
    private boolean isTransactionWithLease(ServerTransaction txn) {

        return txn.getLease() != Long.MAX_VALUE
                && txn.getLease() != 0;
    }

    public int getUnusedXtnCleanTime() {
        return clean_unused_xtns_interval;
    }

    /**
     * Get the transaction entry for given transaction. Check if transaction is valid
     *
     * @return XtnEntry
     */
    public XtnEntry getTransactionEntry(ServerTransaction txn) throws TransactionException {
        XtnEntry xtnEntry = m_XtnTable.get(txn);

        // check if this transaction is still active
        if (xtnEntry != null && !xtnEntry.isActive())
            throw new TransactionException("Transaction is not active: " +
                    txn.toString());
        return xtnEntry;
    }

    /**
     * Join the specified transaction, if not already joined.
     *
     * @param txn the transaction to join
     */
    public XtnEntry attachToXtnGranular(final ServerTransaction txn, boolean fromReplication)
            throws TransactionException, RemoteException {
        XtnEntry xtnEntry = null;

        try {
            // first check whether this transaction is already joined
            xtnEntry = getTransactionEntry(txn);
            final XtnEntry firstFoundXtnEntry = xtnEntry;
            if (xtnEntry != null && !xtnEntry.addUsedIfPossible()) //xtn is unused, so remove it
            {
                removeUnusedTransaction(xtnEntry, true /* needLock*/);
                xtnEntry = null;
            }

            if (xtnEntry != null && xtnEntry.getStatus() != XtnStatus.UNINITIALIZED && xtnEntry.getStatus() != XtnStatus.UNINITIALIZED_FAILED && (txn.isEmbeddedMgrInProxy() || !xtnEntry.isOnlyEmbeddedJoins())) {
                if (txn.isEmbeddedMgrInProxy() && !xtnEntry.isOperatedUpon() && isTransactionWithLease(txn))
                    //transaction may have been detached from manager so perform renew with the interval
                    // interval is updated in user xtn (in embedded join) if renew was performed
                    renewTransactionLease(txn, txn.getLease());
                return xtnEntry; /* already joined this transaction */
            }
            while (true) {
                XtnEntry newXtnEntry = null;
                if (xtnEntry == null) {
                    newXtnEntry = new XtnEntry(txn, _engine.getSpaceImpl().isBackup());
                    xtnEntry = newXtnEntry;
                }

                //lock the xtn
                XtnEntry lockXtnEntry = xtnEntry;
                lockXtnEntry.lock();
                try {
                    if (newXtnEntry != null) {
                        XtnEntry curXtn = createTransactionEntryIfAbsent(txn, newXtnEntry);
                        if (curXtn != newXtnEntry) {//retry-need relock
                            xtnEntry = curXtn;
                            continue;
                        }
                    }
                    if (newXtnEntry == null && xtnEntry != firstFoundXtnEntry && !xtnEntry.addUsedIfPossible()) {//xtn is unused, so remove it
                        removeUnusedTransaction(xtnEntry, false /* needLock*/);
                        xtnEntry = null;
                        continue;
                    }
                    XtnStatus status = xtnEntry.getStatus();
                    if (status != XtnStatus.UNINITIALIZED && status != XtnStatus.UNINITIALIZED_FAILED)
                        if (txn.isEmbeddedMgrInProxy() || !xtnEntry.isOnlyEmbeddedJoins())
                            return xtnEntry; /* already joined this transaction */

                    boolean localXtn = xtnEntry.m_Transaction.mgr instanceof LocalTransactionManager;
                    if (!localXtn && isPhantomGlobalXtn(txn)) {
                        removeUnusedTransaction(xtnEntry, false /* needLock*/);
                        xtnEntry = null;
                        throw new TransactionException(" phantom xtn reached member=" + _engine.getFullSpaceName() + " xtnid=" + txn.id);
                    }

                    if (!localXtn) {
                        XtnEntry joinLockXtnEntry = xtnEntry;
                        synchronized (joinLockXtnEntry.getJoinLock()) {
                            xtnEntry.unlock();
                            lockXtnEntry = null;
                            try {
                                if (xtnEntry.getStatus() == XtnStatus.UNINITIALIZED_FAILED) {//help removing
                                    ((ConcurrentHashMap<ServerTransaction, XtnEntry>) getXtnTable()).remove(xtnEntry.m_Transaction, xtnEntry);
                                    xtnEntry = null;
                                    continue;
                                }
                                if (!fromReplication && !txn.isEmbeddedMgrInProxy() && xtnEntry.setOnlyEmbeddedJoins(false)) {
                                    final PlatformLogicalVersion targetSpaceVersion = LRMIUtilities.getServicePlatformLogicalVersion(((TxnMgrProxy) txn.mgr).getProxy());
                                    if (_engine.getClusterPolicy() != null)
                                        txn.join(_engine.getSpaceImpl().getSpaceStub(), SystemTime.timeMillis()/* crashcount */, _engine.getPartitionIdZeroBased(), _engine.getClusterPolicy().m_ClusterName);
                                    else
                                        txn.join(_engine.getSpaceImpl().getSpaceStub(), SystemTime.timeMillis()/* crashcount */);
                                }
                                // change transaction status
                                if (xtnEntry.getStatus() == XtnStatus.UNINITIALIZED)
                                    xtnEntry.setStatus(XtnStatus.BEGUN);
                            } catch (TransactionException te) {  //join failed , remove uninitialized xtn
                                if (xtnEntry.getStatus() == XtnStatus.UNINITIALIZED) {
                                    xtnEntry.setStatus(XtnStatus.UNINITIALIZED_FAILED);
                                    ((ConcurrentHashMap<ServerTransaction, XtnEntry>) getXtnTable()).remove(xtnEntry.m_Transaction, xtnEntry);
                                }
                                xtnEntry = null;
                                throw te;
                            } catch (RemoteException re) {  //join failed , remove uninitialized xtn
                                if (xtnEntry.getStatus() == XtnStatus.UNINITIALIZED) {
                                    xtnEntry.setStatus(XtnStatus.UNINITIALIZED_FAILED);
                                    ((ConcurrentHashMap<ServerTransaction, XtnEntry>) getXtnTable()).remove(xtnEntry.m_Transaction, xtnEntry);
                                }
                                xtnEntry = null;
                                throw re;
                            }
                        }
                    } else {
                        // local
                        xtnEntry.setStatus(XtnStatus.BEGUN);
                    }
                } finally {
                    if (lockXtnEntry != null)
                        lockXtnEntry.unlock();
                }
                return xtnEntry;
            }
        } finally {
            if (xtnEntry != null)
                setNonEmbeddedJoinIfNeed(xtnEntry, txn, fromReplication);
        }
    }

    /**
     * /** given an entry + template- perform the xtn locks and set the appropriate
     *
     * @param alreadyLockedXtn - if an encapsulating xtn was already locked (for example in
     *                         takemultiple)
     */
    public void xtnLockEntryOnTemplateOperation(Context context, IEntryHolder eh, ITemplateHolder th, XtnEntry alreadyLockedXtn) {
        if (th.getXidOriginated() != alreadyLockedXtn && th.getXidOriginated() != null)
            th.getXidOriginated().lock();
    }

    public void xtnUnlockEntryOnTemplateOperation(ITemplateHolder th, XtnEntry alreadyLockedXtn) {
        if (th.getXidOriginated() != alreadyLockedXtn && th.getXidOriginated() != null)
            th.getXidOriginated().unlock();
    }

    /**
     * xtn is ending (rollbak/commit) - lock + fifo lock if fifo entries in xtn
     */
    public boolean lockXtnOnXtnEnd(XtnEntry xtnEntry) {
        xtnEntry.lock();
        boolean lockedXtnTable = xtnEntry.anyFifoEntriesUnderXtn();
        if (lockedXtnTable)
            getTxWriteLock().lock();
        return lockedXtnTable;
    }

    public void unlockXtnOnXtnEnd(XtnEntry xtnEntry, boolean lockedXtnTable) {
        xtnEntry.unlock();
        if (lockedXtnTable)
            getTxWriteLock().unlock();
    }

    public boolean isLightTransaction(XtnEntry xtnEntry) {
        return !xtnEntry.getXtnData().anyLockedEntries()
                && (xtnEntry.getXtnData().getEntriesForFifoGroupScan() == null || xtnEntry.getXtnData()
                .getEntriesForFifoGroupScan()
                .isEmpty());
    }

    public boolean isPhantomGlobalXtn(ServerTransaction xtn) {
        return (_phantomGlobalXtns.containsKey(xtn));
    }

    public void addToPhantomGlobalXtns(ServerTransaction xtn) {
        _phantomGlobalXtns.put(xtn, (SystemTime.timeMillis() + GLOBAL_XTN_PHANTOM_TIMEOUT));
    }

    public void removeFromPhantomGlobalXtns(ServerTransaction xtn) {
        _phantomGlobalXtns.remove(xtn);
    }

    private void setNonEmbeddedJoinIfNeed(XtnEntry xtnEntry, ServerTransaction txn, boolean fromReplication)
            throws TransactionException, RemoteException {

        if ((txn.isEmbeddedMgrInProxy() && xtnEntry.m_Transaction.isEmbeddedMgrInProxy()) || !xtnEntry.isOnlyEmbeddedJoins())
            return;
        if (xtnEntry.m_Transaction.mgr instanceof LocalTransactionManager)
            return; //backward comp'

        //this participant had only embedded joins, need to perform a remote join
        // in order to report to embedded mahalo that there is a remote proxy involved as well
        //so mahalo will not drop the participant if reported as failed in case of inactive remote space
        if (_engine.isCleanUnusedEmbeddedGlobalXtns() && xtnEntry.getStatus() == XtnStatus.BEGUN) {
            if (!fromReplication) {
                synchronized (xtnEntry.getJoinLock()) {
                    if (xtnEntry.getStatus() == XtnStatus.BEGUN && xtnEntry.setOnlyEmbeddedJoins(false)) {
                        ServerTransaction xtnToUse = !txn.isEmbeddedMgrInProxy() ? txn : xtnEntry.m_Transaction;
                        final PlatformLogicalVersion targetSpaceVersion = LRMIUtilities.getServicePlatformLogicalVersion(((TxnMgrProxy) xtnToUse.mgr).getProxy());
                        if (_engine.getClusterPolicy() != null)
                            xtnToUse.join(_engine.getSpaceImpl().getSpaceStub(), SystemTime.timeMillis()/* crashcount */, _engine.getPartitionIdZeroBased(), _engine.getClusterPolicy().m_ClusterName);
                        else
                            xtnToUse.join(_engine.getSpaceImpl().getSpaceStub(), SystemTime.timeMillis()/* crashcount */);
                    }
                }
            }

        }
    }

    public static long getBasicTimeForPreparedXtn() {
        return BASIC_TIME_FOR_PREPARED_XTN;
    }

    public static boolean isSupportsPreparedXtnsExtentionTime() {
        return SUPPORTS_PREPARED_XTNS_EXTENTION_TIME;
    }

    public static long getExtentionTimeForPreparedXtn() {
        return EXTENTION_TIME_FOR_PREPARED_XTN;
    }

    public static int getMaxNumberOfPreaparedXtnExtentions() {
        return MAX_NUMBER_OF_PREAPARED_XTN_EXTENTIONS;
    }

    public void addToPrepared2PCXtns(XtnEntry xtnEntry) {
        if (!SUPPORT_HANDLING_STUCK_2PC_XTNS)
            return;
        _prepared2PCXtns.put(xtnEntry.getServerTransaction(), new Prepared2PCXtnInfo(xtnEntry, BASIC_TIME_FOR_PREPARED_XTN));
    }

    public void removeFromPrepared2PCXtns(ServerTransaction xtn) {
        if (!SUPPORT_HANDLING_STUCK_2PC_XTNS)
            return;
        _prepared2PCXtns.remove(xtn);
    }

    public Map<ServerTransaction, Prepared2PCXtnInfo> getPrepared2PCXtns() {
        return _prepared2PCXtns;
    }

    public static long getTimeToWaitForTm() {
        return TIME_TO_WAIT_FOR_TM;
    }

    //check the (rare) state in which under xtn op wasnt returned to the client because of disconnection and
    //the proxy retries with same xtn and operation id
    public void checkTransactionDisconnection(OperationID opid, ServerTransaction txn)
            throws TransactionException {
        XtnEntry xtnEntry = m_XtnTable.get(txn);
        Logger logger = CacheManager.getCacheLogger();
        //Do no remove/change logger, using in unitTest see @PollingContainerTest
        if (logger.isDebugEnabled()) {
            if (!(_engine.getSpaceImpl().isBackup() || opid == null)) {

                logger.debug("DebugForTest:ThreadID:" + Thread.currentThread().getName() + "TransactionID= " + txn.id + "   operationID={" + opid.toString() + "}");
            }
        }
        if (xtnEntry == null || !xtnEntry.createdOnNonBackup() || opid == null || !xtnEntry.getXtnData().isOperationID(opid)) {
            return;
        }
        TransactionException exception = new TransactionException("Transaction was disconnected due to communication fault: " +
                txn.toString());
        _engine.getLogger().warn("Transaction disconnection has been detected, Transaction will be aborted", exception);
        try{
            _engine.abort(txn.getTransactionManager(), txn, false, opid);
        } catch (Exception e){
            _engine.getLogger().warn("Failed to abort transaction "+txn.toString(), e);
        }
        addToPhantomGlobalXtns(txn);
        throw exception;
    }


    public boolean waitForActiveTransactions(long timeoutInMillis) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutInMillis;
        while (System.currentTimeMillis() < end) {
            if (getXtnTable().isEmpty()) {
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }

    public boolean waitForFinalizingTransactions(long timeoutInMillis) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutInMillis;
        if (getXtnTable().isEmpty()) {
            return true;
        }
        Thread.sleep(TXN_SETTLE_DURATION);

        while (System.currentTimeMillis() < end) {
            final boolean hasFinalizing = getXtnTable().entrySet().stream().anyMatch(e -> e.getValue().getStatus().isFinalizing());
            if(!hasFinalizing){
                return true;
            }
            Thread.sleep(500);
        }
        return false;
    }
}
