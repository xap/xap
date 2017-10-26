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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.admin.DirectPersistencySyncListAdmin;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.EmbeddedSyncHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.ioImpl.IDirectPersistencyIoHandler;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.cache.blobStore.BlobStoreEntryHolder;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.kernel.SystemProperties;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_SYNC_LIST_THREAD_INTERVAL_TIME_DEFAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_SYNC_LIST_THREAD_INTERVAL_TIME_PROP;

/**
 * The synchronizing direct-persistency handler
 *
 * @author yechielf
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencySyncHandler implements IDirectPersistencySyncHandler {

    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_DIRECT_PERSISTENCY);
    private final long _currentGenerationId;
    private final DirectPersistencyListHandler _directPersistencyListHandler;
    private final DirectPersistencySyncListAdmin _admin;
    private IReplicationGroupBacklog _backlog;
    private final String _peerSpace;      //currently we support only 1 peer
    private final String _currentSpace;
    private final Syncer _syncer;
    private long _confirmed;
    private final SpaceEngine _spaceEngine;

    private final EmbeddedSyncHandler _embeddedSyncHandler;
    private final ReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
    private final Lock _readLock = _readWriteLock.readLock();
    private final Lock _writeLock = _readWriteLock.writeLock();
    private volatile boolean _afterRecoveryStarted;

    private final int SYNC_THREAD_INTERVAL_TIME;
    private final int SYNC_THREAD_INTERVAL_TIME_IF_OVERFLOW;

    public DirectPersistencySyncHandler(SpaceEngine engine) {
        ClusterPolicy clusterPolicy = engine.getClusterPolicy();

        if (!clusterPolicy.isReplicated() || clusterPolicy.isActiveActive() || !clusterPolicy.isPrimaryElectionAvailable()
                || clusterPolicy.m_FailOverPolicy == null)
            throw new UnsupportedOperationException("DirectPersistencySyncHandler cannot be created-cluster policy dont suit");

        if (clusterPolicy.m_FailOverPolicy.failOverGroupMembersNames.size() == 1)
            throw new UnsupportedOperationException("DirectPersistencySyncHandler cannot be created-cluster policy dont suit");


        _spaceEngine = engine;
        String peerSpace = null;
        for (String name : clusterPolicy.m_FailOverPolicy.failOverGroupMembersNames) {
            if (!name.equals(clusterPolicy.m_ClusterGroupMember)) {
                peerSpace = name;
                break;
            }
        }
        _peerSpace = peerSpace;
        _currentSpace = clusterPolicy.m_ClusterGroupMember;
        _currentGenerationId = System.currentTimeMillis();
        _directPersistencyListHandler = new DirectPersistencyListHandler(this, engine);
        _syncer = new Syncer("DirectPersistencySyncerThread", this);
        _confirmed = -1;

        boolean embeddedSyncListEnabled = Boolean.parseBoolean(System.getProperty(SystemProperties.USE_BLOBSTORE_EMBEDDED_SYNC_LIST, SystemProperties.USE_BLOBSTORE_EMBEDDED_SYNC_LIST_DEFAULT));
        _embeddedSyncHandler = embeddedSyncListEnabled ? new EmbeddedSyncHandler(this) : null;

        _admin = new DirectPersistencySyncListAdmin(this);

        SYNC_THREAD_INTERVAL_TIME = _spaceEngine.getConfigReader().getIntSpaceProperty(CACHE_MANAGER_SYNC_LIST_THREAD_INTERVAL_TIME_PROP, String.valueOf(CACHE_MANAGER_SYNC_LIST_THREAD_INTERVAL_TIME_DEFAULT));
        SYNC_THREAD_INTERVAL_TIME_IF_OVERFLOW = SYNC_THREAD_INTERVAL_TIME / 10;

        _logger.info("created direct-persistency handler peerSpace=" + _peerSpace + " thread-interval =" + SYNC_THREAD_INTERVAL_TIME + " max-in-memory=" + _directPersistencyListHandler.MAX_ENTRIES_IN_MEMORY + " embedded sync list is used? [" + isEmbeddedListUsed() + "]");
    }


    //should be called after blobstore/direct-persistency is initialized
    @Override
    public void initialize() {
        //remove all overflow records from prev' generations
        _directPersistencyListHandler.removeAllOverflow();
        _syncer.start();
        if (isEmbeddedListUsed()) {
            _embeddedSyncHandler.initialize();
        }
    }

    @Override
    public void afterInitializedBlobStoreIO(IDirectPersistencyIoHandler ioHandler) {
        _directPersistencyListHandler.setDirectPersistencyIoHandler(ioHandler);
        _admin.initialize(_currentGenerationId);
        if (isEmbeddedListUsed()) {
            _embeddedSyncHandler.afterInitializedBlobStoreIO();
        }
    }


    @Override
    public long getCurrentGenerationId() {
        return _currentGenerationId;
    }

    @Override
    public boolean isOverflow() {
        return _directPersistencyListHandler.isOverflow();
    }

    @Override
    public void beforeDirectPersistencyOp(IReplicationOutContext replContext, IEntryHolder entryHolder, boolean phantom) {
        DirectPersistencySingleUidOpInfo e = _directPersistencyListHandler.onDirectPersistencyOpStart(entryHolder.getUID());
        // call embedded api to create the embedded part
        if (isEmbeddedListUsed()) {
            _embeddedSyncHandler.beforeEmbeddedSyncOp(e, phantom);
            ((BlobStoreEntryHolder) entryHolder).setEmbeddedSyncOpInfo(e.getGenerationId(), e.getSequenceNumber(), phantom, false);
        }

        replContext.setDirectPersistencyPendingEntry(e);
        replContext.setDirectPesistencySyncHandler(this);

    }

    @Override
    public void beforeDirectPersistencyOp(IReplicationOutContext replContext, List<String> uids, Set<String> phantoms, Map<String, IEntryHolder> entryHolderMap) {
        DirectPersistencyMultipleUidsOpInfo e = _directPersistencyListHandler.onDirectPersistencyOpStart(uids);
        // call embedded api to create the embedded part
        if (isEmbeddedListUsed()) {
            _embeddedSyncHandler.beforeEmbeddedSyncOp(e, phantoms);
            for (IEntryHolder iEntryHolder : entryHolderMap.values()) {
                ((BlobStoreEntryHolder) iEntryHolder).setEmbeddedSyncOpInfo(e.getGenerationId(), e.getSequenceNumber(), (phantoms != null && phantoms.contains(iEntryHolder.getUID())), true);
            }
        }


        replContext.setDirectPersistencyPendingEntry(e);
        replContext.setDirectPesistencySyncHandler(this);
    }


    @Override
    public void onEmbeddedOpFromInitialLoad(String uid, long gen, long seq, boolean phantom) {
        _embeddedSyncHandler.onEmbeddedOpFromInitialLoad(_directPersistencyListHandler.onEmbeddedOpFromInitialLoad(uid, gen, seq), phantom);
    }

    @Override
    public void onEmbeddedOpFromInitialLoad(List<String> uids, long gen, long seq, Set<String> phantoms) {
        _embeddedSyncHandler.onEmbeddedOpFromInitialLoad(_directPersistencyListHandler.onEmbeddedOpFromInitialLoad(uids, gen, seq), phantoms);
    }


    public void onEmbeddedListRecordTransferStart(IDirectPersistencyOpInfo o, boolean onlyIfNotExists) {
        _directPersistencyListHandler.onEmbeddedListRecordTransferStart(o, onlyIfNotExists);
    }

    public void onEmbeddedListRecordTransferEnd(IDirectPersistencyOpInfo o) {
        _directPersistencyListHandler.onEmbeddedListRecordTransferEnd(o);
    }


    /* get the prev generation entries- an iterator of uids*/
    @Override
    public Iterator<String> getEntriesForRecovery() {
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "space " + _currentSpace + " DirectPersistencySyncHandler: getEntriesForRecovery called");
        return _directPersistencyListHandler.getRecoveryIterator();
    }

    @Override
    public void afterRecovery() {
        // lock which prevents old generation initial loaded entries be inserted to embedded sync list if afterRecovery was called
        _writeLock.lock();
        try {
            _afterRecoveryStarted = true;
        } finally {
            _writeLock.unlock();
        }
        if (isEmbeddedListUsed()) {
            _embeddedSyncHandler.afterRecovery();
        }
        _directPersistencyListHandler.cleanPreviousGenerationsOps();
        _admin.cleanOlderGens(_currentGenerationId);
    }

    @Override
    public void afterInsertingToRedolog(IReplicationOutContext context, long redoKey) {
        if (context.getDirectPersistencyPendingEntry() != null) {
            context.getDirectPersistencyPendingEntry().setRedoKey(redoKey);
            _directPersistencyListHandler.checkDirectPersistencyOpCompleletion(context.getDirectPersistencyPendingEntry());
            context.setDirectPersistencyPendingEntry(null);
        }
    }

    @Override
    public void afterOperationPersisted(IDirectPersistencyOpInfo e) {
        e.setPersisted();
        _directPersistencyListHandler.checkDirectPersistencyOpCompleletion(e);
    }

    @Override
    public void setBackLog(IReplicationGroupBacklog backlog) {
        _backlog = backlog;
    }

    @Override
    public IReplicationGroupBacklog getBackLog() {
        return _backlog;
    }

    public Logger getLogger() {
        return _logger;
    }

    @Override
    public DirectPersistencyListHandler getListHandler() {
        return _directPersistencyListHandler;
    }

    @Override
    public long getLastConfirmed() {
        return _confirmed;
    }

    @Override
    public void setLastConfirmed(long confirmed) {
        _confirmed = confirmed;
    }

    public Lock getReadLock() {
        return _readLock;
    }

    public boolean afterRecoveryStarted() {
        return _afterRecoveryStarted;
    }

    @Override
    public void close() {
        if (isEmbeddedListUsed()) {
            _embeddedSyncHandler.close();
        }
        _syncer.close();
    }

    public SpaceEngine getSpaceEngine() {
        return _spaceEngine;
    }

    @Override
    public boolean isEmbeddedListUsed() {
        return _embeddedSyncHandler != null;
    }

    @Override
    public EmbeddedSyncHandler getEmbeddedSyncHandler() {
        return _embeddedSyncHandler;
    }

    private class Syncer extends GSThread {
        private volatile boolean _closed;
        private final DirectPersistencySyncHandler _handler;

        /**
         * Timer daemon thread.
         *
         * @param threadName the name of this daemon thread.
         */
        public Syncer(String threadName, DirectPersistencySyncHandler handler) {
            super(threadName);
            _handler = handler;
            this.setDaemon(true);
        }


        @Override
        public void run() {
            int synced = 0;
            try {
                while (!_closed) {
                    try {
                        long timeToSleep = _handler.isOverflow() ? _handler.SYNC_THREAD_INTERVAL_TIME_IF_OVERFLOW : _handler.SYNC_THREAD_INTERVAL_TIME;
                        try {
                            if (synced == 0 || !_handler.isOverflow())
                                Thread.sleep(timeToSleep);
                        } catch (InterruptedException ex) {
                            if (_handler.getLogger().isLoggable(Level.FINE)) {
                                _handler.getLogger().warning("[" + _handler._currentSpace + "]" + " DirectPersistencySyncHandler Syncer thread interrupted");
                            }
                            if (_closed)
                                break;
                            continue;
                        }
                        if (_closed)
                            break;
                        synced = handelListIfConfirmationsOccured();

                        moveToOverflowIfNeeded();
                    } catch (Throwable ex) {
                        if (_handler.getLogger().isLoggable(Level.SEVERE)) {
                            _handler.getLogger().severe("[" + _handler._currentSpace + "]" + " DirectPersistencySyncHandler Syncer thread got unexpected exception " + ex);
                        }
                        if (_closed)
                            break;
                    }
                }
            } finally {
                synchronized (this) {
                    notifyAll();
                }
            }
        }

        private int handelListIfConfirmationsOccured() {
            if (_backlog == null) {
                if (_logger.isLoggable(Level.FINE) && _spaceEngine.getSpaceImpl().isPrimary())
                    _logger.log(Level.FINE, "[" + _handler._currentSpace + "]" + " DirectPersistencySyncHandler:Syncer backlog not set yet");
                return 0;
            }
            long confirmed = _backlog.getConfirmed(_peerSpace);
            if (confirmed > _handler.getLastConfirmed())
                _handler.setLastConfirmed(confirmed);

            int synced = _directPersistencyListHandler.syncToRedologConfirmation(_handler.getLastConfirmed());
            if (synced > 0) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.log(Level.FINE, "[" + _handler._currentSpace + "]" + " DirectPersistencySyncHandler ripped " + synced + " confirmed=" + _handler.getLastConfirmed() + " confirmed ops, remained in memory=" + _handler._directPersistencyListHandler.getNumMemoryOps() + " overflow=" + _handler.isOverflow());
                }
            }
            return synced;
        }

        private void moveToOverflowIfNeeded() {
            int overf = _directPersistencyListHandler.moveTooverflowIfNeeded();
            if (overf > 0) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.log(Level.FINE, "[" + _handler._currentSpace + "]" + " DirectPersistencySyncHandler " + overf + " moved to overflow");
                }
            }


        }


        public void close() {
            synchronized (this) {
                if (_closed)
                    return;
                _closed = true;
                interrupt();
                try {
                    wait(10000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

}
