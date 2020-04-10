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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin.EmbeddedSyncListTransferredInfoHandler;
import com.gigaspaces.internal.utils.concurrent.GSThread;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The synchronizing direct-persistency  embedded segment handler
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedSyncSegment {
    public static final long EMBEDDED_SYNC_SLEEP_TIME = 300;
    public static final int SHORT_SLEEP_QUEUE_SIZE = 1000;
    public static final int MIN_SIZE_TO_ITER = 1000;


    private final EmbeddedSyncHandler _embeddedHandler;
    private final ConcurrentLinkedQueue<IEmbeddedSyncOpInfo> _inputRecords; //incomming records before transfer
    private final int _segmentNumber;
    private final EmbeddedSyncListTransferredInfoHandler _infoHandler;
    private final long _currentGenerationId;
    private final EmbeddedSyncer _syncer;
    private final AtomicInteger _size;
    private final Lock _readLock;

    EmbeddedSyncSegment(EmbeddedSyncHandler embeddedHandler, int segmentNumber) {
        _embeddedHandler = embeddedHandler;
        _segmentNumber = segmentNumber;
        _inputRecords = new ConcurrentLinkedQueue<IEmbeddedSyncOpInfo>();
        _currentGenerationId = _embeddedHandler.getMainSyncHandler().getCurrentGenerationId();
        _infoHandler = new EmbeddedSyncListTransferredInfoHandler(this);
        _syncer = new EmbeddedSyncer("EMBEDDED SYNCER SEGMENT " + segmentNumber, embeddedHandler.getMainSyncHandler(), this);
        _size = new AtomicInteger();
        _readLock = _embeddedHandler.getMainSyncHandler().getReadLock();

    }

    public void initialize() {
        _syncer.start();
        _infoHandler.initialize();
    }

    int size() {
        return _size.get();
    }

    private Logger getLogger() {
        return _embeddedHandler.getMainSyncHandler().getLogger();
    }

    public void afterInitializedBlobStoreIO() {
        _infoHandler.afterInitializedBlobStoreIO((_embeddedHandler.getGensHandler().getRelevantGenerations()));
    }

    public void add(IEmbeddedSyncOpInfo op) {
        _size.incrementAndGet();
        _inputRecords.add(op);
    }

    public EmbeddedSyncHandler getEmbeddedHandler() {
        return _embeddedHandler;
    }

    public boolean transferIfPossible(IEmbeddedSyncOpInfo oi) {
        if (!oi.getOriginalOpInfo().isMultiUids())
            return transferIfPossibleImpl((EmbeddedSingeUidSyncOpInfo) oi);
        else
            return transferIfPossibleImpl((EmbeddedMultiUidsSyncOpInfo) oi);

    }

    private boolean transferIfPossibleImpl(EmbeddedSingeUidSyncOpInfo oi) {
        if (!oi.canTransferToMainList(_currentGenerationId)) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("[" + getEmbeddedHandler().getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment will not transfer " + oi.getOriginalOpInfo() + " current generation [" + _currentGenerationId + "]");
            }
            return false;
        }
        synchronized (oi) {
            //write to sync list if relevant
            boolean relevant = _embeddedHandler.getGensHandler().isRelevant(oi.getOriginalOpInfo().getGenerationId());
            boolean lockAcquired = false;
            try {
                // if old generation arrived from initial load don't persist it to sync list if after recovery handled the entry already
                if (relevant && oi.getOriginalOpInfo().getGenerationId() != _embeddedHandler.getMainSyncHandler().getCurrentGenerationId()) {
                    _readLock.lock();
                    lockAcquired = true;
                    if (_embeddedHandler.getMainSyncHandler().afterRecoveryStarted()) {
                        if (getLogger().isDebugEnabled()) {
                            getLogger().debug("[" + getEmbeddedHandler().getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment will not transfer " + oi.getOriginalOpInfo() +
                                    " current generation [" + _currentGenerationId + "] because afterRecovery already cleared it");
                        }
                        relevant = false;
                    }
                }
                boolean persist = relevant && !oi.isPersistedToMainList() && !oi.getOriginalOpInfo().isConfirmedByRemote(_embeddedHandler.getMainSyncHandler());
                boolean updateTransferredInfo = relevant && !oi.isPersistedToMainList() && oi.getOriginalOpInfo().getGenerationId() == _currentGenerationId;
                boolean addToMainList = persist && oi.getOriginalOpInfo().getGenerationId() == _currentGenerationId;

                if (oi.containsAnyPhantom() && oi.isPersistedToMainList()) {
                    throw new RuntimeException("inconsistent state in EmbeddedSegment:transferIfPossibleImpl contains phantom && persisted to main list SINGLE-uid=" + oi.getOriginalOpInfo().getUid());
                }

                if (!relevant || !addToMainList) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("[" + getEmbeddedHandler().getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment will not transfer " + oi.getOriginalOpInfo() +
                                " current generation [" + _currentGenerationId + "], relevant? [" + relevant + "], should add to main sync list? [" + addToMainList + "]");
                    }
                }

                if (persist) {
                    _embeddedHandler.getMainSyncHandler().onEmbeddedListRecordTransferStart(oi.getOriginalOpInfo(), oi.getOriginalOpInfo().getGenerationId() != _currentGenerationId);
                    oi.setPersistedToMainList(); //written to list
                }

                if (oi.containsAnyPhantom()) {
                    _embeddedHandler.getPhantomsHandler().removePhantom(oi, oi.getOriginalOpInfo().getUid());
                }
                if (addToMainList)
                    _embeddedHandler.getMainSyncHandler().onEmbeddedListRecordTransferEnd(oi.getOriginalOpInfo());
                //update the transferred info record since we dont alter the entries
                if (updateTransferredInfo)
                    _infoHandler.updateTranferredInfo(oi);
            } finally {
                if (lockAcquired) {
                    _readLock.unlock();
                }
            }
        }

        return true;// can remove from q
    }

    private boolean transferIfPossibleImpl(EmbeddedMultiUidsSyncOpInfo oi) {
        if (!oi.canTransferToMainList(_currentGenerationId)) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("[" + getEmbeddedHandler().getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment will not transfer " + oi.getOriginalOpInfo() + " current generation [" + _currentGenerationId + "]");
            }
            return false;
        }
        synchronized (oi) {
            //write to sync list
            boolean relevant = _embeddedHandler.getGensHandler().isRelevant(oi.getOriginalOpInfo().getGenerationId());
            boolean lockAcquired = false;
            try {
                // if old generation arrived from initial load don't persist it to sync list if after recovery handled the entry already
                if (relevant && oi.getOriginalOpInfo().getGenerationId() != _embeddedHandler.getMainSyncHandler().getCurrentGenerationId()) {
                    _readLock.lock();
                    lockAcquired = true;
                    if (_embeddedHandler.getMainSyncHandler().afterRecoveryStarted()) {
                        if (getLogger().isDebugEnabled()) {
                            getLogger().debug("[" + getEmbeddedHandler().getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment will not transfer " + oi.getOriginalOpInfo() +
                                    " current generation [" + _currentGenerationId + "] because afterRecovery already cleared it");
                        }
                        relevant = false;
                    }
                }

                boolean persist = relevant && !oi.isPersistedToMainList() && !oi.getOriginalOpInfo().isConfirmedByRemote(_embeddedHandler.getMainSyncHandler());
                boolean updateTransferredInfo = relevant && !oi.isPersistedToMainList() && oi.getOriginalOpInfo().getGenerationId() == _currentGenerationId;
                boolean addToMainList = persist && oi.getOriginalOpInfo().getGenerationId() == _currentGenerationId;

                if (!relevant || !addToMainList) {
                    if (getLogger().isDebugEnabled()) {
                        getLogger().debug("[" + getEmbeddedHandler().getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment will not transfer " + oi.getOriginalOpInfo() +
                                " current generation [" + _currentGenerationId + "], relevant? [" + relevant + "], should add to main sync list? [" + addToMainList + "]");
                    }
                }

                if (persist) {
                    _embeddedHandler.getMainSyncHandler().onEmbeddedListRecordTransferStart(oi.getOriginalOpInfo(), oi.getOriginalOpInfo().getGenerationId() != _currentGenerationId);
                    oi.setPersistedToMainList(); //written to list
                }

                if (oi.containsAnyPhantom()) {
                    Iterator<String> iter = oi.getPhantoms().iterator();

                    while (iter.hasNext()) {
                        String uid = iter.next();
                        _embeddedHandler.getPhantomsHandler().removePhantom(oi, uid);
                    }
                }
                if (addToMainList)
                    _embeddedHandler.getMainSyncHandler().onEmbeddedListRecordTransferEnd(oi.getOriginalOpInfo());
                //update the transferred info record since we dont alter the entries
                if (updateTransferredInfo)
                    _infoHandler.updateTranferredInfo(oi);
            } finally {
                if (lockAcquired) {
                    _readLock.unlock();
                }
            }
        }

        return true; //can remove from q

    }

    public int getSegmentNumber() {
        return _segmentNumber;
    }

    public EmbeddedSyncListTransferredInfoHandler getInfoHandlerOfSegment() {
        return _infoHandler;
    }

    public Iterator<IEmbeddedSyncOpInfo> getEmbeddedRecordsIterator() {
        return _inputRecords.iterator();
    }

    public void close() {
        if (_syncer != null) {
            _syncer.close();
        }
    }

    private class EmbeddedSyncer extends GSThread {
        private volatile boolean _closed;
        private final DirectPersistencySyncHandler _mainHandler;
        private final EmbeddedSyncSegment _segment;
        private boolean _allCurrentGensRecordsInSegment;

        /**
         * Timer daemon thread.
         *
         * @param threadName the name of this daemon thread.
         */
        public EmbeddedSyncer(String threadName, DirectPersistencySyncHandler handler, EmbeddedSyncSegment segment) {
            super(threadName);
            _mainHandler = handler;
            _segment = segment;
            this.setDaemon(true);
        }


        @Override
        public void run() {
            int synced = 0;
            try {
                while (!_closed) {
                    try {
                        long timeToSleep = size() < SHORT_SLEEP_QUEUE_SIZE ? EMBEDDED_SYNC_SLEEP_TIME : (EMBEDDED_SYNC_SLEEP_TIME / 10);
                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException ex) {
                            if (_mainHandler.getLogger().isDebugEnabled()) {
                                _mainHandler.getLogger().warn("[" + _mainHandler.getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment EmbeddedSyncer thread interrupted");
                            }
                            if (_closed)
                                break;
                            continue;
                        }
                        if (_closed)
                            break;
                        synced = handeRecordsRipeForTransfer();
                    } catch (Throwable ex) {
                        if (_mainHandler.getLogger().isErrorEnabled()) {
                            _mainHandler.getLogger().error("[" + _mainHandler.getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment EmbeddedSyncer thread got unexpected exception " + ex);
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

        private int handeRecordsRipeForTransfer() {
            int reaped = 0;
            //run over the records, every record ripe can be transferred
            if (!_allCurrentGensRecordsInSegment && !_inputRecords.isEmpty()) {
                IEmbeddedSyncOpInfo f = _inputRecords.peek();
                if (f.getOriginalOpInfo().getGenerationId() == _embeddedHandler.getMainSyncHandler().getCurrentGenerationId()) {
                    _allCurrentGensRecordsInSegment = true;
                    _infoHandler.removeOldGenerationsTransferredInfo();
                    _embeddedHandler.onAllCurrentGensRecordsInSegment(_segmentNumber);
                }
            }

            Iterator<IEmbeddedSyncOpInfo> iter = _inputRecords.iterator();
            int limit = size() + MIN_SIZE_TO_ITER;
            int scanned = 0;
            while (iter.hasNext()) {
                IEmbeddedSyncOpInfo oi = iter.next();
                scanned++;
                if (_segment.transferIfPossible(oi)) {
                    iter.remove();
                    reaped++;
                    _size.decrementAndGet();
                }
                if (scanned > limit)
                    break;  //dont allow endless scan
            }

            if (reaped > 0) {
                if (_mainHandler.getLogger().isDebugEnabled()) {
                    _mainHandler.getLogger().debug("[" + _mainHandler.getSpaceEngine().getFullSpaceName() + "]" + " EmbeddedSyncSegment:handeRecordsRipeForTransfer moved=" + reaped + " left=" + _segment.size());
                }
            }
            return reaped;
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
