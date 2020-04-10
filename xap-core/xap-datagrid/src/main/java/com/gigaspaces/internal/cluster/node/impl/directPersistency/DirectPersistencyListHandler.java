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

import com.gigaspaces.internal.cluster.node.impl.directPersistency.ioImpl.IDirectPersistencyIoHandler;
import com.gigaspaces.internal.server.space.SpaceEngine;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_DEFAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_PROP;

/**
 * The synchronizing direct-persistency list handler
 *
 * @author yechielf
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencyListHandler {

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_DIRECT_PERSISTENCY);
    private final static int NUM_MEM_SEGMENTS = 8;

    final int MAX_ENTRIES_IN_MEMORY;


    private final IDirectPersistencySyncHandler _handler;
    private IDirectPersistencyIoHandler _ioHandler;
    private final MemorySegment[] _memSegments;

    private final DirectPersistencyOverflowList _overFlow;

    DirectPersistencyListHandler(IDirectPersistencySyncHandler handler, SpaceEngine spaceEngine) {
        _handler = handler;
        _memSegments = new MemorySegment[NUM_MEM_SEGMENTS];
        for (int i = 0; i < NUM_MEM_SEGMENTS; i++)
            _memSegments[i] = new MemorySegment(this);

        MAX_ENTRIES_IN_MEMORY = spaceEngine.getConfigReader().getIntSpaceProperty(CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_PROP, String.valueOf(CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_DEFAULT));


        _overFlow = new DirectPersistencyOverflowList(_handler);

    }

    void setDirectPersistencyIoHandler(IDirectPersistencyIoHandler ioHandler) {
        _ioHandler = ioHandler;
    }

    public int getNumSegments() {
        return NUM_MEM_SEGMENTS;
    }


    DirectPersistencySingleUidOpInfo onDirectPersistencyOpStart(String uid) {
        int seg = getSegmentNumberForUid(uid);
        DirectPersistencySingleUidOpInfo e = new DirectPersistencySingleUidOpInfo(_handler.getCurrentGenerationId(), uid, seg, getNextSeqNumber(seg));
        if (!_handler.isEmbeddedListUsed())
            _ioHandler.insert(e);

        return e;
    }

    DirectPersistencyMultipleUidsOpInfo onDirectPersistencyOpStart(List<String> uids) {
        int seg = getSegmentNumberForUid(uids.get(0));
        DirectPersistencyMultipleUidsOpInfo e = new DirectPersistencyMultipleUidsOpInfo(_handler.getCurrentGenerationId(), uids, seg, getNextSeqNumber(seg));
        if (!_handler.isEmbeddedListUsed())
            _ioHandler.insert(e);

        return e;
    }

    DirectPersistencySingleUidOpInfo onEmbeddedOpFromInitialLoad(String uid, long gen, long seq) {
        DirectPersistencySingleUidOpInfo e = new DirectPersistencySingleUidOpInfo(gen, uid, seq);
        return e;
    }

    DirectPersistencyMultipleUidsOpInfo onEmbeddedOpFromInitialLoad(List<String> uids, long gen, long seq) {
        DirectPersistencyMultipleUidsOpInfo e = new DirectPersistencyMultipleUidsOpInfo(gen, uids, seq);
        return e;
    }


    private int getSegmentNumberForUid(String uid) {
        return (Math.abs(uid.hashCode())) % NUM_MEM_SEGMENTS;
    }

    int getNextSeqNumber(int segmentNumber) {
        return _memSegments[segmentNumber].getNextSeqNumber();
    }


    void onEmbeddedListRecordTransferStart(IDirectPersistencyOpInfo o, boolean onlyIfNotExists) {
        insertToPersistentSyncListFromEmbeddedList(o, onlyIfNotExists);
    }

    public void insertToPersistentSyncListFromEmbeddedList(IDirectPersistencyOpInfo o, boolean onlyIfNotExists) {
        if (onlyIfNotExists) {//check existance of the list record. can exist in case of initial load
            if (_ioHandler.get(o.getGenerationId(), o.getSequenceNumber()) != null) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("[" + _handler.getSpaceEngine().getFullSpaceName() + "] DirectPersistencyListHandler will not move " + o +
                            " from embedded sync list to persistent sync list because it is already there");
                }
                return;
            }
        }
        _ioHandler.insert(o);
    }


    void onEmbeddedListRecordTransferEnd(IDirectPersistencyOpInfo e) {
        addToSyncListSegment(e);
    }


    //is both redokey & persistency indicator set? if so move to resident list
    //NOTE- called when the record is locked
    public void checkDirectPersistencyOpCompleletion(IDirectPersistencyOpInfo e) {
        if (e.getGenerationId() != _handler.getCurrentGenerationId())
            return; //old gen (result of initial load) not to be inserted to main list

        if (!e.isInMainList() && e.hasRedoKey() && e.isPersisted()) {
            if (_handler.isEmbeddedListUsed()) {
                if (e.isMultiUids())
                    _handler.getEmbeddedSyncHandler().onEmbeddedSyncOp((DirectPersistencyMultipleUidsOpInfo) e);
                else
                    _handler.getEmbeddedSyncHandler().onEmbeddedSyncOp((DirectPersistencySingleUidOpInfo) e);
                return;
            }
            addToSyncListSegment(e);
        }
    }

    private void addToSyncListSegment(IDirectPersistencyOpInfo e) {
        e.setInMainList();
        // add op into sync memory segment only if it is NOT an old generation operation
        if (!_handler.isEmbeddedListUsed() || (e.getGenerationId() == _handler.getCurrentGenerationId())) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("[" + _handler.getSpaceEngine().getFullSpaceName() + "] DirectPersistencyListHandler adding to SyncList: uid= " + e.getUid() + " ,redokey= " + e.getRedoKey());
            }
            _memSegments[MemorySegment.getMemSegmentNumber(e)].add(e);
        }
    }


    int syncToRedologConfirmation(long confirmed) {
        int res = 0;

        //scan memory entries
        res = syncToRedologConfirmationMemory(confirmed);

        //if overflow created erase from memory up-to
        res += _overFlow.syncToRedologConfirmation(confirmed);

        return res;
    }

    //move to overflow if need to
    int moveTooverflowIfNeeded() {
        int ovf = 0;
        final int max_per_segment = MAX_ENTRIES_IN_MEMORY / NUM_MEM_SEGMENTS;
        for (int i = 0; i < NUM_MEM_SEGMENTS; i++)
            ovf += _memSegments[i].overflowIfNeeded(max_per_segment);

        return ovf;

    }


    boolean isOverflow() {
        return !_overFlow.isEmpty();
    }

    private int syncToRedologConfirmationMemory(long confirmed) {
        int removed = 0;
        for (int i = 0; i < NUM_MEM_SEGMENTS; i++)
            removed += _memSegments[i].syncToRedologConfirmationMemory(confirmed);

        return removed;
    }

    int getNumMemoryOps() {
        int memOps = 0;
        for (int i = 0; i < NUM_MEM_SEGMENTS; i++)
            memOps += _memSegments[i].getNumOps();

        return memOps;
    }


    Iterator<String> getRecoveryIterator() {
        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _handler.getSpaceEngine().getFullSpaceName() + "] DirectPersistencyListHandler getting entries to recover");
        }
        return new DirectPersistencyRecoveryIterator(this);
    }


    //TBD- currently done by iterator,  can we optimize?????
    void cleanPreviousGenerationsOps() {
        List<IDirectPersistencyOpInfo> toRemove = new LinkedList<IDirectPersistencyOpInfo>();

        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + _handler.getSpaceEngine().getFullSpaceName() + "] DirectPersistencyListHandler cleanPreviousGenerationsOps");
        }

        Iterator<IDirectPersistencyOpInfo> prevGenIter = _ioHandler.iterateOps(false/*currentGeneration*/);
        ;
        while (prevGenIter.hasNext()) {
            toRemove.add(prevGenIter.next());
            //    prevGenIter.remove();
            if (toRemove.size() >= IDirectPersistencyIoHandler.BULK_SIZE) {
                _ioHandler.removeOpsBulk(toRemove);
                toRemove.clear();
            }
        }
        if (!toRemove.isEmpty())
            _ioHandler.removeOpsBulk(toRemove);
    }

    public IDirectPersistencyIoHandler getIoHandler() {
        return _ioHandler;
    }


    void removeAllOverflow() {
        List<DirectPersistencyOverflowListSegment> toRemove = new LinkedList<DirectPersistencyOverflowListSegment>();
        Iterator<DirectPersistencyOverflowListSegment> iter = _ioHandler.iterateOverflow(false);
        while (iter.hasNext()) {
            toRemove.add(iter.next());
            if (toRemove.size() >= IDirectPersistencyIoHandler.BULK_SIZE) {
                _ioHandler.removeOvfBulk(toRemove);
                toRemove.clear();
            }
        }
        if (!toRemove.isEmpty())
            _ioHandler.removeOvfBulk(toRemove);


    }

    public IDirectPersistencySyncHandler getSyncHandler() {
        return _handler;
    }

    public static class DirectPersistencyCurrentGenerationMemoryIterator implements Iterator<IDirectPersistencyOpInfo> {

        private final DirectPersistencyListHandler _listHandler;
        private final Iterator<IDirectPersistencyOpInfo> _entriesMemoryIter;
        private boolean _closed;

        public DirectPersistencyCurrentGenerationMemoryIterator(DirectPersistencyListHandler listHandler, MemorySegment memSegment) {
            _listHandler = listHandler;
            _entriesMemoryIter = memSegment.getOps().iterator();
        }

        @Override
        public IDirectPersistencyOpInfo next() {
            return _entriesMemoryIter.next();
        }

        @Override
        public boolean hasNext() {
            if (_closed)
                return false;
            if (!_entriesMemoryIter.hasNext()) {
                _closed = true;
                return false;
            }
            return true;
        }

        @Override
        public void remove() {
            if (_closed)
                throw new UnsupportedOperationException("remove-irrelevant");

            _entriesMemoryIter.remove();
        }

        public void close() {
            _closed = true;
        }
    }

    public static class DirectPersistencyRecoveryIterator implements Iterator<String> {
        //returns uids for recovery
        private final HashSet<String> _usedUids;
        private Iterator<String> _multiUidsIter;
        private final DirectPersistencyListHandler _listHandler;
        private final Iterator<IDirectPersistencyOpInfo> _prevGenIter;
        private Iterator<String> _embeddedListForRecovery;
        private boolean _closed;
        private String _cur;

        public DirectPersistencyRecoveryIterator(DirectPersistencyListHandler listHandler) {
            _listHandler = listHandler;
            _usedUids = new HashSet<String>();
            _prevGenIter = _listHandler._ioHandler.iterateOps(false/*currentGeneration*/);
            _embeddedListForRecovery = _listHandler.getSyncHandler().isEmbeddedListUsed() ?
                    listHandler.getSyncHandler().getEmbeddedSyncHandler().getEmbeddedListForRecovery() : null;
        }

        @Override
        public String next() {
            if (_closed || _cur == null)
                throw new UnsupportedOperationException("next but iter terminated");
            String cur = _cur;
            _cur = null;
            return cur;

        }

        @Override
        public boolean hasNext() {
            _cur = null;
            String cur = null;
            while (true) {
                if (_closed)
                    return false;
                if (_embeddedListForRecovery != null) {
                    if (_embeddedListForRecovery.hasNext()) {
                        String nextEmbedded = _embeddedListForRecovery.next();
                        if (_usedUids.add(nextEmbedded)) {
                            _cur = nextEmbedded;
                            return true;
                        }
                        continue;
                    } else {
                        _embeddedListForRecovery = null;
                    }
                }
                if (_multiUidsIter != null) {
                    if (_multiUidsIter.hasNext()) {
                        cur = _multiUidsIter.next();
                        if (_usedUids.add(cur)) {
                            _cur = cur;
                            return true;
                        } else
                            continue;
                    } else {
                        _multiUidsIter = null;
                        continue;
                    }
                }
                if (!_prevGenIter.hasNext()) {
                    _closed = true;
                    return false;
                }
                IDirectPersistencyOpInfo e = _prevGenIter.next();
                if (!e.isMultiUids()) {
                    if (_usedUids.add(e.getUid())) {
                        _cur = e.getUid();
                        return true;
                    }
                    continue;
                } else {
                    _multiUidsIter = e.getUids().iterator();
                    continue;
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove-irrelevant");
        }

    }


    static class MemorySegment {
        private final ConcurrentLinkedDeque<IDirectPersistencyOpInfo> _ops;
        private final DirectPersistencyListHandler _listHandler;
        private final AtomicInteger _seq;

        public MemorySegment(DirectPersistencyListHandler listHandler) {
            _ops = new ConcurrentLinkedDeque();
            _listHandler = listHandler;
            _seq = new AtomicInteger(-1);
        }

        static int getMemSegmentNumber(IDirectPersistencyOpInfo e) {
            return Math.abs((int) (e.getSequenceNumber() % DirectPersistencyListHandler.NUM_MEM_SEGMENTS));

        }

        int getNextSeqNumber() {
            return _seq.incrementAndGet();
        }

        ConcurrentLinkedDeque<IDirectPersistencyOpInfo> getOps() {
            return _ops;
        }

        void add(IDirectPersistencyOpInfo e) {
            _ops.add(e);
        }

        int getNumOps() {
            return _ops.size();
        }

        int syncToRedologConfirmationMemory(long confirmed) {
            int removed = 0;
            if (_ops.isEmpty())
                return removed;
            IDirectPersistencyOpInfo peeked = _ops.peek();
            if (peeked.getRedoKey() > confirmed)
                return removed;

            DirectPersistencyCurrentGenerationMemoryIterator iter = new DirectPersistencyCurrentGenerationMemoryIterator(_listHandler, this);
            while (iter.hasNext()) {
                IDirectPersistencyOpInfo e = iter.next();
                if (e.getRedoKey() > confirmed)
                    break;
                //remove this record from disk and memory
                _listHandler.getIoHandler().remove(e);
                if (_logger.isDebugEnabled()) {
                    _logger.debug("[" + _listHandler._handler.getSpaceEngine().getFullSpaceName() + "] DirectPersistencyListHandler received redo log confirmation on uid= " + e.getUid() + " ,redokey= " + e.getRedoKey());
                }
                iter.remove();
                removed++;
            }
            return removed;
        }


        int overflowIfNeeded(int max_to_allow) {
            if (_ops.size() <= max_to_allow)
                return 0;
            return
                    _listHandler._overFlow.moveToOverflow(_ops.iterator(), (_ops.size() - max_to_allow));

        }


    }
}
