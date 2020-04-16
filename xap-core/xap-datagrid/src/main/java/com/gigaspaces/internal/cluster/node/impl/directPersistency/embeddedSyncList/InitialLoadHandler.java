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

import com.gigaspaces.internal.cluster.node.impl.directPersistency.AbstractDirectPersistencyOpInfo;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.blobStore.BlobStoreEntryHolder;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The synchronizing direct-persistency  embedded initial load handler
 *
 * @author yechielf
 * @since 11.0
 */
//NOTE- this class is used befoe DirectPersistencySyncHandler::initialize() called
@com.gigaspaces.api.InternalApi
public class InitialLoadHandler {

    private final EmbeddedSyncHandler _embeddedHandler;
    //NOTE- currently initial load inserts into space are done by single thread but we use concurrent
    //for generality
    private final ConcurrentHashMap<InitialLoadBasicInfo, InitialLoadBasicInfo> _initialLoaded;

    public InitialLoadHandler(EmbeddedSyncHandler embeddedHandler) {
        _initialLoaded = new ConcurrentHashMap<InitialLoadBasicInfo, InitialLoadBasicInfo>();
        _embeddedHandler = embeddedHandler;
    }


    //called for every entry during initial load, return true if entry can be loaded
    //NOTE- this class is used befoe DirectPersistencySyncHandler::initialize() called
    public boolean onLoadingEntry(IEntryHolder eh) {
        if (!eh.isBlobStoreEntry())
            return true;

        BlobStoreEntryHolder oheh = (BlobStoreEntryHolder) eh;
        if (oheh.getEmbeddedSyncOpInfo() == null || oheh.getEmbeddedSyncOpInfo().getGenerationId() == 0) {
            return true;
        }
        long genId = oheh.getEmbeddedSyncOpInfo().getGenerationId();
        long seq = oheh.getEmbeddedSyncOpInfo().getSequenceId();
        boolean phantom = oheh.getEmbeddedSyncOpInfo().isPhantom();
        boolean multiuids = oheh.getEmbeddedSyncOpInfo().isPartOfMultipleUidsInfo();

        //still rellevant to embedded list?
        if (!phantom && !_embeddedHandler.getGensHandler().isRelevant(genId))
            return true;
        int segmentNum = (AbstractDirectPersistencyOpInfo.getSegmentNumber(seq)) % _embeddedHandler.getNumSegments();
        int seqInSegment = AbstractDirectPersistencyOpInfo.getOrderWithinSegment(seq);

        if (!phantom && _embeddedHandler.getSegment(segmentNum).getInfoHandlerOfSegment().isSeqNumberTransferredForSure(genId, seqInSegment))
            return true;
        if (multiuids)
            createMultiUidsInitialLoadInfo(eh, genId, seq, phantom);
        else
            createSingleUidInitialLoadInfo(eh, genId, seq, phantom);

        return !phantom;
    }

    private void createMultiUidsInitialLoadInfo(IEntryHolder eh, long genId, long seq, boolean phantom) {
        MultiUidsInitialLoadBasicInfo li = new MultiUidsInitialLoadBasicInfo(genId, seq, eh.getUID(), phantom);
        MultiUidsInitialLoadBasicInfo cur = null;
        cur = (MultiUidsInitialLoadBasicInfo) _initialLoaded.putIfAbsent(li, li);
        if (cur != null)
            cur.add(eh.getUID(), phantom);
    }

    private void createSingleUidInitialLoadInfo(IEntryHolder eh, long genId, long seq, boolean phantom) {
        SingleUidInitialLoadBasicInfo li = new SingleUidInitialLoadBasicInfo(genId, seq, eh.getUID(), phantom);
        InitialLoadBasicInfo cur = null;
        cur = _initialLoaded.putIfAbsent(li, li);
        if (cur != null) {
            if (getLogger().isWarnEnabled()) {
                getLogger().warn("[" + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " internal error- EmbeddedSyncInitialLoadHandler mixed single & multiuids gen=" + genId + " seq=" + seq);
            }
            throw new RuntimeException("space " + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "internal error- EmbeddedSyncInitialLoadHandler mixed single & multiuids gen=" + genId + " seq=" + seq + " segment=" + AbstractDirectPersistencyOpInfo.getSegmentNumber(seq) + " order=" + AbstractDirectPersistencyOpInfo.getOrderWithinSegment(seq));
        }
    }


    public void onInitialLoadEnd() {
        moveToEmbeddedList();
        _initialLoaded.clear();
    }

    //transfer from hashset to the queues creating propper records
    //NOTE- initialize not called yet for threads
    private void moveToEmbeddedList() {
        Iterator<InitialLoadBasicInfo> iter = _initialLoaded.values().iterator();
        int inEmbeddedListFromInitialLoad = 0;
        int numPhantomsInEmbeddedListFromInitialLoad = 0;
        while (iter.hasNext()) {
            InitialLoadBasicInfo il = iter.next();
            if (il.isSingleUid()) {
                moveToSegment((SingleUidInitialLoadBasicInfo) il);
                if (il.containsPhantom())
                    numPhantomsInEmbeddedListFromInitialLoad++;
            } else {
                moveToSegment((MultiUidsInitialLoadBasicInfo) il);
                if (il.containsPhantom())
                    numPhantomsInEmbeddedListFromInitialLoad += il.getPhantoms().size();
            }
            inEmbeddedListFromInitialLoad++;
        }
        if (getLogger().isInfoEnabled()) {
            getLogger().info("[" + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " loaded " + inEmbeddedListFromInitialLoad
                    + " entries to embedded sync list during initial load. Number of phantoms=" + numPhantomsInEmbeddedListFromInitialLoad);
        }
    }

    private void moveToSegment(SingleUidInitialLoadBasicInfo il) {
        _embeddedHandler.getMainSyncHandler().onEmbeddedOpFromInitialLoad(il.getUid(), il.getGenerationId(), il.getSeq(), il.containsPhantom());
    }

    private void moveToSegment(MultiUidsInitialLoadBasicInfo il) {
        _embeddedHandler.getMainSyncHandler().onEmbeddedOpFromInitialLoad(il.getUids(), il.getGenerationId(), il.getSeq(), il.getPhantoms());
    }

    private Logger getLogger() {
        return _embeddedHandler.getMainSyncHandler().getLogger();
    }

    private static abstract class InitialLoadBasicInfo {

        private final long _generationId;
        private final long _seq;

        public InitialLoadBasicInfo(long generationId, long seq) {
            _generationId = generationId;
            _seq = seq;
        }

        public long getGenerationId() {
            return _generationId;
        }

        public long getSeq() {
            return _seq;
        }

        public abstract boolean isSingleUid();

        public abstract boolean containsPhantom();

        public abstract Set<String> getPhantoms();

        public abstract String getUid();

        public abstract List<String> getUids();

        public abstract void add(String uid, boolean phantom);

        @Override
        public int hashCode() {
            return ((Long) (getSeq())).hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            InitialLoadBasicInfo other = (InitialLoadBasicInfo) o;
            return (other.getSeq() == getSeq() &&
                    other.getGenerationId() == getGenerationId());
        }

    }

    private static class SingleUidInitialLoadBasicInfo extends InitialLoadBasicInfo {
        private final String _uid;
        private final boolean _phantom;

        public SingleUidInitialLoadBasicInfo(long gen, long seq, String uid, boolean phantom) {
            super(gen, seq);
            _uid = uid;
            _phantom = phantom;
        }

        @Override
        public boolean isSingleUid() {
            return true;
        }

        @Override
        public boolean containsPhantom() {
            return _phantom;
        }

        @Override
        public Set<String> getPhantoms() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getUid() {
            return _uid;
        }

        @Override
        public List<String> getUids() {
            throw new UnsupportedOperationException();

        }

        public void add(String uid, boolean phantom) {
            throw new UnsupportedOperationException();

        }
    }

    private static class MultiUidsInitialLoadBasicInfo extends InitialLoadBasicInfo {
        private final List<String> _uids;
        private Set<String> _phantoms;

        public MultiUidsInitialLoadBasicInfo(long gen, long seq, String uid, boolean phantom) {
            super(gen, seq);
            _uids = new LinkedList<String>();
            _uids.add(uid);
            if (phantom) {
                _phantoms = new HashSet<String>();
                _phantoms.add(uid);
            }
        }

        @Override
        public boolean isSingleUid() {
            return false;
        }

        @Override
        public boolean containsPhantom() {
            return _phantoms != null && !_phantoms.isEmpty();
        }

        @Override
        public Set<String> getPhantoms() {
            return _phantoms;
        }

        @Override
        public String getUid() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> getUids() {
            return _uids;
        }

        public synchronized void add(String uid, boolean phantom) {
            _uids.add(uid);
            if (phantom) {
                if (_phantoms != null)
                    _phantoms = new HashSet<String>();
                _phantoms.add(uid);
            }

        }
    }

}
