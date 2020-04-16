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

import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyMultipleUidsOpInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySingleUidOpInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin.EmbeddedRelevantGenerationIdsHandler;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The synchronizing direct-persistency  embedded list handler
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class EmbeddedSyncHandler {

    private final DirectPersistencySyncHandler _mainSyncHandler;
    private final EmbeddedSyncSegment[] _segments;
    private final EmbeddedRelevantGenerationIdsHandler _gensHandler;
    private final PhantomsHandler _phantomsHandler;
    private final InitialLoadHandler _initialLoadHandler;
    private final AtomicInteger _allCurrentGensRecordsInSegment;

    public EmbeddedSyncHandler(DirectPersistencySyncHandler mainSyncHandler) {
        _mainSyncHandler = mainSyncHandler;
        int numSegments = mainSyncHandler.getListHandler().getNumSegments();
        _allCurrentGensRecordsInSegment = new AtomicInteger();
        _segments = new EmbeddedSyncSegment[numSegments];
        for (int i = 0; i < numSegments; i++)
            _segments[i] = new EmbeddedSyncSegment(this, i);
        _gensHandler = new EmbeddedRelevantGenerationIdsHandler(this);
        _phantomsHandler = new PhantomsHandler(this);
        _initialLoadHandler = new InitialLoadHandler(this);
    }

    private Logger getLogger() {
        return _mainSyncHandler.getLogger();
    }

    public void afterInitializedBlobStoreIO() {
        _gensHandler.afterInitializedBlobStoreIO();
        for (EmbeddedSyncSegment seg : _segments)
            seg.afterInitializedBlobStoreIO();
    }

    public void initialize() {
        for (EmbeddedSyncSegment seg : _segments)
            seg.initialize();
        _gensHandler.initialize();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[" + _mainSyncHandler.getSpaceEngine().getFullSpaceName() + "]" + " Embedded sync list handler was initialized with " + getNumSegments() + " segments");
        }
    }


    public static java.io.Serializable getStorageKeyForPhantom(String uid) {
        return uid;
    }

    //create embedded info
    public void beforeEmbeddedSyncOp(DirectPersistencySingleUidOpInfo originalOpInfo, boolean phantom) {
        EmbeddedSingeUidSyncOpInfo oi = new EmbeddedSingeUidSyncOpInfo(originalOpInfo, phantom);
        originalOpInfo.setEmbeddedSyncOpInfo(oi);
    }

    //create embedded info
    public void beforeEmbeddedSyncOp(DirectPersistencyMultipleUidsOpInfo originalOpInfo, Set<String> phantoms) {
        EmbeddedMultiUidsSyncOpInfo oi = new EmbeddedMultiUidsSyncOpInfo(originalOpInfo, phantoms);
        originalOpInfo.setEmbeddedSyncOpInfo(oi);
    }


    //add to the handler, note- for xtn we add a record per uid
    public void onEmbeddedSyncOp(DirectPersistencySingleUidOpInfo originalOpInfo) {
        IEmbeddedSyncOpInfo oi = originalOpInfo.getEmbeddedSyncOpInfo();
        int segmentNumber = originalOpInfo.getSegmentNumber();
        synchronized (oi) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("[" + _mainSyncHandler.getSpaceEngine().getFullSpaceName() + "]" + " Adding " + originalOpInfo
                        + " to embedded sync list handler segment [" + segmentNumber + "] , phantom? [" + oi.containsPhantom(originalOpInfo.getUid()) + "]");
            }
            if (oi.containsPhantom(originalOpInfo.getUid()))
                _phantomsHandler.add(oi, originalOpInfo.getUid());
            _segments[segmentNumber].add(oi);
        }
    }

    public void onEmbeddedSyncOp(DirectPersistencyMultipleUidsOpInfo originalOpInfo) {
        int segmentNumber = originalOpInfo.getSegmentNumber();
        EmbeddedMultiUidsSyncOpInfo oi = (EmbeddedMultiUidsSyncOpInfo) originalOpInfo.getEmbeddedSyncOpInfo();
        synchronized (oi) {
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("[" + _mainSyncHandler.getSpaceEngine().getFullSpaceName() + "]" + " Adding " + originalOpInfo
                        + " to embedded sync list handler segment [" + segmentNumber + "] , has phantoms? [" + oi.containsAnyPhantom() + "]");
            }
            if (oi.containsAnyPhantom()) {
                synchronized (oi) {
                    for (String uid : oi.getPhantoms())
                        _phantomsHandler.add(oi, uid);
                }
            }
            _segments[segmentNumber].add(oi);
        }
    }

    public void onEmbeddedOpFromInitialLoad(DirectPersistencySingleUidOpInfo originalOpInfo, boolean phantom) {
        beforeEmbeddedSyncOp(originalOpInfo, phantom);
        originalOpInfo.setPersisted();
        onEmbeddedSyncOp(originalOpInfo);
    }

    public void onEmbeddedOpFromInitialLoad(DirectPersistencyMultipleUidsOpInfo originalOpInfo, Set<String> phantoms) {
        beforeEmbeddedSyncOp(originalOpInfo, phantoms);
        originalOpInfo.setPersisted();
        onEmbeddedSyncOp(originalOpInfo);
    }


    public DirectPersistencySyncHandler getMainSyncHandler() {
        return _mainSyncHandler;
    }

    public int getNumSegments() {
        return _segments.length;
    }

    public EmbeddedSyncSegment getSegment(int num) {
        return num < getNumSegments() ? _segments[num] : null;
    }

    public EmbeddedRelevantGenerationIdsHandler getGensHandler() {
        return _gensHandler;
    }

    public void onSpaceOpRemovePhantomIfExists(String uid) {
        _phantomsHandler.onSpaceOpRemovePhantomIfExists(uid);
    }

    public PhantomsHandler getPhantomsHandler() {
        return _phantomsHandler;
    }

    public InitialLoadHandler getInitialLoadHandler() {
        return _initialLoadHandler;
    }

    public void afterRecovery() {//remove old gens
        _gensHandler.removeOldGens();
    }


    void onAllCurrentGensRecordsInSegment(int segmentNumber) {
        if (_allCurrentGensRecordsInSegment.incrementAndGet() == getNumSegments())
            _gensHandler.removeOldGens();
    }

    //using the segments an iterator of the still not transferred records.
    //!!!!!!!!!!!!!!!!
    //NOTE
    //    when recovery iterator is called this iterator should be used first before the SSD sync-list iterator
    //    in order to prevent skipped records
    //!!!!!!!!!!!!!!!!
    public Iterator<String> getEmbeddedListForRecovery() {
        return new RecoveryEmbeddedIter(this);
    }

    public void close() {
        for (EmbeddedSyncSegment seg : _segments) {
            seg.close();
        }
    }


    private class RecoveryEmbeddedIter implements Iterator<String> {
        private final EmbeddedSyncHandler _sh;
        private int _curSeg;
        private Iterator<IEmbeddedSyncOpInfo> _segIter;
        private String _cur;
        private final HashSet<String> _usedUids;
        private Iterator<String> _multiUidsIter;

        RecoveryEmbeddedIter(EmbeddedSyncHandler sh) {
            _usedUids = new HashSet<String>();
            _sh = sh;
            _curSeg = -1;
        }

        @Override
        public boolean hasNext() {
            String cur = null;
            while (true) {
                _cur = null;
                if (_segIter == null && _curSeg >= _sh.getNumSegments())
                    return false;
                if (_segIter != null) {
                    if (_multiUidsIter != null) {
                        if (_multiUidsIter.hasNext()) {
                            cur = _multiUidsIter.next();
                            if (_usedUids.add(cur)) {
                                _cur = cur;
                                return true;
                            } else {
                                continue;
                            }
                        } else {
                            _multiUidsIter = null;
                            continue;
                        }
                    }
                    if (!_segIter.hasNext()) {
                        _segIter = null;
                        continue;
                    }
                    IEmbeddedSyncOpInfo ei = _segIter.next();
                    if (isValidForRecovery(ei)) {
                        if (!ei.isMultiUids()) {
                            if (_usedUids.add(ei.getOriginalOpInfo().getUid())) {
                                _cur = ei.getOriginalOpInfo().getUid();
                                return true;
                            }
                            continue;
                        } else {
                            _multiUidsIter = ei.getOriginalOpInfo().getUids().iterator();
                            continue;
                        }
                    } else
                        continue;
                }
                _curSeg++;
                if (_curSeg >= _sh.getNumSegments())
                    return false;
                _segIter = _sh.getSegment(_curSeg).getEmbeddedRecordsIterator();
            }
        }

        private boolean isValidForRecovery(IEmbeddedSyncOpInfo ei) {
            if (!_sh.getGensHandler().isRelevant(ei.getOriginalOpInfo().getGenerationId()))
                return false;  //generation is not relevant
            if (ei.getOriginalOpInfo().getGenerationId() == _sh.getMainSyncHandler().getCurrentGenerationId())
                return false;  //current gen irrelevant for recovery unless i was backup & switched to primary
            return true;
        }

        @Override
        public String next() {
            return _cur;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
