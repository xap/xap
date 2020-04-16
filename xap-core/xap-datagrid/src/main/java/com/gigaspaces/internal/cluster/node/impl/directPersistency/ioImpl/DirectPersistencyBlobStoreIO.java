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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.ioImpl;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyOverflowListSegment;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencyOpInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.admin.DirectPersistencySyncAdminInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.EmbeddedSyncHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin.EmbeddedRelevantGenerationIdsInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList.embeddedAdmin.EmbeddedSyncTransferredInfo;
import com.gigaspaces.server.blobstore.BlobStoreBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.blobstore.BlobStoreGetBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.gigaspaces.server.blobstore.BlobStoreRemoveBulkOperationRequest;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.blobStore.BlobStoreEntryLayout;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The actual i/o of DirectPersistencyOpInfo objects
 *
 * @author yechielf
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencyBlobStoreIO implements IDirectPersistencyIoHandler {

    private final CacheManager _cacheManager;
    private final Logger _logger;
    private final long _currentGenerationId;

    public DirectPersistencyBlobStoreIO(CacheManager cacheManager, Logger logger, long currentGenerationId) {
        _cacheManager = cacheManager;
        _logger = logger;
        _currentGenerationId = currentGenerationId;
    }


    private static String getOpInfoStorageKey(long generationId, long seq) {
        StringBuffer sb = new StringBuffer();
        sb.append(generationId).append("_").append(seq);
        return sb.toString();

    }

    public static String getOverflowSegmentStorageKey(long gen, long seq) {
        StringBuffer sb = new StringBuffer();
        sb.append("OVF_").append(gen).append("_").append(seq);
        return sb.toString();
    }

    @Override
    public void insert(IDirectPersistencyOpInfo entry) {
        try {
            _cacheManager.getBlobStoreStorageHandler().add(getOpInfoStorageKey(entry.getGenerationId(), entry.getSequenceNumber()), entry, BlobStoreObjectType.SYNC, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::add got exception: " + ex);
            throw ex;
        }

    }

    @Override
    public void remove(IDirectPersistencyOpInfo entry) {
        try {
            _cacheManager.getBlobStoreStorageHandler().remove(getOpInfoStorageKey(entry.getGenerationId(), entry.getSequenceNumber()), null, BlobStoreObjectType.SYNC, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::remove got exception: " + ex);
            throw ex;
        }

    }

    @Override
    public void update(IDirectPersistencyOpInfo entry) {
        try {
            _cacheManager.getBlobStoreStorageHandler().replace(getOpInfoStorageKey(entry.getGenerationId(), entry.getSequenceNumber()), entry, null, BlobStoreObjectType.SYNC, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::replace got exception: " + ex);
            throw ex;
        }
    }

    @Override
    public IDirectPersistencyOpInfo get(long getGenerationId, long sequenceNumber) {
        try {
            return (IDirectPersistencyOpInfo) _cacheManager.getBlobStoreStorageHandler().get( getOpInfoStorageKey(getGenerationId, sequenceNumber), null, BlobStoreObjectType.SYNC, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::get exception: " + ex);
            throw ex;
        }


    }

    @Override
    public Iterator<IDirectPersistencyOpInfo> iterateOps(boolean currentGeneration) {
        return new
                DirectPersistencyOpIterBlobStoreIO(this, _cacheManager, _logger, currentGeneration);
    }

    @Override
    public void removeOpsBulk(List<IDirectPersistencyOpInfo> ops) {
        List<BlobStoreBulkOperationRequest> blk = new LinkedList<BlobStoreBulkOperationRequest>();
        for (IDirectPersistencyOpInfo op : ops)
            blk.add(new BlobStoreRemoveBulkOperationRequest(getOpInfoStorageKey(op.getGenerationId(), op.getSequenceNumber()), null, null));
        _cacheManager.getBlobStoreStorageHandler().executeBulk(blk, BlobStoreObjectType.SYNC, false);
    }


    @Override
    public void insert(DirectPersistencyOverflowListSegment segment) {
        try {
            _cacheManager.getBlobStoreStorageHandler().add(getOverflowSegmentStorageKey(segment.getGenerationId(), segment.getSequenceNumber()), segment, BlobStoreObjectType.SYNC_OVERFLOW, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::add-OVF got exception: " + ex);
            throw ex;
        }
    }

    @Override
    public void remove(DirectPersistencyOverflowListSegment segment) {
        try {
            _cacheManager.getBlobStoreStorageHandler().remove(getOverflowSegmentStorageKey(segment.getGenerationId(), segment.getSequenceNumber()), null, BlobStoreObjectType.SYNC_OVERFLOW, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::remove-OVF got exception: " + ex);
            throw ex;
        }
    }

    @Override
    public void update(DirectPersistencyOverflowListSegment segment) {
        try {
            _cacheManager.getBlobStoreStorageHandler().replace(getOverflowSegmentStorageKey(segment.getGenerationId(), segment.getSequenceNumber()), segment, null, BlobStoreObjectType.SYNC_OVERFLOW, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::replace_OVF got exception: " + ex);
            throw ex;
        }
    }

    @Override
    public DirectPersistencyOverflowListSegment getOverflowSegment(long generationId, long seq) {
        try {
            return (DirectPersistencyOverflowListSegment) _cacheManager.getBlobStoreStorageHandler().get(getOverflowSegmentStorageKey(generationId, seq), null, BlobStoreObjectType.SYNC_OVERFLOW, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::getOverflowSegment exception: " + ex);
            throw ex;
        }
    }

    @Override
    public Iterator<DirectPersistencyOverflowListSegment> iterateOverflow(boolean currentGeneration) {
        return new
                DirectPersistencyOverflowIterBlobStoreIO(this, _cacheManager, _logger, currentGeneration);
    }


    @Override
    public void removeOvfBulk(List<DirectPersistencyOverflowListSegment> ovfs) {
        List<BlobStoreBulkOperationRequest> blk = new LinkedList<BlobStoreBulkOperationRequest>();
        for (DirectPersistencyOverflowListSegment segment : ovfs)
            blk.add(new BlobStoreRemoveBulkOperationRequest(getOverflowSegmentStorageKey(segment.getGenerationId(), segment.getSequenceNumber()), null, null));
        _cacheManager.getBlobStoreStorageHandler().executeBulk(blk, BlobStoreObjectType.SYNC_OVERFLOW, false);
    }


    //admin
    @Override
    public void insert(DirectPersistencySyncAdminInfo ai) {
        try {
            _cacheManager.getBlobStoreStorageHandler().add(DirectPersistencySyncAdminInfo.getStorageKey(), ai, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::add-syncadmin got exception: " + ex);
            throw ex;
        }

    }

    @Override
    public void update(DirectPersistencySyncAdminInfo ai) {
        try {
            _cacheManager.getBlobStoreStorageHandler().replace(DirectPersistencySyncAdminInfo.getStorageKey(), ai, null, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::replace_syncadmin got exception: " + ex);
            throw ex;
        }

    }

    @Override
    public DirectPersistencySyncAdminInfo getSyncAdminIfExists() {
        try {
            return (DirectPersistencySyncAdminInfo) _cacheManager.getBlobStoreStorageHandler().get(DirectPersistencySyncAdminInfo.getStorageKey(), null, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::getsyncadmin exception: " + ex);
            throw ex;
        }

    }

    //+++++++++++++++++++++++++++++++++  EMBEDDED SYNC RELATED +++++++++++++++++++++++++++++++++++++++++++++//

    @Override
    public void removePhantom(String uid, boolean checkExistance, long generationId, long seq) {
        try {
            if (checkExistance) {
                BlobStoreEntryLayout el = (BlobStoreEntryLayout) _cacheManager.getBlobStoreStorageHandler().get(EmbeddedSyncHandler.getStorageKeyForPhantom(uid), null, BlobStoreObjectType.DATA, null);
                if (el == null)
                    return; //nothing in space
//TBD check if phantom & if seq && gen match

            }
            _cacheManager.getBlobStoreStorageHandler().remove(EmbeddedSyncHandler.getStorageKeyForPhantom(uid), null, BlobStoreObjectType.DATA, null);

        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::removePhantom exception: " + ex);
            throw ex;
        }

    }

    private EmbeddedSyncTransferredInfo getEmbeddedTransferredInfo(long generationId, int segment) {
        try {
            return (EmbeddedSyncTransferredInfo) _cacheManager.getBlobStoreStorageHandler().get(EmbeddedSyncTransferredInfo.getStorageKey(generationId, segment), null, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::EmbeddedSyncTransferredInfo exception: " + ex);
            throw ex;
        }
    }

    @Override
    public void insert(EmbeddedSyncTransferredInfo ai) {
        try {
            _cacheManager.getBlobStoreStorageHandler().add(EmbeddedSyncTransferredInfo.getStorageKey(ai.getGenerationId(), ai.getSegmentNum()), ai, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::add-EmbeddedSyncTransferredInfo got exception: " + ex);
            throw ex;
        }

    }

    @Override
    public void update(EmbeddedSyncTransferredInfo ai) {
        try {
            _cacheManager.getBlobStoreStorageHandler().replace(EmbeddedSyncTransferredInfo.getStorageKey(ai.getGenerationId(), ai.getSegmentNum()), ai, null, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::replace_EmbeddedSyncTransferredInfo got exception: " + ex);
            throw ex;
        }

    }


    @Override
    public void remove(EmbeddedSyncTransferredInfo ai) {
        try {
            _cacheManager.getBlobStoreStorageHandler().remove(EmbeddedSyncTransferredInfo.getStorageKey(ai.getGenerationId(), ai.getSegmentNum()), null, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::remove_EmbeddedSyncTransferredInfo got exception: " + ex);
            throw ex;
        }

    }


    @Override
    public EmbeddedRelevantGenerationIdsInfo getEmbeddedRelevantGenerationIdsInfo() {
        try {
            return (EmbeddedRelevantGenerationIdsInfo) _cacheManager.getBlobStoreStorageHandler().get( EmbeddedRelevantGenerationIdsInfo.getStorageKey(), null, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::getEmbeddedRelevantGenerationIdsInfo exception: " + ex);
            throw ex;
        }

    }

    @Override
    public void update(EmbeddedRelevantGenerationIdsInfo ai) {
        try {
            _cacheManager.getBlobStoreStorageHandler().replace(EmbeddedRelevantGenerationIdsInfo.getStorageKey(), ai, null, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::replace EmbeddedRelevantGenerationIdsInfo got exception: " + ex);
            throw ex;
        }

    }

    @Override
    public void insert(EmbeddedRelevantGenerationIdsInfo ai) {
        try {
            _cacheManager.getBlobStoreStorageHandler().add(EmbeddedRelevantGenerationIdsInfo.getStorageKey(), ai, BlobStoreObjectType.ADMIN, null);
        } catch (BlobStoreException ex) {
            _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::add-EmbeddedRelevantGenerationIdsInfo got exception: " + ex);
            throw ex;
        }

    }


    @Override
    public List<EmbeddedSyncTransferredInfo> getAllTransferredInfo(EmbeddedSyncHandler h, Collection<Long> generations) {
        List<EmbeddedSyncTransferredInfo> res = new LinkedList<EmbeddedSyncTransferredInfo>();
        for (long gen : generations) {
            if (gen == h.getMainSyncHandler().getCurrentGenerationId())
                continue;
            for (int s = 0; s < h.getNumSegments(); s++) {
                EmbeddedSyncTransferredInfo oi = getEmbeddedTransferredInfo(gen, s);
                if (oi != null)
                    res.add(oi);
            }
        }
        return res;
    }


    public static class DirectPersistencyOpIterBlobStoreIO implements Iterator<IDirectPersistencyOpInfo> {

        private final CacheManager _cacheManager;
        private final Logger _logger;
        private boolean _closed;
        private final DataIterator<BlobStoreGetBulkOperationResult> _iter;
        private IDirectPersistencyOpInfo _cur;
        private final DirectPersistencyBlobStoreIO _ioMgr;
        private final boolean _isCurrentGeneration;

        public DirectPersistencyOpIterBlobStoreIO(DirectPersistencyBlobStoreIO ioMgr, CacheManager cacheManager, Logger logger, boolean isCurrentGeneration) {
            _cacheManager = cacheManager;
            _logger = logger;
            _ioMgr = ioMgr;
            _isCurrentGeneration = isCurrentGeneration;
            try {
                _iter = _cacheManager.getBlobStoreStorageHandler().iterator(BlobStoreObjectType.SYNC);
            } catch (BlobStoreException ex) {
                _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::create-iterator got exception: " + ex);
                _closed = true;
                throw ex;
            }

        }

        @Override
        public boolean hasNext() {
            try {
                _cur = null;
                if (_closed)
                    return false;
                IDirectPersistencyOpInfo cur = null;
                BlobStoreGetBulkOperationResult curSync = null;

                while (true) {
                    if (!_iter.hasNext()) {
                        close();
                        return false;
                    }
                    curSync = nextSync();
                    if (curSync != null) {
                        cur = getCur(curSync);
                    } else {
                        close();
                        return false;
                    }
                    boolean curGen = (cur.getGenerationId() == _ioMgr._currentGenerationId);

                    if (curGen == _isCurrentGeneration) {
                        _cur = cur;
                        return true;
                    }
                }
            } catch (BlobStoreException ex) {
                _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::iterator-hasnext got exception: " + ex);
                _closed = true;
                throw ex;
            }
        }

        private void close() {
            if (_closed)
                return;
            _iter.close();
            _closed = true;
        }

        private BlobStoreGetBulkOperationResult nextSync() {
            try {
                return _iter.next();
            } catch (BlobStoreException ex) {
                _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::iterator-next got exception: " + ex);
                _closed = true;
                throw ex;
            }
        }


        private IDirectPersistencyOpInfo getCur(BlobStoreGetBulkOperationResult current) {
            return (IDirectPersistencyOpInfo) current.getData();
        }


        @Override
        public IDirectPersistencyOpInfo next() {
            if (_closed)
                return null;
            return _cur;
        }

        @Override
        public void remove() {
            if (_closed || _cur == null)
                throw new UnsupportedOperationException("remove- iter inactive");
            _ioMgr.remove(_cur);
        }
    }


    public static class DirectPersistencyOverflowIterBlobStoreIO implements Iterator<DirectPersistencyOverflowListSegment> {

        private final CacheManager _cacheManager;
        private final Logger _logger;
        private boolean _closed;
        private final DataIterator<BlobStoreGetBulkOperationResult> _iter;
        private DirectPersistencyOverflowListSegment _cur;
        private final DirectPersistencyBlobStoreIO _ioMgr;
        private final boolean _isCurrentGeneration;

        public DirectPersistencyOverflowIterBlobStoreIO(DirectPersistencyBlobStoreIO ioMgr, CacheManager cacheManager, Logger logger, boolean isCurrentGeneration) {
            _cacheManager = cacheManager;
            _logger = logger;
            _ioMgr = ioMgr;
            _isCurrentGeneration = isCurrentGeneration;
            try {
                _iter = _cacheManager.getBlobStoreStorageHandler().iterator(BlobStoreObjectType.SYNC_OVERFLOW);
            } catch (BlobStoreException ex) {
                _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::create-iterator-OVERFLOW got exception: " + ex);
                _closed = true;
                throw ex;
            }

        }

        @Override
        public boolean hasNext() {
            try {
                _cur = null;
                if (_closed)
                    return false;
                while (true) {
                    if (!_iter.hasNext()) {
                        _iter.close();
                        _closed = true;
                        return false;
                    }
                    getCur();
                    if (_cur != null)
                        return true;
                }
            } catch (BlobStoreException ex) {
                _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::iterator-hasnext got exception: " + ex);
                _closed = true;
                throw ex;
            }
        }


        private void getCur() {
            try {
                DirectPersistencyOverflowListSegment cur = (DirectPersistencyOverflowListSegment) _iter.next().getData();
                boolean curGen = (cur.getGenerationId() == _ioMgr._currentGenerationId);

                if (curGen == _isCurrentGeneration)
                    _cur = cur;
                return;
            } catch (BlobStoreException ex) {
                _logger.error("space " + _cacheManager.getEngine().getFullSpaceName() + " DirectPersistencyBlobStoreIO::iteratorOVF-next got exception: " + ex);
                _closed = true;
                throw ex;
            }

        }


        @Override
        public DirectPersistencyOverflowListSegment next() {
            if (_closed)
                return null;
            return _cur;
        }

        @Override
        public void remove() {
            if (_closed || _cur == null)
                throw new UnsupportedOperationException("remove- iter inactive");
            _ioMgr.remove(_cur);
        }
    }

}
