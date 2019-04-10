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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.replica.data.AbstractEntryReplicaData;
import com.gigaspaces.internal.cluster.node.impl.replica.data.DirectPersistencyEntryReplicaData;
import com.gigaspaces.internal.cluster.node.impl.replica.data.EntryCopyReplicaData;
import com.gigaspaces.internal.cluster.node.impl.replica.data.EntrySynchronizeReplicaData;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.MatchResult;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateHolderFactory;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.TransactionConflictException;
import com.j_spaces.core.XtnEntry;
import com.j_spaces.core.XtnStatus;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.locks.ILockObject;

import net.jini.space.InternalSpaceException;

import java.util.logging.Logger;


@com.gigaspaces.api.InternalApi
public class EntryReplicaProducer
        implements ISingleStageReplicaDataProducer<AbstractEntryReplicaData> {
    protected final static Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_REPLICA);

    /**
     * keeps already recovered UIDs
     */
    protected final SpaceEngine _engine;
    private final boolean _isFullReplication;
    private final Context _context;

    private final ISAdapterIterator<IEntryHolder> _entriesIterSA;
    private final ITemplateHolder _templateHolder;
    private final SpaceCopyReplicaParameters _parameters;
    private final Object _requestContext;

    private int _generatedDataCount;
    private boolean _isClosed;
    private boolean _forcedClose;

    public EntryReplicaProducer(SpaceEngine engine,
                                SpaceCopyReplicaParameters parameters,
                                ITemplatePacket templatePacket, Object requestContext) {
        _engine = engine;
        _parameters = parameters;
        _requestContext = requestContext;
        ReplicationPolicy replicationPolicy = _engine.getClusterPolicy() == null ? null
                : _engine.getClusterPolicy()
                .getReplicationPolicy();
        _isFullReplication = replicationPolicy == null
                || replicationPolicy.isFullReplication();

        _context = _engine.getCacheManager().getCacheContext();
        _context.setInMemoryRecovery(true);

        if (templatePacket == null) {
            templatePacket = new TemplatePacket();
            templatePacket.setFieldsValues(new Object[0]);
        }

        try {
            IServerTypeDesc typeDesc = _engine.getTypeManager()
                    .loadServerTypeDesc(templatePacket);

            _templateHolder = TemplateHolderFactory.createTemplateHolder(typeDesc,
                    templatePacket,
                    _engine.generateUid(),
                    Long.MAX_VALUE /*
                                                                                         * expiration
                                                                                         * time
                                                                                         */);

            // iterator for entries
            _entriesIterSA = _engine.getCacheManager()
                    .makeEntriesIter(_context,
                            _templateHolder,
                            typeDesc,
                            0,
                            SystemTime.timeMillis(),
                            parameters.isMemoryOnly(),
                            parameters.isTransient());
        } catch (Exception ex) {
            throw new ReplicationInternalSpaceException("", ex);
        }

    }

    public Object getRequestContext() {
        return _requestContext;
    }

    /**
     * Creates the next entry that will be copied to the source space
     *
     * @return the next data to replicate or null if there is not more data
     */
    public synchronized AbstractEntryReplicaData produceNextData(
            ISynchronizationCallback syncCallback) {
        if (isForcedClose()) {
            throw new RuntimeException("space=" + _engine.getFullSpaceName() + " replica forced closing");
        }
        if (_isClosed)
            return null;

        try {
            while (true) {
                if (isForcedClose()) {
                    this.notifyAll(); //i am done
                    throw new RuntimeException("space=" + _engine.getFullSpaceName() + " replica forced closing");
                }
                IEntryHolder entry = _entriesIterSA.next();
                // no more entries
                AbstractEntryReplicaData replicaData = produceDataFromEntry(syncCallback, entry);
                if (replicaData == null && !_isClosed) {
                    continue;
                }
                if (replicaData == null && _isClosed) {
                    return null;
                }
                return replicaData;
            }
        } catch (Exception ex) {
            throw new ReplicationInternalSpaceException("Failure in .", ex);
        }
    }

    protected AbstractEntryReplicaData produceDataFromEntry(ISynchronizationCallback syncCallback, IEntryHolder entry) {
        if (entry == null) {
            // Any consecutive call should return null, so we close this
            // producer.
            close(false /*forced*/);
            return null;
        }

        ITypeDesc typeDesc = _engine.getTypeManager()
                .getTypeDesc(entry.getClassName());
        if (!isRelevant(entry, typeDesc))
            return null;

        AbstractEntryReplicaData replicaData = buildEntryReplicaData(entry,
                syncCallback);

        // for some reason no data was created - go to another entry
        if (replicaData == null)
            return null;

        increaseGeneratedDataCount();
        return replicaData;
    }

    private boolean isRelevant(IEntryHolder entry, ITypeDesc typeDesc) {
        if (!_isFullReplication && !typeDesc.isReplicable())
            return false; // non replicable entry - ignore

        return true;
    }


    public synchronized CloseStatus close(boolean forced) {
        if (_isClosed)
            return CloseStatus.CLOSED;

        _isClosed = true;
        _forcedClose = forced;
        try {
            if (forced) {//wait for closing of the producer after marking
                wait(ISingleStageReplicaDataProducer.FORCED_CLOSE_WAIT_TIME);
            }
        } catch (Exception ex) {
        }

        try {
            if (_entriesIterSA != null)
                _entriesIterSA.close();

        } catch (SAException e) {
            throw new ReplicationInternalSpaceException("Failed to close entries iterator.",
                    e);
        } finally {
            _engine.getCacheManager().freeCacheContext(_context);
        }
        return CloseStatus.CLOSED;
    }

    /**
     * Creates the replica data from the entry holder
     *
     * @return The constructed recovery entry. Assume IEntryHolder under lock.
     */
    private AbstractEntryReplicaData buildEntryReplicaData(IEntryHolder entry,
                                                           ISynchronizationCallback syncCallback) {

        while (entry != null) {
            XtnEntry wlXtn = entry.getWriteLockOwner();
            boolean locked = acquireTransactionLock(entry, wlXtn);

            ILockObject entryLock = _engine.getCacheManager()
                    .getLockManager()
                    .getLockObject(entry);
            try {
                synchronized (entryLock) {
                    IEntryPacket entryPacket = getEntryPacketSafely(entry, wlXtn, locked);
                    if (entryPacket == null)
                        return null;

                    AbstractEntryReplicaData data = newEntryReplicaData(entryPacket);
                    boolean duplicateUid = syncCallback.synchronizationDataGenerated(data);

                    if (duplicateUid)
                        return null;

                    return data;
                }
            } catch (TransactionConflictException e) {
                entry = handleTransactionConflict(entry);
            } finally {
                _engine.getCacheManager()
                        .getLockManager()
                        .freeLockObject(entryLock);
                releaseTransactionLock(wlXtn, locked);
            }
        }

        return null;
    }

    private AbstractEntryReplicaData newEntryReplicaData(
            IEntryPacket entryPacket) {
        switch (_parameters.getReplicaType()) {
            case SYNCRONIZE:
                IMarker marker = null;
                if (_parameters.isIncludeEvictionReplicationMarkers()
                        && _engine.getCacheManager().requiresEvictionReplicationProtection())
                    marker = _engine.getCacheManager()
                            .getEvictionReplicationsMarkersRepository()
                            .getMarker(entryPacket.getUID());

                return getParameters().getSynchronizationListFetcher() != null ? new DirectPersistencyEntryReplicaData(entryPacket)
                        : new EntrySynchronizeReplicaData(entryPacket, marker);
            case COPY:
                return new EntryCopyReplicaData(entryPacket);
            default:
                throw new IllegalArgumentException("Illegal ReplicaType "
                        + _parameters.getReplicaType());
        }

    }

    private IEntryHolder handleTransactionConflict(IEntryHolder entry) {
        if (!_engine.getCacheManager().isMemorySpace()
                && _engine.getCacheManager().isEvictableCachePolicy()) {
            try {
                entry = _engine.getCacheManager()
                        .getEntry(_context,
                                entry,
                                false /* tryInsertToCache */,
                                false /* lockeEntry */);

            } catch (SAException ex) {
                throw new InternalSpaceException("Recovery operation failed. Reason: Failed to getEntry from the CacheManager of "
                        + _engine.getFullSpaceName(),
                        ex);
            }
        }
        return entry;
    }

    private void releaseTransactionLock(XtnEntry wlXtn, boolean locked) {
        if (locked)
            wlXtn.unlock();
    }

    private boolean acquireTransactionLock(IEntryHolder entry, XtnEntry wlXtn) {
        boolean locked = wlXtn != null;
        if (locked)
            wlXtn.lock();
        return locked;
    }

    /**
     * Get the EntryHolder while holding the necessary locks, entry lock is not released - will be
     * on call to releaseLock method
     */
    private IEntryPacket getEntryPacketSafely(IEntryHolder entry,
                                              XtnEntry wlXtn, boolean xtnsLocked)
            throws TransactionConflictException {

        IEntryHolder original = entry;
        if (_engine.getCacheManager().needReReadAfterEntryLock()) {
            try {
                entry = _engine.getCacheManager()
                        .getEntry(_context,
                                entry,
                                false /* tryInsertToCache */,
                                true /* lockeEntry */);
            } catch (SAException ex) {
                throw new InternalSpaceException("Recovery operation failed. Reason: Failed to getEntry from the CacheManager of "
                        + _engine.getFullSpaceName(),
                        ex);
            }
        }

        // grab the xtn entry from table
        if (entry == null || entry.isDeleted() || (entry.isBlobStoreEntry() && !entry.isSameEntryInstance(original)))
            return null;

        // Rematch after we acquire the lock since the entry could have changed
        // till now
        MatchResult matchResult = _templateHolder.match(_engine.getCacheManager(),
                entry,
                -1,
                null,
                true,
                _context,
                _engine.getTemplateScanner()
                        .getRegexCache());
        if (matchResult == MatchResult.NONE)
            return null;

        IEntryHolder entryToUse = entry;

        if (wlXtn != entry.getWriteLockOwner()) {
            if (entry.getWriteLockOwner() == null)
                wlXtn = null;
            else
                throw new TransactionConflictException(null, null);
        }

        if (wlXtn != null) {
            XtnEntry xtnEntry = wlXtn;
            switch (entry.getWriteLockOperation()) {
                case SpaceOperations.TAKE:
                case SpaceOperations.TAKE_IE:
                    if (isConsideredInBacklog(xtnEntry))
                        return null;
                    break;
                case SpaceOperations.UPDATE:
                    if (entry.hasShadow(true /* safeEntry */)) {
                        if (isConsideredInBacklog(xtnEntry)) {
                            if (matchResult == MatchResult.SHADOW)
                                return null;
                        } else {
                            if (matchResult == MatchResult.MASTER)
                                return null;

                            entryToUse = entry.getShadow();
                        }
                    }
                    break;
                case SpaceOperations.WRITE: {
                    if (xtnEntry.getStatus() == XtnStatus.ROLLED
                            || xtnEntry.getStatus() == XtnStatus.ROLLING
                            || xtnEntry.getStatus() == XtnStatus.BEGUN)
                        return null;
                }
            }
        }

        if (entry.isExpired()
                && (!entry.isEntryUnderWriteLockXtn() || !_engine.getLeaseManager()
                .isNoReapUnderXtnLeases()))
            return null;

        final IEntryPacket entryPacket = buildEntryPacket(entryToUse);
        if (_templateHolder.getProjectionTemplate() != null)
            _templateHolder.getProjectionTemplate().filterOutNonProjectionProperties(entryPacket);
        return entryPacket;
    }

    private boolean isConsideredInBacklog(XtnEntry xtnEntry) {
        //A transaction is considered in backlog during synchronization
        //if it is already committed or if it is a single participant then after prepared
        //As a multi participant transaction at prepared state will be filtered. 
        return xtnEntry.getStatus() == XtnStatus.COMMITED
                || xtnEntry.getStatus() == XtnStatus.COMMITING
                || (xtnEntry.getStatus() == XtnStatus.PREPARED && xtnEntry.m_SingleParticipant);
    }

    private IEntryPacket buildEntryPacket(IEntryHolder eh) {
        long exp = eh.getEntryData().getExpirationTime();
        if (eh.hasShadow(true /* safeEntry */))
            exp = Math.max(exp, eh.getShadow().getExpirationTime());
        long ttl = getTimeTolive(exp);
        if (ttl <= 0)
            return null;

        IEntryPacket entryPacket = EntryPacketFactory.createFullPacketForReplication(eh, null, eh.getUID(), ttl);
        entryPacket.setSerializeTypeDesc(true);
        return entryPacket;
    }

    /**
     * @return timeToLeave of IEntryHolder
     */
    private long getTimeTolive(long ttl) {
        return ttl == Long.MAX_VALUE ? ttl : ttl - SystemTime.timeMillis();
    }

    public IReplicationFilterEntry toFilterEntry(AbstractEntryReplicaData data) {
        return data.toFilterEntry(_engine.getTypeManager());
    }

    private String getLogPrefix() {
        return _engine.getReplicationNode() + "context [" + _requestContext
                + "] ";
    }

    public SpaceEngine getEngine() {
        return _engine;
    }

    public SpaceCopyReplicaParameters getParameters() {
        return _parameters;
    }

    public boolean isClosed() {
        return _isClosed;
    }

    public boolean isForcedClose() {
        return isClosed() && _forcedClose;
    }

    public void increaseGeneratedDataCount() {
        this._generatedDataCount++;
    }

    public String dumpState() {
        return "Entries replica producer: completed [" + _isClosed
                + "] generated data count [" + _generatedDataCount + "]";
    }

    @Override
    public String getName() {
        return "EntryReplicaProducer";
    }
}
