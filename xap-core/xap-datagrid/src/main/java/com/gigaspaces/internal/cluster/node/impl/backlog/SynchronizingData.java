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

package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelDataFilterResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ObjectLongMap;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import java.util.logging.Level;
import java.util.logging.Logger;


@com.gigaspaces.api.InternalApi
public class SynchronizingData<T extends IReplicationPacketData<?>> {
    private static final int _maxAfterIterationDoneStageLength = 100000;

    private final ObjectLongMap<String> _entriesUidMap = CollectionsFactory.getInstance().createObjectLongMap();
    private final Logger _logger;
    private final boolean _isDirectPersistencySync;
    private final ObjectLongMap<Object> _dataIdMap = CollectionsFactory.getInstance().createObjectLongMap();
    private long _keyWhenCopyStageCompleted = -1L;

    public SynchronizingData(Logger logger, boolean isDirectPersistencySync) {
        _logger = logger;
        _isDirectPersistencySync = isDirectPersistencySync;
    }

    /**
     * @return true if the data with the same uid already exists, otherwise false
     */
    public synchronized boolean updateUidKey(String uid, long currentKey) {
        // Update the current key when this uid data was generated
        if (_entriesUidMap.containsKey(uid))
            return true;
        _entriesUidMap.put(uid, currentKey);
        // Update last synchronize key if needed to know when this
        // synchronization
        // process is complete
        _keyWhenCopyStageCompleted = Math.max(_keyWhenCopyStageCompleted, currentKey);
        return false;
    }

    // Should be called when map is in readonly state, meaning after all
    // update uid occurred, must go through a memory barrier first to get
    // updated _lastSynchronizingKey
    // that's why the method is synchronized
    public synchronized boolean isDone(long key) {
        if (_keyWhenCopyStageCompleted >= key)
            return false;

        // To avoid being at this state for infinite time we allow a certain
        // number of packets after which we break this stage
        // (This can happen for example if there is a prepared transaction but
        // the mahalo crashed and the abort/commit never occurred.
        if (key - _keyWhenCopyStageCompleted >= _maxAfterIterationDoneStageLength) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.warning("Synchronization state at completion stage exceeded the allowed number of packets, synchronization state is moving to done");
            return true;
        }
        // We are done when data synchronization state is done
        return _dataIdMap.isEmpty();
    }

    // Should be called when map is in readonly state, meaning after all
    // update uid occurred
    // Currently not thread safe and support logic of only 1 thread calling
    // getPackets
    public synchronized boolean filterEntryData(String uid, long packetKey,
                                                boolean filterIfNotPresentInReplicaState, boolean isTransientEntry) {
        if (!_entriesUidMap.containsKey(uid)) {
            if (_logger.isLoggable(Level.INFO)) {
                _logger.info("[SynchronizingData::filterEntryData] not in _entriesUidMap: uid=" + uid + " ,_keyWhenCopyStageCompleted=" + _keyWhenCopyStageCompleted +
                        " ,packetKey=" + packetKey + " ,isDirectPersistency=" + _isDirectPersistencySync);
            }
            //don't filter any data from redo log (which is not in sync list) when doing direct persistency sync list recovery
            if (_isDirectPersistencySync) {
                return false;
            }
            // If this operation is not filtered if not present in the replica,
            // it should be inserted to the uid map
            // (e.g, Write of entry which occurred after the replica iterator
            // passed this entry)
            if (!filterIfNotPresentInReplicaState)
                _entriesUidMap.put(uid, packetKey);


//            return filterIfNotPresentInReplicaState; changed by Mishel due to Tadiran transition from 7.1.4 to 14.0.1 - GS-14365 ->>
            if (isTransientEntry) {
                return filterIfNotPresentInReplicaState;
            }
            //return false when it's not transient
            return false;
        }

        long keyAtGenerationTime = _entriesUidMap.get(uid);

        // If not filter we are already processing replication data which
        // occurred
        // after the data generation related to this uid happened.
        // since replication is sequential from now on we will only process
        // newer events
        if (_logger.isLoggable(Level.FINER)) {
            _logger.finer("[SynchronizingData::filterEntryData] in _entriesUidMap: uid=" + uid + " ,_keyWhenCopyStageCompleted=" + _keyWhenCopyStageCompleted +
                    " ,packetKey=" + packetKey + " ,isDirectPersistency=" + _isDirectPersistencySync);
        }
        return keyAtGenerationTime >= packetKey;
    }

    public synchronized void setKeyWhenCopyStageCompleted(
            long keyWhenCopyStageCompleted) {
        _keyWhenCopyStageCompleted = keyWhenCopyStageCompleted;
    }

    public synchronized ReplicationChannelDataFilterResult filterData(Object dataId, long packetKey, ReplicationMultipleOperationType operationType) {
        // Don't filter whole transaction packets in direct persistency sync recovery since in this case we don't drop the redo log.
        // Filtering single operations inside a transaction (multiple operations) will be done in filterEntryData method.
        // A transaction of X operations can be filtered into a transaction with X-Y operations if Y operations had redo key
        // which is smaller than it's key at recovery time.
        if (_isDirectPersistencySync) {
            return ReplicationChannelDataFilterResult.PASS;
        }
        switch (operationType) {
            case TRANSACTION_TWO_PHASE_PREPARE: {
                //Transaction at prepare state should be filtered out if they were generated during
                //the synchronization copy stage, the following commit will be sent as converted instead (or abort as discarded) 
                //Keep the packet's key at which this prepare was encountered for proper handling by the commit/abort
                _dataIdMap.put(dataId, packetKey);
                if (packetKey > _keyWhenCopyStageCompleted)
                    return ReplicationChannelDataFilterResult.PASS;
                else
                    return ReplicationChannelDataFilterResult.FILTER_DATA;

            }
            case TRANSACTION_TWO_PHASE_COMMIT: {
                if (_dataIdMap.containsKey(dataId)) {
                    long removedDataKey = _dataIdMap.remove(dataId);
                    if (removedDataKey > _keyWhenCopyStageCompleted)
                        //Prepare will pass filter, maintain two phase commit cycle
                        return ReplicationChannelDataFilterResult.PASS;
                }
                //Corresponding prepare will be filtered or even removed at the beginning of synchronization when
                //redo log is cleared
                return ReplicationChannelDataFilterResult.getConvertToOperationResult(ReplicationMultipleOperationType.TRANSACTION_ONE_PHASE);
            }
            case TRANSACTION_TWO_PHASE_ABORT: {

                if (_dataIdMap.containsKey(dataId)) {
                    long removedDataKey = _dataIdMap.remove(dataId);
                    if (removedDataKey > _keyWhenCopyStageCompleted)
                        //Prepare will pass filter, maintain two phase abort cycle
                        return ReplicationChannelDataFilterResult.PASS;
                }
                //Corresponding prepare will be filtered or even removed at the beginning of synchronization when
                //redo log is cleared
                return ReplicationChannelDataFilterResult.FILTER_DATA;
            }
            default:
                throw new ReplicationInternalSpaceException("Unexpected packet type " + operationType);
        }


    }
}