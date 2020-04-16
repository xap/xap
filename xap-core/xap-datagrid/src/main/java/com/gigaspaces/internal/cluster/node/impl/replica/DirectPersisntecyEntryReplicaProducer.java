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

import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.replica.data.AbstractEntryReplicaData;
import com.gigaspaces.internal.cluster.node.impl.replica.data.DirectPersistencyEntryReplicaData;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class DirectPersisntecyEntryReplicaProducer extends EntryReplicaProducer {
    protected final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_REPLICA);
    private Set<String> _unitedSyncList;
    private Iterator<String> _iterator;

    public DirectPersisntecyEntryReplicaProducer(SpaceEngine engine,
                                                 SpaceCopyReplicaParameters parameters,
                                                 ITemplatePacket templatePacket, Object requestContext) {
        super(engine, parameters, templatePacket, requestContext);
        _unitedSyncList = new HashSet<String>();
        // merge local sync list with the remote sync list which was received from the backup remote space
        mergeSyncLists(parameters.getSyncList());
        _iterator = _unitedSyncList.iterator();
    }

    private void mergeSyncLists(List<String> remoteSyncList) {
        // if backup has entries in it's list
        if (remoteSyncList != null) {
            _unitedSyncList.addAll(remoteSyncList);
            if (_logger.isInfoEnabled()) {
                _logger.info("[" + getEngine().getFullSpaceName() + "]" + " received sync list with size " + remoteSyncList.size() + " from backup remote space");
            }
        }
        Iterator<String> entriesForRecovery = getEngine().getReplicationManager().getReplicationNode().getDirectPesistencySyncHandler().getEntriesForRecovery();
        int localSyncListSize = 0;
        while (entriesForRecovery.hasNext()) {
            localSyncListSize++;
            _unitedSyncList.add(entriesForRecovery.next());
        }
        if (_logger.isInfoEnabled()) {
            _logger.info("[" + getEngine().getFullSpaceName() + "]" + " local sync list size is " + localSyncListSize);
        }
        if (_logger.isDebugEnabled()) {
            _logger.debug("[" + getEngine().getFullSpaceName() + "]" + " united sync list size is " + _unitedSyncList.size());
        }
        if (_logger.isDebugEnabled()) {
            for (String entry : _unitedSyncList) {
                _logger.debug("[" + getEngine().getFullSpaceName() + "]" + " merging sync list, uid=" + entry);
            }
        }
    }

    @Override
    public synchronized AbstractEntryReplicaData produceNextData(
            ISynchronizationCallback syncCallback) {
        if (isForcedClose()) {
            throw new RuntimeException("space=" + _engine.getFullSpaceName() + " replica forced closing");
        }
        if (isClosed())
            return null;

        try {
            while (true) {
                if (isForcedClose()) {
                    this.notifyAll(); //i am done
                    throw new RuntimeException("space=" + _engine.getFullSpaceName() + " replica forced closing");
                }

                if (!_iterator.hasNext()) {
                    close(false /*forced*/);
                    return null;
                }
                String uid = _iterator.next();
                // read entry by uid from space
                AbstractEntryReplicaData replicaData = null;
                while (true) {//single entry level
                    IEntryHolder entry = getEngine().getCacheManager().getEntryByUidFromPureCache(uid);
                    IEntryHolder base = entry;
                    // if entry exists call produceDataFromEntry with the entry
                    if (entry != null) {
                        replicaData = produceDataFromEntry(syncCallback, entry);
                        // if returned null means the entry was removed during the recovery operation - mark as removed
                        if (replicaData != null)
                            break;  //valid entry
                    }
                    //recheck under lock and if entry doesn't "exist" -
                    // create Data with empty packet which indicates it does not exists in the space
                    ReplicationNode replicationNode = (ReplicationNode) _engine.getReplicationNode();
                    IReplicationGroupBacklog groupBacklog = replicationNode.getGroupBacklogByRequestContext(getRequestContext());
                    groupBacklog.writeLock();
                    try {
                        entry = getEngine().getCacheManager().getEntryByUidFromPureCache(uid);
                        if (entry == null || entry == base) {
                            replicaData = new DirectPersistencyEntryReplicaData(uid);
                            boolean duplicateUid = syncCallback.synchronizationDataGenerated(replicaData);

                            if (duplicateUid) {
                                replicaData = null;
                                break;
                            }
                            increaseGeneratedDataCount();
                            break;
                        }
                    } finally {
                        groupBacklog.freeWriteLock();
                    }

                    if (isClosed()) {
                        return null;
                    }
                }//single entry level end
                if (replicaData != null)
                    return replicaData;
            }
        } catch (Exception ex) {
            throw new ReplicationInternalSpaceException("Failure in .", ex);
        }
    }

    @Override
    public String getName() {
        return "DirectPersistencyEntryReplicaProducer";
    }
}
