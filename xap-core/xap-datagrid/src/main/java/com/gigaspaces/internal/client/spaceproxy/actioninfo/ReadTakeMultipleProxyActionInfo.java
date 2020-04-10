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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.client.ReadMultipleException;
import com.gigaspaces.client.TakeMultipleException;
import com.gigaspaces.cluster.replication.TakeConsistencyLevelCompromisedException;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.SpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.operations.ReplicationLevel;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.multiple.query.QueryMultiplePartialFailureException;

import net.jini.core.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;


@com.gigaspaces.api.InternalApi
public class ReadTakeMultipleProxyActionInfo extends QueryProxyActionInfo {
    public final int maxResults;
    public final boolean returnOnlyUids;
    public final boolean isTake;
    public final long timeout;
    public final int minEntriesToWaitFor;
    public boolean ifExist;
    private int syncReplicationLevel;
    private List<ReplicationLevel> syncReplicationLevels;


    public ReadTakeMultipleProxyActionInfo(ISpaceProxy spaceProxy, Object template, Transaction txn, long timeout, int maxEntries, int minEntriesToWaitFor, int modifiers, boolean returnOnlyUids, boolean isTake, boolean ifExist) {
        super(spaceProxy, template, txn, modifiers, isTake);

        try {
            // If query is a uids query, the maximum number of results is also limited by the uids length:
            if (queryPacket.getMultipleUIDs() != null) {
                this.maxResults = Math.min(maxEntries, queryPacket.getMultipleUIDs().length);
                this.minEntriesToWaitFor = Math.min(this.maxResults, minEntriesToWaitFor);
            } else {
                this.maxResults = maxEntries;
                this.minEntriesToWaitFor = minEntriesToWaitFor;
            }

            this.returnOnlyUids = returnOnlyUids | queryPacket.isReturnOnlyUids();
            this.isTake = isTake;
            this.timeout = timeout;
            this.ifExist = ifExist;
            if (ReadModifiers.isFifoGroupingPoll(modifiers))
                verifyFifoGroupsCallParams(isTake);
            if (_devLogger.isTraceEnabled()) {
                if (verifyLogScannedEntriesCountParams(this.timeout))
                    this.modifiers |= Modifiers.LOG_SCANNED_ENTRIES_COUNT;
            }
            setFifoIfNeeded(spaceProxy);
        } catch (SpaceMetadataException e) {
            if (isTake)
                throw new TakeMultipleException(e);
            else
                throw new ReadMultipleException(e);
        }
    }

    public Object[] convertQueryResults(ISpaceProxy spaceProxy, IEntryPacket[] results, AbstractProjectionTemplate projectionTemplate) {
        boolean returnPacket = _query == queryPacket;
        Object[] returnedObjects = spaceProxy.getDirectProxy().getTypeManager().convertQueryResults(results,
                queryPacket,
                returnPacket,
                projectionTemplate);
        if (syncReplicationLevels == null && isTake) {
            syncReplicationLevels = new ArrayList<ReplicationLevel>(returnedObjects.length);
            for (int i = 0; i < returnedObjects.length; ++i) {
                syncReplicationLevels.add(i, new ReplicationLevel(i, 1, syncReplicationLevel));
            }
        }
        checkConsistencyLevel(syncReplicationLevels, returnedObjects);
        return returnedObjects;
    }

    private void checkConsistencyLevel(List<ReplicationLevel> levels, Object[] returnedObjects) {
        if (isTake && levels != null) {
            List<Object> results = new ArrayList<Object>();
            List<Throwable> errors = new ArrayList<Throwable>();
            int requiredConsistencyLevel = SpaceProxyTypeManager.requiredConsistencyLevel();
            for (int i = 0; i < returnedObjects.length; ++i) {
                int actualReplicationLevel = getReplicationLevelFor(i, levels);
                if (actualReplicationLevel + 1 < requiredConsistencyLevel) {
                    errors.add(new TakeConsistencyLevelCompromisedException(actualReplicationLevel + 1, returnedObjects[i]));
                } else {
                    results.add(returnedObjects[i]);
                }
            }
            if (!errors.isEmpty()) {
                throw new TakeMultipleException(results, errors);
            }
        }
    }

    private int getReplicationLevelFor(int index, List<ReplicationLevel> levels) {
        for (ReplicationLevel level : levels) {
            if (level.getStart() <= index && index < level.getStart() + level.getLength()) {
                return level.getLevel();
            }
        }
        return 0;
    }


    @SuppressWarnings("deprecation")
    public QueryMultiplePartialFailureException convertExceptionResults(
            ISpaceProxy spaceProxy, QueryMultiplePartialFailureException e, AbstractProjectionTemplate projectionTemplate) {
        if (_query == queryPacket)
            return e;

        Object[] results = e.getResults();
        IEntryPacket[] packets = (IEntryPacket[]) results;
        if (results != null && 0 < results.length) {
            e.setResults(convertQueryResults(spaceProxy, packets, projectionTemplate));
        }
        return e;
    }

    private void verifyFifoGroupsCallParams(boolean isTake) {
        if (txn == null)
            throw new IllegalArgumentException(" fifo-groups operation must be under transaction");
        if (!isTake && !(ReadModifiers.isExclusiveReadLock(modifiers)))
            throw new IllegalArgumentException(" fifo-groups read operation must be exclusive-read-lock");
    }

    public int getSyncReplicationLevel() {
        return syncReplicationLevel;
    }

    public void setSyncReplicationLevel(int syncReplicationLevel) {
        this.syncReplicationLevel = syncReplicationLevel;
    }

    public void setSyncReplicationLevels(List<ReplicationLevel> syncReplicationLevels) {
        this.syncReplicationLevels = syncReplicationLevels;
    }

    public List<ReplicationLevel> getSyncReplicationLevels() {
        return syncReplicationLevels;
    }
}
