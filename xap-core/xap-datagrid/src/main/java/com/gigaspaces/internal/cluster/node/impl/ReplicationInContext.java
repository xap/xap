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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.ReplicationBlobstoreBulkContext;
import com.gigaspaces.internal.cluster.node.ReplicationInContentContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class ReplicationInContext
        implements IReplicationInContext {

    private final String _sourceLookupName;
    private final Logger _contextLogger;
    private final String _groupName;
    private final ReplicationInContentContext _contentContext;
    private final IReplicationReliableAsyncMediator _reliableAsyncMediator;
    private final boolean _supportDistTransactionConsolidation;
    private IReplicationOrderedPacket _packet;
    private ReplicationBlobstoreBulkContext _replicationBlobstoreBulkContext;
    private long _lastProcessedKey;

    public ReplicationInContext(String sourceLookupName, String groupName, Logger specificLogger, boolean hasContentContext, boolean supportDistTransactionConsolidation) {
        this(sourceLookupName, groupName, specificLogger, hasContentContext, supportDistTransactionConsolidation, null);
    }

    public ReplicationInContext(String sourceLookupName, String groupName, Logger specificLogger, boolean hasContentContext, boolean supportDistTransactionConsolidation, IReplicationReliableAsyncMediator reliableAsyncMediator) {
        _sourceLookupName = sourceLookupName;
        _groupName = groupName;
        _contextLogger = specificLogger;
        _reliableAsyncMediator = reliableAsyncMediator;
        _supportDistTransactionConsolidation = supportDistTransactionConsolidation;
        _contentContext = hasContentContext ? new ReplicationInContentContext() : null;
    }

    public String getSourceLookupName() {
        return _sourceLookupName;
    }

    public Logger getContextLogger() {
        return _contextLogger;
    }

    public String getGroupName() {
        return _groupName;
    }

    @Override
    public ReplicationInContentContext getContentContext() {
        return _contentContext;
    }

    @Override
    public boolean isBatchContext() {
        return false;
    }

    @Override
    public IMarker getContextMarker(String membersGroupName) {
        if (_reliableAsyncMediator == null)
            throw new UnsupportedOperationException();

        if (_packet == null)
            throw new IllegalStateException("Cannot get a context marker when there is no context packet set");

        return _reliableAsyncMediator.getMarker(_packet, membersGroupName);
    }

    public void setContextPacket(IReplicationOrderedPacket packet) {
        _packet = packet;
    }

    @Override
    public boolean supportsDistributedTransactionConsolidation() {
        return _supportDistTransactionConsolidation;
    }

    @Override
    public void setReplicationBlobstoreBulkContext(ReplicationBlobstoreBulkContext replicationBlobstoreBulkContext) {
        _replicationBlobstoreBulkContext = replicationBlobstoreBulkContext;
    }

    @Override
    public ReplicationBlobstoreBulkContext getReplicationBlobstoreBulkContext() {
        return _replicationBlobstoreBulkContext;
    }

    @Override
    public long getLastProcessedKey() {
        return _lastProcessedKey;
    }

    public void setLastProcessedKey(long lastProcessedKey) {
        _lastProcessedKey = lastProcessedKey;
    }
}
