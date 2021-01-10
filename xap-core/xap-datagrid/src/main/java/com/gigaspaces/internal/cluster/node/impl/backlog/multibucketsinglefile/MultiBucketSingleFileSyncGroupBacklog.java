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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IReplicationSyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.ISyncReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.server.storage.IEntryHolder;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;


@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileSyncGroupBacklog extends AbstractMultiBucketSingleFileGroupBacklog
        implements IReplicationSyncGroupBacklog {
    public MultiBucketSingleFileSyncGroupBacklog (
            DynamicSourceGroupConfigHolder sourceConfig, String name,
            IReplicationPacketDataProducer<?> dataProducer) {
        super(sourceConfig, name, dataProducer);
    }

    public void add(ISyncReplicationGroupOutContext groupContext,
                    IEntryHolder entryHolder,
                    ReplicationSingleOperationType operationType) {
        SingleBucketOrderedPacket packet = addSingleOperationPacket(groupContext, entryHolder, operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }

    public void addTransaction(ISyncReplicationGroupOutContext groupContext,
                               ServerTransaction transaction,
                               ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        IMultiBucketSingleFileReplicationOrderedPacket packet = addTransactionOperationPacket(groupContext,
                transaction,
                lockedEntries,
                operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }

    public void addGeneric(ISyncReplicationGroupOutContext groupContext,
                           Object operationData, ReplicationSingleOperationType operationType) {
        SingleBucketOrderedPacket packet = addGenericOperationPacket(groupContext, operationData, operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }
}
