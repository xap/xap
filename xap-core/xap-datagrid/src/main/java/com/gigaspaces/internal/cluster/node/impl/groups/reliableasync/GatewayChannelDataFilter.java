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

package com.gigaspaces.internal.cluster.node.impl.groups.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationUnreliableOperation;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelDataFilterResult;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelEntryDataFilterResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryDataContentExtractor;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class GatewayChannelDataFilter extends ReliableAsyncChannelDataFilter {

    private final boolean _sendChangeAsUpdate;

    public GatewayChannelDataFilter(boolean replicateChangeAsUpdate) {
        _sendChangeAsUpdate = replicateChangeAsUpdate;
    }

    public ReplicationChannelEntryDataFilterResult filterBeforeReplicatingEntryData(IReplicationPacketEntryData entryData, PlatformLogicalVersion targetLogicalVersion, IReplicationPacketEntryDataContentExtractor contentExtractor, Logger contextLogger, IReplicationPacketData data) {
        //Filter all operations that originated from a gateway (currently gateway usage is always from wan so any
        //incoming replication from any gateway is considered from wan and we should filter it)
        if (entryData.isFromGateway())
            return ReplicationChannelEntryDataFilterResult.FILTER_PACKET;

        if(entryData.isBackupOnly()){
            return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
        }

        switch (entryData.getOperationType()) {
            case WRITE:
            case UPDATE:
            case REMOVE_ENTRY:
            case DATA_TYPE_INTRODUCE:
            case DATA_TYPE_ADD_INDEX:
            case DISCARD:
                return ReplicationChannelEntryDataFilterResult.PASS;
            case CANCEL_LEASE:
            case EXTEND_ENTRY_LEASE:
                if (targetLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v9_1_0))
                    return ReplicationChannelEntryDataFilterResult.PASS;
                break;
            case CHANGE:
                if (_sendChangeAsUpdate || targetLogicalVersion.lessThan(PlatformLogicalVersion.v9_5_0))
                    return ReplicationChannelEntryDataFilterResult.CONVERT_UPDATE;
                return ReplicationChannelEntryDataFilterResult.PASS;
            default:
                break;
        }

        return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
    }

    @Override
    public ReplicationChannelDataFilterResult filterBeforeReplicatingData(IReplicationPacketData<?> data,
                                                                          PlatformLogicalVersion targetLogicalVersion, Logger contextLogger) {
        final ReplicationChannelDataFilterResult filterResult = super.filterBeforeReplicatingData(data, targetLogicalVersion, contextLogger);
        //Filter all operations that originated from a gateway (currently gateway usage is always from wan so any
        //incoming replication from any gateway is considered from wan and we should filter it)	
        return data.isFromGateway() ? ReplicationChannelDataFilterResult.FILTER_PACKET : filterResult;
    }

    public boolean filterBeforeReplicatingUnreliableOperation(IReplicationUnreliableOperation operation, PlatformLogicalVersion targetLogicalVersion) {
        return false;
    }

    @Override
    public Object[] getConstructionArgument() {
        return new Object[]{_sendChangeAsUpdate};
    }

}
