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

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.impl.config.IMirrorChannelReplicationSettings;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationUnreliableOperation;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelEntryDataFilterResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryDataContentExtractor;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class MirrorChannelDataFilter
        extends ReliableAsyncChannelDataFilter {

    private final IMirrorChannelReplicationSettings _mirrorPolicy;

    public MirrorChannelDataFilter(
            IMirrorChannelReplicationSettings mirrorPolicy) {
        _mirrorPolicy = mirrorPolicy;
    }

    public ReplicationChannelEntryDataFilterResult filterBeforeReplicatingEntryData(IReplicationPacketEntryData entryData, PlatformLogicalVersion targetLogicalVersion, IReplicationPacketEntryDataContentExtractor contentExtractor, Logger contextLogger, IReplicationPacketData data) {
        if(entryData.isBackupOnly()){
            return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
        }

        if (entryData.isTransient())
            return ReplicationChannelEntryDataFilterResult.FILTER_DATA;

        switch (entryData.getOperationType()) {
            case WRITE:
            case UPDATE:
            case REMOVE_ENTRY:
            case DATA_TYPE_INTRODUCE:
            case DATA_TYPE_ADD_INDEX:
            case DISCARD:
                return ReplicationChannelEntryDataFilterResult.PASS;
            case CHANGE: {
                if (!_mirrorPolicy.supportsChange())
                    return ReplicationChannelEntryDataFilterResult.CONVERT_UPDATE;
                Collection<SpaceEntryMutator> mutators = contentExtractor.getCustomContent(entryData);
                for (SpaceEntryMutator spaceEntryMutator : mutators) {
                    if (!_mirrorPolicy.getSupportedChangeOperations().contains(spaceEntryMutator.getName()))
                        return ReplicationChannelEntryDataFilterResult.CONVERT_UPDATE;
                }
                return ReplicationChannelEntryDataFilterResult.PASS;
            }
        }

        return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
    }

    public boolean filterBeforeReplicatingUnreliableOperation(IReplicationUnreliableOperation operation, PlatformLogicalVersion targetLogicalVersion) {
        switch (operation.getOperationType()) {
            case NotificationSent:
                return false;
        }
        return true;
    }

    @Override
    public Object[] getConstructionArgument() {
        return null;
    }

}
