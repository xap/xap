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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryDataContentExtractor;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General replication filter. Currently handles backwards compatibility support for new replication
 * operations.
 *
 * @author yechiel
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class GeneralReplicationChannelDataFilter
        extends AbstractReplicationChannelDataFilter {

    private final static GeneralReplicationChannelDataFilter _filter = new GeneralReplicationChannelDataFilter();

    public static GeneralReplicationChannelDataFilter getInstance() {
        return _filter;
    }

    protected GeneralReplicationChannelDataFilter() {
    }

    @Override
    public ReplicationChannelEntryDataFilterResult filterBeforeReplicatingEntryData(IReplicationPacketEntryData entryData,
                                                                                    PlatformLogicalVersion targetLogicalVersion, IReplicationPacketEntryDataContentExtractor contentExtractor, Logger contextLogger, IReplicationPacketData data) {
        switch (entryData.getOperationType()) {
            case ENTRY_LEASE_EXPIRED:
            case NOTIFY_TEMPLATE_LEASE_EXPIRED:
            case REMOVE_ENTRY:
                break;
            case CHANGE:
                if (targetLogicalVersion.lessThan(PlatformLogicalVersion.v9_1_0))
                    return ReplicationChannelEntryDataFilterResult.CONVERT_UPDATE;
        }

        return ReplicationChannelEntryDataFilterResult.PASS;
    }

    @Override
    public ReplicationChannelDataFilterResult filterBeforeReplicatingData(
            IReplicationPacketData<?> data,
            PlatformLogicalVersion targetLogicalVersion,
            Logger contextLogger) {
        return ReplicationChannelDataFilterResult.PASS;
    }

    @Override
    public boolean filterBeforeReplicatingUnreliableOperation(IReplicationUnreliableOperation operation,
                                                              PlatformLogicalVersion targetLogicalVersion) {
        return true;
    }

    @Override
    public Object[] getConstructionArgument() {
        return null;
    }


}
