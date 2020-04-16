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


public interface IReplicationChannelDataFilter {

    public enum FilterOperation {
        PASS,
        FILTER_DATA,
        CONVERT,
        /**
         * Filter entire packet - for example: gateway resonance protection.
         */
        FILTER_PACKET
    }

    /**
     * Specifies whether the given entry data should pass the filter as is, be converted or removed
     *
     * @param targetLogicalVersion The logical version of the target
     * @param contentExtractor     extracts content from the data
     * @param contextLogger        context Logger
     */
    ReplicationChannelEntryDataFilterResult filterBeforeReplicatingEntryData(IReplicationPacketEntryData entryData, PlatformLogicalVersion targetLogicalVersion, IReplicationPacketEntryDataContentExtractor contentExtractor, Logger contextLogger, IReplicationPacketData<?> data);

    /**
     * Specifies whether the given data should pass the filter as is, be converted or removed
     *
     * @param targetLogicalVersion The logical version of the target
     * @param contextLogger        context Logger
     * @since 9.0
     */
    ReplicationChannelDataFilterResult filterBeforeReplicatingData(IReplicationPacketData<?> data, PlatformLogicalVersion targetLogicalVersion, Logger contextLogger);

    /**
     * Specifies whether the given unreliable operation should pass the filter and not be removed
     *
     * @param targetLogicalVersion The logical version of the target
     * @return <code>true</code> if the unreliable operation should not be filtered,
     * <code>false</code> otherwise
     */
    boolean filterBeforeReplicatingUnreliableOperation(IReplicationUnreliableOperation operation, PlatformLogicalVersion targetLogicalVersion);

    /**
     * Called after entry data was replicated successfully
     *
     * @param targetLogicalVersion The logical version of the target
     * @param contentExtractor     extracts content from the data
     * @param contextLogger        context Logger
     */
    void filterAfterReplicatedEntryData(IReplicationPacketEntryData data, PlatformLogicalVersion targetLogicalVersion, IReplicationPacketEntryDataContentExtractor contentExtractor, Logger contextLogger);

    /**
     * Specifies wheter the filter operations have a side effect and cannot be called more than once
     * for the same entry data
     */
    boolean filterBeforeReplicatingEntryDataHasSideEffects();

    /**
     * @return construction argument that are used in order to duplicate this filter instance using
     * a {@link IReplicationChannelDataFilterBuilder} this is generally used when serializing the
     * filter between replication members (reliable async)
     */
    Object[] getConstructionArgument();
}
