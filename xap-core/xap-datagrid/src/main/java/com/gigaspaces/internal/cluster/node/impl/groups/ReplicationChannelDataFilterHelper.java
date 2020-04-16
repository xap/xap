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

import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter.FilterOperation;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketEntryDataConversionException;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class ReplicationChannelDataFilterHelper {
    public static <T extends IReplicationOrderedPacket> T filterPacket(
            IReplicationChannelDataFilter filter,
            PlatformLogicalVersion targetMemberVersion, T packet,
            IReplicationPacketDataProducer dataProducer,
            IReplicationGroupBacklog groupBacklog, T previousDiscardedPacket,
            Logger logger, String targetMemberName) {
        if (!packet.isDataPacket())
            return packet;

        // Optimize path of single entry data
        if (packet.getData().isSingleEntryData()) {
            return filterSingleEntryDataPacket(filter,
                    targetMemberVersion,
                    packet,
                    dataProducer,
                    groupBacklog,
                    previousDiscardedPacket,
                    logger,
                    targetMemberName);
        } else {
            ReplicationChannelDataFilterResult filterResult = filter.filterBeforeReplicatingData(packet.getData(), targetMemberVersion, logger);
            switch (filterResult.getFilterOperation()) {
                case PASS:
                    break;
                case FILTER_DATA:
                    return discardPacket(packet,
                            previousDiscardedPacket,
                            groupBacklog,
                            targetMemberVersion,
                            logger,
                            targetMemberName,
                            false);
                case FILTER_PACKET:
                    return discardPacket(packet,
                            previousDiscardedPacket,
                            groupBacklog,
                            targetMemberVersion,
                            logger,
                            targetMemberName,
                            true);
                case CONVERT:
                    try {
                        IReplicationPacketData<?> convertedData = dataProducer.convertData(packet.getData(),
                                filterResult.getConvertToOperation(),
                                targetMemberVersion);
                        packet = (T) packet.cloneWithNewData(convertedData);
                    } catch (ReplicationPacketEntryDataConversionException e) {
                        throw new ReplicationInternalSpaceException(e.getMessage(),
                                e);
                    }
            }
        }

        //Scan the data to check if there should be any filtering done
        if (!filter.filterBeforeReplicatingEntryDataHasSideEffects() &&
                !shouldFilterData(filter, targetMemberVersion, packet, dataProducer, logger))
            return packet;

        // Only clone on demand
        IReplicationPacketData<?> data = packet.getData();
        IReplicationPacketData newData = dataProducer.createEmptyMultipleEntryData(data);

        for (IReplicationPacketEntryData entryData : data) {
            ReplicationChannelEntryDataFilterResult filterResult = filter.filterBeforeReplicatingEntryData(entryData,
                    targetMemberVersion,
                    dataProducer,
                    logger,
                    data);

            switch (filterResult.getFilterOperation()) {
                case FILTER_DATA:
                case FILTER_PACKET:
                    break;
                case PASS:
                    newData.add(entryData);
                    break;
                case CONVERT:
                    try {
                        IReplicationPacketEntryData convertedEntryData = dataProducer.convertEntryData(entryData,
                                filterResult.getConversionMetadata(),
                                targetMemberVersion);
                        newData.add(convertedEntryData);
                    } catch (ReplicationPacketEntryDataConversionException e) {
                        throw new ReplicationInternalSpaceException(e.getMessage(),
                                e);
                    }
            }
        }

        // Create a clone of this packet with the new data, we do this before discard since discard
        // may choose to return the original packet but we need it with the new data in it.
        packet = (T) packet.cloneWithNewData(newData);
        // If all data is filtered, we should discard the packet
        if (newData.isEmpty()) {
            return discardPacket(packet,
                    previousDiscardedPacket,
                    groupBacklog,
                    targetMemberVersion,
                    logger,
                    targetMemberName,
                    false);
        }
        // Otherwise return the packet 
        return packet;
    }

    protected static <T extends IReplicationOrderedPacket> boolean shouldFilterData(
            IReplicationChannelDataFilter filter,
            PlatformLogicalVersion targetMemberVersion, T packet,
            IReplicationPacketDataProducer dataProducer,
            Logger logger) {
        // First we scan the entire packet, assuming most packets will have have
        // only pass result, this way we avoid cloning
        for (IReplicationPacketEntryData entryData : packet.getData()) {
            if (filter.filterBeforeReplicatingEntryData(entryData, targetMemberVersion, dataProducer, logger, packet.getData())
                    .getFilterOperation() == FilterOperation.PASS) {
                continue;
            }
            return true;
        }
        return false;
    }

    protected static <T extends IReplicationOrderedPacket> T filterSingleEntryDataPacket(
            IReplicationChannelDataFilter filter,
            PlatformLogicalVersion targetMemberVersion, T packet,
            IReplicationPacketDataProducer dataProducer,
            IReplicationGroupBacklog groupBacklog, T previousDiscardedPacket,
            Logger logger, String targetMemberName) {
        ReplicationChannelEntryDataFilterResult filterResult = filter.filterBeforeReplicatingEntryData(packet.getData()
                        .getSingleEntryData(),
                targetMemberVersion,
                dataProducer,
                logger,
                packet.getData());
        switch (filterResult.getFilterOperation()) {
            case PASS:
                return packet;
            case FILTER_DATA:
                return discardPacket(packet,
                        previousDiscardedPacket,
                        groupBacklog,
                        targetMemberVersion,
                        logger,
                        targetMemberName,
                        false);
            case FILTER_PACKET:
                return discardPacket(packet,
                        previousDiscardedPacket,
                        groupBacklog,
                        targetMemberVersion,
                        logger,
                        targetMemberName,
                        true);
            case CONVERT:
                try {
                    IReplicationPacketData convertedData = dataProducer.convertSingleEntryData(packet.getData(),
                            filterResult.getConversionMetadata(),
                            targetMemberVersion);
                    return (T) packet.cloneWithNewData(convertedData);
                } catch (ReplicationPacketEntryDataConversionException e) {
                    // TODO LOG ERROR HERE
                    throw new ReplicationInternalSpaceException(e.getMessage(),
                            e);
                }
            default:
                throw new ReplicationInternalSpaceException("illegal filter operation "
                        + filterResult.getFilterOperation());
        }
    }

    private static <T extends IReplicationOrderedPacket> T discardPacket(
            T packet, T previousDiscardedPacket,
            IReplicationGroupBacklog groupBacklog,
            PlatformLogicalVersion targetMemberVersion, Logger logger,
            String targetMemberName,
            boolean forceDiscard) {
        if (logger.isTraceEnabled())
            logger.trace("Packet [" + packet.toString()
                    + "] discarded by the channel filter.");
        // This is the first discarded packet, create a new discarded packet
        if (previousDiscardedPacket == null
                || !groupBacklog.supportDiscardMerge())
            return (T) groupBacklog.replaceWithDiscarded(packet, forceDiscard);

        // We have a consecutive discarded packets, merge with previous one.
        boolean merged = groupBacklog.mergeWithDiscarded(previousDiscardedPacket,
                packet,
                targetMemberName);
        // Failed merging, discard current packet without merging
        if (!merged)
            return (T) groupBacklog.replaceWithDiscarded(packet, forceDiscard);

        return previousDiscardedPacket;
    }

}
