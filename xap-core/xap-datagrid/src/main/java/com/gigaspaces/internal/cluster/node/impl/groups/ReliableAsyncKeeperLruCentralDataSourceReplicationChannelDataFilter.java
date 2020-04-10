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

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder.IDynamicSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.config.MemberAddedEvent;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryDataContentExtractor;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationEntryDataConversionMetadata;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.CANCEL_LEASE;
import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.CHANGE;
import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.ENTRY_LEASE_EXPIRED;
import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.REMOVE_ENTRY;
import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.UPDATE;

/**
 * If the keeper is in lru with central data source, it will convert update operations to remove, we
 * need to send the update partial with the previous state of the entry if we have view targets
 *
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class ReliableAsyncKeeperLruCentralDataSourceReplicationChannelDataFilter
        extends GeneralReplicationChannelDataFilter
        implements IDynamicSourceGroupStateListener {

    private final Object _lock = new Object();

    private final CopyOnUpdateMap<ReplicationMode, Integer> _targetCount = new CopyOnUpdateMap<ReplicationMode, Integer>();
    private final CopyOnUpdateMap<String, ReplicationMode> _memberReplicationMode = new CopyOnUpdateMap<String, ReplicationMode>();

    private final boolean _requiresEvictionReplicationProtection;

    public ReliableAsyncKeeperLruCentralDataSourceReplicationChannelDataFilter(
            boolean requiresEvictionReplicationProtection) {
        _requiresEvictionReplicationProtection = requiresEvictionReplicationProtection;
    }

    @Override
    public ReplicationChannelEntryDataFilterResult filterBeforeReplicatingEntryData(
            IReplicationPacketEntryData entryData,
            PlatformLogicalVersion targetLogicalVersion,
            IReplicationPacketEntryDataContentExtractor contentExtractor,
            Logger contextLogger,
            IReplicationPacketData data) {
        final boolean isPartOfTwoPhasePrepare = (!data.isSingleEntryData() && data.getMultipleOperationType() == ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_PREPARE);


        ReplicationChannelEntryDataFilterResult filterResult = super.filterBeforeReplicatingEntryData(entryData,
                targetLogicalVersion,
                contentExtractor,
                contextLogger,
                data);

        switch (filterResult.getFilterOperation()) {
            case CONVERT:
            case FILTER_DATA:
            case FILTER_PACKET:
                return filterResult;
            case PASS:
                if (entryData.isTransient() || !mayNeedConversion(_targetCount, _requiresEvictionReplicationProtection, isPartOfTwoPhasePrepare))
                    return filterResult;
                //TODO MU: Dont convert change to update, consider about on demand reliable async full content completion
                if ((entryData.getOperationType() == UPDATE || entryData.getOperationType() == CHANGE) && convertUpdate(_targetCount, _requiresEvictionReplicationProtection, isPartOfTwoPhasePrepare))
                    return ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(new ReplicationEntryDataConversionMetadata(UPDATE).requiresReliableAsyncFullContent());
                if (entryData.getOperationType() == CANCEL_LEASE && convertLeaseCancelled(_targetCount))
                    return ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(new ReplicationEntryDataConversionMetadata(CANCEL_LEASE).requiresFullEntryData());
                if (entryData.getOperationType() == ENTRY_LEASE_EXPIRED && convertLeaseExpired(_targetCount))
                    return ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(new ReplicationEntryDataConversionMetadata(ENTRY_LEASE_EXPIRED).requiresFullEntryData());
                if (entryData.getOperationType() == REMOVE_ENTRY && convertRemove(_targetCount))
                    return ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(new ReplicationEntryDataConversionMetadata(REMOVE_ENTRY).requiresFullEntryData());
            default:
                return filterResult;

        }
    }

    @Override
    public void memberAdded(MemberAddedEvent memberAddedParam,
                            SourceGroupConfig newConfig) {
        if (memberAddedParam.getCustomData() instanceof AsyncChannelConfig) {
            AsyncChannelConfig config = (AsyncChannelConfig) memberAddedParam.getCustomData();

            switch (config.getChannelType()) {
                case LOCAL_VIEW:
                    increaseCount(memberAddedParam.getMemberName(), ReplicationMode.LOCAL_VIEW);
                    break;
                case DURABLE_NOTIFICATION:
                    increaseCount(memberAddedParam.getMemberName(), ReplicationMode.DURABLE_NOTIFICATION);
                    break;
                default:
                    break;
            }

        }
    }

    @Override
    public void memberRemoved(String memberName, SourceGroupConfig newConfig) {
        decreaseCount(memberName);
    }

    private void increaseCount(String memberName,
                               ReplicationMode replicationMode) {
        synchronized (_lock) {
            _memberReplicationMode.put(memberName, replicationMode);
            Integer count = _targetCount.get(replicationMode);
            if (count == null)
                count = 0;
            _targetCount.put(replicationMode, count + 1);
        }
    }

    private void decreaseCount(String memberName) {
        synchronized (_lock) {
            ReplicationMode replicationMode = _memberReplicationMode.get(memberName);
            if (replicationMode == null)
                return;

            Integer count = _targetCount.get(replicationMode);
            if (count == null)
                return;

            if (count == 1)
                _targetCount.remove(replicationMode);
            else
                _targetCount.put(replicationMode, count - 1);

            _memberReplicationMode.remove(memberName);
        }

    }

    private static boolean convertUpdate(
            Map<ReplicationMode, Integer> targetCount,
            boolean requiresEvictionReplicationProtection,
            boolean partOfTwoPhasePrepare) {
        return durableNoticationExists(targetCount) || localViewExists(targetCount) || (requiresEvictionReplicationProtection && partOfTwoPhasePrepare);
    }

    private static boolean convertRemove(Map<ReplicationMode, Integer> targetCount) {
        return durableNoticationExists(targetCount);
    }

    private static boolean convertLeaseExpired(Map<ReplicationMode, Integer> targetCount) {
        return durableNoticationExists(targetCount);
    }

    private static boolean convertLeaseCancelled(CopyOnUpdateMap<ReplicationMode, Integer> targetCount) {
        return durableNoticationExists(targetCount);
    }

    private static boolean mayNeedConversion(Map<ReplicationMode, Integer> targetCount,
                                             boolean requiresEvictionReplicationProtection,
                                             boolean partOfTwoPhasePrepare) {
        return durableNoticationExists(targetCount) || localViewExists(targetCount) || (requiresEvictionReplicationProtection && partOfTwoPhasePrepare);
    }

    private static boolean durableNoticationExists(Map<ReplicationMode, Integer> targetCount) {
        return targetCount.containsKey(ReplicationMode.DURABLE_NOTIFICATION);
    }

    private static boolean localViewExists(Map<ReplicationMode, Integer> targetCount) {
        return targetCount.containsKey(ReplicationMode.LOCAL_VIEW);
    }

}
