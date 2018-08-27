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

import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncSourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.GroupConsistencyLevelPolicy;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


@com.gigaspaces.api.InternalApi
public class ReliableAsyncSourceGroupConfig
        extends AsyncSourceGroupConfig {

    private final String[] _syncMembersLookupNames;
    private final String[] _asyncMembersLookupNames;
    private final int _backlogCompletionBatchSize;
    private final long _completionNotifierInterval;
    private final long _completionNotifierPacketsThreshold;

    public ReliableAsyncSourceGroupConfig(String groupName,
                                          BacklogConfig backlogConfig,
                                          GroupConsistencyLevelPolicy groupConsistencyLevelPolicy,
                                          Map<String, IReplicationChannelDataFilter> filters,
                                          Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles,
                                          Map<String, String[]> membersGrouping,
                                          String[] syncMembersLookupNames,
                                          String[] asyncMembersLookupNames,
                                          int backlogCompletionBatchSize, long completionNotifierInterval, long completionNotifierPacketsThreshold) {
        this(groupName,
                backlogConfig,
                groupConsistencyLevelPolicy,
                filters,
                lifeCycles,
                membersGrouping,
                syncMembersLookupNames,
                asyncMembersLookupNames,
                backlogCompletionBatchSize,
                completionNotifierInterval,
                completionNotifierPacketsThreshold,
                true);
    }

    public ReliableAsyncSourceGroupConfig(String groupName,
                                          BacklogConfig backlogConfig,
                                          GroupConsistencyLevelPolicy groupConsistencyLevelPolicy,
                                          Map<String, IReplicationChannelDataFilter> filters,
                                          Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles,
                                          Map<String, String[]> membersGrouping,
                                          String[] syncMembersLookupNames,
                                          String[] asyncMembersLookupNames,
                                          int backlogCompletionBatchSize, long completionNotifierInterval, long completionNotifierPacketsThreshold, boolean networkCompressionEnabled) {
        super(groupName,
                backlogConfig,
                groupConsistencyLevelPolicy,
                filters,
                lifeCycles,
                membersGrouping,
                networkCompressionEnabled,
                mergeMembersLists(syncMembersLookupNames, asyncMembersLookupNames));
        _syncMembersLookupNames = syncMembersLookupNames;
        _asyncMembersLookupNames = asyncMembersLookupNames;
        _backlogCompletionBatchSize = backlogCompletionBatchSize;
        _completionNotifierInterval = completionNotifierInterval;
        _completionNotifierPacketsThreshold = completionNotifierPacketsThreshold;
    }

    private static String[] mergeMembersLists(String[] syncMembersLookupNames,
                                              String[] asyncMembersLookupNames) {
        List<String> merged = new LinkedList<String>(Arrays.asList(syncMembersLookupNames));
        merged.addAll(Arrays.asList(asyncMembersLookupNames));
        return merged.toArray(new String[merged.size()]);
    }

    public String[] getSyncMembersLookupNames() {
        return _syncMembersLookupNames;
    }

    public String[] getAsyncMembersLookupNames() {
        return _asyncMembersLookupNames;
    }

    public int getBacklogCompletionBatchSize() {
        return _backlogCompletionBatchSize;
    }

    public long getCompletionNotifierInterval() {
        return _completionNotifierInterval;
    }

    public long getCompletionNotifierPacketsThreshold() {
        return _completionNotifierPacketsThreshold;
    }

    @Override
    public String toString() {
        return "ReliableAsyncSourceGroupConfig [_syncMembersLookupNames="
                + Arrays.toString(_syncMembersLookupNames)
                + ", _asyncMembersLookupNames="
                + Arrays.toString(_asyncMembersLookupNames)
                + ", _backlogCompletionBatchSize="
                + _backlogCompletionBatchSize
                + ", _completionNotifierInterval="
                + _completionNotifierInterval + ", "
                + ", _completionNotifierPacketsThreshold="
                + _completionNotifierPacketsThreshold + ", "
                + super.toString() + "]";
    }

}
