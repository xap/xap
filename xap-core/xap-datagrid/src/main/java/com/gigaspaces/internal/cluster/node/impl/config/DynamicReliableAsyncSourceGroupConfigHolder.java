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

package com.gigaspaces.internal.cluster.node.impl.config;

import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncSourceGroupConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class DynamicReliableAsyncSourceGroupConfigHolder
        extends DynamicSourceGroupConfigHolder {

    public DynamicReliableAsyncSourceGroupConfigHolder(
            ReliableAsyncSourceGroupConfig config) {
        super(config);
    }

    @Override
    protected SourceGroupConfig generateUpdatedConfig(String memberName,
                                                      SourceGroupConfig config, BacklogConfig backlogConfig,
                                                      Map<String, IReplicationChannelDataFilter> filters,
                                                      Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles,
                                                      Object customMetadata) {
        ReliableAsyncSourceGroupConfig reliableAsyncSourceGroupConfig = (ReliableAsyncSourceGroupConfig) config;
        AsyncChannelConfig asyncChannelConfig = (AsyncChannelConfig) customMetadata;
        //Add member to async members
        String[] newAsyncMembers = Arrays.copyOf(reliableAsyncSourceGroupConfig.getAsyncMembersLookupNames(), reliableAsyncSourceGroupConfig.getAsyncMembersLookupNames().length + 1);
        newAsyncMembers[newAsyncMembers.length - 1] = memberName;
        Map<String, String[]> membersGrouping = reliableAsyncSourceGroupConfig.getMembersGrouping(); //TODO support dynamic addition and removal to groups when relevant (dynamic mirror)
        ReliableAsyncSourceGroupConfig newConfig = new ReliableAsyncSourceGroupConfig(config.getName(),
                backlogConfig,
                config.getGroupConsistencyLevelPolicy(),
                filters,
                lifeCycles,
                membersGrouping,
                reliableAsyncSourceGroupConfig.getSyncMembersLookupNames(),
                newAsyncMembers,
                reliableAsyncSourceGroupConfig.getBacklogCompletionBatchSize(), reliableAsyncSourceGroupConfig.getCompletionNotifierInterval(), reliableAsyncSourceGroupConfig.getCompletionNotifierPacketsThreshold(), reliableAsyncSourceGroupConfig.isNetworkCompressionEnabled());

        for (String oldMemberName : reliableAsyncSourceGroupConfig.getMembersLookupNames()) {
            AsyncChannelConfig oldMemberConfig = reliableAsyncSourceGroupConfig.getChannelConfig(oldMemberName);
            if (oldMemberConfig != null)
                newConfig.setChannelConfig(oldMemberName, oldMemberConfig);
        }

        if (asyncChannelConfig != null)
            newConfig.setChannelConfig(memberName, asyncChannelConfig);

        return newConfig;
    }

    @Override
    protected SourceGroupConfig generateNewConfigWithoutMember(
            String memberName, BacklogConfig backlogConfig,
            Map<String, IReplicationChannelDataFilter> filters,
            Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles,
            SourceGroupConfig config) {
        ReliableAsyncSourceGroupConfig reliableAsyncSourceGroupConfig = (ReliableAsyncSourceGroupConfig) config;

        List<String> newAsyncMembersList = new ArrayList<String>((Arrays.asList(reliableAsyncSourceGroupConfig.getAsyncMembersLookupNames())));
        newAsyncMembersList.remove(memberName);
        String[] newAsyncMembers = newAsyncMembersList.toArray(new String[newAsyncMembersList.size()]);
        Map<String, String[]> membersGrouping = reliableAsyncSourceGroupConfig.getMembersGrouping(); //TODO support dynamic addition and removal to groups when relevant (dynamic mirror)
        ReliableAsyncSourceGroupConfig newConfig = new ReliableAsyncSourceGroupConfig(config.getName(),
                backlogConfig,
                config.getGroupConsistencyLevelPolicy(),
                filters,
                lifeCycles,
                membersGrouping,
                reliableAsyncSourceGroupConfig.getSyncMembersLookupNames(),
                newAsyncMembers, reliableAsyncSourceGroupConfig.getBacklogCompletionBatchSize(), reliableAsyncSourceGroupConfig.getCompletionNotifierInterval(), reliableAsyncSourceGroupConfig.getCompletionNotifierPacketsThreshold(), reliableAsyncSourceGroupConfig.isNetworkCompressionEnabled());

        for (String oldMemberName : newAsyncMembers) {
            AsyncChannelConfig oldMemberConfig = reliableAsyncSourceGroupConfig.getChannelConfig(oldMemberName);
            if (oldMemberConfig != null)
                newConfig.setChannelConfig(oldMemberName, oldMemberConfig);
        }

        return newConfig;
    }

}
