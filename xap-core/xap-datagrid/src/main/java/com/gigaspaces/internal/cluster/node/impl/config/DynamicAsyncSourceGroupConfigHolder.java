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
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncSourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@com.gigaspaces.api.InternalApi
public class DynamicAsyncSourceGroupConfigHolder
        extends DynamicSourceGroupConfigHolder {

    public DynamicAsyncSourceGroupConfigHolder(AsyncSourceGroupConfig config) {
        super(config);
    }

    @Override
    protected SourceGroupConfig generateUpdatedConfig(String memberName,
                                                      SourceGroupConfig config, BacklogConfig backlogConfig,
                                                      Map<String, IReplicationChannelDataFilter> filters,
                                                      Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles,
                                                      Object customMetadata) {
        AsyncSourceGroupConfig oldConfig = (AsyncSourceGroupConfig) config;
        AsyncChannelConfig asyncChannelConfig = (AsyncChannelConfig) customMetadata;
        String[] newMembers = Arrays.copyOf(oldConfig.getMembersLookupNames(), oldConfig.getMembersLookupNames().length + 1);
        newMembers[newMembers.length - 1] = memberName;
        Map<String, String[]> membersGrouping = oldConfig.getMembersGrouping(); //TODO support dynamic addition and removal to groups when relevant (dynamic mirror)
        AsyncSourceGroupConfig newConfig = new AsyncSourceGroupConfig(oldConfig.getName(),
                backlogConfig,
                oldConfig.getGroupConsistencyLevelPolicy(),
                filters,
                lifeCycles,
                membersGrouping,
                config.isNetworkCompressionEnabled(),
                newMembers);

        for (String oldMemberName : oldConfig.getMembersLookupNames()) {
            AsyncChannelConfig oldMemberConfig = oldConfig.getChannelConfig(oldMemberName);
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
        AsyncSourceGroupConfig oldConfig = (AsyncSourceGroupConfig) config;
        List<String> newMembersList = new ArrayList<String>((Arrays.asList(config.getMembersLookupNames())));
        if (!newMembersList.remove(memberName))
            throw new UnsupportedOperationException("Only reliable asynchronous members can be removed from the group");
        String[] newMembers = newMembersList.toArray(new String[newMembersList.size()]);
        Map<String, String[]> membersGrouping = oldConfig.getMembersGrouping(); //TODO support dynamic addition and removal to groups when relevant (dynamic mirror)
        AsyncSourceGroupConfig newConfig = new AsyncSourceGroupConfig(config.getName(),
                backlogConfig,
                oldConfig.getGroupConsistencyLevelPolicy(),
                filters,
                lifeCycles,
                membersGrouping,
                oldConfig.isNetworkCompressionEnabled(),
                newMembers);

        for (String oldMemberName : newMembers) {
            AsyncChannelConfig oldMemberConfig = oldConfig.getChannelConfig(oldMemberName);
            if (oldMemberConfig != null)
                newConfig.setChannelConfig(oldMemberName, oldMemberConfig);
        }

        return newConfig;
    }

}
