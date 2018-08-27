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

package com.gigaspaces.internal.cluster.node.impl.groups.async;

import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.GroupConsistencyLevelPolicy;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;

import java.util.Map;

@com.gigaspaces.api.InternalApi
public class AsyncSourceGroupConfig
        extends SourceGroupConfig<AsyncChannelConfig> {

    public AsyncSourceGroupConfig(String groupName,
                                  BacklogConfig backlogConfig,
                                  GroupConsistencyLevelPolicy groupConsistencyLevelPolicy,
                                  Map<String, IReplicationChannelDataFilter> filters, Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles, Map<String, String[]> membersGrouping, String[] membersLookupNames) {
        super(groupName, backlogConfig, groupConsistencyLevelPolicy, filters, lifeCycles, membersGrouping, true, membersLookupNames);
    }

    public AsyncSourceGroupConfig(String groupName,
                                  BacklogConfig backlogConfig,
                                  GroupConsistencyLevelPolicy groupConsistencyLevelPolicy,
                                  Map<String, IReplicationChannelDataFilter> filters, Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles, Map<String, String[]> membersGrouping, boolean enableNetworkCompression, String[] membersLookupNames) {
        super(groupName, backlogConfig, groupConsistencyLevelPolicy, filters, lifeCycles, membersGrouping, enableNetworkCompression, membersLookupNames);
    }

    @Override
    public String toString() {
        return "AsyncSourceGroupConfig [" + super.toString() + "]";
    }


}
