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
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.GroupConsistencyLevelPolicy;

import java.util.HashMap;
import java.util.Map;


@com.gigaspaces.api.InternalApi
public class SourceGroupConfig<T extends SourceChannelConfig>
        extends GroupConfig {

    private final BacklogConfig _backlogConfig;
    private final Map<String, IReplicationChannelDataFilter> _filters;
    private final Map<String, T> _channelsConfig;
    private final Map<String, DynamicSourceGroupMemberLifeCycle> _lifeCycles;
    private final Map<String, String[]> _membersGrouping;
    private final GroupConsistencyLevelPolicy _groupConsistencyLevelPolicy;
    private long _inconsistentStateDelay = 5000; //TODO configurable?
    private int _inconsistentStateRetries = 1;

    public SourceGroupConfig(String groupName, BacklogConfig backlogConfig,
                             String... membersLookupNames) {
        this(groupName, backlogConfig, null,
                new HashMap<String, IReplicationChannelDataFilter>(), new HashMap<String, DynamicSourceGroupMemberLifeCycle>(), new HashMap<String, String[]>(), true, membersLookupNames);
    }

    public SourceGroupConfig(String groupName, BacklogConfig backlogConfig, boolean enableNetworkCompression,
                             String... membersLookupNames) {
        this(groupName, backlogConfig, null,
                new HashMap<String, IReplicationChannelDataFilter>(), new HashMap<String, DynamicSourceGroupMemberLifeCycle>(), new HashMap<String, String[]>(), enableNetworkCompression, membersLookupNames);
    }

    public SourceGroupConfig(String groupName, BacklogConfig backlogConfig, GroupConsistencyLevelPolicy groupConsistencyLevelPolicy, Map<String, IReplicationChannelDataFilter> filters, Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles, Map<String, String[]> membersGrouping, boolean enableNetworkCompression, String... membersLookupNames) {
        super(groupName, enableNetworkCompression, membersLookupNames);
        _backlogConfig = backlogConfig;
        _groupConsistencyLevelPolicy = groupConsistencyLevelPolicy;
        _lifeCycles = lifeCycles == null ? new HashMap<String, DynamicSourceGroupMemberLifeCycle>() : lifeCycles;
        _filters = filters == null ? new HashMap<String, IReplicationChannelDataFilter>() : filters;
        _membersGrouping = membersGrouping == null ? new HashMap<String, String[]>() : membersGrouping;
        _channelsConfig = new HashMap<String, T>();
    }

    public BacklogConfig getBacklogConfig() {
        return _backlogConfig;
    }

    public IReplicationChannelDataFilter getFilter(String memberLookupName) {
        if (_filters == null || !_filters.containsKey(memberLookupName))
            return null;

        return _filters.get(memberLookupName);
    }

    public Map<String, IReplicationChannelDataFilter> getFilters() {
        return _filters;
    }

    public long getInconsistentStateDelay() {
        return _inconsistentStateDelay;
    }

    public void setInconsistentStateDelay(long inconsistentStateDelay) {
        _inconsistentStateDelay = inconsistentStateDelay;
    }

    public int getInconsistentStateRetries() {
        return _inconsistentStateRetries;
    }

    public void setInconsistentStateRetries(int inconsistentStateRetries) {
        _inconsistentStateRetries = inconsistentStateRetries;
    }

    public void setChannelConfig(String memberName, T config) {
        _channelsConfig.put(memberName, config);
    }

    public T getChannelConfig(String memberName) {
        return _channelsConfig.get(memberName);
    }

    public void addMembersGrouping(String groupName, String... memberNames) {
        _membersGrouping.put(groupName, memberNames);
    }

    public Map<String, String[]> getMembersGrouping() {
        return _membersGrouping;
    }

    public DynamicSourceGroupMemberLifeCycle getLifeCycle(
            String memberName) {
        return _lifeCycles.get(memberName);
    }

    public Map<String, DynamicSourceGroupMemberLifeCycle> getLifeCycles() {
        return _lifeCycles;
    }

    public GroupConsistencyLevelPolicy getGroupConsistencyLevelPolicy() {
        return _groupConsistencyLevelPolicy;
    }

    @Override
    public String toString() {
        return "SourceGroupConfig [_backlogConfig=" + _backlogConfig
                + ", _groupConsistencyLevelPolicy=" + _groupConsistencyLevelPolicy
                + ", _filters=" + _filters + ", _lifeCycles=" + _lifeCycles + ", _membersGrouping=" + _membersGrouping
                + ", _channelsConfig=" + _channelsConfig
                + ", _inconsistentStateDelay=" + _inconsistentStateDelay + ", _inconsistentStateRetries="
                + _inconsistentStateRetries + ", " + super.toString() + "]";
    }
}
