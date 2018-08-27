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
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogMemberLimitationConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Holds a source group config which allows dynamic atomic updates to the group config and notify
 * listener once such a change is done. Any change in the config needs to first copy the original
 * config and update the config once the updated configuration is fully constructed.
 *
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class DynamicSourceGroupConfigHolder {
    public static interface IDynamicSourceGroupStateListener {

        void memberAdded(MemberAddedEvent memberAddedParam, SourceGroupConfig newConfig);

        void memberRemoved(String memberName, SourceGroupConfig newConfig);

    }

    private volatile SourceGroupConfig _config;
    private volatile SourceGroupConfig _configWithMemberBeingAdded;
    private final Set<IDynamicSourceGroupStateListener> _listeners = new HashSet<IDynamicSourceGroupStateListener>();

    public DynamicSourceGroupConfigHolder(SourceGroupConfig config) {
        _config = config;
        _configWithMemberBeingAdded = null;
    }

    public SourceGroupConfig getConfig() {
        return _config;
    }

    public SourceGroupConfig getConfigWithMemberBeingAdded() {
        return _configWithMemberBeingAdded;
    }

    public void addListener(IDynamicSourceGroupStateListener listener) {
        _listeners.add(listener);
    }

    private void updateConfigWithNewMember(
            SourceGroupConfig newConfig,
            String memberName,
            Object customData,
            BacklogMemberLimitationConfig memberBacklogLimitations,
            IReplicationChannelDataFilter filter,
            DynamicSourceGroupMemberLifeCycle lifeCycle) {

        final MemberAddedEvent memberAddedEvent = new MemberAddedEvent(memberName, customData, memberBacklogLimitations, filter, lifeCycle);
        triggerBeforeMemberAdded(memberAddedEvent);

        //Keep the new config before addition because reliable async
        //addition notification may want this
        _configWithMemberBeingAdded = newConfig;
        try {
            //first notify about the member addition
            for (IDynamicSourceGroupStateListener listener : _listeners) {
                listener.memberAdded(memberAddedEvent, newConfig);
            }
            //once event is sent update the configuration
            _config = newConfig;

            triggerAfterMemberAdded(memberAddedEvent);
        } finally {
            _configWithMemberBeingAdded = null;
        }
    }

    private void updateConfigWithRemovedMember(SourceGroupConfig newConfig,
                                               String memberName) {

        DynamicSourceGroupMemberLifeCycle lifeCycle = _config.getLifeCycle(memberName);

        //first remove the member from the configuration
        _config = newConfig;

        try {
            // now notify the listener (order is opposite to addition of the member to preserve consistent state)
            for (IDynamicSourceGroupStateListener listener : _listeners) {
                listener.memberRemoved(memberName, newConfig);
            }
        } finally {
            triggerAfterMemberRemoved(memberName, lifeCycle);
        }
    }

    private void triggerBeforeMemberAdded(MemberAddedEvent memberAddedEvent) {
        if (memberAddedEvent.getLifeCycle() != null)
            memberAddedEvent.getLifeCycle().beforeMemberAdded(memberAddedEvent);
    }

    private void triggerAfterMemberAdded(MemberAddedEvent memberAddedEvent) {
        if (memberAddedEvent.getLifeCycle() != null)
            memberAddedEvent.getLifeCycle().afterMemberAdded(memberAddedEvent);
    }

    private void triggerAfterMemberRemoved(String memberName,
                                           DynamicSourceGroupMemberLifeCycle lifeCycle) {
        if (lifeCycle != null)
            lifeCycle.afterMemberRemoved(new MemberRemovedEvent(memberName));
    }

    public synchronized void addMember(
            String memberName,
            IReplicationChannelDataFilter filter,
            BacklogMemberLimitationConfig memberBacklogLimitations,
            Object metadata,
            DynamicSourceGroupMemberLifeCycle lifeCycle) {
        SourceGroupConfig config = getConfig();

        if (Arrays.asList(config.getMembersLookupNames()).contains(memberName))
            throw new IllegalStateException("Cannot add an already existing member ["
                    + memberName + "]");
        //Update backlog config with new member settings
        BacklogConfig backlogConfig = copyAndApplyBacklogUpdate(memberName,
                memberBacklogLimitations,
                config);
        //Add channel filter
        Map<String, IReplicationChannelDataFilter> filters = copyAndApplyFilterUpdate(memberName,
                filter,
                config);

        // Add channel lifecycle
        Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles = copyAndApplyLifeCycleUpdate(memberName,
                lifeCycle,
                config);


        //Generate new config
        SourceGroupConfig newConfig = generateUpdatedConfig(memberName,
                config,
                backlogConfig,
                filters,
                lifeCycles,
                metadata);
        copyAndApplyCommonProperties(config, newConfig);

        //update config and notify member added
        updateConfigWithNewMember(newConfig, memberName, metadata, memberBacklogLimitations, filter, lifeCycle);
    }

    public synchronized void removeMember(String memberName) {
        SourceGroupConfig config = getConfig();
        if (!Arrays.asList(config.getMembersLookupNames()).contains(memberName))
            return;

        //Update backlog config with new member settings
        BacklogConfig backlogConfig = copyAndRemoveBacklogUpdate(memberName,
                config);
        //Remove channel filter
        Map<String, IReplicationChannelDataFilter> filters = copyAndRemoveFilterUpdate(memberName,
                config);

        // Remove channel life cycle
        Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles = copyAndRemoveLifeCycleUpdate(memberName,
                config);

        //Remove member from new config
        SourceGroupConfig newConfig = generateNewConfigWithoutMember(memberName,
                backlogConfig,
                filters,
                lifeCycles,
                config);
        copyAndApplyCommonProperties(config, newConfig);

        //update config and notify member removed
        updateConfigWithRemovedMember(newConfig, memberName);

    }

    private Map<String, DynamicSourceGroupMemberLifeCycle> copyAndRemoveLifeCycleUpdate(
            String memberName, SourceGroupConfig config) {
        Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles = new HashMap<String, DynamicSourceGroupMemberLifeCycle>(config.getLifeCycles());
        lifeCycles.remove(memberName);
        return lifeCycles;
    }

    protected SourceGroupConfig generateNewConfigWithoutMember(String memberName,
                                                               BacklogConfig backlogConfig,
                                                               Map<String, IReplicationChannelDataFilter> filters,
                                                               Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles,
                                                               SourceGroupConfig config) {
        List<String> newMembersList = new ArrayList<String>((Arrays.asList(config.getMembersLookupNames())));
        if (!newMembersList.remove(memberName))
            throw new UnsupportedOperationException("Only reliable asynchronous members can be removed from the group");
        String[] newMembers = newMembersList.toArray(new String[newMembersList.size()]);
        SourceGroupConfig newConfig = new SourceGroupConfig(config.getName(),
                backlogConfig,
                config.getGroupConsistencyLevelPolicy(),
                filters,
                lifeCycles,
                config.getMembersGrouping(),
                config.isNetworkCompressionEnabled(),
                newMembers);
        return newConfig;
    }

    protected SourceGroupConfig generateUpdatedConfig(String memberName,
                                                      SourceGroupConfig config, BacklogConfig backlogConfig,
                                                      Map<String, IReplicationChannelDataFilter> filters,
                                                      Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles,
                                                      Object customMetadata) {
        String[] newMembers = Arrays.copyOf(config.getMembersLookupNames(), config.getMembersLookupNames().length + 1);
        newMembers[newMembers.length - 1] = memberName;
        SourceGroupConfig newConfig = new SourceGroupConfig(config.getName(),
                backlogConfig,
                config.getGroupConsistencyLevelPolicy(),
                filters,
                lifeCycles,
                config.getMembersGrouping(),
                config.isNetworkCompressionEnabled(),
                newMembers);
        return newConfig;
    }

    private void copyAndApplyCommonProperties(SourceGroupConfig config,
                                              SourceGroupConfig newConfig) {
        newConfig.setHistoryLength(config.getHistoryLength());
        newConfig.setInconsistentStateDelay(config.getInconsistentStateDelay());
        newConfig.setInconsistentStateRetries(config.getInconsistentStateRetries());
    }

    private Map<String, IReplicationChannelDataFilter> copyAndApplyFilterUpdate(
            String memberName, IReplicationChannelDataFilter filter,
            SourceGroupConfig config) {
        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>(config.getFilters());
        if (filter != null)
            filters.put(memberName, filter);
        return filters;
    }

    private Map<String, DynamicSourceGroupMemberLifeCycle> copyAndApplyLifeCycleUpdate(
            String memberName, DynamicSourceGroupMemberLifeCycle lifeCycle,
            SourceGroupConfig config) {
        Map<String, DynamicSourceGroupMemberLifeCycle> lifeCycles = new HashMap<String, DynamicSourceGroupMemberLifeCycle>(config.getLifeCycles());
        if (lifeCycle != null)
            lifeCycles.put(memberName, lifeCycle);
        return lifeCycles;
    }

    private Map<String, IReplicationChannelDataFilter> copyAndRemoveFilterUpdate(
            String memberName, SourceGroupConfig config) {
        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>(config.getFilters());
        filters.remove(memberName);
        return filters;
    }

    private BacklogConfig copyAndApplyBacklogUpdate(String memberName,
                                                    BacklogMemberLimitationConfig memberLimitationConfig, SourceGroupConfig config) {
        BacklogConfig backlogConfig = config.getBacklogConfig().clone();
        backlogConfig.setMemberBacklogLimitation(memberName, memberLimitationConfig);
        return backlogConfig;
    }


    private BacklogConfig copyAndRemoveBacklogUpdate(String memberName,
                                                     SourceGroupConfig config) {
        BacklogConfig backlogConfig = config.getBacklogConfig().clone();
        backlogConfig.removeMemberConfig(memberName);
        return backlogConfig;
    }

    @Override
    public String toString() {
        return _config.toString();
    }
}
