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

import com.gigaspaces.internal.cluster.node.impl.GroupMapping;
import com.gigaspaces.internal.cluster.node.impl.IReplicationNodeBuilder;
import com.gigaspaces.internal.cluster.node.impl.IReplicationNodePluginFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationDynamicTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationStaticTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncSingleOriginReplicationTargetGroupBuilder;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.filters.ReplicationStatistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Provides configuration needed in order to construct a {@link ReplicationNode}
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNodeConfig {
    private final Map<String, GroupMapping> _sourceGroupsMapping = new HashMap<String, GroupMapping>();

    private final Collection<IReplicationSourceGroupBuilder> _activeSourceGroupsBuilders = new LinkedList<IReplicationSourceGroupBuilder>();
    private final Collection<IReplicationSourceGroupBuilder> _passiveSourceGroupsBuilders = new LinkedList<IReplicationSourceGroupBuilder>();
    private final Collection<IReplicationSourceGroupBuilder> _alwaysSourceGroupsBuilders = new LinkedList<IReplicationSourceGroupBuilder>();

    private final Collection<IReplicationStaticTargetGroupBuilder> _activeStaticTargetGroupsBuilders = new LinkedList<IReplicationStaticTargetGroupBuilder>();
    private final Collection<IReplicationStaticTargetGroupBuilder> _passiveStaticTargetGroupsBuilders = new LinkedList<IReplicationStaticTargetGroupBuilder>();
    private final Collection<IReplicationStaticTargetGroupBuilder> _alwaysStaticTargetGroupsBuilders = new LinkedList<IReplicationStaticTargetGroupBuilder>();

    private final Collection<IReplicationDynamicTargetGroupBuilder> _activeDynamicTargetGroupsBuilders = new LinkedList<IReplicationDynamicTargetGroupBuilder>();
    private final Collection<IReplicationDynamicTargetGroupBuilder> _passiveDynamicTargetGroupsBuilders = new LinkedList<IReplicationDynamicTargetGroupBuilder>();
    private final Collection<IReplicationDynamicTargetGroupBuilder> _alwaysDynamicTargetGroupsBuilders = new LinkedList<IReplicationDynamicTargetGroupBuilder>();

    private IReplicationInFilter _inFilter;

    private IReplicationOutFilter _outFilter;

    private ISpaceCopyReplicaOutFilter _spaceCopyReplicaOutFilter;

    private ISpaceCopyReplicaInFilter _spaceCopyReplicaInFilter;

    private IReplicationNodePluginFacade _pluginFacade;

    public void mapSourceGroups(String packetAddress, GroupMapping groupMapping) {
        _sourceGroupsMapping.put(packetAddress, groupMapping);
    }

    public Map<String, GroupMapping> getSourceGroupsMapping() {
        return _sourceGroupsMapping;
    }

    public void addSourceGroupBuilder(IReplicationSourceGroupBuilder groupBuilder,
                                      ReplicationNodeMode nodeMode) {
        switch (nodeMode) {
            case ACTIVE:
                _activeSourceGroupsBuilders.add(groupBuilder);
                break;
            case PASSIVE:
                _passiveSourceGroupsBuilders.add(groupBuilder);
                break;
            case ALWAYS:
                _alwaysSourceGroupsBuilders.add(groupBuilder);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    public void addTargetGroupBuilder(IReplicationStaticTargetGroupBuilder groupBuilder,
                                      ReplicationNodeMode nodeMode) {
        switch (nodeMode) {
            case ACTIVE:
                _activeStaticTargetGroupsBuilders.add(groupBuilder);
                break;
            case PASSIVE:
                _passiveStaticTargetGroupsBuilders.add(groupBuilder);
                break;
            case ALWAYS:
                _alwaysStaticTargetGroupsBuilders.add(groupBuilder);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    public Collection<IReplicationSourceGroupBuilder> getSourceGroupBuilders(ReplicationNodeMode nodeMode) {
        switch (nodeMode) {
            case ACTIVE:
                return _activeSourceGroupsBuilders;
            case PASSIVE:
                return _passiveSourceGroupsBuilders;
            case ALWAYS:
                return _alwaysSourceGroupsBuilders;
            default:
                throw new IllegalArgumentException();
        }
    }

    public Collection<IReplicationStaticTargetGroupBuilder> getTargetGroupBuilders(ReplicationNodeMode nodeMode) {
        switch (nodeMode) {
            case ACTIVE:
                return _activeStaticTargetGroupsBuilders;
            case PASSIVE:
                return _passiveStaticTargetGroupsBuilders;
            case ALWAYS:
                return _alwaysStaticTargetGroupsBuilders;
            default:
                throw new IllegalArgumentException();
        }
    }

    public void addDynamicTargetGroupBuilder(
            IReplicationDynamicTargetGroupBuilder groupBuilder,
            ReplicationNodeMode nodeMode) {
        switch (nodeMode) {
            case ACTIVE:
                _activeDynamicTargetGroupsBuilders.add(groupBuilder);
                break;
            case PASSIVE:
                _passiveDynamicTargetGroupsBuilders.add(groupBuilder);
                break;
            case ALWAYS:
                _alwaysDynamicTargetGroupsBuilders.add(groupBuilder);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }


    /***
     *  When new partition is introduces by scale out we search for another partition IReplicationDynamicTargetGroupBuilder
     *  and to clone and create a IReplicationDynamicTargetGroupBuilder for the new partition
     *  primary-backup-reliable-async-mirror-
     * @param groupName
     * @param nodeMode
     * @param nodeBuilder
     * @return config builder for the new partition
     */
    public IReplicationDynamicTargetGroupBuilder createMatchingTargetGroupBuilder(
            String groupName, ReplicationNodeMode nodeMode, IReplicationNodeBuilder nodeBuilder) {
        Collection<IReplicationDynamicTargetGroupBuilder> relevantDynList;
        switch (nodeMode) {
            case ACTIVE:
                relevantDynList = _activeDynamicTargetGroupsBuilders;
                break;
            case PASSIVE:
                relevantDynList = _passiveDynamicTargetGroupsBuilders;
                break;
            case ALWAYS:
                relevantDynList = _alwaysDynamicTargetGroupsBuilders;
                break;
            default:
                throw new IllegalArgumentException();
        }

        IReplicationDynamicTargetGroupBuilder matchingBuilder = null;
        String nameWithoutPartitionNumber;
        String newPartitionId;
        Pattern groupNamePattern = Pattern.compile("(.*)-(\\d+)");
        Matcher matcher = groupNamePattern.matcher(groupName);
        if(matcher.matches()){
            nameWithoutPartitionNumber = matcher.group(1);
            newPartitionId = matcher.group(2);
        }else {
            return null;
        }

        Pattern pattern = Pattern.compile("\\("+nameWithoutPartitionNumber+"-\\d+\\)\\|\\("+Pattern.quote("(.*):")+".*\\d+\\)");
        for (IReplicationDynamicTargetGroupBuilder builder : relevantDynList) {
            String originalTemplate = builder.getGroupNameTemplate();
            if(pattern.matcher(originalTemplate).matches()){
                matchingBuilder = builder;
                break;
            }
        }

        if(matchingBuilder != null){
            GroupConfig groupConfig = matchingBuilder.getGroupConfig();
            String[] names = groupConfig.getMembersLookupNames();
            String[] newNames = new String[names.length];
            Pattern memberNamePattern = Pattern.compile("(.*?)(\\d+)(.*)");
            for (int i = 0; i < names.length; i++) {
                Matcher memberMatcher = memberNamePattern.matcher(names[i]);
                if (memberMatcher.matches()) {
                    newNames[i] = memberMatcher.group(1) + newPartitionId + memberMatcher.group(3);
                } else {
                    throw new IllegalArgumentException("couldn't generate new partition member name from " + names[i]);
                }
            }

            Pattern templatePattern = Pattern.compile("\\((.*?)(\\d+)\\)\\|\\((.*?)(\\d+)\\)");
            Matcher templateMatcher = templatePattern.matcher(matchingBuilder.getGroupNameTemplate());
            String newTemplate;
            if(templateMatcher.matches()) {
                newTemplate = "("+templateMatcher.group(1)+newPartitionId+")|"+templateMatcher.group(3)+newPartitionId+")";
            }else {
                throw new IllegalArgumentException("couldn't generate new group name template from " + matchingBuilder.getGroupNameTemplate());
            }

            TargetGroupConfig targetGroupConfig = new TargetGroupConfig("NOT SET",
                    null,
                    ReplicationStatistics.ReplicationMode.MIRROR, newNames);
            ReliableAsyncSingleOriginReplicationTargetGroupBuilder mirrorGroupBuilder = new ReliableAsyncSingleOriginReplicationTargetGroupBuilder(targetGroupConfig);
            mirrorGroupBuilder.setGroupNameTemplate(newTemplate);
            mirrorGroupBuilder.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
            addDynamicTargetGroupBuilder(mirrorGroupBuilder,
                    ReplicationNodeMode.ACTIVE);
            addDynamicTargetGroupBuilder(mirrorGroupBuilder, nodeMode);
            return mirrorGroupBuilder;
        }

        return null;
    }

    public IReplicationDynamicTargetGroupBuilder getMatchingTargetGroupBuilder(
            String groupName, ReplicationNodeMode nodeMode) {
        Collection<IReplicationDynamicTargetGroupBuilder> relevantDynList;
        switch (nodeMode) {
            case ACTIVE:
                relevantDynList = _activeDynamicTargetGroupsBuilders;
                break;
            case PASSIVE:
                relevantDynList = _passiveDynamicTargetGroupsBuilders;
                break;
            case ALWAYS:
                relevantDynList = _alwaysDynamicTargetGroupsBuilders;
                break;
            default:
                throw new IllegalArgumentException();
        }
        for (IReplicationDynamicTargetGroupBuilder builder : relevantDynList) {
            if (groupName.matches(builder.getGroupNameTemplate()))
                return builder;
        }
        return null;
    }

    public void setReplicationInFilter(IReplicationInFilter inFilter) {
        _inFilter = inFilter;
    }

    public void setReplicationOutFilter(IReplicationOutFilter outFilter) {
        _outFilter = outFilter;
    }

    public IReplicationInFilter getReplicationInFilter() {
        return _inFilter;
    }

    public IReplicationOutFilter getReplicationOutFilter() {
        return _outFilter;
    }

    public ISpaceCopyReplicaOutFilter getSpaceCopyReplicaOutFilter() {
        return _spaceCopyReplicaOutFilter;
    }

    public void setSpaceCopyReplicaOutFilter(ISpaceCopyReplicaOutFilter spaceCopyReplicaOutFilter) {
        _spaceCopyReplicaOutFilter = spaceCopyReplicaOutFilter;
    }

    public ISpaceCopyReplicaInFilter getSpaceCopyReplicaInFilter() {
        return _spaceCopyReplicaInFilter;
    }

    public void setSpaceCopyReplicaInFilter(ISpaceCopyReplicaInFilter spaceCopyReplicaInFilter) {
        _spaceCopyReplicaInFilter = spaceCopyReplicaInFilter;
    }

    public void setPluginFacade(IReplicationNodePluginFacade pluginFacade) {
        _pluginFacade = pluginFacade;
    }

    public IReplicationNodePluginFacade getPluginFacade() {
        return _pluginFacade;
    }

    @Override
    public String toString() {
        return "ReplicationNodeConfig [" + StringUtils.NEW_LINE +
                "\t_sourceGroupsMapping=" + _sourceGroupsMapping + StringUtils.NEW_LINE +
                "\t_activeSourceGroupsBuilders=" + printGroupCollection(_activeSourceGroupsBuilders) + StringUtils.NEW_LINE +
                "\t_passiveSourceGroupsBuilders=" + printGroupCollection(_passiveSourceGroupsBuilders) + StringUtils.NEW_LINE +
                "\t_alwaysSourceGroupsBuilders=" + printGroupCollection(_alwaysSourceGroupsBuilders) + StringUtils.NEW_LINE +
                "\t_activeTargetGroupsBuilders=" + printGroupCollection(_activeStaticTargetGroupsBuilders) + StringUtils.NEW_LINE +
                "\t_passiveTargetGroupsBuilders=" + printGroupCollection(_passiveStaticTargetGroupsBuilders) + StringUtils.NEW_LINE +
                "\t_alwaysTargetGroupsBuilders=" + printGroupCollection(_alwaysStaticTargetGroupsBuilders) + StringUtils.NEW_LINE +
                "\t_inFilter=" + _inFilter + StringUtils.NEW_LINE +
                "\t_outFilter=" + _outFilter + StringUtils.NEW_LINE +
                "\t_spaceCopyReplicaOutFilter=" + _spaceCopyReplicaOutFilter + StringUtils.NEW_LINE +
                "\t_spaceCopyReplicaInFilter=" + _spaceCopyReplicaInFilter + StringUtils.NEW_LINE +
                "]";
    }

    private static String printGroupCollection(Collection<?> collection) {
        StringBuilder result = new StringBuilder();
        if (collection.isEmpty())
            return "NONE";
        for (Object object : collection) {
            result.append(StringUtils.NEW_LINE);
            result.append(object);
        }
        return result.toString();
    }

}
