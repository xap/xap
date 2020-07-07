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

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.config.GroupConfig;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationDynamicTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationStaticTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;


@com.gigaspaces.api.InternalApi
public class ReliableAsyncSingleOriginReplicationTargetGroupBuilder
        implements IReplicationStaticTargetGroupBuilder,
        IReplicationDynamicTargetGroupBuilder {

    private final TargetGroupConfig _groupConfig;
    private IReplicationReliableAsyncProcessLogBuilder _processLogBuilder;
    private String _groupNameTemplate;

    public ReliableAsyncSingleOriginReplicationTargetGroupBuilder(
            TargetGroupConfig groupConfig) {
        _groupConfig = groupConfig;
    }

    public void setGroupNameTemplate(String groupNameTemplate) {
        _groupNameTemplate = groupNameTemplate;
    }

    public void setProcessLogBuilder(
            IReplicationReliableAsyncProcessLogBuilder processLogBuilder) {
        _processLogBuilder = processLogBuilder;
    }

    public IReplicationTargetGroup createStaticGroup(
            IReplicationRouter replicationRouter,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationInFilter inFilter,
            IReplicationTargetGroupStateListener stateListener) {
        return new ReliableAsyncSingleOriginReplicationTargetGroup(_groupConfig,
                replicationRouter,
                inFilter,
                _processLogBuilder,
                replicationInFacade,
                exceptionHandlerBuilder,
                stateListener);
    }

    public String getGroupNameTemplate() {
        return _groupNameTemplate;
    }

    public IReplicationTargetGroup createDynamicGroup(
            String groupName,
            IReplicationRouter replicationRouter,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandlerBuilder exceptionHandlerBuilder,
            IReplicationInFilter inFilter,
            IReplicationTargetGroupStateListener stateListener) {
        TargetGroupConfig targetGroupConfig = _groupConfig.duplicate(groupName);
        return new ReliableAsyncSingleOriginReplicationTargetGroup(targetGroupConfig,
                replicationRouter,
                inFilter,
                _processLogBuilder,
                replicationInFacade,
                exceptionHandlerBuilder,
                stateListener);
    }

    @Override
    public GroupConfig getGroupConfig() {
        return this._groupConfig;
    }

    @Override
    public String toString() {
        return "ReliableAsyncReplicationTargetGroupBuilder [_groupConfig="
                + _groupConfig + ", _processLogBuilder=" + _processLogBuilder
                + "]";
    }

}
