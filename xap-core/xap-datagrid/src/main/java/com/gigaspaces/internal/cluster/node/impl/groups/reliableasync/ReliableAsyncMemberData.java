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

import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogMemberLimitationConfig;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class ReliableAsyncMemberData implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private BacklogMemberLimitationConfig _memberBacklogLimitationConfig;
    private RouterStubHolder _routerStubHolder;
    private String _memberFilterName;
    private Object[] _memberFilterConstructionArgument;
    private String _memberLifeCycleName;
    private Object[] _memberLifeCycleConstructionArguements;
    private AsyncChannelConfig _asyncChannelConfig;


    public ReliableAsyncMemberData() {
    }

    public ReliableAsyncMemberData(
            BacklogMemberLimitationConfig memberBacklogLimitationConfig,
            IReplicationChannelDataFilter memberFilter,
            DynamicSourceGroupMemberLifeCycle memberLifeCycle,
            RouterStubHolder routerStubHolder,
            AsyncChannelConfig asyncChannelConfig) {
        _memberBacklogLimitationConfig = memberBacklogLimitationConfig;
        _routerStubHolder = routerStubHolder;
        _asyncChannelConfig = asyncChannelConfig;
        if (memberFilter != null) {
            _memberFilterName = memberFilter.getClass().getName();
            _memberFilterConstructionArgument = memberFilter.getConstructionArgument();
        }
        if (memberLifeCycle != null) {
            _memberLifeCycleName = memberLifeCycle.getClass().getName();
            _memberLifeCycleConstructionArguements = memberLifeCycle.getConstructionArguments();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        _memberBacklogLimitationConfig.writeExternal(out);
        IOUtils.writeObject(out, _routerStubHolder);
        IOUtils.writeString(out, _memberFilterName);
        IOUtils.writeObjectArray(out, _memberFilterConstructionArgument);
        IOUtils.writeObject(out, _asyncChannelConfig);
        IOUtils.writeString(out, _memberLifeCycleName);
        IOUtils.writeObjectArray(out, _memberLifeCycleConstructionArguements);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _memberBacklogLimitationConfig = new BacklogMemberLimitationConfig();
        _memberBacklogLimitationConfig.readExternal(in);
        _routerStubHolder = IOUtils.readObject(in);
        _memberFilterName = IOUtils.readString(in);
        _memberFilterConstructionArgument = IOUtils.readObjectArray(in);
        _asyncChannelConfig = IOUtils.readObject(in);
        _memberLifeCycleName = IOUtils.readString(in);
        _memberLifeCycleConstructionArguements = IOUtils.readObjectArray(in);
    }

    public BacklogMemberLimitationConfig getMemberBacklogLimitationConfig() {
        return _memberBacklogLimitationConfig;
    }

    public RouterStubHolder getRouterStubHolder() {
        return _routerStubHolder;
    }

    public String getMemberFilterName() {
        return _memberFilterName;
    }

    public Object[] getMemberFilterConstructArguments() {
        return _memberFilterConstructionArgument;
    }

    public AsyncChannelConfig getAsyncChannelConfig() {
        return _asyncChannelConfig;
    }

    public Object[] getMemberLifeCycleConstructArguments() {
        return _memberLifeCycleConstructionArguements;
    }

    public String getMemberLifeCycleName() {
        return _memberLifeCycleName;
    }

}
