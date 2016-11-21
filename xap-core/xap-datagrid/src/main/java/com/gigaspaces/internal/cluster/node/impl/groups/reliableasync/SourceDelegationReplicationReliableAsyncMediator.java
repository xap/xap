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

import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogMemberLimitationConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.NoSuchReplicationMemberException;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.MissingReliableAsyncTargetStateException;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicReliableAsyncSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilterBuilder;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.utils.StringUtils;

import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Delegates synchronization issues to the source in order to get a solution
 *
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class SourceDelegationReplicationReliableAsyncMediator
        implements IReplicationReliableAsyncMediator {

    protected final Logger _specificLogger;

    private final IReplicationConnection _sourceConnection;
    private final IReplicationReliableAsyncGroupBacklog _reliableAsyncGroupBacklog;
    private final DynamicReliableAsyncSourceGroupConfigHolder _groupConfigHolder;
    private final IReplicationRouter _router;
    private final IReplicationChannelDataFilterBuilder _filterBuilder;
    private final IDynamicSourceGroupMemberLifeCycleBuilder _lifeCycleBuilder;

    public SourceDelegationReplicationReliableAsyncMediator(
            DynamicReliableAsyncSourceGroupConfigHolder groupConfigHolder,
            IReplicationConnection sourceConnection,
            IReplicationReliableAsyncGroupBacklog reliableAsyncGroupBacklog,
            String sourceLookupName, String myLookupName,
            IReplicationRouter router,
            IReplicationChannelDataFilterBuilder filterBuilder,
            IDynamicSourceGroupMemberLifeCycleBuilder lifeCycleBuilder) {
        _groupConfigHolder = groupConfigHolder;
        _sourceConnection = sourceConnection;
        _reliableAsyncGroupBacklog = reliableAsyncGroupBacklog;
        _router = router;
        _filterBuilder = filterBuilder;
        _lifeCycleBuilder = lifeCycleBuilder;
        _specificLogger = ReplicationLogUtils.createChannelSpecificLogger(sourceLookupName,
                myLookupName,
                _groupConfigHolder.getConfig()
                        .getName());
    }

    @Override
    public void reliableAsyncSourceAdd(String sourceLookupName,
                                       IReplicationOrderedPacket packet) {
        packet.getData().setWeight(_groupConfigHolder.getConfig().getBacklogConfig().getBackLogWeightPolicy().calculateWeight(packet.getData()));
        _reliableAsyncGroupBacklog.reliableAsyncSourceAdd(sourceLookupName,
                packet);
    }

    @Override
    public void reliableAsyncSourceKeep(String sourceMemberName,
                                        IReplicationOrderedPacket packet) {
        _reliableAsyncGroupBacklog.reliableAsyncSourceKeep(sourceMemberName,
                packet);
    }

    @Override
    public void afterHandshake(IProcessLogHandshakeResponse response) {
        _reliableAsyncGroupBacklog.afterHandshake(response);
    }

    @Override
    public void updateReliableAsyncState(IReliableAsyncState reliableAsyncState, String sourceMemberName) {
        boolean succesfullyUpdated = false;
        do {
            try {
                _reliableAsyncGroupBacklog.updateReliableAsyncState(reliableAsyncState, sourceMemberName);
                succesfullyUpdated = true;
            } catch (MissingReliableAsyncTargetStateException e) {
                String missingMember = e.getMissingMember();
                if (_specificLogger.isLoggable(Level.FINER))
                    _specificLogger.finer("Received a missing replication member exception [" + missingMember + "] when updating async state, removing member from group");
                _groupConfigHolder.removeMember(missingMember);
                _router.getAdmin().removeRemoteStubHolder(missingMember);
            } catch (NoSuchReplicationMemberException e) {
                // We have a missing member, fetch missing data from the
                // replication channel source.
                final String missingMemberName = e.getMissingMemberName();
                if (_specificLogger.isLoggable(Level.FINER))
                    _specificLogger.finer("Received no such replication member exception [" + missingMemberName + "] when updating async state, retrieving member data from source");
                FetchReliableAsyncMissingMemberDataPacket packet = new FetchReliableAsyncMissingMemberDataPacket(_groupConfigHolder.getConfig().getName(),
                        missingMemberName);
                try {
                    ReliableAsyncMemberData memberData = _sourceConnection.dispatch(packet);
                    if (memberData == null) {
                        //This member does not longer exist in the source
                        if (_specificLogger.isLoggable(Level.FINER))
                            _specificLogger.finer("The missing member [" + e.getMissingMemberName() + "] is not present at the source, ignoring current update");
                        return;
                    }
                    BacklogMemberLimitationConfig memberBacklogLimitations = memberData.getMemberBacklogLimitationConfig();
                    String filterName = memberData.getMemberFilterName();
                    Object[] filterConstructArgs = memberData.getMemberFilterConstructArguments();
                    String lifeCycleName = memberData.getMemberLifeCycleName();
                    Object[] lifeCycleConstructArgs = memberData.getMemberLifeCycleConstructArguments();
                    RouterStubHolder routerStub = memberData.getRouterStubHolder();
                    AsyncChannelConfig asyncChannelConfig = memberData.getAsyncChannelConfig();
                    IReplicationChannelDataFilter memberFilter = StringUtils.hasText(filterName) ? _filterBuilder.createFilter(filterName, _groupConfigHolder.getConfig().getName(), filterConstructArgs) : null;
                    DynamicSourceGroupMemberLifeCycle memberLifyCycle = StringUtils.hasText(lifeCycleName) ? _lifeCycleBuilder.createLifeCycle(lifeCycleName, _groupConfigHolder.getConfig().getName(), lifeCycleConstructArgs) : null;

                    //First add the stub to the router
                    if (routerStub != null)
                        _router.getAdmin().addRemoteRouterStub(routerStub);

                    if (_specificLogger.isLoggable(Level.FINE))
                        _specificLogger.fine("Synchronized new reliable async member [" + missingMemberName + "] state into replication group");

                    //Add member to the group, this may set this member last confirmed key to be higher than the actual key
                    //since it will take the current last key in redo log and set it as confirmed, however, this is immediately followed
                    //by updateReliableAsyncState with the correct key which will put the confirmation key.
                    _groupConfigHolder.addMember(missingMemberName,
                            memberFilter,
                            memberBacklogLimitations,
                            asyncChannelConfig,
                            memberLifyCycle);
                } catch (Exception innerE) {
                    if (_specificLogger.isLoggable(Level.WARNING))
                        _specificLogger.log(Level.WARNING,
                                "Failed retrieving missing data for reliable async member ["
                                        + missingMemberName
                                        + "], skipping update reliable async state",
                                innerE);
                    break;
                }
            }
        } while (!succesfullyUpdated);
    }

    @Override
    public IMarker getMarker(IReplicationOrderedPacket packet, String membersGroupName) {
        return _reliableAsyncGroupBacklog.getMarker(packet, membersGroupName);
    }

}
