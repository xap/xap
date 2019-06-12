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

package com.gigaspaces.internal.server.space.executors;


import com.gigaspaces.internal.cluster.node.IReplicationNodeAdmin;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogMemberLimitationConfig;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationDynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationReplicationChannelDataFilter;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateHolderFactory;
import com.gigaspaces.internal.space.requests.RegisterReplicationNotificationRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.RegisterReplicationNotificationResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.filters.FilterManager;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceRegisterReplicationNotificationExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        RegisterReplicationNotificationRequestInfo requestInfo = (RegisterReplicationNotificationRequestInfo) spaceRequestInfo;

        RegisterReplicationNotificationResponseInfo responseInfo = new RegisterReplicationNotificationResponseInfo();
        final IReplicationNodeAdmin replicationNodeAdmin = space.getEngine().getReplicationNode().getAdmin();

        // Register the view stub in the space replication node:
        replicationNodeAdmin.getRouterAdmin().addRemoteRouterStub(requestInfo.viewStub);

        String groupName = space.getEngine().generateGroupName();
        DynamicSourceGroupConfigHolder groupConfig = replicationNodeAdmin.getSourceGroupConfigHolder(groupName);

        final SpaceContext spaceContext = spaceRequestInfo.getSpaceContext();

        IServerTypeDesc typeDesc;

        try {
            typeDesc = space.getEngine().getTypeManager().loadServerTypeDesc(requestInfo.template);
            // If security is enabled verify read privileges for template type
            space.assertAuthorizedForType(typeDesc.getTypeName(), SpaceAuthority.SpacePrivilege.READ, spaceContext);
        } catch (UnusableEntryException e) {
            responseInfo.exception = e;
            return responseInfo;
        } catch (UnknownTypeException e) {
            responseInfo.exception = e;
            return responseInfo;
        } catch (SecurityException e) {
            responseInfo.exception = e;
            return responseInfo;
        }

        FilterManager filterManager = space.getEngine().getFilterManager();
        NotifyTemplateHolder tHolder = TemplateHolderFactory.createNotifyTemplateHolder(typeDesc,
                requestInfo.template,
                requestInfo.template.getUID(),
                LeaseManager.toAbsoluteTime(Lease.FOREVER),
                requestInfo.eventId,
                requestInfo.notifyInfo,
                false /* isFifo */);

        String uniqueName = requestInfo.viewStub.getMyEndpointDetails().getLookupName();
        groupConfig.removeMember(uniqueName);

        NotificationReplicationChannelDataFilter notificationFilter = new NotificationReplicationChannelDataFilter(
                space.getEngine().getCacheManager(),
                groupName,
                space.getEngine().getTemplateScanner().getRegexCache(),
                space.getEngine().getTypeManager(),
                filterManager,
                tHolder);

        DynamicSourceGroupMemberLifeCycle notificationsLifeCycle = new NotificationDynamicSourceGroupMemberLifeCycle(
                space,
                filterManager,
                spaceContext);

        BacklogMemberLimitationConfig memberBacklogLimitations = new BacklogMemberLimitationConfig();

        long limit = space.getClusterPolicy().getReplicationPolicy().getDurableNotificationMaxRedologCapacity();
        memberBacklogLimitations.setLimit(limit, LimitReachedPolicy.DROP_MEMBER);
        long notificationMaxDisconnectionTime = space.getClusterPolicy().getReplicationPolicy().getDurableNotificationMaxDisconnectionTime();

        AsyncChannelConfig config = new AsyncChannelConfig(requestInfo.notifyInfo.getBatchSize(),
                requestInfo.notifyInfo.getBatchTime(),
                requestInfo.notifyInfo.getBatchPendingThreshold(),
                ReplicationMode.DURABLE_NOTIFICATION,
                notificationMaxDisconnectionTime);
        config.setTag(requestInfo.tag); //optional (may be null)

        groupConfig.addMember(uniqueName, notificationFilter, memberBacklogLimitations, config, notificationsLifeCycle);

        responseInfo.spaceUID = space.getSpaceUuid();

        return responseInfo;
    }
}
