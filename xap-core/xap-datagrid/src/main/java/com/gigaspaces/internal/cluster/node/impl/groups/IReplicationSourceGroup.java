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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.cluster.replication.ConsistencyLevelViolationException;
import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.metrics.MetricRegistrator;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A replication source group consists of one or more target members that shares the same data that
 * needs to be replicated, each member is being replicated to using a dedicated {@link
 * IReplicationSourceChannel}. Each target member is receiving and processing the replication by a
 * corresponding {@link IReplicationTargetGroup}.
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationSourceGroup {
    /**
     * Creates a single entry holder operation replication data from the given properties and add it
     * to the provided context
     *
     * @param replicationContext context to attach the replication data to
     * @param entryHolder        the entry holder data
     * @param operationType      the operation type
     */
    void beforeExecute(ReplicationOutContext replicationContext, IEntryHolder entryHolder, ReplicationSingleOperationType operationType);

    /**
     * Create a single type descriptor operation replication data and add it to provided context
     */
    void beforeExecuteGeneric(ReplicationOutContext replicationContext, Object operationData, ReplicationSingleOperationType operationType);

    /**
     * Creates transaction replication data and adds it to the provided context
     */
    void beforeTransactionExecute(ReplicationOutContext replicationContext,
                                  ServerTransaction transaction,
                                  ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType);

    /**
     * Executes the replication specified by the provided group context
     *
     * @return true iff the replication ended.
     */
    int execute(IReplicationGroupOutContext groupContext);

    void execute(IReplicationUnreliableOperation operation);

    /**
     * Close this replication group, once closed it can no longer be used
     */
    void close();

    /**
     * @return true is an underlying channel to the specified member is connected
     */
    boolean checkChannelConnected(String sourceMemberLookupName);

    ReplicationEndpointDetails getChannelEndpointDetails(String sourceMemberLookupName);

    /**
     * Signal this source group that one of its channel is now used for space synchronization
     * replica
     *
     * @param synchronizingMemberLookupName the channel target name that is used for
     *                                      synchronization
     */
    void beginSynchronizing(String synchronizingMemberLookupName, Object synchronizingSourceUniqueId);

    void beginSynchronizing(String synchronizingMemberLookupName, Object synchronizingSourceUniqueId, boolean isDirectPersistencySync);

    /**
     * During synchronization process, notify the channel specified by the target name, that a sync
     * data was generated in order for it to know to filter older data when replicating to target
     */
    boolean synchronizationDataGenerated(String synchronizingMemberLookupName, String uid);

    /**
     * During synchronization process, notify the channel specified by the target name that the copy
     * iteration process is done in order for the filtering mechanism to know that any following
     * packets occurred completely after the synchronization copy stage.
     */
    void synchronizationCopyStageDone(String synchronizingMemberLookupName);

    /**
     * Signal this source group that one of its channel synchronization state is aborted {@link
     * #beginSynchronizing(String, Object)}
     */
    void stopSynchronization(String synchronizingMemberLookupName);

    /**
     * @return group name
     */
    String getGroupName();

    /**
     * @return underlying group backlog
     */
    IReplicationGroupBacklog getGroupBacklog();

    Map<String, Boolean> getChannelsStatus();

    /**
     * Monitors sla state of the replication group
     *
     * @throws ConsistencyLevelViolationException if the sla is breached
     * @since 9.5
     */
    void monitorConsistencyLevel() throws ConsistencyLevelViolationException;

    IReplicationSourceGroupStatistics getStatistics();

    void setActive(MetricRegistrator metricRegistrator);

    void setPassive();

    boolean flushPendingReplication(long timeout, TimeUnit units);

    void sampleStatistics();

    String dumpState();

    DynamicSourceGroupConfigHolder getConfigHolder();

    /**
     * Creates a temporary channel to an existing member of this group, if a channel is already
     * created then this method does nothing This method should be called when there is a need to
     * create a temporary channel from this group to the target group to perform some operation and
     * should not be used for a persistent channel
     *
     * @param memberName group member to create the channel to
     * @since 9.0
     */
    void createTemporaryChannel(String memberName, Object customBacklogMetadata);

    /**
     * Closes a temporary created channel
     *
     * @see #createTemporaryChannel(String, Object)
     * @since 9.0
     */
    void closeTemporaryChannel(String sourceMemberName);

    void registerWith(MetricRegistrator metricRegister);
}
