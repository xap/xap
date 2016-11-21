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

package com.gigaspaces.internal.cluster.node;

import com.gigaspaces.cluster.replication.ConsistencyLevelViolationException;
import com.gigaspaces.cluster.replication.RedoLogCapacityExceededException;
import com.gigaspaces.internal.cluster.node.impl.backlog.OperationWeightInfo;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouterAdmin;
import com.j_spaces.core.filters.ReplicationStatistics;

import java.util.concurrent.TimeUnit;


/**
 * Provide administration capabilities of an {@link IReplicationNode}
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationNodeAdmin {
    /**
     * Attempts to flush all pending replication from the backlog The upper layer is in charge of
     * not inserting new data to the replication node once flush is started.
     *
     * @return true if all was flushed successfully during the specified timeout, false otherwise.
     */
    boolean flushPendingReplication(long timeout, TimeUnit units);

    void monitorState(OperationWeightInfo info) throws RedoLogCapacityExceededException, ConsistencyLevelViolationException;

    // TODO to be implementation specific statistics
    ReplicationStatistics getStatistics();

    Object[] getStatus();

    void setNodeStateListener(IReplicationNodeStateListener listener);

    /**
     * Sets the administrated {@link IReplicationNode} as active mode (i.e containing space is not a
     * backup)
     */
    void setActive();

    /**
     * Sets the administrated {@link IReplicationNode} as passive mode (i.e containing space is
     * backup)
     */
    void setPassive();

    String dumpState();

    void clearStaleReplicas(long expirationTime);

    /**
     * @since 8.0.5
     */
    IReplicationRouterAdmin getRouterAdmin();

    /**
     * @since 8.0.5
     */
    DynamicSourceGroupConfigHolder getSourceGroupConfigHolder(String groupName);

}
