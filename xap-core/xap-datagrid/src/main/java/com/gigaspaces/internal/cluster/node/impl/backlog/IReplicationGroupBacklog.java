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

package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.cluster.replication.IRedoLogStatistics;
import com.gigaspaces.cluster.replication.RedoLogCapacityExceededException;
import com.gigaspaces.cluster.replication.ReplicationException;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder.IDynamicSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.metrics.MetricRegistrator;

import java.util.List;
import java.util.logging.Logger;


/**
 * Holds the replication backlog of a single {@link IReplicationSourceGroup}
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationGroupBacklog extends IDynamicSourceGroupStateListener {
    IBacklogHandshakeRequest getHandshakeRequest(String memberName, Object customBacklogMetadata);

    IHandshakeContext processHandshakeResponse(String memberName,
                                               IBacklogHandshakeRequest request, IProcessLogHandshakeResponse response, PlatformLogicalVersion targetLogicalVersion, Object customBacklogMetadata);

    IHandshakeIteration getNextHandshakeIteration(String memberName,
                                                  IHandshakeContext handshakeContext);

    long size(String memberName);

    long size();

    void clearReplicated();

    IBacklogMemberState getState(String memberName);

    void processResult(String memberName, IProcessResult result,
                       List<IReplicationOrderedPacket> packets)
            throws ReplicationException;

    void processResult(String memberName, IProcessResult result,
                       IReplicationOrderedPacket packet) throws ReplicationException;

    /**
     * @since 8.0.1
     */
    void setPendingError(String memberName, Throwable error,
                         List<IReplicationOrderedPacket> replicatedPackets);

    /**
     * @since 8.0.1
     */
    void setPendingError(String memberName, Throwable error,
                         IReplicationOrderedPacket replicatedPacket);

    /**
     * @since 9.0
     */
    void setPendingError(String memberName, Throwable error,
                         IIdleStateData idleStateData);

    List<IReplicationOrderedPacket> getPackets(String memberName, int maxSize,
                                               IReplicationChannelDataFilter filter,
                                               PlatformLogicalVersion targetMemberVersion, Logger logger);

    void beginSynchronizing(String memberName);

    void beginSynchronizing(String memberName, boolean isDirectPersistencySync);

    boolean synchronizationDataGenerated(String memberName, String uid);

    void synchronizationCopyStageDone(String memberName);

    void synchronizationDone(String memberName);

    void stopSynchronization(String memberName);

    /**
     * Returns a marker of the current last position of the backlog
     *
     * @param memberName the member we wish this marker will be attached to
     */
    IMarker getCurrentMarker(String memberName);

    /**
     * Returns a marker of the current unconfirmed packet of this given member
     *
     * @param memberName the member we wish this marker will be attached to
     */
    IMarker getUnconfirmedMarker(String memberName);

    IMarker getMarker(IReplicationOrderedPacket packet, String membersGroupName);

    /**
     * @return the data producer this group backlog is using to generate replication data
     */
    IReplicationPacketDataProducer getDataProducer();

    IReplicationOrderedPacket replaceWithDiscarded(
            IReplicationOrderedPacket packet, boolean forceDiscard);

    /**
     * Merge an already existing discarded packet with a new packet that should be discarded
     *
     * @param previousDiscardedPacket the already existing discarded packet which is a result {@link
     *                                #replaceWithDiscarded(IReplicationOrderedPacket, boolean)}
     * @param mergedPacket            the new packet that needs to be discarded and merged into the
     *                                existing discarded packet
     * @param memberName              TODO
     * @return return true if merge is successful, false otherwise
     */
    boolean mergeWithDiscarded(IReplicationOrderedPacket previousDiscardedPacket,
                               IReplicationOrderedPacket mergedPacket, String memberName);

    /**
     * Called once a new member addition is known to the available keepers
     *
     * @param memberName new member name
     */
    void makeMemberConfirmedOnAll(String memberName);

    boolean supportDiscardMerge();

    IRedoLogStatistics getStatistics();

    void monitor(OperationWeightInfo info) throws RedoLogCapacityExceededException;

    String toLogMessage(String memberName);

    String dumpState();

    void close();

    void setGroupHistory(IReplicationGroupHistory groupHistory);

    void setStateListener(IReplicationBacklogStateListener stateListener);

    IProcessResult fromWireForm(Object wiredProcessResult);

    /**
     * Gets a specific packet inside the backlog
     *
     * @since 9.0
     */
    IReplicationOrderedPacket getSpecificPacket(long packetKey);

    IIdleStateData getIdleStateData(String memberName, PlatformLogicalVersion targetMemberVersion);

    void processIdleStateDataResult(String memberName, IProcessResult result,
                                    IIdleStateData idleStateData) throws ReplicationException;

    void registerWith(MetricRegistrator metricRegister);

    long getConfirmed(String memberName);

    void writeLock();

    void freeWriteLock();

    long getWeight();

    long getWeight(String memberName);

    void increaseWeight(String memberName, long weight);

    void decreaseWeight(String memberName, long fromKey, long toKey);
}
