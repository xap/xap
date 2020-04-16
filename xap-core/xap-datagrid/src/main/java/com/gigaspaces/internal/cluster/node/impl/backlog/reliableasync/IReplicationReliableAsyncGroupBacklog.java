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

package com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.NoSuchReplicationMemberException;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface IReplicationReliableAsyncGroupBacklog extends IReplicationGroupBacklog {

    public class KeeperMemberState {
        public final String memberName;
        public boolean cannotBypass;

        public KeeperMemberState(String memberName) {
            this.memberName = memberName;
        }
    }

    void add(ReliableAsyncReplicationGroupOutContext groupContext,
             IEntryHolder entryHolder,
             ReplicationSingleOperationType operationType);

    void addGeneric(ReliableAsyncReplicationGroupOutContext groupContext,
                    Object operationData,
                    ReplicationSingleOperationType operationType);

    void addTransaction(ReliableAsyncReplicationGroupOutContext groupContext,
                        ServerTransaction transaction,
                        ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType);

    void reliableAsyncSourceAdd(String sourceMemberName, IReplicationOrderedPacket packet);

    /**
     * Keep an already existing packet which was previously processed or will not be processed by
     * the corresponding process log
     */
    void reliableAsyncSourceKeep(String sourceMemberName, IReplicationOrderedPacket packet);

    IReliableAsyncState getEntireReliableAsyncState();

    IReliableAsyncState getReliableAsyncState(String targetMemberName);

    void updateReliableAsyncState(IReliableAsyncState reliableAsyncState, String sourceMemberName) throws NoSuchReplicationMemberException, MissingReliableAsyncTargetStateException;

    /**
     * Gets the next packet for the specified async member that is part of a reliable async group
     *
     * @param keeperMembersState the state of the keeper sync member
     */
    List<IReplicationOrderedPacket> getReliableAsyncPackets(String memberName,
                                                            int maxSize, KeeperMemberState[] keeperMembersState,
                                                            IReplicationChannelDataFilter dataFilter,
                                                            PlatformLogicalVersion targetMemberVersion, Logger logger);

    /**
     * Called after handshake is done with the keeper target group
     */
    void afterHandshake(IProcessLogHandshakeResponse handshakeResponse);

}
