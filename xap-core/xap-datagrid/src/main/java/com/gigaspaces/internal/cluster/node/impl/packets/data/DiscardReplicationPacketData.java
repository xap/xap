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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.filters.DiscardReplicationFilterEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class DiscardReplicationPacketData extends AbstractReplicationPacketSingleEntryData {
    private static final long serialVersionUID = 6511736282520215603L;

    public DiscardReplicationPacketData() {
        super(false);
    }

    private static final DiscardReplicationPacketData PACKET = new DiscardReplicationPacketData();

    public static final DiscardReplicationPacketData instance() {
        return PACKET;
    }

    public void execute(IReplicationInContext context,
                        IReplicationInFacade inReplicationHandler, ReplicationPacketDataMediator dataMediator) throws Exception {
        //Do nothing but if we are in batch, mark the current as consumed
        if (context.isBatchContext())
            ((IReplicationInBatchContext) context).currentConsumed();
    }

    public boolean beforeDelayedReplication() {
        return true;
    }

    @Override
    public IEntryData getMainEntryData() {
        return null;
    }

    @Override
    public IEntryData getSecondaryEntryData() {
        return null;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
    }

    @Override
    public boolean supportsReplicationFilter() {
        return false;
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
    }

    public String getUid() {
        //This packet has no uid, it is just used for discard purposes
        return null;
    }

    @Override
    protected IReplicationFilterEntry toFilterEntry(
            SpaceTypeManager spaceTypeManager) {
        return DiscardReplicationFilterEntryData.ENTRY;
    }

    public ReplicationSingleOperationType getOperationType() {
        return ReplicationSingleOperationType.DISCARD;
    }

    public int getOrderCode() {
        throw new IllegalStateException();
    }

    @Override
    public String toString() {
        return "DISCARDED";
    }

    public boolean isTransient() {
        return false;
    }

    @Override
    public boolean filterIfNotPresentInReplicaState() {
        return false;
    }

    @Override
    public boolean containsFullEntryData() {
        return true;
    }

}
