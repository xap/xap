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

package com.gigaspaces.internal.cluster.node.impl.packets.data.errors;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.packets.data.AbstractDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.DiscardReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFixFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IExecutableReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataConsumer;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.logging.Level;


@com.gigaspaces.api.InternalApi
public class ResurrectEntryFix
        extends AbstractDataConsumeFix {
    private static final long serialVersionUID = 1L;
    private IEntryPacket _entryPacket;

    public ResurrectEntryFix() {
    }

    public ResurrectEntryFix(IEntryPacket entryPacket) {
        _entryPacket = entryPacket;
    }

    @Override
    public IExecutableReplicationPacketData<?> fix(IReplicationInContext context,
                                                   IDataConsumeFixFacade fixFacade,
                                                   ReplicationPacketDataConsumer consumer, IExecutableReplicationPacketData<?> data) throws Exception {
        try {
            fixFacade.write(_entryPacket);

            return DiscardReplicationPacketData.instance();
        } catch (EntryAlreadyInSpaceException e) {
            //If reached here than this must be active active topology and this object have
            //inserted to this space during the extend lease error processing (which was originally
            //generated when this object was not in space)
            if (context.getContextLogger().isLoggable(Level.SEVERE)) {
                context.getContextLogger().severe("Detected conflicting WRITE"
                        + " operation on entry - "
                        + "<"
                        + _entryPacket.getTypeName() + ">"
                        + " uid=<"
                        + _entryPacket.getUID()
                        + ">\n"
                        + "  Ignoring the conflicting operation since it has already been applied to space.\n"
                        + "  Please make sure that the entry was not simultaneously changed in two different space instances.");
            }
            return DiscardReplicationPacketData.instance();
        }
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _entryPacket = IOUtils.readObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _entryPacket);
    }

    @Override
    public String toString() {
        return "ResurrectEntryFix - " + _entryPacket;
    }

}
