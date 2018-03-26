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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.logging.Level;


/**
 * A fix that is returned as a fix of two errors {@link UnknownEntryLeaseConsumeResult}, {@link
 * UnknownNotifyTemplateLeaseConsumeResult}. This fix actually cause a skip of the extend lease
 * packet since the lease is also unknown at the source, it prints a warning as well
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class UnknownLeaseErrorUnfixed
        extends AbstractDataConsumeFix {
    private static final long serialVersionUID = 1L;
    private String _className;
    private String _uid;

    public UnknownLeaseErrorUnfixed() {
    }

    public UnknownLeaseErrorUnfixed(String className, String uid) {
        _className = className;
        _uid = uid;
    }

    @Override
    public IExecutableReplicationPacketData<?> fix(IReplicationInContext context,
                                                   IDataConsumeFixFacade fixFacade,
                                                   ReplicationPacketDataConsumer consumer,
                                                   IExecutableReplicationPacketData<?> data) {
        if (context.getContextLogger().isLoggable(Level.WARNING)) {
            context.getContextLogger().log(Level.WARNING, "Failed to renew Entry lease: "
                    + _className + " UID: " + _uid + "."
                    + "\nThis entry might be already expired or canceled.");
        }
        return DiscardReplicationPacketData.instance();
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _className = IOUtils.readString(in);
        _uid = IOUtils.readString(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _className);
        IOUtils.writeString(out, _uid);
    }

    @Override
    public String toString() {
        return "UnknownLeaseErrorUnfixed - ClassName=" + _className + " UID=" + _uid;
    }

}
