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

package com.gigaspaces.internal.client.spaceproxy.actioninfo;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.kernel.SystemProperties;

import net.jini.core.transaction.Transaction;

@com.gigaspaces.api.InternalApi
public class WriteProxyActionInfo extends CommonProxyActionInfo {
    public final Object entry;
    public final IEntryPacket entryPacket;
    public final long lease;
    public final long timeout;

    private final static boolean oneWaySystemProperty = Boolean.getBoolean(SystemProperties.ONE_WAY_WRITE);

    public WriteProxyActionInfo(ISpaceProxy spaceProxy, Object entry, Transaction txn, long lease, long timeout, int modifiers) {
        super(txn, spaceProxy.initWriteModifiers(modifiers));

        if (entry == null)
            throw new IllegalArgumentException("write() operation cannot accept null entry.");
        if (lease < 0)
            throw new IllegalArgumentException("lease cannot be less than zero");

        this.entry = entry;
        this.lease = lease;
        this.timeout = timeout;
        if (timeout < 0)
            throw new IllegalArgumentException("timeout parameter must be greater than or equal to zero.");

        ObjectType objectType = ObjectType.fromObject(entry);
        entryPacket = toEntryPacket(spaceProxy, entry, objectType);

        if (UpdateModifiers.isPartialUpdate(modifiers) && entryPacket.getDynamicProperties() != null)
            throw new UnsupportedOperationException("Partial update is not supported for dynamic properties.");

        if (oneWaySystemProperty)
            modifiers |= Modifiers.ONE_WAY;

        final boolean oneWay = Modifiers.contains(modifiers, Modifiers.ONE_WAY);
        if (oneWay) {
            if (txn != null || spaceProxy.getContextTransaction() != null)
                throw new UnsupportedOperationException("Oneway write is not supported when the write is being done under a transaction.");
            if (UpdateModifiers.isReturnPrevOnUpdate(modifiers))
                throw new UnsupportedOperationException("Oneway write is not supported with return previous value on update.");
        }
        if (entryPacket.getTypeDescriptor().isBroadcast()) {
            if (txn != null || spaceProxy.getContextTransaction() != null)
                throw new UnsupportedOperationException("Write of broadcast table entry is not supported when the write is being done under a transaction.");
        }
    }

    public LeaseContext<?> convertWriteResult(IDirectSpaceProxy spaceProxy, LeaseContext<?> result) {
        return spaceProxy.getTypeManager().convertWriteOrUpdateResult(result, entry, entryPacket, modifiers);
    }

    public boolean isUpdate() {
        if (UpdateModifiers.isUpdateOnly(modifiers))
            return true;
        if (UpdateModifiers.isWriteOnly(modifiers))
            return false;
        return entryPacket.getUID() != null;
    }
}
