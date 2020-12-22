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

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;

import net.jini.core.transaction.Transaction;



/**
 * @author GigaSpaces
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeProxyActionInfo extends QueryProxyActionInfo {
    public final long timeout;
    public final boolean ifExists;
    public final boolean isTake;
    public final boolean returnOnlyUids;
    private boolean _returnPacket;

    public ReadTakeProxyActionInfo(ISpaceProxy spaceProxy, Object template, Transaction txn, long timeout, int modifiers, boolean ifExists, boolean isTake) {
        super(spaceProxy, template, txn, modifiers, isTake);
        this.timeout = timeout;
        this.ifExists = ifExists || Modifiers.contains(modifiers, Modifiers.IF_EXISTS);
        this.isTake = isTake;
        this.returnOnlyUids = queryPacket.isReturnOnlyUids();
        if (timeout < 0)
            throw new IllegalArgumentException("timeout parameter must be greater than or equal to zero.");
        validateReadModifiers(spaceProxy);
        if (ReadModifiers.isFifoGroupingPoll(modifiers))
            verifyFifoGroupsCallParams(isTake);
        if (_devLogger.isTraceEnabled()) {
            if (verifyLogScannedEntriesCountParams(this.timeout))
                this.modifiers |= Modifiers.LOG_SCANNED_ENTRIES_COUNT;
        }
        setFifoIfNeeded(spaceProxy);
        if(isTake && queryPacket.isBroadcast())
            throw new IllegalArgumentException("Take of broadcast table " + queryPacket.getTypeName() + "  entry is not supported");
    }

    public ReadTakeProxyActionInfo(ISpaceProxy spaceProxy, String className, Object id, Object routing, int version,
                                   Transaction txn, long timeout, int modifiers, QueryResultTypeInternal resultType,
                                   boolean ifExists, boolean isTake, String[] projections, AbstractProjectionTemplate projectionTemplate) {
        super(spaceProxy, txn, modifiers);
        this.timeout = timeout;
        this.ifExists = ifExists;
        this.isTake = isTake;
        this.returnOnlyUids = false;

        // Validate arguments:
        if (id == null)
            throw new IllegalArgumentException("id cannot be null.");
        if (className == null)
            throw new IllegalArgumentException("className cannot be null.");
        validateReadModifiers(spaceProxy);

        // Get type descriptor:
        ITypeDesc typeDesc = spaceProxy.getDirectProxy().getTypeManager().getTypeDescByName(className);
        if (typeDesc == null) {
            UnknownTypeException ute = new UnknownTypeException(
                    "Metadata is unknown. Execute snapshot() before working with desired Object class.",
                    className);
            throw new ProxyInternalSpaceException(ute.getMessage(), ute);
        }

        if (ObjectUtils.equals(typeDesc.getIdPropertyName(), typeDesc.getRoutingPropertyName())) {
            if (routing != null && !routing.equals(id))
                throw new IllegalArgumentException("routing must be null or same as id if routing property and id property are the same.");
        }

        if (projections != null && projections.length > 0)
            projectionTemplate = ProjectionTemplate.create(projections, typeDesc);
        queryPacket = TemplatePacketFactory.createIdOrUidPacket(typeDesc, resultType, routing, id, version, projectionTemplate);

        if (isTake) {
            initOperationId(spaceProxy, queryPacket);
            if(queryPacket.isBroadcast())
                throw new IllegalArgumentException("Take by id of broadcast table " + queryPacket.getTypeName() + "  entry is not supported");
        }
    }

    public ReadTakeProxyActionInfo(ISpaceProxy spaceProxy, String uid, Transaction txn, int modifiers,
                                   QueryResultTypeInternal resultType, boolean returnPacket, boolean ifExists, boolean isTake) {
        super(spaceProxy, txn, modifiers);
        this.timeout = 0;
        this.ifExists = ifExists;
        this.isTake = isTake;
        this.returnOnlyUids = false;
        this._returnPacket = returnPacket;

        // Validate arguments:
        if (uid == null)
            throw new IllegalArgumentException("uid cannot be null.");
        validateReadModifiers(spaceProxy);

        // If no identifier or autogenerate identifier, id is uid:
        queryPacket = TemplatePacketFactory.createUidPacket(uid, null /*routing*/, 0 /*version*/, resultType);
        if (isTake)
            initOperationId(spaceProxy, queryPacket);
    }

    public boolean isAsync() {
        return false;
    }

    public Object convertQueryResult(ISpaceProxy spaceProxy, IEntryPacket result, AbstractProjectionTemplate projectionTemplate) {
        return spaceProxy.getDirectProxy().getTypeManager().convertQueryResult(result, queryPacket, isReturnPacket(), projectionTemplate);
    }

    public boolean isReturnPacket() {
        return _returnPacket || _query == queryPacket;
    }

    private void verifyFifoGroupsCallParams(boolean isTake) {
        if (txn == null)
            throw new IllegalArgumentException("fifo grouping operation must be under transaction");
        if (!isTake && !(ReadModifiers.isExclusiveReadLock(modifiers)))
            throw new IllegalArgumentException("fifo grouping read operation must be exclusive-read-lock");
    }

}
