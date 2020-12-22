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

import com.gigaspaces.client.ReadByIdsException;
import com.gigaspaces.client.ReadTakeByIdResult;
import com.gigaspaces.client.ReadTakeByIdsException;
import com.gigaspaces.client.TakeByIdsException;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ISpaceProxyTypeManager;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.AbstractIdsQueryPacket;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;

import net.jini.core.transaction.Transaction;

@com.gigaspaces.api.InternalApi
public class ReadTakeByIdsProxyActionInfo extends CommonProxyActionInfo {
    public final Object[] ids;
    public final Object routing;
    public Object[] routings;
    public final String className;
    public final boolean isTake;
    public AbstractIdsQueryPacket queryPacket;

    public ReadTakeByIdsProxyActionInfo(ISpaceProxy spaceProxy, String className, Object[] ids, Object routing, Object[] routings,
                                        Transaction txn, boolean isTake, int modifiers, QueryResultTypeInternal queryResultType,
                                        String[] projections, AbstractProjectionTemplate projectionTemplate) {
        super(txn, modifiers);

        try {
            this.ids = ids;
            this.className = className;
            this.isTake = isTake;
            this.routing = routing;
            this.routings = routings;
            this.queryPacket = initialize(spaceProxy, queryResultType, projections, projectionTemplate);
            if(isTake && queryPacket.isBroadcast())
                throw new IllegalArgumentException("Take by Ids of broadcast table " + queryPacket.getTypeName() + " is not supported");
        } catch (SpaceMetadataException e) {
            ReadTakeByIdResult[] results = new ReadTakeByIdResult[ids.length];
            for (int i = 0; i < results.length; i++)
                results[i] = new ReadTakeByIdResult(ids[i], null, e);

            if (isTake)
                throw new TakeByIdsException(results);
            else
                throw new ReadByIdsException(results);
        }
    }

    @Override
    public ReadTakeByIdsProxyActionInfo clone() {
        ReadTakeByIdsProxyActionInfo copy = (ReadTakeByIdsProxyActionInfo) super.clone();
        copy.queryPacket = (AbstractIdsQueryPacket) queryPacket.clone();
        return copy;
    }

    private AbstractIdsQueryPacket initialize(ISpaceProxy spaceProxy, QueryResultTypeInternal queryResultType, String[] projections, AbstractProjectionTemplate projectionTemplate) {
        if (className == null)
            throw new IllegalArgumentException("Class name cannot be null.");

        ITypeDesc typeDesc = spaceProxy.getDirectProxy().getTypeManager().getTypeDescByName(className);
        if (typeDesc == null) {
            UnknownTypeException ex = new UnknownTypeException("Metadata is unknown. Execute snapshot() before working with desired Object class.", className);
            throw new ProxyInternalSpaceException(ex.getMessage(), ex);
        }

        if (ids == null)
            throw new IllegalArgumentException("IDs array cannot be null.");

        boolean validateIdType = typeDesc.getIdPropertyName() == null || typeDesc.isAutoGenerateId();
        if (routings != null) {
            if (routings.length != ids.length)
                throw new IllegalArgumentException("IDs array and routing array size is not the same.");

            for (int i = 0; i < routings.length; i++) {
                if (ids[i] == null)
                    throw new IllegalArgumentException("Value cannot be null - id[" + i + "].");
                if (validateIdType && !(ids[i] instanceof String))
                    throw new IllegalArgumentException("Auto-generated id must be of type string - id[" + i + "].");
                if (routings[i] == null)
                    throw new IllegalArgumentException("Value cannot be null - routing array at [" + i + "].");
            }
        } else {
            for (int i = 0; i < ids.length; i++) {
                if (ids[i] == null)
                    throw new IllegalArgumentException("Value cannot be null - id[" + i + "].");
                if (validateIdType && !(ids[i] instanceof String))
                    throw new IllegalArgumentException("Auto-generated id must be of type string - id[" + i + "].");
            }
        }

        if (ObjectUtils.equals(typeDesc.getIdPropertyName(), typeDesc.getRoutingPropertyName())) {
            if (routing != null || (routings != null && routings != ids))
                throw new IllegalArgumentException("When the id property is used for routing, the routing argument must be null.");

            if (typeDesc.isAutoGenerateRouting()) {
                routings = new Object[ids.length];
                for (int i = 0; i < ids.length; i++)
                    routings[i] = SpaceUidFactory.extractPartitionId((String) ids[i]);
            } else
                routings = ids;
        }

        validateReadModifiers(spaceProxy);

        if (projections != null && projections.length > 0)
            projectionTemplate = ProjectionTemplate.create(projections, typeDesc);

        // The query packet created is used for single routing & later for converting the returned EntryPackets to Pojos.
        AbstractIdsQueryPacket queryPacket;
        if (routings != null)
            queryPacket = TemplatePacketFactory.createIdsPacket(ids, routings, typeDesc, queryResultType, projectionTemplate);
        else
            queryPacket = TemplatePacketFactory.createIdsPacket(ids, routing, typeDesc, queryResultType, projectionTemplate);

        if (isTake)
            initOperationId(spaceProxy, queryPacket);

        return queryPacket;
    }

    public Object[] convertResults(ISpaceProxy spaceProxy, IEntryPacket[] results, boolean returnPackets, AbstractProjectionTemplate projectionTemplate) {
        return spaceProxy.getDirectProxy().getTypeManager().convertQueryResults(results,
                queryPacket,
                returnPackets,
                projectionTemplate);
    }

    public ReadTakeByIdsException convertResults(ISpaceProxy spaceProxy, ReadTakeByIdsException e, boolean returnPackets, AbstractProjectionTemplate projectionTemplate) {
        ReadTakeByIdResult[] results = e.getResults();

        final ISpaceProxyTypeManager typeManager = spaceProxy.getDirectProxy().getTypeManager();
        for (int i = 0; i < results.length; i++) {
            if (results[i].isError())
                continue;
            IEntryPacket packet = (IEntryPacket) results[i].getObject();
            Object entry = typeManager.convertQueryResult(packet, queryPacket, returnPackets, projectionTemplate);
            results[i] = new ReadTakeByIdResult(results[i].getId(), entry, results[i].getError());
        }

        return e;
    }

    public AbstractProjectionTemplate clearProjectionTemplate() {
        AbstractProjectionTemplate projectionTemplate = queryPacket.getProjectionTemplate();
        if (projectionTemplate != null)
            queryPacket.setProjectionTemplate(null);
        return projectionTemplate;
    }
}
