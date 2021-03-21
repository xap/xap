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

package com.gigaspaces.internal.transport;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ExternalEntryIntrospector;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.PropertiesQuery;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.AbstractIdsQueryPacket;
import com.j_spaces.core.ExternalTemplatePacket;
import com.j_spaces.core.IdQueryPacket;
import com.j_spaces.core.IdsMultiRoutingQueryPacket;
import com.j_spaces.core.IdsQueryPacket;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.UidQueryPacket;
import com.j_spaces.core.client.ExternalEntry;

import java.io.Externalizable;
import java.util.Map;

/**
 * Created by IntelliJ IDEA. User: assafr Date: 05/08/2008 Time: 17:47:09 To change this template
 * use File | Settings | File Templates.
 */
@com.gigaspaces.api.InternalApi
public class TemplatePacketFactory {
    public static ITemplatePacket createEmptyPacket(ITypeDesc typeDesc) {
        return new EmptyQueryPacket(typeDesc);
    }

    public static ITemplatePacket createIdPacket(Object id, Object routing, int version,
                                                 ITypeDesc typeDesc, QueryResultTypeInternal resultType, AbstractProjectionTemplate projectionTemplate) {
        return new IdQueryPacket(id, routing, version, typeDesc, resultType, projectionTemplate);
    }

    public static AbstractIdsQueryPacket createIdsPacket(Object[] ids, Object routing, ITypeDesc typeDesc, QueryResultTypeInternal resultType, AbstractProjectionTemplate projectionTemplate) {
        return new IdsQueryPacket(ids, routing, typeDesc, resultType, projectionTemplate);
    }

    public static AbstractIdsQueryPacket createIdsPacket(Object[] ids, Object[] routings, ITypeDesc typeDesc, QueryResultTypeInternal resultType, AbstractProjectionTemplate projectionTemplate) {
        return new IdsMultiRoutingQueryPacket(ids, routings, 0, typeDesc, resultType, projectionTemplate);
    }

    public static ITemplatePacket createUidPacket(String uid, int version, boolean returnOnlyUids) {
        return new UidQueryPacket(null, uid, null, version, QueryResultTypeInternal.EXTERNAL_ENTRY, returnOnlyUids, null);
    }

    public static ITemplatePacket createUidPacket(ITypeDesc typeDesc, String uid, int version) {
        return new UidQueryPacket(typeDesc, uid, null, version, QueryResultTypeInternal.EXTERNAL_ENTRY, false, null);
    }

    public static ITemplatePacket createUidPacket(ITypeDesc typeDesc, String uid, Object routing, int version, QueryResultTypeInternal resultType, AbstractProjectionTemplate projectionTemplate) {
        return new UidQueryPacket(typeDesc, uid, routing, version, resultType, false, projectionTemplate);
    }

    public static ITemplatePacket createUidPacket(String uid, Object routing, int version, QueryResultTypeInternal resultType) {
        return new UidQueryPacket(uid, routing, version, resultType);
    }

    public static ITemplatePacket createUidPacket(String uid, int version) {
        return new UidQueryPacket(null, uid, null, version, QueryResultTypeInternal.NOT_SET, false, null);
    }

    public static ITemplatePacket createUidResponsePacket(String uid, int version) {
        return new UidQueryPacket(null, uid, null, version, QueryResultTypeInternal.NOT_SET, true, null);
    }

    public static ITemplatePacket createUidsPacket(String[] uids, QueryResultTypeInternal resultType, boolean returnOnlyUids) {
        return new UidQueryPacket(uids, 0, resultType, returnOnlyUids);
    }

    public static ITemplatePacket createUidsResponsePacket(String[] uids) {
        return new UidQueryPacket(uids, 0, QueryResultTypeInternal.NOT_SET, true);
    }

    public static ITemplatePacket createIdOrUidPacket(
            final ITypeDesc typeDesc, final QueryResultTypeInternal resultType,
            Object routing, Object id, int version, AbstractProjectionTemplate projectionTemplate) {
        // Translate classname + id to uid:
        // If no identifier or autogenerate identifier, id is uid:
        if (typeDesc.getIdPropertyName() == null || typeDesc.isAutoGenerateId()) {
            if (!(id instanceof String))
                throw new IllegalArgumentException("id must be a String if no SpaceID or SpaceID with AutoGenerate set to true.");
            // Create uid template:
            String uid = (String) id;
            if (typeDesc.isAutoGenerateRouting())
                routing = SpaceUidFactory.extractPartitionId(uid);
            return TemplatePacketFactory.createUidPacket(typeDesc, uid, routing, version, resultType, projectionTemplate);
        }

        return TemplatePacketFactory.createIdPacket(id, routing, version, typeDesc, resultType, projectionTemplate);
    }

    public static <T> ITemplatePacket createFromObject(T entry, ITypeDesc typeDesc, EntryType entryType) {
        final ITypeIntrospector<T> introspector = typeDesc.getIntrospector(entryType);
        Map<String, Object> dynamicProperties = introspector.getDynamicProperties(entry);
        if (!typeDesc.supportsDynamicProperties() && dynamicProperties != null && !dynamicProperties.isEmpty()) {
            final String propertyName = (String) dynamicProperties.keySet().toArray()[0];
            String message = "Cannot access dynamic property '" + propertyName + "' in type '" + typeDesc.getTypeName() + "' - this type does not support dynamic properties.";
            if (typeDesc.getTypeName().equals(Object.class.getName()))
                message += " If you're using SpaceDocument make sure the type name was properly set.";
            throw new SpaceMetadataException(message);
        }

        if (entryType.isConcrete() && typeDesc.isExternalizable()) {
            ICustomQuery customQuery = dynamicProperties == null ? null : new PropertiesQuery(dynamicProperties, typeDesc);
            return new ExternalizableTemplatePacket(typeDesc, entryType, (Externalizable) entry, customQuery);
        }

        final Object[] fixedProperties = introspector.getSerializedValues(entry);
        String uid = introspector.getUID(entry, true, true);
        if (entryType == EntryType.EXTERNAL_ENTRY && uid == null)
            uid = ExternalEntryIntrospector.getUid(typeDesc, fixedProperties);
        final int version = introspector.getVersion(entry);
        final long timeToLive = introspector.getTimeToLive(entry);
        final boolean isTransient = introspector.isTransient(entry);

        switch (entryType) {
            case OBJECT_JAVA:
            case DOCUMENT_JAVA:
                final ICustomQuery customQuery = dynamicProperties == null ? null : new PropertiesQuery(dynamicProperties, typeDesc);
                return new TemplatePacket(typeDesc, entryType, fixedProperties, customQuery,
                        uid, version, timeToLive, isTransient);
            case EXTERNAL_ENTRY:
                return new ExternalTemplatePacket(typeDesc, entryType, fixedProperties,
                        uid, version, timeToLive, isTransient, (ExternalEntry) entry);
            default:
                throw new IllegalArgumentException("Unsupported entry type: " + entryType);
        }
    }

    public static ITemplatePacket createFullPacket(IEntryHolder entryHolder, OperationID operationID) {
        final long timeToLive = entryHolder.getEntryData().getTimeToLive(true);
        final IEntryData entryData = entryHolder.getEntryData();

        return createFullPacket(entryHolder, entryData, entryData.getFixedPropertiesValues(),
                entryHolder.getUID(), timeToLive, operationID);
    }

    public static ITemplatePacket createFullPacket(ITemplateHolder template,
                                                   String uid, long timeToLive, OperationID operationId) {
        return createFullPacket(template, template, uid, timeToLive, operationId);
    }

    public static ITemplatePacket createFullPacket(IEntryHolder entryHolder, ITemplateHolder template,
                                                   String uid, long timeToLive, OperationID operationId) {
        IEntryData entryData = entryHolder.getEntryData();
        return createFullPacket(entryHolder, entryData, entryData.getFixedPropertiesValues(), uid, timeToLive, operationId);
    }

    public static ITemplatePacket createFullPacket(IEntryHolder entryHolder, ITemplateHolder template, String uid, long timeToLive,
                                                   IEntryData entryData, OperationID operationID) {
        return createFullPacket(entryHolder, entryData, entryData.getFixedPropertiesValues(), uid, timeToLive, operationID);
    }

    public static ITemplatePacket createFullPacketForReplication(NotifyTemplateHolder template, OperationID operationID) {
        final IEntryData entryData = template.getEntryData();
        final long timeToLive = entryData.getTimeToLive(true);
        ITemplatePacket generationTemplate = template.getGenerationTemplate().clone();

        //Work around to support empty packet and not break backward competability 
        if (generationTemplate instanceof EmptyQueryPacket)
            generationTemplate = createFullPacket(template, entryData, entryData.getFixedPropertiesValues(),
                    template.getUidToOperateBy(), timeToLive, operationID);

        generationTemplate.setTTL(timeToLive);

        return generationTemplate;
    }

    private static ITemplatePacket createFullPacket(IEntryHolder entryHolder, IEntryData entryData,
                                                    Object[] fixedProperties, String uid, long timeToLive, OperationID operationId) {
        ITemplatePacket packet = createFullPacket(entryData.getEntryTypeDesc().getTypeDesc(), fixedProperties, entryData.getDynamicProperties(),
                uid, entryData.getVersion(), timeToLive, entryHolder.isTransient(), entryData.getEntryTypeDesc().getEntryType());
        packet.setOperationID(operationId);
        return packet;
    }

    public static ITemplatePacket createFullPacket(IEntryData entryData, String uid, OperationID operationId, boolean isTransient) {
        ITemplatePacket packet = createFullPacket(entryData.getEntryTypeDesc().getTypeDesc(),
                entryData.getFixedPropertiesValues(),
                entryData.getDynamicProperties(),
                uid,
                entryData.getVersion(),
                entryData.getTimeToLive(true),
                isTransient,
                entryData.getEntryTypeDesc().getEntryType());
        packet.setOperationID(operationId);
        return packet;
    }


    public static ITemplatePacket createFullPacket(IEntryPacket entryPacket) {
        ITemplatePacket templatePacket = createFullPacket(
                entryPacket.getTypeDescriptor(),
                entryPacket.getFieldValues(),
                entryPacket.getDynamicProperties(),
                entryPacket.getUID(),
                entryPacket.getVersion(),
                entryPacket.getTTL(),
                entryPacket.isTransient(),
                entryPacket.getEntryType());

        templatePacket.setOperationID(entryPacket.getOperationID());
        return templatePacket;
    }

    public static ITemplatePacket createFullPacket(IEntryPacket entryPacket, ITypeDesc typeDesc, boolean isTransient) {
        return createFullPacket(typeDesc, entryPacket.getFieldValues(), entryPacket.getDynamicProperties(),
                null /*uid*/, 0 /*version*/, 0 /*timeToLive*/, isTransient, entryPacket.getEntryType());
    }

    private static ITemplatePacket createFullPacket(ITypeDesc typeDesc, Object[] fixedProperties, Map<String, Object> dynamicProperties,
                                                    String uid, int version, long timeToLive, boolean isTransient, EntryType entryType) {
        if (entryType == null)
            entryType = typeDesc.getObjectType();

        switch (entryType) {
            case OBJECT_JAVA:
            case DOCUMENT_JAVA:
            case OBJECT_DOTNET:
            case DOCUMENT_DOTNET:
            case CPP:
                final ICustomQuery customQuery = dynamicProperties == null ? null : new PropertiesQuery(dynamicProperties, typeDesc);
                if (typeDesc.isExternalizable() && entryType.isConcrete())
                    return new ExternalizableTemplatePacket(typeDesc, entryType, fixedProperties, customQuery,
                            uid, version, timeToLive, isTransient);
                return new TemplatePacket(typeDesc, entryType, fixedProperties, customQuery,
                        uid, version, timeToLive, isTransient);
            case EXTERNAL_ENTRY:
                return new ExternalTemplatePacket(typeDesc, entryType, fixedProperties,
                        uid, version, timeToLive, isTransient, null);
            default:
                throw new IllegalArgumentException("Unsupported packet type: " + entryType);
        }
    }


}
