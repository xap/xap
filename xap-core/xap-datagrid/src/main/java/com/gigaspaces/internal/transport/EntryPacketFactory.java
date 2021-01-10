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
import com.gigaspaces.internal.document.DocumentObjectConverterInternal;
import com.gigaspaces.internal.metadata.*;
import com.gigaspaces.internal.server.storage.*;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.core.ExternalEntryPacket;
import com.j_spaces.core.LocalCacheResponseEntryPacket;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.Modifiers;

import java.io.Externalizable;
import java.util.Map;

/**
 * a factory for IEntryPacket that depends on the type of the request.
 *
 * @author asy ronen
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class EntryPacketFactory {
    /**
     * Creates an entry packet from an object. Used on proxy writes/updates and EDS.
     */
    public static <T> IEntryPacket createFromObject(T entry, ITypeDesc typeDesc, EntryType entryType, boolean ignoreAutoGenerateUid) {
        if (entryType.isConcrete() && typeDesc.isExternalizable())
            return new ExternalizableEntryPacket(typeDesc, entryType, (Externalizable) entry);

        final ITypeIntrospector<T> introspector = typeDesc.getIntrospector(entryType);
        final Object[] fixedProperties = introspector.getSerializedValues(entry);

        Map<String, Object> dynamicProperties = introspector.getDynamicProperties(entry);
        if (!typeDesc.supportsDynamicProperties() && dynamicProperties != null && !dynamicProperties.isEmpty()) {
            final String propertyName = (String) dynamicProperties.keySet().toArray()[0];
            String message = "Cannot access dynamic property '" + propertyName + "' in type '" + typeDesc.getTypeName() + "' - this type does not support dynamic properties.";
            if (typeDesc.getTypeName().equals(Object.class.getName()))
                message += " If you're using SpaceDocument make sure the type name was properly set.";
            throw new SpaceMetadataException(message);
        }

        String uid = introspector.getUID(entry, false, ignoreAutoGenerateUid);
        if (uid == null && entryType == EntryType.EXTERNAL_ENTRY)
            uid = ExternalEntryIntrospector.getUid(typeDesc, fixedProperties);
        if (typeDesc.getClassBinaryStorageAdapter() != null) {
            return new HybridEntryPacket(typeDesc, entryType, dynamicProperties, uid,
                    introspector.getVersion(entry),
                    introspector.getTimeToLive(entry),
                    introspector.isTransient(entry), new HybridPropertiesHolder(typeDesc, fixedProperties));
        } else {
            return new EntryPacket(typeDesc, entryType, fixedProperties, dynamicProperties, uid,
                    introspector.getVersion(entry),
                    introspector.getTimeToLive(entry),
                    introspector.isTransient(entry));
        }
    }

    public static IEntryPacket createFullPacket(IEntryData entryData, OperationID operationID, String uid, boolean isTransient, QueryResultTypeInternal queryResultType) {
        final long timeToLive = entryData.getTimeToLive(true);
        IEntryPacket entryPacket;
        if (entryData.isHybrid() || entryData.isView())  {
            entryPacket = createInternalHybrid(null /*template*/, isTransient, entryData, uid, timeToLive, queryResultType,
                    false, getPropertiesHolder(null, entryData));
        } else {
            entryPacket = createInternal(null /*template*/, isTransient, entryData, entryData.getFixedPropertiesValues(),
                    uid, timeToLive, queryResultType, false);
        }
        entryPacket.setOperationID(operationID);
        return entryPacket;
    }

    public static IEntryPacket createFullPacketForReplication(IEntryHolder entryHolder, OperationID operationID) {
        final IEntryData entryData = entryHolder.getEntryData();
        final long timeToLive = entryData.getTimeToLive(true);
        IEntryPacket entryPacket;
        if (entryData.isHybrid() || entryData.isView())  {
            entryPacket = createHybrid(null /*template*/, entryHolder.isTransient(), entryData, entryHolder.getUID(), timeToLive,
                    true, getPropertiesHolder(null, entryData));
        } else {
            entryPacket = create(null /*template*/, entryHolder.isTransient(), entryData, entryData.getFixedPropertiesValues(), entryHolder.getUID(), timeToLive, true);
        }
        entryPacket.setOperationID(operationID);
        return entryPacket;
    }

    public static IEntryPacket createPartialUpdatePacketForReplication(IEntryHolder entryHolder, OperationID operationID, boolean[] partialUpdatedValuesIndicators) {
        final IEntryData entryData = entryHolder.getEntryData();
        final long timeToLive = entryData.getTimeToLive(true);
        final Object[] fixedProperties = getPartialUpdateFieldValues(entryData, partialUpdatedValuesIndicators);
        IEntryPacket entryPacket;
        if (entryData.isHybrid()) {
            entryPacket = createHybrid(null /*template*/, entryHolder.isTransient(), entryData,
                    entryHolder.getUID(), timeToLive, true, new HybridPropertiesHolder(entryData.getEntryTypeDesc().getTypeDesc(), fixedProperties));
        } else {
            entryPacket = create(null /*template*/, entryHolder.isTransient(), entryData, fixedProperties,
                    entryHolder.getUID(), timeToLive, true);
        }

        entryPacket.setOperationID(operationID);
        return entryPacket;
    }

    public static IEntryPacket createFullPacketForReplication(IEntryHolder entryHolder, ITemplateHolder template, String uid, long timeToLive) {
        IEntryData entryData = entryHolder.getEntryData();
        if (entryData.isHybrid()) {
            return createHybrid(template, entryHolder.isTransient(), entryData, uid, timeToLive, true, getPropertiesHolder(null, entryData));
        } else {
            return create(template, entryHolder.isTransient(), entryData, entryData.getFixedPropertiesValues(), uid, timeToLive, true);
        }
    }

    public static IEntryPacket createFullPacket(IEntryHolder entryHolder, ITemplateHolder template, String uid, long timeToLive,
                                                IEntryData entryData, OperationID operationId) {
        IEntryPacket packet;
        if (entryData.isHybrid() || entryData.isView())  {
            packet = createHybrid(template, entryHolder.isTransient(), entryData, uid, timeToLive, false, getPropertiesHolder(null, entryData));
        } else {
            packet = create(template, entryHolder.isTransient(), entryData, entryData.getFixedPropertiesValues(), uid, timeToLive, false);
        }
        packet.setOperationID(operationId);
        return packet;
    }

    public static IEntryPacket createFullPacket(Context context, IEntryHolder entry, ITemplateHolder template) {
        return createFullPacket(context, template, entry.getEntryData(), entry.getUID(), entry.isTransient());
    }

    public static IEntryPacket createFullPacket(Context context, IEntryHolder entry, ITemplateHolder template, IEntryData entryData) {
        return createFullPacket(context, template, entryData, entry.getUID(), entry.isTransient());
    }

    public static IEntryPacket createFullPacket(Context context, IEntryHolder entry, ITemplateHolder template, String uid) {
        return createFullPacket(context, template, entry.getEntryData(), uid, entry.isTransient());
    }

    public static IEntryPacket createFullPacket(Context context, ITemplateHolder template, IEntryData entryData, String uid, boolean isTransient) {
        if (entryData.getEntryDataType() == EntryDataType.USER_TYPE) {
            if(entryData instanceof ViewPropertiesEntryData){
                return new LocalCacheResponseEntryPacket((UserTypeEntryData) ((ViewPropertiesEntryData) entryData).getEntry(), uid);
            }else {
                return new LocalCacheResponseEntryPacket((UserTypeEntryData) entryData, uid);
            }
        }
        final long timeToLive = entryData.getTimeToLive(false);
        if (entryData.isHybrid() || entryData.isView()) {
            return createHybrid(template, isTransient, entryData, uid, timeToLive, false, getPropertiesHolder(context, entryData));
        }else {
            return create(template, isTransient, entryData, entryData.getFixedPropertiesValues(), uid, timeToLive, false);
        }
    }

    private static IEntryPacket create(ITemplateHolder template, boolean isTransient, IEntryData entryData, Object[] fixedProperties,
                                       String uid, long timeToLive, boolean forceNonExternalizable) {
        return createInternal(template, isTransient, entryData, fixedProperties, uid, timeToLive, QueryResultTypeInternal.NOT_SET, forceNonExternalizable);
    }

    public static IEntryPacket createRemovePacketForPersistency(IEntryHolder entryHolder, OperationID operationID) {
        final IEntryData entryData = entryHolder.getEntryData();
        final Object[] fieldValues = getPartialFieldValuesForPersistency(entryData);

        IEntryPacket entryPacket = createFullPacketForReplication(entryHolder, operationID);
        entryPacket.setFieldsValues(fieldValues);

        return entryPacket;
    }

    private static IEntryPacket createInternal(ITemplateHolder template, boolean isTransient, IEntryData entryData, Object[] fixedProperties,
                                               String uid, long timeToLive, QueryResultTypeInternal packetType, boolean forceNotExternalizable) {
        final ITypeDesc typeDesc = entryData.getEntryTypeDesc().getTypeDesc();
        final EntryType entryType = entryData.getEntryTypeDesc().getEntryType();

        if (template != null)
            packetType = template.getQueryResultType();

        boolean isReturnWeaklyTypeProperties = template != null &&
                (Modifiers.contains(template.getOperationModifiers(), Modifiers.RETURN_STRING_PROPERTIES) || Modifiers.contains(template.getOperationModifiers(), Modifiers.RETURN_DOCUMENT_PROPERTIES));
        if (packetType == QueryResultTypeInternal.NOT_SET)
            packetType = QueryResultTypeInternal.fromEntryType(entryType);

        switch (packetType) {
            case OBJECT_JAVA:
            case DOCUMENT_ENTRY:
                if (!forceNotExternalizable && typeDesc.isExternalizable() && entryType.isConcrete() && !isReturnWeaklyTypeProperties)
                    return new ExternalizableEntryPacket(typeDesc, entryType, fixedProperties, entryData.getDynamicProperties(),
                            uid, entryData.getVersion(), timeToLive, isTransient);
                return new EntryPacket(typeDesc, entryType, fixedProperties, entryData.getDynamicProperties(),
                        uid, entryData.getVersion(), timeToLive, isTransient);
            case EXTERNAL_ENTRY:
                final String eeImplClassName = template != null ? template.getExternalEntryImplClassName() : null;
                return new ExternalEntryPacket(typeDesc, entryType, fixedProperties,
                        uid, entryData.getVersion(), timeToLive, isTransient, eeImplClassName);

            case OBJECT_DOTNET:
            case CPP:
            case PBS_OLD:
                return new PbsEntryPacket(typeDesc, entryType, fixedProperties, entryData.getDynamicProperties(),
                        uid, entryData.getVersion(), timeToLive, isTransient);

            case DOCUMENT_DOTNET:
                Map<String, Object> dynamicProperties = entryData.getDynamicProperties();
                if (entryType != EntryType.DOCUMENT_DOTNET && entryType != EntryType.OBJECT_DOTNET) {
                    fixedProperties = DocumentObjectConverterInternal.instance().convertNonPrimitiveFixedPropertiesToDocuments(fixedProperties, typeDesc);
                    dynamicProperties = DocumentObjectConverterInternal.instance().convertNonPrimitiveDynamicPropertiesToDocuments(dynamicProperties);
                }
                return new PbsEntryPacket(typeDesc, entryType, fixedProperties, dynamicProperties, uid,
                        entryData.getVersion(), timeToLive, isTransient);

            default:
                throw new UnsupportedOperationException("Unsupported reply packet type: " + packetType);
        }
    }

    private static Object[] getPartialUpdateFieldValues(IEntryData entryData, boolean[] partialUpdatedValuesIndicators) {
        Object[] fieldValues = entryData.getFixedPropertiesValues();
        if (fieldValues != null && fieldValues.length > 0 && partialUpdatedValuesIndicators != null) {
            Object[] partialFieldValues = new Object[fieldValues.length];
            for (int i = 0; i < partialUpdatedValuesIndicators.length; i++)
                if (!partialUpdatedValuesIndicators[i])
                    partialFieldValues[i] = fieldValues[i];

            fieldValues = partialFieldValues;
        }
        return fieldValues;
    }

    private static Object[] getPartialFieldValuesForPersistency(IEntryData entryData) {
        Object[] fieldValues = entryData.getFixedPropertiesValues();
        if (fieldValues != null && fieldValues.length > 0) {
            Object[] newValues = new Object[fieldValues.length];
            ITypeDesc typeDesc = entryData.getEntryTypeDesc().getTypeDesc();
            final int idIndex = typeDesc.getIdentifierPropertyId();
            // special handling for non-pojo(entries/metadataentries)
            // instead ID use the first index
            final boolean useFirstIndex = idIndex == -1;
            final PropertyInfo[] properties = typeDesc.getProperties();

            for (int i = 0; i < fieldValues.length; ++i) {
                final boolean isPrimitive = ObjectUtils.isPrimitive(properties[i].getTypeName());
                final boolean isIdField = !useFirstIndex && i == idIndex;
                final boolean isFirstIndex = useFirstIndex && typeDesc.getIndexedPropertyID(i) == 0;
                // We do not reset primitive fields
                // We do not reset id field or first index in case there's no id
                if (isPrimitive || isIdField || isFirstIndex)
                    newValues[i] = fieldValues[i];
            }
            fieldValues = newValues;
        }
        return fieldValues;
    }


    private static IEntryPacket createHybrid(ITemplateHolder template, boolean isTransient, IEntryData entryData,
                                             String uid, long timeToLive, boolean forceNonExternalizable, HybridPropertiesHolder propertiesHolder) {
        return createInternalHybrid(template, isTransient, entryData, uid, timeToLive, QueryResultTypeInternal.NOT_SET, forceNonExternalizable, propertiesHolder);
    }

    private static IEntryPacket createInternalHybrid(ITemplateHolder template, boolean isTransient, IEntryData entryData,
                                                     String uid, long timeToLive, QueryResultTypeInternal packetType, boolean forceNotExternalizable, HybridPropertiesHolder propertiesHolder) {
        final ITypeDesc typeDesc = entryData.getEntryTypeDesc().getTypeDesc();
        final EntryType entryType = entryData.getEntryTypeDesc().getEntryType();

        if (template != null)
            packetType = template.getQueryResultType();

        boolean isReturnWeaklyTypeProperties = template != null &&
                (Modifiers.contains(template.getOperationModifiers(), Modifiers.RETURN_STRING_PROPERTIES) || Modifiers.contains(template.getOperationModifiers(), Modifiers.RETURN_DOCUMENT_PROPERTIES));
        if (packetType == QueryResultTypeInternal.NOT_SET)
            packetType = QueryResultTypeInternal.fromEntryType(entryType);

        switch (packetType) {
            case OBJECT_JAVA:
            case DOCUMENT_ENTRY:
                if (!forceNotExternalizable && typeDesc.isExternalizable() && entryType.isConcrete() && !isReturnWeaklyTypeProperties) {
                    return new ExternalizableEntryPacket(typeDesc, entryType, propertiesHolder.getFixedProperties(typeDesc), entryData.getDynamicProperties(),
                            uid, entryData.getVersion(), timeToLive, isTransient);
                }
                return new HybridEntryPacket(typeDesc, entryType, entryData.getDynamicProperties(), uid, entryData.getVersion(), timeToLive, isTransient, propertiesHolder);
            case EXTERNAL_ENTRY:
                final String eeImplClassName = template != null ? template.getExternalEntryImplClassName() : null;
                return new ExternalEntryPacket(typeDesc, entryType, propertiesHolder.getFixedProperties(typeDesc), uid, entryData.getVersion(), timeToLive, isTransient, eeImplClassName);

            case OBJECT_DOTNET:
            case CPP:
            case PBS_OLD:
                return new PbsEntryPacket(typeDesc, entryType,
                        propertiesHolder.getFixedProperties(typeDesc),
                        entryData.getDynamicProperties(),
                        uid, entryData.getVersion(), timeToLive, isTransient);

            case DOCUMENT_DOTNET:
                Object[] fixedProperties = propertiesHolder.getFixedProperties(typeDesc);
                Map<String, Object> dynamicProperties = entryData.getDynamicProperties();
                if (entryType != EntryType.DOCUMENT_DOTNET && entryType != EntryType.OBJECT_DOTNET) {
                    fixedProperties = DocumentObjectConverterInternal.instance().convertNonPrimitiveFixedPropertiesToDocuments(fixedProperties, typeDesc);
                    dynamicProperties = DocumentObjectConverterInternal.instance().convertNonPrimitiveDynamicPropertiesToDocuments(dynamicProperties);
                }
                return new PbsEntryPacket(typeDesc, entryType, fixedProperties, dynamicProperties, uid,
                        entryData.getVersion(), timeToLive, isTransient);

            default:
                throw new UnsupportedOperationException("Unsupported reply packet type: " + packetType);
        }
    }

    private static HybridPropertiesHolder getPropertiesHolder(Context context, IEntryData entryData) {
        HybridPropertiesHolder holder;
        if (entryData.isView()) {
            return ((ViewPropertiesEntryData) entryData).getPropertiesHolder();
        }

        if(!entryData.isHybrid()){
            throw new IllegalStateException("trying to create property holder for non hybrid entry data : "+entryData);
        }

        if (context != null && context.getViewEntryData() != null && context.getViewEntryData().isViewOf(entryData)) {
            return context.getViewEntryData().getPropertiesHolder();
        }

        return new HybridPropertiesHolder(entryData.getEntryTypeDesc().getTypeDesc(),
                    ((HybridEntryData) entryData).getNonSerializedProperties(), ((HybridEntryData)entryData).getPackedSerializedProperties());

    }
}
