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

package com.gigaspaces.internal.client.spaceproxy.metadata;

import com.gigaspaces.cluster.replication.WriteConsistencyLevelCompromisedException;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeByIdsProxyActionInfo;
import com.gigaspaces.internal.lease.SpaceEntryLease;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.ITypeIntrospector;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.query.PropertiesQuery;
import com.gigaspaces.internal.server.space.SpaceUidFactory;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageConfig;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageTableConfig;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.EntryPacket;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.internal.utils.ObjectUtils;
import com.gigaspaces.lrmi.classloading.LRMIClassLoadersHolder;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.IdsQuery;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.LeaseInitializer;
import com.j_spaces.core.client.EntrySnapshot;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;
import com.j_spaces.kernel.SystemProperties;

import net.jini.core.entry.UnusableEntryException;

import java.lang.reflect.Array;
import java.util.Map;

/**
 * Manages EntryPacket construction and TypeDescription
 *
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyTypeManager implements ISpaceProxyTypeManager {
    private static final IEntryPacket DUMMY_PACKET = new EntryPacket();
    private static final int _requiredConsistencyLevel = Integer.getInteger(SystemProperties.REQUIRED_CONSISTENCY_LEVEL, SystemProperties.REQUIRED_CONSISTENCY_LEVEL_DEFAULT);

    private final IDirectSpaceProxy _proxy;
    private final ClientTypeDescRepository _typeDescRepository;

    public SpaceProxyTypeManager(IDirectSpaceProxy proxy) {
        this._proxy = proxy;
        this._typeDescRepository = new ClientTypeDescRepository(proxy);
    }

    public Object getLockObject() {
        return _typeDescRepository;
    }

    public void close() {
        _typeDescRepository.clear();
    }

    public ITypeDesc getTypeDescIfExistsInProxy(String className) {
        return _typeDescRepository.getTypeDescIfExistsInProxy(className);
    }

    public ITypeDesc getTypeDescByName(String typeName) {
        return _typeDescRepository.getTypeDescByName(typeName, null);
    }

    public ITypeDesc getTypeDescByName(String typeName, String codebase) {
        return _typeDescRepository.getTypeDescByName(typeName, codebase);
    }

    public ITypeDesc getTypeDescByNameIfExists(String typeName) {
        return _typeDescRepository.getTypeDescIfExistsInServer(typeName);
    }

    public void loadTypeDescToPacket(ITransportPacket packet) {
        _typeDescRepository.loadTypeDescToPacket(packet);
    }

    public void registerTypeDesc(ITypeDesc typeDesc) {
        _typeDescRepository.registerTypeDesc(typeDesc);
    }

    public void deleteTypeDesc(String className) {
        _typeDescRepository.remove(className);
    }

    public void deleteAllTypeDescs() {
        _typeDescRepository.clear();
        LRMIClassLoadersHolder.dropAllClasses();
    }

    public String getCommonSuperTypeName(ITypeDesc typeDesc1, ITypeDesc typeDesc2) {
        return _typeDescRepository.getCommonSuperTypeName(typeDesc1, typeDesc2);
    }

    public SpaceTypeInfoRepository getSpaceTypeInfoRepository() {
        return SpaceTypeInfoRepository.getGlobalRepository();
    }

    @Override
    public Object getObjectFromEntryPacket(IEntryPacket entryPacket, QueryResultTypeInternal resultType, boolean returnPacket) {
        return getObjectFromEntryPacket(entryPacket, resultType, returnPacket, StorageTypeDeserialization.EAGER, null);
    }

    @Override
    public Object getObjectFromEntryPacket(IEntryPacket entryPacket, QueryResultTypeInternal resultType, boolean returnPacket, AbstractProjectionTemplate projectionTemplate) {
        return getObjectFromEntryPacket(entryPacket, resultType, returnPacket, StorageTypeDeserialization.EAGER, projectionTemplate);
    }

    @Override
    public Object getObjectFromEntryPacket(IEntryPacket entryPacket, QueryResultTypeInternal resultType, boolean returnPacket,
                                           StorageTypeDeserialization storageTypeDeserialization, AbstractProjectionTemplate projectionTemplate) {
        if (entryPacket == null)
            return null;

        loadTypeDescToPacket(entryPacket);
        if (projectionTemplate != null)
            projectionTemplate.filterOutNonProjectionProperties(entryPacket);
        return returnPacket ? entryPacket : entryPacket.toObject(resultType, storageTypeDeserialization);
    }

    @Override
    public LeaseContext<?> convertWriteOrUpdateResult(LeaseContext<?> lease, Object entry,
                                                      IEntryPacket entryPacekt, int modifiers) {
        Object oldValue = lease == null ? null : lease.getObject();

        if (oldValue == null && lease != null)
            lease = convertWriteResult(entry, entryPacekt, lease);
        else {
            // UPDATE operation convert the old value packet
            oldValue = convertUpdateResult((IEntryPacket) oldValue, entryPacekt, entry, modifiers);
            if (oldValue != null)
                LeaseInitializer.setPreviousObject(lease, oldValue);
        }

        return lease;
    }

    @Override
    public LeaseContext<?> convertWriteResult(Object entry, IEntryPacket entryPacket, LeaseContext<?> lease) {
        final String uid = lease != null ? lease.getUID() : entryPacket.getUID();
        // GS-1421 write in remote or cluster. set the version to 1. in embedded Dcache just copy the version
        final int version = (!_proxy.isEmbedded() || _proxy.isClustered()) ? 1 : entryPacket.getVersion();

        if (entry instanceof IEntryPacket) {
            IEntryPacket ep = ((IEntryPacket) entry);
            ep.setUID(uid);
            ep.setVersion(version);
        } else {
            _typeDescRepository.loadTypeDescToPacket(entryPacket);
            ITypeIntrospector<Object> introspector = entryPacket.getTypeDescriptor().getIntrospector(entryPacket.getEntryType());
            introspector.setUID(entry, uid);
            introspector.setVersion(entry, version);
        }

        return lease == null || LeaseInitializer.isDummyLease(lease) ? null : lease;
    }

    /**
     * Prepare update result, update VersionID of newEntry if lease>=0 then writeIfAbsent.
     */
    private Object convertUpdateResult(IEntryPacket oldPacket, IEntryPacket newPacket, Object newEntry, int modifiers) {
        Object oldEntry = null;
        final boolean returnPacket = newPacket == newEntry;
        if (oldPacket != null && oldPacket != DUMMY_PACKET) {
            oldEntry = getObjectFromEntryPacket(oldPacket, QueryResultTypeInternal.getUpdateResultType(newPacket), returnPacket);
            final boolean overrideVersion = Modifiers.contains(modifiers, Modifiers.OVERRIDE_VERSION);
            if (returnPacket) {
                if (!overrideVersion)
                    newPacket.setVersion(oldPacket.getVersion() + 1);
            } else {
                ITypeIntrospector introspector = oldPacket.getTypeDescriptor().getIntrospector(newPacket.getEntryType());
                int version = overrideVersion ? newPacket.getVersion() : oldPacket.getVersion() + 1;
                introspector.setEntryInfo(newEntry, oldPacket.getUID(), version, oldPacket.getTTL());
            }
        } else if (UpdateModifiers.isUpdateOrWrite(modifiers) || UpdateModifiers.isWriteOnly(modifiers)) // write was done
        {
            /* in case of embedded-non-clustered space we copy the version to support DCache.  */
            final int newVer = (!_proxy.isEmbedded() || _proxy.isClustered()) ? 1 : newPacket.getVersion();
            _typeDescRepository.loadTypeDescToPacket(newPacket);

            ITypeDesc typeDesc = newPacket.getTypeDescriptor();

            if (returnPacket) {
                if (oldPacket != null)
                    oldPacket.setTypeDesc(typeDesc, false);

                newPacket.setVersion(newVer);
                oldEntry = oldPacket;
            } else {
                typeDesc.getIntrospector(newPacket.getEntryType()).setEntryInfo(newEntry, newPacket.getUID(), newVer, newPacket.getTTL());
            }
        }

        return oldEntry;
    }

    @Override
    public Object convertQueryResult(IEntryPacket resultPacket, ITemplatePacket queryPacket, boolean returnEntryPacket) {
        return getObjectFromEntryPacket(resultPacket, queryPacket.getQueryResultType(), returnEntryPacket);
    }

    @Override
    public Object convertQueryResult(IEntryPacket resultPacket, ITemplatePacket queryPacket, boolean returnEntryPacket, AbstractProjectionTemplate projectionTemplate) {
        return getObjectFromEntryPacket(resultPacket, queryPacket.getQueryResultType(), returnEntryPacket, projectionTemplate);
    }

    @Override
    public Object[] convertQueryResults(IEntryPacket[] resultPackets, ITemplatePacket query, boolean returnEntryPacket, AbstractProjectionTemplate projectionTemplate) {
        if (resultPackets == null)
            return null;

        Object[] results;
        if (returnEntryPacket)
            results = resultPackets;
        else {
            final Class<?> resultClass = getResultClass(query);
            results = (Object[]) Array.newInstance(resultClass, resultPackets.length);
        }

        for (int i = 0; i < resultPackets.length; i++)
            results[i] = convertQueryResult(resultPackets[i], query, returnEntryPacket, projectionTemplate);

        return results;
    }

    public Class<?> getResultClass(ITemplatePacket templatePacket) {
        Class<?> type = null;
        ITypeDesc typeDesc = templatePacket.getTypeDescriptor();
        if (typeDesc != null)
            type = typeDesc.getIntrospector(templatePacket.getQueryResultType().getEntryType()).getType();

        return type != null ? type : Object.class;
    }

    public Object getObjectFromIGSEntry(IGSEntry entry)
            throws UnusableEntryException {
        final ITypeDesc typeDesc = getTypeDescByName(entry.getClassName(), entry.getCodebase());
        ITypeIntrospector<Object> introspector = typeDesc.getIntrospector(typeDesc.getObjectType());
        return introspector.toObject(entry, typeDesc);
    }

    public IEntryPacket getEntryPacketFromObject(Object object, ObjectType objectType) {
        IEntryPacket packet;

        if (objectType.isConcrete()) {
            ITypeDesc typeDesc = _typeDescRepository.getTypeDescByJavaObject(object, objectType);
            packet = EntryPacketFactory.createFromObject(object, typeDesc, EntryType.OBJECT_JAVA, true);
        } else if (objectType == ObjectType.ENTRY_PACKET) {
            packet = (IEntryPacket) object;
            loadTypeDescToPacket(packet);
        } else if (objectType == ObjectType.DOCUMENT) {
            final String typeName = ((SpaceDocument) object).getTypeName();
            final ITypeDesc typeDesc = _typeDescRepository.getTypeDescByName(typeName, null);
            packet = EntryPacketFactory.createFromObject(object, typeDesc, EntryType.DOCUMENT_JAVA, true);
        } else if (objectType == ObjectType.EXTERNAL_ENTRY) {
            final ITypeDesc typeDesc = _typeDescRepository.getTypeDescByExternalEntry((ExternalEntry) object);
            packet = EntryPacketFactory.createFromObject(object, typeDesc, EntryType.EXTERNAL_ENTRY, true);
        } else if (objectType == ObjectType.NULL)
            throw new IllegalArgumentException("Cannot create IEntryPacket from null.");
        else if (objectType == ObjectType.ENTRY_SNAPSHOT)
            throw new IllegalArgumentException("Cannot create IEntryPacket from EntrySnapshot.");
        else if (objectType == ObjectType.SQL)
            throw new IllegalArgumentException("Cannot create IEntryPacket from SQLQuery.");
        else if (objectType == ObjectType.ID_QUERY)
            throw new IllegalArgumentException("Cannot create IEntryPacket from IdQuery.");
        else
            throw new IllegalArgumentException("Unsupported entry type: " + objectType);

        packet.setSerializeTypeDesc(false);

        //disable version only for real objects - not internal (local cache/local view)
        if (objectType != ObjectType.ENTRY_PACKET)
            disablePacketVersionIfNotSupported(packet);

        if(_proxy.getSpaceClusterInfo() != null && _proxy.getSpaceClusterInfo().isTieredStorage()){
            validateTimeValueNotNull(object, packet);
        }
        return packet;
    }

    private void validateTimeValueNotNull(Object object, IEntryPacket packet){
        String typeName = object.getClass().getTypeName();
        TieredStorageConfig tieredStorageConfig = _proxy.getSpaceClusterInfo().getCustomComponent("TieredStorage");
        if (tieredStorageConfig.hasCacheRule(typeName)){
            TieredStorageTableConfig tieredStorageTableConfig = tieredStorageConfig.getTables().get(typeName);
            if (tieredStorageTableConfig.isTimeRule()){
                String timeColumn = tieredStorageTableConfig.getTimeColumn();
                if (packet.getPropertyValue(timeColumn) == null){
                    throw new SpaceMetadataException("property which set as time rule cannot be null.");
                }
            }
        }
    }

    public ITemplatePacket getTemplatePacketFromObject(Object object, ObjectType objectType) {
        ITemplatePacket packet;

        if (objectType.isConcrete()) {
            ITypeDesc typeDesc = _typeDescRepository.getTypeDescByJavaObject(object, objectType);
            packet = TemplatePacketFactory.createFromObject(object, typeDesc, EntryType.OBJECT_JAVA);
        } else if (objectType == ObjectType.TEMPLATE_PACKET) {
            packet = (ITemplatePacket) object;
            loadTypeDescToPacket(packet);
            //Handle special case where the template packet has dynamic properties we need to
            //covert it to corresponding custom query (Specifically for PBS case)
            Map<String, Object> dynamicProperties = packet.getDynamicProperties();
            if (dynamicProperties != null) {
                packet.setCustomQuery(new PropertiesQuery(dynamicProperties, packet.getTypeDescriptor()));
                //Reset dynamic properties because the template do not need it anymore
                packet.setDynamicProperties(null);
            }
        } else if (objectType == ObjectType.DOCUMENT) {
            final String typeName = ((SpaceDocument) object).getTypeName();
            final ITypeDesc typeDesc = _typeDescRepository.getTypeDescByName(typeName, null);
            packet = TemplatePacketFactory.createFromObject(object, typeDesc, EntryType.DOCUMENT_JAVA);
        } else if (objectType == ObjectType.EXTERNAL_ENTRY)
            packet = createTemplatePacketFromExternalEntry((ExternalEntry) object);
        else if (objectType == ObjectType.ENTRY_SNAPSHOT) {
            packet = ((EntrySnapshot<?>) object).getTemplatePacket();
            if (packet.getTypeDescriptor() == null)
                loadTypeDescToPacket(packet);
        } else if (objectType == ObjectType.SQL)
            packet = getTemplatePacketFromSqlQuery((SQLQuery<?>) object);
        else if (objectType == ObjectType.ID_QUERY)
            packet = getTemplatePacketFromIdQuery((IdQuery<?>) object);
        else if (objectType == ObjectType.IDS_QUERY) {
            IdsQuery<?> idsQuery = ((IdsQuery<?>) object);
            ReadTakeByIdsProxyActionInfo actionInfo = new ReadTakeByIdsProxyActionInfo(
                    _proxy, idsQuery.getTypeName(), idsQuery.getIds()
                    , null, idsQuery.getRoutings(), null, true, 0, QueryResultTypeInternal.NOT_SET, idsQuery.getProjections(), null);
            packet = actionInfo.queryPacket;
        } else if (objectType == ObjectType.NULL) {
            ITypeDesc objectTypeDesc = _typeDescRepository.getTypeDescByName(Object.class.getName(), null);
            packet = TemplatePacketFactory.createEmptyPacket(objectTypeDesc);
        }
        /*
        else if (objectType == ObjectType.CUSTOM_QUERY)
        {
        	AbstractSpaceQuery<?> query = (AbstractSpaceQuery<?>)object;
        	ITypeDesc typeDesc = getTypeDescriptor(query.getEntryTypeName());
        	Object template;
			try
			{
				template = typeDesc.getIntrospector(ObjectType.POJO).newInstance();
			} 
			catch (InstantiationException e)
			{
				throw new UnusableEntryException(e);
			} 
			catch (IllegalAccessException e)
			{
				throw new UnusableEntryException(e);
			} 
			catch (InvocationTargetException e)
			{
				throw new UnusableEntryException(e);
			}
            result = buildTemplatePacket(template, spaceTargetStub, fullPacket);
        	result.setCustomQuery(query);        	
        }
        */
        else
            throw new IllegalArgumentException("Unsupported entry type: " + objectType);

        if (objectType != ObjectType.TEMPLATE_PACKET)
            disablePacketVersionIfNotSupported(packet);

        packet.validate();

        return packet;
    }

    private ITemplatePacket createTemplatePacketFromExternalEntry(ExternalEntry entry) {
        if (entry.getMultipleUIDs() != null) {
            QueryResultTypeInternal resultType = entry._returnTrueType ? QueryResultTypeInternal.NOT_SET : QueryResultTypeInternal.EXTERNAL_ENTRY;
            return TemplatePacketFactory.createUidsPacket(entry.getMultipleUIDs(), resultType, entry.m_ReturnOnlyUids);
        }

        if (entry.getClassName() == null) {
            if (entry.getUID() != null)
                return TemplatePacketFactory.createUidPacket(entry.getUID(), entry.getVersion(), entry.m_ReturnOnlyUids);

            ITypeDesc objectTypeDesc = _typeDescRepository.getTypeDescByName(Object.class.getName(), null);
            return TemplatePacketFactory.createEmptyPacket(objectTypeDesc);
        }

        final ITypeDesc typeDesc = _typeDescRepository.getTypeDescByExternalEntry(entry);
        return TemplatePacketFactory.createFromObject(entry, typeDesc, EntryType.EXTERNAL_ENTRY);
    }

    private ITemplatePacket getTemplatePacketFromSqlQuery(SQLQuery<?> sqlQuery) {
        final ITypeDesc typeDesc = _typeDescRepository.getTypeDescBySQLQuery(sqlQuery);
        final QueryResultTypeInternal resultType = getSqlQueryResultType(sqlQuery);
        return new SQLQueryTemplatePacket(sqlQuery, typeDesc, resultType);
    }

    private QueryResultTypeInternal getSqlQueryResultType(SQLQuery<?> sqlQuery) {
        switch (sqlQuery.getQueryResultType()) {
            case OBJECT:
                return QueryResultTypeInternal.OBJECT_JAVA;
            case DOCUMENT:
                return QueryResultTypeInternal.DOCUMENT_ENTRY;
            case DEFAULT:
            case NOT_SET:
                final Object template = sqlQuery.getObject();
                if (template instanceof ITemplatePacket)
                    return ((ITemplatePacket) template).getQueryResultType();
                return QueryResultTypeInternal.NOT_SET;
            default:
                throw new IllegalStateException("Unsupported query result type: " + sqlQuery.getQueryResultType());
        }
    }

    @SuppressWarnings("deprecation")
    private QueryResultTypeInternal getIdQueryResultType(IdQuery<?> idQuery) {
        switch (idQuery.getQueryResultType()) {
            case OBJECT:
                return QueryResultTypeInternal.OBJECT_JAVA;
            case DOCUMENT:
                return QueryResultTypeInternal.DOCUMENT_ENTRY;
            case DEFAULT:
            case NOT_SET:
                return QueryResultTypeInternal.NOT_SET;
            default:
                throw new IllegalStateException("Unsupported query result type: " + idQuery.getQueryResultType());
        }
    }


    private ITemplatePacket getTemplatePacketFromIdQuery(IdQuery<?> idQuery) {
        final ITypeDesc typeDesc = _typeDescRepository.getTypeDescByName(idQuery.getTypeName(), null);
        final QueryResultTypeInternal resultType = getIdQueryResultType(idQuery);

        Object routing = idQuery.getRouting();
        Object id = idQuery.getId();
        int version = idQuery.getVersion();

        if (ObjectUtils.equals(typeDesc.getIdPropertyName(), typeDesc.getRoutingPropertyName())) {
            if (routing != null && !routing.equals(id))
                throw new IllegalArgumentException("routing must be null or same as id if routing property and id property are the same.");
        }

        ProjectionTemplate projectionTemplate = idQuery.getProjections() != null ? ProjectionTemplate.create(idQuery.getProjections(), typeDesc) : null;
        return TemplatePacketFactory.createIdOrUidPacket(typeDesc,
                resultType,
                routing,
                id,
                version,
                projectionTemplate);
    }

    private void disablePacketVersionIfNotSupported(ITransportPacket entryPacket) {
        final ITypeDesc typeDesc = entryPacket.getTypeDescriptor();
        final boolean isTypeVersioned = (typeDesc != null && typeDesc.supportsOptimisticLocking());
        if (!_proxy.getProxySettings().isVersioned() || !isTypeVersioned)
            entryPacket.setVersion(0);
    }

    @Override
    public LeaseContext<?> processWriteResult(WriteEntryResult writeResult, Object entry, IEntryPacket entryPacket) {
        if (writeResult == null)
            throw new OperationTimeoutException();
        // Update uid + version on user entry:
        String writeResultUid = writeResult.getUid();
        if (entry == entryPacket) {
            if (writeResultUid != null)
                entryPacket.setUID(writeResultUid);
            entryPacket.setVersion(writeResult.getVersion());
        } else {
            ITypeIntrospector<Object> introspector = entryPacket.getTypeDescriptor().getIntrospector(entryPacket.getEntryType());
            if (writeResultUid != null)
                introspector.setUID(entry, writeResultUid);
            introspector.setVersion(entry, writeResult.getVersion());
        }

        Object prevEntry = getObjectFromEntryPacket(writeResult.getPrevEntry(), QueryResultTypeInternal.getUpdateResultType(entryPacket), entry == entryPacket);
        Object routingFieldValue = entryPacket.getRoutingFieldValue();
        if (routingFieldValue == null && writeResultUid != null && entryPacket.getTypeDescriptor().isAutoGenerateRouting())
            routingFieldValue = SpaceUidFactory.extractPartitionId(writeResultUid);

        SpaceEntryLease<Object> objectSpaceEntryLease = new SpaceEntryLease<Object>(_proxy,
                entryPacket.getTypeName(),
                writeResultUid != null ? writeResultUid : entryPacket.getUID(),
                writeResult.getVersion(),
                routingFieldValue,
                writeResult.getExpiration(),
                prevEntry);
        if (writeResult.getSyncReplicationLevel() + 1 < requiredConsistencyLevel()) {
            throw new WriteConsistencyLevelCompromisedException(writeResult.getSyncReplicationLevel() + 1, objectSpaceEntryLease);
        }
        return objectSpaceEntryLease;
    }

    public static int requiredConsistencyLevel() {
        return _requiredConsistencyLevel;
    }

}
