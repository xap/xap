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

package com.gigaspaces.internal.sync;

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.DataSourceIdQueryImpl;
import com.gigaspaces.datasource.DataSourceIdsQueryImpl;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.DataSourceQueryImpl;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.datasource.concurrentaccess.SharedIteratorSpaceDataSourceDecorator;
import com.gigaspaces.internal.datasource.EDSAdapterSpaceDataSource;
import com.gigaspaces.internal.datasource.EDSAdapterSynchronizationEndpoint;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.EntryDataType;
import com.gigaspaces.internal.server.storage.EntryHolderFactory;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.sync.hybrid.SyncHybridOperationDetails;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.LogUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.sync.*;
import com.j_spaces.core.Constants;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.sadapter.datasource.*;

import net.jini.core.transaction.server.ServerTransaction;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.j_spaces.core.Constants.DataAdapter.DATA_CLASS_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_PROP;

/**
 * @author idan
 * @author eitany
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class SynchronizationStorageAdapter implements IStorageAdapter {
    private final static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT);

    private final SpaceEngine _engine;
    private final String _spaceName;
    private final SpaceConfigReader _configReader;
    private final EntryDataType _entryDataType;
    private final SpaceTypeManager _typeManager;
    private final boolean _mirrorService;
    private final SpaceSynchronizationEndpoint _syncEndpoint;
    private SpaceDataSource _spaceDataSource;
    private final IDataConverter<IEntryPacket> _converter;
    private final EntryAdapter _conversionAdapter;
    private final SQLQueryBuilder _queryBuilder;
    private final Class<?> _dataClass;
    private final boolean _supportsPartialUpdate;
    private final boolean _centralDataSource;
    private final boolean _supportsInheritance;

    public SynchronizationStorageAdapter(SpaceEngine engine, SpaceDataSource spaceDataSource,
                                         SpaceSynchronizationEndpoint synchronizationEndpointInterceptor)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this._engine = engine;
        this._spaceDataSource = spaceDataSource;
        this._syncEndpoint = synchronizationEndpointInterceptor;
        this._spaceName = engine.getSpaceName();
        this._configReader = engine.getConfigReader();
        this._entryDataType = engine.getEntryDataType();
        this._typeManager = engine.getTypeManager();
        this._mirrorService = _configReader.getBooleanSpaceProperty(Constants.Mirror.MIRROR_SERVICE_ENABLED_PROP, Constants.Mirror.MIRROR_SERVICE_ENABLED_DEFAULT);
        this._supportsInheritance = _spaceDataSource != null ? _spaceDataSource.supportsInheritance() : true;

        if (_spaceDataSource == null && !_mirrorService)
            throw new IllegalArgumentException("Cannot start a persistent space without specifying a SpaceDataSource implementation");

        if (_mirrorService && _spaceDataSource != null && !isEDSAdapter())
            throw new IllegalArgumentException("Cannot start a mirror space since a SpaceDataSource implementation was specified - mirror space only supports SpaceSynchronizationEndpoint specification");

        JSpaceAttributes spaceAttr = engine.getSpaceImpl().getJspaceAttr();
        this._supportsPartialUpdate = spaceAttr.isSupportsPartialUpdateEnabled();

        this._dataClass = ClassLoaderHelper.loadClass(spaceAttr.getDataClass());
        this._queryBuilder = (SQLQueryBuilder) ClassLoaderHelper.loadClass(spaceAttr.getQueryBuilderClass()).newInstance();
        this._converter = new EntryPacketDataConverter(_typeManager, _dataClass);
        this._conversionAdapter = new EntryAdapter(_converter);
        this._centralDataSource = engine.getClusterPolicy() != null ? engine.getClusterPolicy().m_CacheLoaderConfig.centralDataSource : false;
    }

    @Override
    public void initialize() throws SAException {
        try {
            initOldDataStorage();
            initializeSharedModeIteratorIfNecessary();
        } catch (Exception e) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, getClass(), "Initialize", e);
            throw new SAException(e);
        }
    }

    private void initializeSharedModeIteratorIfNecessary() {
        final boolean shareIteratorMode = _engine.getSpaceImpl().getJspaceAttr().getDataSourceSharedIteratorMode();

        if (_spaceDataSource == null || !shareIteratorMode)
            return;

        long leaseManagerExpirationTimeRecentDeletes = _configReader.getLongSpaceProperty(
                LM_EXPIRATION_TIME_RECENT_DELETES_PROP, String.valueOf(LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT));
        long leaseManagerExpirationTimeRecentUpdates = _configReader.getLongSpaceProperty(
                LM_EXPIRATION_TIME_RECENT_UPDATES_PROP, String.valueOf(LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT));

        long sharedIteratorTimeToLive = _engine.getSpaceImpl().getJspaceAttr().getDataSourceSharedIteratorTimeToLive();

        long warnThreshold = (long) (Math.min(leaseManagerExpirationTimeRecentDeletes, leaseManagerExpirationTimeRecentUpdates) * 0.8);
        if (shareIteratorMode && sharedIteratorTimeToLive > warnThreshold)
            _logger.warn("shared iterator time to live [" + sharedIteratorTimeToLive + "] exceeds lease manager expiration time of recent deletes or recent updates threshold [" + warnThreshold + "], this can cause correctness issues. ");

        _spaceDataSource = new SharedIteratorSpaceDataSourceDecorator(_spaceDataSource, sharedIteratorTimeToLive);
    }

    private void initOldDataStorage() throws SAException, DataSourceException {
        final DataStorage<Object> dataStorage = getOldEDSDataStorage();
        if (dataStorage == null || !dataStorage.isManagedDataSource())
            return;

        Properties dataProperties = new Properties();

        // Add the data-class properties - relevant for the data source
        dataProperties.put(DATA_CLASS_PROP, _dataClass.getName());
        dataProperties.put(ManagedDataSource.NUMBER_OF_PARTITIONS, _engine.getNumberOfPartitions());
        dataProperties.put(ManagedDataSource.STATIC_PARTITION_NUMBER, _engine.getPartitionIdOneBased());

        String dataPropertiesFile = _engine.getSpaceImpl().getJspaceAttr().getDataPropertiesFile();

        if (dataPropertiesFile != null && dataPropertiesFile.trim().length() != 0) {
            try {
                InputStream is = ResourceLoader.getResourceStream(dataPropertiesFile);
                if (is == null)
                    throw new FileNotFoundException("Failed to find resource: " + dataPropertiesFile);
                dataProperties.load(is);
            } catch (IOException e) {
                throw new SAException(e);
            }
        }
        dataStorage.init(dataProperties);
    }

    @Override
    public ISAdapterIterator<?> initialLoad(Context context, ITemplateHolder template)
            throws SAException {
        //  if template is transient - don't refer to the external-data-source
        if (template.isTransient() || _mirrorService)
            return null;

        final DataIterator<SpaceTypeDescriptor> metadataIterator = _spaceDataSource.initialMetadataLoad();
        if (metadataIterator != null) {
            try {
                while (metadataIterator.hasNext()) {
                    final ITypeDesc typeDescriptor = (ITypeDesc) metadataIterator.next();
                    final String[] superClassesNames = typeDescriptor.getRestrictSuperClassesNames();
                    if (superClassesNames != null) {
                        for (String superClassName : superClassesNames) {
                            if (_typeManager.getServerTypeDesc(superClassName) == null)
                                throw new IllegalArgumentException("Missing super class type descriptor ["
                                        + superClassName
                                        + "] for type ["
                                        + typeDescriptor.getTypeName() + "]");
                        }
                    }
                    _typeManager.addTypeDesc(typeDescriptor);
                }
            } catch (Exception e) {
                if (_logger.isDebugEnabled())
                    LogUtils.throwing(_logger, getClass(), "Initial Metadata Load", e);
                throw new SAException(e);
            }
        }

        try {
            // Create adapter iterator that holds all the subclasses iterators
            // in case of a storage that supports inheritance - only one iterator is used
            DataAdaptorIterator cacheAdapterIterator = new DataAdaptorIterator(_typeManager, _entryDataType);
            //  iterate
            DataIterator<Object> iterator = _spaceDataSource.initialDataLoad();
            if (iterator != null) {
                // wrap user iterator with EntryAdapterIterator 
                // - that will handle entry conversion during iteration                 
                EntryAdapterIterator entryAdapterIterator = new EntryAdapterIterator(iterator, new EntryAdapter(_converter));

                //  Aggregate iterator results
                cacheAdapterIterator.add(entryAdapterIterator);
            }
            return cacheAdapterIterator;
        } catch (Exception e) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, getClass(), "Initial Load", e);
            throw new SAException(e);
        }
    }

    @Override
    public void insertEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean shouldReplicate)
            throws SAException {
        try {
            if (!isPersistable(entryHolder, origin))
                return;
            final ITypeDesc typeDescriptor = getType(entryHolder.getClassName());
            final DataSyncOperation operation = new BulkDataItem(entryHolder, typeDescriptor, BulkItem.WRITE, _converter);
            if (_engine.getCacheManager().isSyncHybrid()) {
                injectSyncHybridOperationsDetails(context, new DataSyncOperation[]{operation});
            }
            _syncEndpoint.onOperationsBatchSynchronization(new OperationsDataBatchImpl(operation, _spaceName));
        } catch (Throwable t) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, getClass(), "Insert Entry", t);

            throw new SAException(t);
        }
    }

    private boolean isPersistable(IEntryHolder entryHolder, boolean origin) {
        if (isReadOnly() || entryHolder.isTransient())
            return false;
        // persistable if not from replication or not central data source
        return origin || !_centralDataSource;
    }

    @Override
    public void updateEntry(Context context, IEntryHolder updatedEntry, boolean updateRedoLog,
                            boolean origin, boolean[] partialUpdateValuesIndicators)
            throws SAException {
        if (!isPersistable(updatedEntry, origin))
            return;

        final ITypeDesc typeDescriptor = getType(updatedEntry.getClassName());

        DataSyncOperation operation;
        if (partialUpdateValuesIndicators != null
                && partialUpdateValuesIndicators.length > 0
                && _supportsPartialUpdate) {
            operation = new PartialUpdateBulkDataItem(updatedEntry, partialUpdateValuesIndicators, typeDescriptor, _converter);
        } else {
            operation = new BulkDataItem(updatedEntry, typeDescriptor, BulkItem.UPDATE, _converter);
        }
        if (_engine.getCacheManager().isSyncHybrid()) {
            injectSyncHybridOperationsDetails(context, new DataSyncOperation[]{operation});
        }
        _syncEndpoint.onOperationsBatchSynchronization(new OperationsDataBatchImpl(operation, _spaceName));
    }

    @Override
    public void removeEntry(Context context, IEntryHolder entryHolder, boolean origin,
                            boolean fromLeaseExpiration, boolean shouldReplicate) throws SAException {
        try {
            if (!isPersistable(entryHolder, origin) || fromLeaseExpiration)
                return;

            final ITypeDesc typeDescriptor = getType(entryHolder.getClassName());
            final DataSyncOperation operation = new BulkDataItem(entryHolder, typeDescriptor, BulkItem.REMOVE, _converter);
            if (_engine.getCacheManager().isSyncHybrid()) {
                injectSyncHybridOperationsDetails(context, new DataSyncOperation[]{operation});
            }
            _syncEndpoint.onOperationsBatchSynchronization(new OperationsDataBatchImpl(operation, _spaceName));
        } catch (Throwable t) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, getClass(), "Remove Entry", t);

            throw new SAException(t);
        }
    }

    @Override
    public void prepare(Context context, ServerTransaction xtn, ArrayList<IEntryHolder> pLocked,
                        boolean singleParticipant,
                        Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate)
            throws SAException {
        if (isReadOnly())
            return;

        try {
            final ArrayList<DataSyncOperation> operations = new ArrayList<DataSyncOperation>(pLocked.size());
            for (IEntryHolder entryHolder : pLocked) {
                if (entryHolder == null || entryHolder.isDeleted()
                        || entryHolder.isTransient()
                        || entryHolder.getWriteLockTransaction() == null
                        || !entryHolder.getWriteLockTransaction().equals(xtn))
                    continue;

                // ignore replicated transactions on central db
                if (_centralDataSource && entryHolder.getTxnEntryData().getWriteLockOwner().isFromReplication())
                    break;

                final ITypeDesc typeDescriptor = getType(entryHolder.getClassName());

                switch (entryHolder.getWriteLockOperation()) {
                    case SpaceOperations.WRITE:
                        operations.add(new BulkDataItem(entryHolder, typeDescriptor, BulkItem.WRITE, _converter));
                        break;
                    case SpaceOperations.UPDATE:
                        final Object updateInfo = partialUpdatesAndInPlaceUpdatesInfo != null ? partialUpdatesAndInPlaceUpdatesInfo.get(entryHolder.getUID())
                                : null;
                        final boolean[] partialUpdateValuesIndicators = (updateInfo != null && (updateInfo instanceof boolean[])) ? (boolean[]) updateInfo
                                : null;
                        if (partialUpdateValuesIndicators != null
                                && partialUpdateValuesIndicators.length > 0
                                && supportsPartialUpdate()) {
                            operations.add(new PartialUpdateBulkDataItem(entryHolder, partialUpdateValuesIndicators, typeDescriptor, _converter));
                        } else {
                            operations.add(new BulkDataItem(entryHolder, typeDescriptor, BulkItem.UPDATE, _converter));
                        }
                        break;
                    case SpaceOperations.TAKE:
                    default:
                        operations.add(new BulkDataItem(entryHolder, typeDescriptor, BulkItem.REMOVE, _converter));
                        break;
                }

            }
            if (!operations.isEmpty()) {
                if (_engine.getCacheManager().isSyncHybrid()) {
                    injectSyncHybridOperationsDetails(context, operations.toArray(new DataSyncOperation[operations.size()]));
                }
                _syncEndpoint.onTransactionSynchronization(new TransactionDataImpl(operations.toArray(new DataSyncOperation[operations.size()]),
                        xtn.getMetaData(),
                        _spaceName));
            }
        } catch (Throwable t) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, getClass(), "Prepare", t);
            throw new SAException(t);
        }
    }

    @Override
    public void rollback(ServerTransaction xtn, boolean anyUpdates)
            throws SAException {
    }

    @Override
    public boolean supportsGetEntries() {
        return true;
    }

    @Override
    public Map<String, IEntryHolder> getEntries(Context context, Object[] ids,
                                                String typeName,
                                                IEntryHolder[] templates) throws SAException {
        try {
            Map<String, IEntryHolder> results = Collections.emptyMap();
            IServerTypeDesc serverTypeDesc = getServerType(typeName);
            ITypeDesc typeDescriptor = serverTypeDesc.getTypeDesc();
            DataIterator<Object> dataSourceResults = _spaceDataSource.getDataIteratorByIds(new DataSourceIdsQueryImpl(typeDescriptor,
                    ids,
                    templates,
                    _queryBuilder,
                    _dataClass,
                    _converter));
            if (dataSourceResults == null)
                return results;

            while (dataSourceResults.hasNext()) {
                Object dataSourceResult = dataSourceResults.next();
                IEntryPacket entryPacket = _conversionAdapter.toEntry(dataSourceResult);
                IEntryHolder entryHolder = EntryHolderFactory.createEntryHolder(serverTypeDesc, entryPacket, _entryDataType);

                if (_engine.isPartitionedSpace() && !_engine.isEntryFromPartition(entryHolder))
                    continue;

                if (results.isEmpty())
                    results = new HashMap<String, IEntryHolder>();

                results.put(entryHolder.getUID(), entryHolder);
            }
            return results;
        } catch (Exception e) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, getClass(), "Get Entries", e);
            throw new SAException(e);
        }
    }

    @Override
    public IEntryHolder getEntry(Context context, Object uid, String typeName,
                                 IEntryHolder template) throws SAException {
        return
                getEntry_impl(uid, typeName, template);
    }

    private IEntryHolder getEntry_impl(Object uid, String typeName,
                                       IEntryHolder template) throws SAException {
        try {
            final IServerTypeDesc serverTypeDesc = getServerType(typeName);
            final ITypeDesc typeDescriptor = serverTypeDesc.getTypeDesc();
            // TODO DATASOURCE: add reason to query
            final EntryAdapter entryAdapter = new EntryAdapter(template,
                    typeDescriptor,
                    _converter);
            final DataSourceIdQueryImpl idQuery = new DataSourceIdQueryImpl(typeDescriptor,
                    entryAdapter.toEntryPacket(),
                    _queryBuilder,
                    _dataClass,
                    entryAdapter);
            Object result = _spaceDataSource.getById(idQuery);
            if (result == null)
                return null;

            final IEntryPacket entryPacket = entryAdapter.toEntry(result);
            final IServerTypeDesc serverTypeDescriptor = _typeManager.loadServerTypeDesc(entryPacket);
            IEntryHolder holder = EntryHolderFactory.createEntryHolder(serverTypeDescriptor, entryPacket, _entryDataType);

            if (_engine.isPartitionedSpace() && !_engine.isEntryFromPartition(holder))
                return null;

            return holder;
        } catch (Exception e) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, getClass(), "Get Entry", e);
            //could be: UnusableEntryException, UnknownTypeException or other
            throw new SAException(e);
        }
    }

    @Override
    public ISAdapterIterator<IEntryHolder> makeEntriesIter(ITemplateHolder template, long SCNFilter, long leaseFilter,
                                                           IServerTypeDesc[] subTypes)
            throws SAException {
        try {
            //  if transient, don't refer to the external-data-source
            if (template.isTransient() || _mirrorService)
                return null;

            if (template.isIdQuery()) {
                final IEntryHolder idQueryResult = getEntry_impl(template.getUID(), template.getClassName(), template);
                if (idQueryResult == null)
                    return null;

                return new ISAdapterIterator<IEntryHolder>() {
                    private IEntryHolder holder = idQueryResult;

                    public IEntryHolder next() {
                        if (holder == null)
                            return null;
                        IEntryHolder result = holder;
                        holder = null;
                        return result;
                    }

                    public void close() {
                    }
                };
            }

            // Create adapter iterator that holds all the subclasses iterators
            // in case of a storage that supports inheritance - only one iterator is used
            DataAdaptorIterator cacheAdapterIterator = new DataAdaptorIterator(_typeManager, _entryDataType);

            final ITypeDesc typeDescriptor = getType(template.getClassName());
            final EntryAdapter entry = new EntryAdapter(template, typeDescriptor, _converter);

            if (_supportsInheritance) {
                createDataSourceIteratorForType(template,
                        cacheAdapterIterator,
                        entry,
                        typeDescriptor);
            } else {
                for (int i = 0; i < subTypes.length; i++) {
                    final String typeName = subTypes[i].getTypeName();
                    final ITypeDesc typeDesc = getType(typeName);
                    createDataSourceIteratorForType(template,
                            cacheAdapterIterator,
                            entry,
                            typeDesc);
                }
            }
            return cacheAdapterIterator;
        } catch (Throwable t) {
            if (_logger.isDebugEnabled())
                LogUtils.throwing(_logger, SynchronizationStorageAdapter.class, "Entries Iterator", t);

            throw new SAException(t);
        }
    }

    private void createDataSourceIteratorForType(ITemplateHolder template,
                                                 DataAdaptorIterator cacheAdapterIterator,
                                                 EntryAdapter entryAdapter, ITypeDesc typeDescriptor) {
        int maxResults = template.isBatchOperation() ? template.getBatchOperationContext().getMaxEntries() : 1;

        final DataSourceQuery dataSourceQuery = new DataSourceQueryImpl(template, typeDescriptor, _dataClass, _queryBuilder, entryAdapter, maxResults);
        final DataIterator<Object> dataIterator = _spaceDataSource.getDataIterator(dataSourceQuery);
        if (dataIterator != null) {
            // wrap user iterator with EntryAdapterIterator 
            // - that will handle entry conversion during iteration                 
            EntryAdapterIterator entryAdapterIterator = new EntryAdapterIterator(dataIterator, new EntryAdapter(_converter));

            //  Aggregate iterator results
            cacheAdapterIterator.add(entryAdapterIterator);
        }
    }

    private ITypeDesc getType(String typeName) {
        IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(typeName);
        return serverTypeDesc != null ? serverTypeDesc.getTypeDesc() : null;
    }

    private IServerTypeDesc getServerType(String typeName) {
        return _typeManager.getServerTypeDesc(typeName);
    }

    @Override
    public void commit(ServerTransaction xtn, boolean anyUpdates)
            throws SAException {
    }

    @Override
    public int count(ITemplateHolder template, String[] subClasses)
            throws SAException {
        // TODO DATASOURCE: count is currently not wired.
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutDown() throws SAException {
        final DataStorage<Object> dataStorage = getOldEDSDataStorage();
        if (dataStorage != null && dataStorage.isManagedDataSource()) {
            try {
                dataStorage.shutdown();
            } catch (DataSourceException e) {
                throw new SAException(e);
            }
        }
        if (_spaceDataSource instanceof SharedIteratorSpaceDataSourceDecorator) {
            SharedIteratorSpaceDataSourceDecorator sharedIteratorSpaceDataSourceDecorator = (SharedIteratorSpaceDataSourceDecorator) _spaceDataSource;
            try {
                sharedIteratorSpaceDataSourceDecorator.shutdown();
            } catch (DataSourceException e) {
                throw new SAException(e);
            }
        }
    }

    private boolean isEDSAdapter() {
        return getOldEDSDataStorage() != null;
    }

    private DataStorage<Object> getOldEDSDataStorage() {
        if (_spaceDataSource != null && _spaceDataSource instanceof EDSAdapterSpaceDataSource) {
            EDSAdapterSpaceDataSource edsAdapterSpaceDataSource = (EDSAdapterSpaceDataSource) _spaceDataSource;
            return edsAdapterSpaceDataSource.getDataStorage();
        } else if (_syncEndpoint != null && _syncEndpoint instanceof EDSAdapterSynchronizationEndpoint) {
            EDSAdapterSynchronizationEndpoint edsInterceptor = (EDSAdapterSynchronizationEndpoint) _syncEndpoint;
            return edsInterceptor.getDataStorage();
        }
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return _syncEndpoint == null;
    }

    @Override
    public boolean supportsExternalDB() {
        return true;
    }

    @Override
    public boolean supportsPartialUpdate() {
        return _supportsPartialUpdate;
    }

    @Override
    public void introduceDataType(final ITypeDesc typeDesc) {
        if (_syncEndpoint != null)
            _syncEndpoint.onIntroduceType(new IntroduceTypeDataImpl(typeDesc));
    }

    @Override
    public void addIndexes(final String typeName, final SpaceIndex[] indexes) {
        if (_syncEndpoint != null)
            _syncEndpoint.onAddIndex(new AddIndexDataImpl(typeName, indexes));
    }

    @Override
    public SpaceSynchronizationEndpoint getSynchronizationInterceptor() {
        return _syncEndpoint;
    }

    @Override
    public Class<?> getDataClass() {
        return _dataClass;
    }

    private void injectSyncHybridOperationsDetails(Context ctx, DataSyncOperation[] operations) {
        SyncHybridOperationDetails[] syncHybridOperationsDetails = new SyncHybridOperationDetails[operations.length];
        for (int i = 0; i < operations.length; i++) {
            DataSyncOperationType dataSyncOperationType= operations[i].getDataSyncOperationType();
            IEntryPacket entryPacket = ((InternalBulkItem) operations[i]).toEntryPacket();
            syncHybridOperationsDetails[i] = new SyncHybridOperationDetails(_spaceName, dataSyncOperationType, entryPacket);
        }
        ctx.setSyncHybridOperationDetails(syncHybridOperationsDetails);
    }
}
