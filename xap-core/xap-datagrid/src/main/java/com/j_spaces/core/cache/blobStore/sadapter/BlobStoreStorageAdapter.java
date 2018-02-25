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

//
package com.j_spaces.core.cache.blobStore.sadapter;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.server.blobstore.BlobStoreBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreBulkOperationResult;
import com.gigaspaces.server.blobstore.BlobStoreObjectType;
import com.gigaspaces.server.blobstore.BlobStoreRemoveBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreReplaceBulkOperationRequest;
import com.gigaspaces.server.blobstore.BlobStoreAddBulkOperationRequest;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreEntryHolder;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationBasicInfo;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationInsertInfo;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationRemoveInfo;
import com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication.DelayedReplicationUpdateInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * blobStore storage adapter to handle common blobStore i/o
 *
 * @author yechiel
 * @since 10.0
 */

@com.gigaspaces.api.InternalApi
public class BlobStoreStorageAdapter implements IStorageAdapter, IBlobStoreStorageAdapter {

    private final static Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT);

    private final SpaceEngine _engine;
    private final SpaceTypeManager _typeManager;
    //a mirror may be used for recovery
    private final IStorageAdapter _possibleRecoverySA;
    private final boolean _persistentBlobStore;
    private boolean _localBlobStoreRecoveryPerformed;
    private final BlobStoreStorageAdapterClasses _classes;
    private final Object _classesLock;


    public BlobStoreStorageAdapter(SpaceEngine engine, boolean persistentBlobStore) {
        this(engine, persistentBlobStore, null);
    }

    public BlobStoreStorageAdapter(SpaceEngine engine, boolean persistentBlobStore, IStorageAdapter recoverySA) {
        _engine = engine;
        _typeManager = engine.getTypeManager();
        _persistentBlobStore = persistentBlobStore;
        _possibleRecoverySA = recoverySA;
        _classes = new BlobStoreStorageAdapterClasses();
        _classesLock = new Object();


    }

    @Override
    public void initialize() throws SAException {
        // TODO Auto-generated method stub
        if (_possibleRecoverySA != null)
            _possibleRecoverySA.initialize();
    }


    @Override
    public ISAdapterIterator initialLoad(Context context, ITemplateHolder template)
            throws SAException {
        // TODO Auto-generated method stub
        if (_localBlobStoreRecoveryPerformed || !_persistentBlobStore) {//local blob store tried. now try recovery from mirror if exist
            return _possibleRecoverySA != null ? _possibleRecoverySA.initialLoad(context, template) : null;
        }

        //first always try our local blob store
        _localBlobStoreRecoveryPerformed = true;
        //load metadata first
        final BlobStoreMetaDataIterator metadataIterator = new BlobStoreMetaDataIterator(_engine);
        Map<String, BlobStoreStorageAdapterClassInfo> classesInfo = metadataIterator.getClassesInfo();


        try {
            while (true) {
                final ITypeDesc typeDescriptor = (ITypeDesc) metadataIterator.next();
                if (typeDescriptor == null)
                    break;
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
                _classes.put(typeDescriptor.getTypeName(), classesInfo.get(typeDescriptor.getTypeName()));
                _typeManager.addTypeDesc(typeDescriptor);
            }
        } catch (Exception e) {
            if (_logger.isLoggable(Level.FINER))
                _logger.throwing(getClass().getName(), "Initial Metadata Load", e);
            throw new SAException(e);
        } finally {
            metadataIterator.close();
        }

        //get all the loaded types and verify we have proper index setting (do we have to do this check ???)
        Iterator<String> iter = classesInfo.keySet().iterator();
        while (iter.hasNext()) {
            String className = iter.next();
            //check contains, if not then call introduce type
            TypeData typeData = _engine.getCacheManager().getTypeData(_engine.getTypeManager().getServerTypeDesc(className));
            if (typeData != null) {

                BlobStoreStorageAdapterClassInfo cur = _classes.get(className);
                if (!_classes.isContained(className, typeData)) {
                    introduceDataType_impl(_engine.getTypeManager().getTypeDesc(className));
                }
            }
        }


        return new BlobStoreInitialLoadDataIterator(_engine);
    }

    @Override
    public boolean hasAnotherInitialLoadSource() {
        return (_possibleRecoverySA != null);
    }

    @Override
    public boolean shouldRevertOpOnBlobStoreError() {
        return _engine.getSpaceImpl().isBackup();
    }


    @Override
    public void insertEntry(Context context, IEntryHolder entryHolder,
                            boolean origin, boolean shouldReplicate) throws SAException {
        // TODO Auto-generated method stub
        if (!entryHolder.isBlobStoreEntry())
            return;
        context.setDelayedReplicationForbulkOpUsed(false);
        boolean needDirectPersistencySync = handleDirectPersistencyConsistency(context, entryHolder, shouldReplicate, false/*removeEntry*/);

        //setdirty will also prepare for flushing
        ((IBlobStoreEntryHolder) entryHolder).setDirty(_engine.getCacheManager());
        if (!context.isActiveBlobStoreBulk()) {
            //in bulk op the flushing is done in the bulk is execution
            if (_logger.isLoggable(Level.FINER)) {
                _logger.finer("[" + _engine.getFullSpaceName() + "] inserting entry with uid=" + entryHolder.getUID() + " using BlobStoreStorageAdapter");
            }
            ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().flush(_engine.getCacheManager(), context);
            if (needDirectPersistencySync)
                reportOpPersisted(context);
        } else {
            ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().bulkRegister(context, context.getBlobStoreBulkInfo(), SpaceOperations.WRITE, needDirectPersistencySync);
            if (useEmbeddedSyncOnPrimary(needDirectPersistencySync)) {
                context.setDelayedReplicationForbulkOpUsed(true);
                DelayedReplicationBasicInfo dr = new DelayedReplicationInsertInfo(context.getBlobStoreEntry());
                //register for later replication in the bulk
                context.getBlobStoreBulkInfo().addDelayedReplicationInfo(context, ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart(), dr);
            }

            context.getBlobStoreBulkInfo().bulk_flush(context, true/*only_if_chunk_reached*/);
        }
    }


    @Override
    public void updateEntry(Context context, IEntryHolder updatedEntry,
                            boolean shouldReplicate, boolean origin,
                            boolean[] partialUpdateValuesIndicators) throws SAException {
        // TODO Auto-generated method stub
        if (!updatedEntry.isBlobStoreEntry())
            return;
        context.setDelayedReplicationForbulkOpUsed(false);
        boolean needDirectPersistencySync = handleDirectPersistencyConsistency(context, updatedEntry, shouldReplicate, false/*removeEntry*/);
        //setdirty will also prepare for flushing
        ((IBlobStoreEntryHolder) updatedEntry).setDirty(_engine.getCacheManager());
        if (!context.isActiveBlobStoreBulk()) {
            if (_logger.isLoggable(Level.FINER)) {
                _logger.finer("[" + _engine.getFullSpaceName() + "] updating entry with uid=" + updatedEntry.getUID() + " using BlobStoreStorageAdapter");
            }
            //in bulk op the flushing is done in the bulk is execution
            ((IBlobStoreEntryHolder) updatedEntry).getBlobStoreResidentPart().flush(_engine.getCacheManager(), context);
            if (needDirectPersistencySync)
                reportOpPersisted(context);
        } else {
            ((IBlobStoreEntryHolder) updatedEntry).getBlobStoreResidentPart().bulkRegister(context, context.getBlobStoreBulkInfo(), SpaceOperations.UPDATE, needDirectPersistencySync);
            if (useEmbeddedSyncOnPrimary(needDirectPersistencySync)) {
                context.setDelayedReplicationForbulkOpUsed(true);
                DelayedReplicationBasicInfo dr = new DelayedReplicationUpdateInfo(context.getBlobStoreEntry(), context.getOriginalData(), context.getMutators());
                //register for later replication in the bulk
                context.getBlobStoreBulkInfo().addDelayedReplicationInfo(context, ((IBlobStoreEntryHolder) updatedEntry).getBlobStoreResidentPart(), dr);
            }

            context.getBlobStoreBulkInfo().bulk_flush(context, true/*only_if_chunk_reached*/);
        }
    }

    @Override
    public void removeEntry(Context context, IEntryHolder entryHolder,
                            boolean origin, boolean fromLeaseExpiration, boolean shouldReplicate) throws SAException {
        // TODO Auto-generated method stub
        if (!entryHolder.isBlobStoreEntry())
            return;
        context.setDelayedReplicationForbulkOpUsed(false);
        boolean needDirectPersistencySync = handleDirectPersistencyConsistency(context, entryHolder, shouldReplicate, true/*removeEntry*/);
        //setdirty will also prepare for flushing
        ((IBlobStoreEntryHolder) entryHolder).setDirty(_engine.getCacheManager());
        if (!context.isActiveBlobStoreBulk() || !context.getBlobStoreBulkInfo().isTakeMultipleBulk()) {
            if (_logger.isLoggable(Level.FINER)) {
                _logger.finer("[" + _engine.getFullSpaceName() + "] removing entry with uid=" + entryHolder.getUID() + " using BlobStoreStorageAdapter");
            }
            //in bulk op the flushing is done in the bulk is execution
            ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().flush(_engine.getCacheManager(), context);
            if (needDirectPersistencySync)
                reportOpPersisted(context);
        } else {
            ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().bulkRegister(context, context.getBlobStoreBulkInfo(), SpaceOperations.TAKE, needDirectPersistencySync);
            if (useEmbeddedSyncOnPrimary(needDirectPersistencySync)) {
                context.setDelayedReplicationForbulkOpUsed(true);
                DelayedReplicationBasicInfo dr = new DelayedReplicationRemoveInfo(context.getBlobStoreEntry(), context.getRemoveReason());
                //register for later replication in the bulk
                context.getBlobStoreBulkInfo().addDelayedReplicationInfo(context, ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart(), dr);
            }

            context.getBlobStoreBulkInfo().bulk_flush(context, true/*only_if_chunk_reached*/);
        }

    }

    @Override
    public void prepare(Context context, ServerTransaction xtn,
                        ArrayList<IEntryHolder> pLocked, boolean singleParticipant,
                        Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate)
            throws SAException {
        // TODO Auto-generated method stub

        //note- all relevant off-heap entries are pinned and logically locked by the txn
        //since they are all pinned concurrent readers can retrieve the entry without reading from blobStore


        HashMap<String, IEntryHolder> ohEntries = new HashMap<String, IEntryHolder>();
        List<String> uids = (shouldReplicate && _engine.getReplicationNode().getDirectPesistencySyncHandler() != null) ?
                new ArrayList<String>(pLocked.size()) : null;
        Set<String> takes = null;
        boolean checkedOnTake = false;
        //1. locate & setDirty to the relevant entries
        for (IEntryHolder inputeh : pLocked) {
            IEntryHolder entryHolder = inputeh.getOriginalEntryHolder();
            if (!entryHolder.isBlobStoreEntry())
                continue;
            if (entryHolder == null || entryHolder.isDeleted()
                    || entryHolder.getWriteLockTransaction() == null
                    || !entryHolder.getWriteLockTransaction().equals(xtn))
                continue;

            if ((entryHolder.getWriteLockOperation() == SpaceOperations.TAKE || entryHolder.getWriteLockOperation() == SpaceOperations.TAKE_IE) &&
                    !((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().isInBlobStoreStorage())
                continue;  //nothing to do, dummy op

            //setdirty will also prepare for flushing
            ((IBlobStoreEntryHolder) entryHolder).setDirty(_engine.getCacheManager());
            ohEntries.put(entryHolder.getUID(), entryHolder);
            if ((entryHolder.getWriteLockOperation() == SpaceOperations.TAKE || entryHolder.getWriteLockOperation() == SpaceOperations.TAKE_IE)) {
                if (!checkedOnTake) {
                    if (useEmbeddedSyncOnPrimary(useDirectPersistencySync(shouldReplicate)))
                        takes = new HashSet<String>();
                    checkedOnTake = true;
                }
                if (takes != null)
                    takes.add(entryHolder.getUID());
            }
            if (uids != null)
                uids.add(entryHolder.getUID());
        }

        if (ohEntries.isEmpty())
            return;  //nothing to do

        boolean needDirectPersistencySync = false;
        if (uids != null)
            needDirectPersistencySync = handleDirectPersistencyConsistency(context, uids, shouldReplicate, takes, ohEntries);

        //call the underlying blobStore
        try {
            List<BlobStoreBulkOperationRequest> operations = new LinkedList<BlobStoreBulkOperationRequest>();
            for (IEntryHolder inputeh : pLocked) {
                if (!ohEntries.containsKey(inputeh.getUID()))
                    continue;
                IEntryHolder entryHolder = inputeh.getOriginalEntryHolder();
                 BlobStoreRefEntryCacheInfo entryCacheInfo = ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart();
                switch (entryHolder.getWriteLockOperation()) {
                    case SpaceOperations.WRITE:
                        operations.add(new BlobStoreAddBulkOperationRequest(entryCacheInfo.getStorageKey(), entryCacheInfo.getEntryLayout(_engine.getCacheManager()), entryCacheInfo));
                        break;
                    case SpaceOperations.UPDATE:
                        operations.add(new BlobStoreReplaceBulkOperationRequest(entryCacheInfo.getStorageKey(),
                                entryCacheInfo.getEntryLayout(_engine.getCacheManager()), entryCacheInfo.getBlobStoreStoragePos(), entryCacheInfo));
                        break;
                    case SpaceOperations.TAKE:
                    case SpaceOperations.TAKE_IE:
                        boolean phantom = ((IBlobStoreEntryHolder) entryHolder).isPhantom();
                        if (!phantom) { //actual remove
                            operations.add(new BlobStoreRemoveBulkOperationRequest(entryCacheInfo.getStorageKey(), entryCacheInfo.getBlobStoreStoragePos(), entryCacheInfo));
                        }
                        else //update
                            operations.add(new BlobStoreReplaceBulkOperationRequest(entryCacheInfo.getStorageKey(),
                                    entryCacheInfo.getEntryLayout(_engine.getCacheManager()), entryCacheInfo.getBlobStoreStoragePos(), entryCacheInfo));
                        break;
                    default:
                        throw new UnsupportedOperationException("uid=" + entryHolder.getUID() + " operation=" + entryHolder.getWriteLockOperation());
                }

            }

            List<BlobStoreBulkOperationResult> results = _engine.getCacheManager().getBlobStoreStorageHandler().executeBulk(operations, BlobStoreObjectType.DATA, true/*transactional*/);
            //scan and if execption in any result- throw it
            for (BlobStoreBulkOperationResult res : results) {
                if (res.getException() != null)
                    throw res.getException();
            }

            for (BlobStoreBulkOperationResult res : results) {
                IEntryHolder entryHolder = ohEntries.get(res.getId());

                //for each entry in result list perform setDirty to false, and set the BlobStoreStoragePosition if applicable
                switch (entryHolder.getWriteLockOperation()) {
                    case SpaceOperations.WRITE:
                    case SpaceOperations.UPDATE:
                        ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().flushedFromBulk(context,_engine.getCacheManager(), res.getPosition(), false/*removed*/,true /*onXtnEnd*/);
                        break;
                    case SpaceOperations.TAKE:
                    case SpaceOperations.TAKE_IE:
                        ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart().flushedFromBulk(context,_engine.getCacheManager(), null, true/*removed*/,true /*onXtnEnd*/);
                        break;
                }
            }
        } catch (Throwable t) {
            _logger.severe(getClass().getName() + " Prepare " + t);
            throw new SAException(t);
        }

        if (needDirectPersistencySync)
            reportOpPersisted(context);


    }

    @Override
    public void rollback(ServerTransaction xtn, boolean anyUpdates)
            throws SAException {
        // TODO Auto-generated method stub

    }

    @Override
    public IEntryHolder getEntry(Context context, Object uid, String classname,
                                 IEntryHolder template) throws SAException {
        // TODO Auto-generated method stub
        IEntryHolder eh = context.getBlobStoreOpEntryHolder();
        if (eh == null && context.getBlobStoreOpEntryCacheInfo() == null)
            throw new UnsupportedOperationException();

        if ((eh != null && !eh.isBlobStoreEntry()) || (context.getBlobStoreOpEntryCacheInfo() != null && !context.getBlobStoreOpEntryCacheInfo().isBlobStoreEntry()))
            throw new UnsupportedOperationException();

        if (eh == null)
            eh = context.getBlobStoreOpEntryCacheInfo().getEntryHolder(_engine.getCacheManager());
        if (context.getBlobStoreOpPin() && eh.isDeleted())
            return null;
        if (context.getOptimizedBlobStoreReadEnabled() == null) {
            return ((IBlobStoreEntryHolder) eh).getLatestEntryVersion(_engine.getCacheManager(), context.getBlobStoreOpPin(), context);
        }else {
            return ((IBlobStoreEntryHolder) eh).getBlobStoreResidentPart().getLatestEntryVersion(_engine.getCacheManager(), context.getBlobStoreOpPin(), (IBlobStoreEntryHolder) eh, context, context.getOptimizedBlobStoreReadEnabled());
        }
    }


    @Override
    public Map<String, IEntryHolder> getEntries(Context context, Object[] ids,
                                                String typeName, IEntryHolder[] templates) throws SAException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ISAdapterIterator<IEntryHolder> makeEntriesIter(
            ITemplateHolder template, long SCNFilter, long leaseFilter,
            IServerTypeDesc[] subClasses) throws SAException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void commit(ServerTransaction xtn, boolean anyUpdates)
            throws SAException {
        // TODO Auto-generated method stub

    }

    @Override
    public int count(ITemplateHolder template, String[] subClasses)
            throws SAException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void shutDown() throws SAException {
        // TODO Auto-generated method stub
        _engine.getCacheManager().getBlobStoreStorageHandler().close();
        if (_possibleRecoverySA != null)
            _possibleRecoverySA.shutDown();

    }

    @Override
    public boolean isReadOnly() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsExternalDB() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsPartialUpdate() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsGetEntries() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void introduceDataType(ITypeDesc typeDesc) {
        // TODO Auto-generated method stub

        String typeName = typeDesc.getTypeName();
        if (typeName.equals(Object.class.getName()))
            return; //skip root

        if (!typeDesc.isBlobstoreEnabled())
            return; //irrelevant

        synchronized (_classesLock) {
            introduceDataType_impl(typeDesc);
        }
    }

    //note- should be called under classesLock
    private void introduceDataType_impl(ITypeDesc typeDesc) {

        String typeName = typeDesc.getTypeName();
        TypeData typeData = _engine.getCacheManager().getTypeData(_engine.getTypeManager().getServerTypeDesc(typeName));
        BlobStoreStorageAdapterClassInfo cur = _classes.get(typeName);
        boolean renew = (cur == null || !_classes.isContained(typeName, typeData));

        BlobStoreStorageAdapterClassInfo updated = null;
        if (renew) {
            if (typeData == null)  //inactive type
                updated = new BlobStoreStorageAdapterClassInfo(new boolean[typeDesc.getNumOfFixedProperties()], null, (short) 0);
            else
                updated = new BlobStoreStorageAdapterClassInfo(typeData.getIndexesRelatedFixedProperties(), typeData.getIndexesRelatedDynamicProperties(), (cur == null ? 0 : (short) (cur.getStoredVersion() + 1)));
        }


        BlobStoreTypeDescSerializable stored = new BlobStoreTypeDescSerializable((TypeDesc) typeDesc, renew ? updated : cur);
        //NOTE- currently we ignore the blobStorePosition in metadata- should be added later as a field in typeDesc
        if (_engine.getCacheManager().getBlobStoreStorageHandler().get(null, typeName, null, BlobStoreObjectType.METADATA) != null)
            _engine.getCacheManager().getBlobStoreStorageHandler().replace(null, typeName, stored, null, BlobStoreObjectType.METADATA);
        else
            _engine.getCacheManager().getBlobStoreStorageHandler().add(null, typeName, stored, BlobStoreObjectType.METADATA);

        if (renew)
            _classes.put(typeName, updated);
    }

    @Override
    public BlobStoreStorageAdapterClassInfo getBlobStoreStorageAdapterClassInfo(String typeName) {
        return _classes.get(typeName);
    }


    @Override
    public SpaceSynchronizationEndpoint getSynchronizationInterceptor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<?> getDataClass() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addIndexes(String typeName, SpaceIndex[] indexes) {
        // TODO Auto-generated method stub
        if (typeName.equals(Object.class.getName()))
            return; //skip root

        ITypeDesc typeDesc = getType(typeName);
        if (!typeDesc.isBlobstoreEnabled())
            return; //irrelevant
        synchronized (_classesLock) {
            introduceDataType_impl(typeDesc);
        }

    }

    private ITypeDesc getType(String typeName) {
        IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(typeName);
        return serverTypeDesc != null ? serverTypeDesc.getTypeDesc() : null;
    }

    //handle consistenct between primary and backup in case of reloading
    private boolean handleDirectPersistencyConsistency(Context context, IEntryHolder entryHolder, boolean shouldReplicate, boolean removeEntry) {
        if (!useDirectPersistencySync(shouldReplicate))
            return false;
        if (_logger.isLoggable(Level.FINER)) {
            _logger.finer("[" + _engine.getFullSpaceName() + "] handling adding entry with uid=" + entryHolder.getUID() + " to sync using BlobStoreStorageAdapter");
        }
        _engine.getReplicationNode().getDirectPesistencySyncHandler().beforeDirectPersistencyOp(_engine.getCacheManager().getReplicationContext(context), entryHolder, removeEntry);
        return true;
    }

    //handle consistenct between primary and backup in case of reloading
    private boolean handleDirectPersistencyConsistency(Context context, List<String> uids, boolean shouldReplicate, Set<String> removedUids, Map<String, IEntryHolder> entryHolderMap) {
        if (!useDirectPersistencySync(shouldReplicate))
            return false;
        if (_logger.isLoggable(Level.FINER)) {
            _logger.finer("[" + _engine.getFullSpaceName() + "] handling adding entries with uids=" + uids + " to sync using BlobStoreStorageAdapter");
        }
        _engine.getReplicationNode().getDirectPesistencySyncHandler().beforeDirectPersistencyOp(_engine.getCacheManager().getReplicationContext(context), uids, removedUids, entryHolderMap);
        return true;
    }


    private boolean useDirectPersistencySync(boolean shouldReplicate) {
        return (shouldReplicate && _engine.getReplicationNode().getDirectPesistencySyncHandler() != null && _engine.getSpaceImpl().isPrimary());

    }

    private boolean useEmbeddedSyncOnPrimary(boolean directPersistencySyncUsed) {
        return (directPersistencySyncUsed && _engine.getReplicationNode().getDirectPesistencySyncHandler().isEmbeddedListUsed());

    }

    private void reportOpPersisted(Context context) {
        _engine.getReplicationNode().getDirectPesistencySyncHandler().afterOperationPersisted(context.getReplicationContext().getDirectPersistencyPendingEntry());
    }


}
