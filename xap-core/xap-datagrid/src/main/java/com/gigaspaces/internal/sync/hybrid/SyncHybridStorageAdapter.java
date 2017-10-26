/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces.
 * You may use the software source code solely under the terms and limitations of
 * The license agreement granted to you by GigaSpaces.
 *******************************************************************************/
package com.gigaspaces.internal.sync.hybrid;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.sync.SynchronizationStorageAdapter;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.blobStore.sadapter.BlobStoreStorageAdapter;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.blobStore.IBlobStoreEntryHolder;
import com.j_spaces.core.cache.blobStore.errors.BlobStoreErrorsHandler;
import com.j_spaces.core.cache.blobStore.sadapter.BlobStoreStorageAdapterClassInfo;
import com.j_spaces.core.cache.blobStore.sadapter.IBlobStoreStorageAdapter;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.core.sadapter.MemorySA;
import com.j_spaces.core.sadapter.SAException;
import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author yaeln
 * @since 11.0.1
 */
public class SyncHybridStorageAdapter implements IStorageAdapter, IBlobStoreStorageAdapter {
    private final static Logger logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT);
    private final CacheManager cacheManager;
    private final SynchronizationStorageAdapter synchronizationStorageAdapter;
    private final BlobStoreStorageAdapter blobStoreStorageAdapter;

    public SyncHybridStorageAdapter(CacheManager cacheManager, IStorageAdapter syncSA, IStorageAdapter blobStoreSA){
        this.cacheManager = cacheManager;
        if (syncSA instanceof SynchronizationStorageAdapter && blobStoreSA instanceof BlobStoreStorageAdapter) {
            this.synchronizationStorageAdapter = (SynchronizationStorageAdapter) syncSA;
            this.blobStoreStorageAdapter = (BlobStoreStorageAdapter) blobStoreSA;
        }
        else {
            throw new IllegalStateException("SyncHybrid is supported only with a combination of SynchronizationStorageAdapter and BlobStoreStorageAdapter");
        }
        validateConfiguration(syncSA);
    }

    private void validateConfiguration(IStorageAdapter syncSA) {
        if((syncSA instanceof MemorySA))
            throw new IllegalStateException("SyncHybrid without SpaceSynchronizationEndpoint is not supported");

        if(!cacheManager.isBlobStoreCachePolicy())
            throw new IllegalStateException("SyncHybrid without blobstore is not supported");

        if(cacheManager.isPersistentBlobStore())
            throw new IllegalStateException("SyncHybrid with persistent blobstore is not supported, please set persistent=false in blobstore handler");

        if(cacheManager.getEngine().hasMirror())
            throw new IllegalStateException("SyncHybrid with mirror service is not supported");

        if(cacheManager.useBlobStoreBulks())
            throw new IllegalStateException("SyncHybrid with blobstore bulks is not supported");

        if(synchronizationStorageAdapter.getSynchronizationInterceptor() == null)
            throw new IllegalStateException("SyncHybrid without space-sync-endpoint is not supported, please provide space-sync-endpoint");
    }

    @Override
    public BlobStoreStorageAdapterClassInfo getBlobStoreStorageAdapterClassInfo(String typeName){
        return blobStoreStorageAdapter.getBlobStoreStorageAdapterClassInfo(typeName);
    }

    @Override
    public boolean hasAnotherInitialLoadSource() {
        return false;
    }

    @Override
    public void initialize() throws SAException {
        if (logger.isLoggable(Level.INFO)) {
            logger.info("["+cacheManager.getEngine().getFullSpaceName()+"] Initializing sync hybrid storage adapter");
        }
        synchronizationStorageAdapter.initialize();
        blobStoreStorageAdapter.initialize();
    }

    @Override
    public ISAdapterIterator initialLoad(Context context, ITemplateHolder template) throws SAException {
         return synchronizationStorageAdapter.initialLoad(context, template);
    }

    @Override
    public void insertEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean shouldReplicate) throws SAException {
        if (origin) { // don't persist to DB in backup side
            try {
                synchronizationStorageAdapter.insertEntry(context, entryHolder, true, shouldReplicate);
            } catch (SAException ex) {
                handleFailure(context, entryHolder, true/*on db*/, SpaceOperations.WRITE, ex);
            }
        }
        try {
            blobStoreStorageAdapter.insertEntry(context, entryHolder, origin, shouldReplicate);
        } catch (Exception ex) {
            handleFailure(context, entryHolder, false/*on blobstore*/, SpaceOperations.WRITE, ex);
        }
    }

    @Override
    public void updateEntry(Context context, IEntryHolder updatedEntry, boolean updateRedoLog, boolean origin, boolean[] partialUpdateValuesIndicators) throws SAException {
        if (origin) { // don't persist to DB in backup side
            try {
                synchronizationStorageAdapter.updateEntry(context, updatedEntry, updateRedoLog, true, partialUpdateValuesIndicators);
            } catch (SAException ex) {
                handleFailure(context, updatedEntry, true/*on db*/, SpaceOperations.UPDATE, ex);
            }
        }
        try {
            blobStoreStorageAdapter.updateEntry(context, updatedEntry, updateRedoLog, origin, partialUpdateValuesIndicators);
        }catch (Exception ex){
            handleFailure(context, updatedEntry, false/*on blobstore*/, SpaceOperations.UPDATE, ex);
        }
    }

    @Override
    public void removeEntry(Context context, IEntryHolder entryHolder, boolean origin, boolean fromLeaseExpiration, boolean shouldReplicate) throws SAException {
        if (origin) { // don't persist to DB in backup side
            try {
                synchronizationStorageAdapter.removeEntry(context, entryHolder, true, fromLeaseExpiration, shouldReplicate);
            } catch (SAException ex) {
                handleFailure(context, entryHolder, true/*on db*/, SpaceOperations.TAKE, ex);
            }
        }
        try {
            blobStoreStorageAdapter.removeEntry(context, entryHolder, origin, fromLeaseExpiration, shouldReplicate);
        }catch (Exception ex){
            handleFailure(context, entryHolder, false/*on blobstore*/, SpaceOperations.TAKE, ex);
        }
    }

    @Override
    public void prepare(Context context, ServerTransaction xtn, ArrayList<IEntryHolder> pLocked, boolean singleParticipant, Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo, boolean shouldReplicate) throws SAException {
        try {
            synchronizationStorageAdapter.prepare(context, xtn, pLocked, singleParticipant, partialUpdatesAndInPlaceUpdatesInfo, shouldReplicate);
        }
        catch (Exception ex){
            throw new SyncHybridSAException(ex, context.getSyncHybridOperationDetails(), SyncHybridExceptionOrigin.SYNCHRONIZATION);
        }
        try {
            blobStoreStorageAdapter.prepare(context, xtn, pLocked, singleParticipant, partialUpdatesAndInPlaceUpdatesInfo, shouldReplicate);
        }
        catch (Exception ex){
            throw new SyncHybridSAException(ex, context.getSyncHybridOperationDetails(), SyncHybridExceptionOrigin.BLOBSTORE);
        }
    }

    @Override
    public void rollback(ServerTransaction xtn, boolean anyUpdates) throws SAException {
        synchronizationStorageAdapter.rollback(xtn, anyUpdates);
        blobStoreStorageAdapter.rollback(xtn, anyUpdates);
    }

    @Override
    public IEntryHolder getEntry(Context context, Object uid, String classname, IEntryHolder template) throws SAException {
        return blobStoreStorageAdapter.getEntry(context, uid, classname, template);
    }

    @Override
    public Map<String, IEntryHolder> getEntries(Context context, Object[] ids, String typeName, IEntryHolder[] templates) throws SAException {
        return blobStoreStorageAdapter.getEntries(context, ids, typeName, templates);
    }

    @Override
    public ISAdapterIterator<IEntryHolder> makeEntriesIter(ITemplateHolder template, long SCNFilter, long leaseFilter, IServerTypeDesc[] subClasses) throws SAException {
        return blobStoreStorageAdapter.makeEntriesIter(template, SCNFilter, leaseFilter, subClasses);
    }

    @Override
    public void commit(ServerTransaction xtn, boolean anyUpdates) throws SAException {
        synchronizationStorageAdapter.commit(xtn, anyUpdates);
        blobStoreStorageAdapter.commit(xtn, anyUpdates);
    }

    @Override
    public int count(ITemplateHolder template, String[] subClasses) throws SAException {
        return blobStoreStorageAdapter.count(template, subClasses);
    }

    @Override
    public void shutDown() throws SAException {
        synchronizationStorageAdapter.shutDown();
        blobStoreStorageAdapter.shutDown();
    }

    @Override
    public boolean isReadOnly() {
        return blobStoreStorageAdapter.isReadOnly();
    }

    @Override
    public boolean supportsExternalDB() {
        return blobStoreStorageAdapter.supportsExternalDB();
    }

    @Override
    public boolean supportsPartialUpdate() {
        return blobStoreStorageAdapter.supportsPartialUpdate();
    }

    @Override
    public boolean supportsGetEntries() {
        return blobStoreStorageAdapter.supportsGetEntries();
    }

    @Override
    public void introduceDataType(ITypeDesc typeDesc) {
        synchronizationStorageAdapter.introduceDataType(typeDesc);
        blobStoreStorageAdapter.introduceDataType(typeDesc);
    }

    @Override
    public SpaceSynchronizationEndpoint getSynchronizationInterceptor() {
        return synchronizationStorageAdapter.getSynchronizationInterceptor();
    }

    @Override
    public Class<?> getDataClass() {
        return synchronizationStorageAdapter.getDataClass();
    }

    @Override
    public void addIndexes(String typeName, SpaceIndex[] indexes) {
        synchronizationStorageAdapter.addIndexes(typeName, indexes);
        blobStoreStorageAdapter.addIndexes(typeName, indexes);
    }

    @Override
    public boolean shouldRevertOpOnBlobStoreError() {
        return true;
    }

    /**
     * Revert operation only if failed on DB, throw a fully detailed exception about the failure operations
     */
    private void handleFailure(Context context, IEntryHolder entryHolder, boolean db, int spaceOperation, Exception ex) throws SyncHybridSAException {
        String operationType = null;
        SyncHybridExceptionOrigin exceptionOrigin = db ? SyncHybridExceptionOrigin.SYNCHRONIZATION : SyncHybridExceptionOrigin.BLOBSTORE;

        switch (spaceOperation)
        {
            case SpaceOperations.WRITE:
                if (db) {
                    BlobStoreErrorsHandler.onFailedWrite(cacheManager, context, ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart(), entryHolder);
                }
                operationType = "write";
                break;
            case SpaceOperations.UPDATE:
                if (db) {
                    BlobStoreErrorsHandler.onFailedUpdate(cacheManager, context, ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart(), entryHolder);
                }
                operationType = "update";
                break;
            case SpaceOperations.TAKE:
                if (db) {
                    BlobStoreErrorsHandler.onFailedRemove(cacheManager, context, ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart(), entryHolder);
                }
                operationType = "remove";
                break;
        }
        if (logger.isLoggable(Level.FINER)) {
            logger.throwing(getClass().getName(), "failed to execute " + operationType + " operation on " + exceptionOrigin, ex);
        }
        throw new SyncHybridSAException(ex, context.getSyncHybridOperationDetails(), exceptionOrigin);
    }
}
