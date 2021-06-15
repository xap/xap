package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.LongCounter;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InternalRDBMSManager {

    InternalRDBMS internalRDBMS;
    private final LongCounter readDisk = new LongCounter();
    private final LongCounter writeDisk = new LongCounter();
    private final EntriesInfo entriesInfo = new EntriesInfo();

    public InternalRDBMSManager(InternalRDBMS internalRDBMS) {
        this.internalRDBMS = internalRDBMS;
    }

    public boolean initialize(String spaceName, String fullMemberName, SpaceTypeManager typeManager, boolean isBackup) throws SAException{
        return internalRDBMS.initialize(spaceName, fullMemberName, typeManager, isBackup);
    }

    public long getDiskSize() throws SAException, IOException{
        return internalRDBMS.getDiskSize();
    }

    public long getFreeSpaceSize() throws SAException, IOException{
        return internalRDBMS.getFreeSpaceSize();
    }

    public void createTable(ITypeDesc typeDesc) throws SAException{
        internalRDBMS.createTable(typeDesc);
    }

    public EntriesInfo getEntriesInfo(){
        return entriesInfo;
    }

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryHolder entry to insert
     * @param initialLoadOrigin
     */
    public void insertEntry(Context context, IEntryHolder entryHolder, CacheManager.InitialLoadOrigin initialLoadOrigin) throws SAException{
        if(initialLoadOrigin != CacheManager.InitialLoadOrigin.FROM_TIERED_STORAGE && context.isDiskEntry() && entryHolder.getXidOriginatedTransaction() == null) {
            writeDisk.inc();
            internalRDBMS.insertEntry(context, entryHolder);
        }
        String type = entryHolder.getServerTypeDesc().getTypeName();
        entriesInfo.increaseCounterMap(type);
        if(context.isRAMEntry()){
            entriesInfo.increaseRamCounterMap(type);
        }
    }

    /**
     * updates an entry.
     *
     * @param updatedEntry new content, same UID and class
     */
    public void updateEntry(Context context, IEntryHolder updatedEntry) throws SAException{
        internalRDBMS.updateEntry(context, updatedEntry);

    }

    /**
     * Removes an entry from the  internalDiskStorage
     *
     * @param entryHolder entry to remove
     */
    public boolean removeEntry(Context context, IEntryHolder entryHolder) throws SAException{
        boolean removed = false;
        if(context.getEntryTieredState() != TieredState.TIERED_HOT) {
            removed = internalRDBMS.removeEntry(context, entryHolder);
        }
        String type = entryHolder.getServerTypeDesc().getTypeName();
        if(removed || context.getEntryTieredState() == TieredState.TIERED_HOT){
            entriesInfo.decreaseCounterMap(type);
            if(context.isRAMEntry()){
                entriesInfo.decreaseRamCounterMap(type);
            }
        }
        return removed;
    }



    public void updateRamCounterAfterUpdate(String type, boolean isUpdatedEntryHot, boolean isOriginEntryHot){
        if(isOriginEntryHot != isUpdatedEntryHot){
            if(isUpdatedEntryHot){
                entriesInfo.increaseRamCounterMap(type);
            }
            else{
                entriesInfo.decreaseRamCounterMap(type);
            }
        }
    }

    public IEntryHolder getEntryById(Context context, String typeName, Object id, ITemplateHolder templateHolder) throws SAException{
        if (templateHolder != null && templateHolder.isReadOperation()){
            readDisk.inc();
        }

        return internalRDBMS.getEntryById(context, typeName, id);
    }

    public IEntryHolder getEntryByUID(Context context, String typeName, String uid, ITemplateHolder templateHolder) throws SAException{
        if (templateHolder != null && templateHolder.isReadOperation()){
            readDisk.inc();
        }
        return internalRDBMS.getEntryByUID(context, typeName, uid);
    }

    public ISAdapterIterator<IEntryHolder> makeEntriesIter(Context context, String typeName, ITemplateHolder templateHolder) throws SAException{
        if (templateHolder != null && templateHolder.isReadOperation() && !context.isDisableTieredStorageMetric()){
            readDisk.inc();
        }

        return internalRDBMS.makeEntriesIter(context, typeName, templateHolder);
    }

    public boolean isKnownType(String name){
        return internalRDBMS.isKnownType(name);
    }

    public void shutDown(){
        internalRDBMS.shutDown();
    }

    public LongCounter getReadDisk() {
        return readDisk;
    }

    public LongCounter getWriteDisk() {
        return writeDisk;
    }

    public void deleteData() throws SAException {
        internalRDBMS.deleteData();
    }

    public void persistType(ITypeDesc typeDesc) throws SAException {
        internalRDBMS.persistType(typeDesc);
    }

    public void initialLoad(Context context, SpaceEngine engine, InitialLoadInfo initialLoadInfo) throws SAException {
        internalRDBMS.initialLoad(context, engine, initialLoadInfo);
    }

    public SpaceTypeManager getTypeManager() {
        return internalRDBMS.getTypeManager();
    }
}

