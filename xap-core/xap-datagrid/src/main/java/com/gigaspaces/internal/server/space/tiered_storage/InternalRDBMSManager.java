package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.LongCounter;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class InternalRDBMSManager {

    InternalRDBMS internalRDBMS;
    private final LongCounter readDisk = new LongCounter();
    private final LongCounter writeDisk = new LongCounter();
    Map<String,LongCounter> totalCounterMap = new HashMap<>();
    Map<String, LongCounter> ramCounterMap = new HashMap<>();

    public InternalRDBMSManager(InternalRDBMS internalRDBMS) {
        this.internalRDBMS = internalRDBMS;
    }


    public void initialize(String spaceName, String fullMemberName, SpaceTypeManager typeManager) throws SAException{
        internalRDBMS.initialize(spaceName, fullMemberName, typeManager);
        totalCounterMap.put("java.lang.Object",new LongCounter());
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

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryHolder entry to insert
     */
    public void insertEntry(Context context,  IEntryHolder entryHolder) throws SAException{
        if(context.isColdEntry() && entryHolder.getXidOriginatedTransaction() == null) {
            writeDisk.inc();
            internalRDBMS.insertEntry(context, entryHolder);
        }
        String type = entryHolder.getServerTypeDesc().getTypeName();
        if(!totalCounterMap.containsKey(type)){
            totalCounterMap.put(type,new LongCounter());
            if(context.isHotEntry()){
                ramCounterMap.put(type,new LongCounter());
            }
        }
        getCounterFromCounterMap(type).inc();
        if(context.isHotEntry()){
            getRamCounterFromCounterMap(type).inc();
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
            getCounterFromCounterMap(type).dec();
            if(context.isHotEntry()){
                getRamCounterFromCounterMap(type).dec();
            }
        }
        return removed;
    }

    private LongCounter getCounterFromCounterMap(String type){
        if(!totalCounterMap.containsKey(type)){
            totalCounterMap.put(type, new LongCounter());
        }
        return totalCounterMap.get(type);
    }

    private LongCounter getRamCounterFromCounterMap(String type){
        if(!ramCounterMap.containsKey(type)){
            ramCounterMap.put(type, new LongCounter());
        }
        return ramCounterMap.get(type);
    }

    public void updateRamCounterAfterUpdate(String type, boolean isUpdatedEntryHot, boolean isOriginEntryHot){
        if(isOriginEntryHot != isUpdatedEntryHot){
            if(isUpdatedEntryHot){
                getRamCounterFromCounterMap(type).inc();
            }
            else{
                getRamCounterFromCounterMap(type).dec();
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

    public Map<String,Integer> getCounterMap() {
        return totalCounterMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (int) e.getValue().getCount()));
    }

    public Map<String,Integer> getRamCounterMap() {
        return ramCounterMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (int) e.getValue().getCount()));
    }
}

