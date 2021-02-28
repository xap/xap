package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.ILeasedEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import org.slf4j.Logger;

import java.util.ArrayList;

public class RDBMSEntryCacheInfo implements IEntryCacheInfo {

    final private IEntryHolder entryHolder;

    public RDBMSEntryCacheInfo(IEntryHolder entryHolder) {
        this.entryHolder = entryHolder;
    }


    @Override
    public IEntryHolder getEntryHolder(CacheManager cacheManager) {
        return entryHolder;
    }

    public IEntryHolder getEntryHolder() {
        return entryHolder;
    }

    @Override
    public IEntryHolder getEntryHolder(CacheManager cacheManager, Context context) {
        return entryHolder;
    }


    public boolean isDeleted() {
        return getEntryHolder().isDeleted();
    }

    @Override
    public ArrayList<IObjectInfo<IEntryCacheInfo>> getBackRefs() {
        return null;
    }

    @Override
    public void setBackRefs(ArrayList<IObjectInfo<IEntryCacheInfo>> backRefs) {

    }

    @Override
    public IObjectInfo<IEntryCacheInfo> getMainListBackRef() {
        return null;
    }

    @Override
    public boolean indexesBackRefsKept() {
        return false;
    }

    @Override
    public void setMainListBackRef(IObjectInfo<IEntryCacheInfo> mainListBackref) {

    }


    public void setLeaseManagerListRefAndPosition(IStoredList<Object> entriesList, IObjectInfo<Object> entryPos) {

    }


    public IStoredList<Object> getLeaseManagerListRef() {
        return null;
    }

    public IObjectInfo<Object> getLeaseManagerPosition() {
        return null;
    }

    public boolean isConnectedToLeaseManager() {
        return false;
    }

    public boolean isSameLeaseManagerRef(ILeasedEntryCacheInfo other) {
        return false;
    }

    @Override
    public boolean isBlobStoreEntry() {
        return false;
    }

    @Override
    public Object getObjectStoredInLeaseManager() {
        return entryHolder;
    }

    @Override
    public IServerTypeDesc getServerTypeDesc()
    {
        return entryHolder.getServerTypeDesc();
    }


    public int getLatestIndexCreationNumber() {
        return 0;
    }

    public void setLatestIndexCreationNumber(int val) {

    }


    public String getClassName() {
        return getEntryHolder().getClassName();
    }


    public void setEvictionPayLoad(Object evictionBackRef) {
        throw new RuntimeException("setEvictionBackref invalid here");
    }


    public Object getEvictionPayLoad() {
        return null;
    }


    public boolean isTransient() {
        return getEntryHolder().isTransient();
    }


    public String getUID() {
        return getEntryHolder().getUID();
    }

    public void setInCache(boolean checkPendingPin) {
    }

    public boolean isInCache() {
        return true;
    }

    public boolean isInserted() {
        return true;
    }

    public boolean setPinned(boolean value, boolean waitIfPendingInsertion) {
        return true;
    }

    public boolean setPinned(boolean value) {
        return setPinned(value, false /*waitIfPendingInsertion*/);
    }

    public boolean isPinned() {
        return true;
    }

    public boolean setRemoving(boolean isPinned) {
        return true;
    }

    public boolean isRemoving() {
        return false;
    }

    public void setRemoved() {
    }

    public boolean isRemoved() {
        return false;
    }

    public boolean isRemovingOrRemoved() {
        return false;
    }


    public boolean wasInserted() {
        return true;
    }

    public boolean isRecentDelete() {
        return false;
    }

    public void setRecentDelete() {
        throw new RuntimeException("invalid usage !!!");
    }

    @Override
    public boolean preMatch(Context context, ITemplateHolder template) {
        throw new RuntimeException("invalid usage !!!");
    }


    public IObjectInfo<IEntryCacheInfo> getHead() {
        throw new RuntimeException(" invalid usage");
    }

    public IEntryCacheInfo getObjectFromHead() {
        return this;
    }

    public void freeSLHolder(IStoredListIterator slh) {
        throw new RuntimeException(" invalid usage");
    }

    public void remove(IObjectInfo oi) {
        throw new RuntimeException(" invalid usage");
    }

    public void removeUnlocked(IObjectInfo oi) {
        throw new RuntimeException(" invalid usage");
    }

    public boolean invalidate() {
        return false;
    }

    public void dump(Logger logger, String msg) {
    }

    public boolean isEmpty() {
        return false;
    }

    public IStoredListIterator<IEntryCacheInfo> establishListScan(boolean random_scan) {
        return this;
    }

    public IStoredListIterator<IEntryCacheInfo> next(IStoredListIterator<IEntryCacheInfo> slh) {
        return null;
    }

    public IObjectInfo<IEntryCacheInfo> add(IEntryCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    public IObjectInfo<IEntryCacheInfo> addUnlocked(IEntryCacheInfo subject) {
        throw new RuntimeException(" invalid usage");
    }

    public boolean removeByObject(IEntryCacheInfo obj) {
        return false;
    }

    public boolean contains(IEntryCacheInfo obj) {
        return this == obj;
    }

    public void setSubject(IEntryCacheInfo subject) {

    }

    public IEntryCacheInfo getSubject() {
        return this;
    }

    public boolean isMultiObjectCollection() {
        //its a single entry and not a container
        return false;
    }

    /**
     * return true if we can save iterator creation and get a single entry
     *
     * @return true if we can optimize
     */
    public boolean optimizeScanForSingleObject() {
        return true;
    }


    public boolean hasNext()
            throws SAException {
        return true;
    }

    public IEntryCacheInfo next()
            throws SAException {
        return this;
    }


    public void releaseScan()
            throws SAException {
    }


    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }


    public boolean isAlreadyMatched() {
        return false;
    }

    public boolean isIterator() {
        return false;
    }


    public SpaceTypeDescriptor getSpaceTypeDescriptor() {
        return getEntryHolder().getEntryData().getSpaceTypeDescriptor();
    }


    public Object getFixedPropertyValue(int position) {
        return getEntryHolder().getEntryData().getFixedPropertyValue(position);

    }

    public Object getPropertyValue(String name) {
        return getEntryHolder().getEntryData().getPropertyValue(name);
    }

    @Override
    public Object getPathValue(String path) {
        return getEntryHolder().getEntryData().getPathValue(path);
    }

    public int getVersion() {
        return getEntryHolder().getEntryData().getVersion();

    }


    public long getExpirationTime() {
        return getEntryHolder().getEntryData().getExpirationTime();

    }


    public int getHashCode(int id) {
        return getKey(id).hashCode();
    }

    public Object getKey(int id) {
        if (id == UID_HASH_INDICATOR)
            return getUID();
        return getEntryHolder().getEntryData().getFixedPropertyValue(id);
    }

    public IStoredList<IEntryCacheInfo> getValue(int id) {
        return this;
    }

    public boolean isNativeHashEntry() {
        return false;
    }

    public void release() {

    }
}
