package com.j_spaces.core.cache.offheap;

import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class OffHeapUniqueEntriesStore implements ConcurrentMap<Object, IEntryCacheInfo> {


    private CacheManager cacheManager;
    private OffHeapObjectStringMap offHeapMap;

    public OffHeapUniqueEntriesStore(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.offHeapMap = new OffHeapObjectStringMap();
    }


    @Override
    public int size() {
        return offHeapMap.size();
    }

    @Override
    public boolean isEmpty() {
        return offHeapMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return offHeapMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IEntryCacheInfo get(Object key) {
        String uid = offHeapMap.get(key);
        if(uid == null){
            return null;
        }
        return cacheManager.getPEntryByUid(uid);
    }

    @Override
    public IEntryCacheInfo put(Object key, IEntryCacheInfo value) {
        String uid = offHeapMap.put(key, value.getUID());
        if(uid == null){
            return null;
        }
        return cacheManager.getPEntryByUid(uid);
    }

    @Override
    public IEntryCacheInfo remove(Object key) {
        String uid = offHeapMap.remove(key);
        if(uid == null){
            return null;
        }
        return cacheManager.getPEntryByUid(uid);
    }

    @Override
    public void putAll(Map<?, ? extends IEntryCacheInfo> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Object> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<IEntryCacheInfo> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<Object, IEntryCacheInfo>> entrySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IEntryCacheInfo putIfAbsent(Object key, IEntryCacheInfo value) {
        String uid = offHeapMap.putIfAbsent(key, value.getUID());
        if(uid == null){
            return null;
        }
        return cacheManager.getPEntryByUid(uid);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return offHeapMap.remove(key, ((IEntryCacheInfo) value).getUID());
    }

    @Override
    public boolean replace(Object key, IEntryCacheInfo oldValue, IEntryCacheInfo newValue) {
        return offHeapMap.replace(key, oldValue.getUID(), newValue.getUID());
    }

    @Override
    public IEntryCacheInfo replace(Object key, IEntryCacheInfo value) {
        String uid = offHeapMap.replace(key, value.getUID());
        if(uid == null){
            return null;
        }
        return cacheManager.getPEntryByUid(uid);
    }
}
