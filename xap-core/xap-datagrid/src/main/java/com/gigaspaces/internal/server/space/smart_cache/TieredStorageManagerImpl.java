package com.gigaspaces.internal.server.space.smart_cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TieredStorageManagerImpl implements TieredStorageManager {
    private final Map<String, CachePredicate> retentionRules = new HashMap<>();
    private Map<String, CachePredicate> hotCacheRules = new ConcurrentHashMap<>();

    public TieredStorageManagerImpl() {
        //TODO - init rules
    }

    @Override
    public CachePredicate getCacheRule(String typeName) {
        return hotCacheRules.get(typeName);
    }

    @Override
    public CachePredicate getRetentionRule(String typeName) {
        return retentionRules.get(typeName);
    }

    @Override
    public void setCacheRule(String typeName, CachePredicate newRule) {
        hotCacheRules.put(typeName, newRule);
        //TODO - handle update (shuffle / evict)
    }
}
