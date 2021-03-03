package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.TieredState;

import java.util.Map;

public class TieredStorageManagerImpl implements TieredStorageManager {
    private Map<String, TimePredicate> retentionRules;
    private Map<String, CachePredicate> hotCacheRules;
    private InternalRDBMS internalDiskStorage;

    public TieredStorageManagerImpl() {

    }

    public TieredStorageManagerImpl(Map<String, TimePredicate> retentionRules, Map<String, CachePredicate> hotCacheRules, InternalRDBMS internalDiskStorage) {
        this.retentionRules = retentionRules;
        this.hotCacheRules = hotCacheRules;
        this.internalDiskStorage = internalDiskStorage;
    }

    @Override
    public CachePredicate getCacheRule(String typeName) {
        return hotCacheRules.get(typeName);
    }

    @Override
    public TimePredicate getRetentionRule(String typeName) {
        return retentionRules.get(typeName);
    }

    @Override
    public void setCacheRule(String typeName, CachePredicate newRule) {
        hotCacheRules.put(typeName, newRule);
        //TODO - handle update (shuffle / evict)
    }

    @Override
    public InternalRDBMS getInternalStorage() {
        return this.internalDiskStorage;
    }

    @Override
    public TieredState getEntryTieredState(IEntryData entryData) {
        String typeName = entryData.getSpaceTypeDescriptor().getTypeName();
        CachePredicate cacheRule = getCacheRule(typeName);
        if (cacheRule == null) {
            return TieredState.TIERED_COLD;
        } else if (cacheRule.evaluate(entryData)) {
            if (cacheRule.isTransient()) {
                return TieredState.TIERED_HOT;
            } else {
                return TieredState.TIERED_HOT_AND_COLD;
            }
        } else {
            return TieredState.TIERED_COLD;
        }
    }

    @Override
    public TieredState guessEntryTieredState(String typeName) {
        CachePredicate cacheRule = getCacheRule(typeName);
        if (cacheRule == null) {
            return TieredState.TIERED_COLD;
        } else if (cacheRule.isTransient()) {
            return TieredState.TIERED_HOT;
        } else {
            return TieredState.TIERED_HOT_AND_COLD;
        }
    }

    @Override
    public TieredState guessTemplateTier(ITemplateHolder templateHolder) { // TODO - tiered storage - return TemplateMatchTier, hot and cold
        String typeName = templateHolder.getServerTypeDesc().getTypeName();
        CachePredicate cacheRule = getCacheRule(typeName);
        if (cacheRule == null) {
            return TieredState.TIERED_COLD;
        } else {
            if (cacheRule.isTransient()) {
                return TieredState.TIERED_HOT;
            } else if (templateHolder.isIdQuery()) {
                return TieredState.TIERED_HOT_AND_COLD;
            } else {
                if (!cacheRule.evaluate(templateHolder)) {
                    return TieredState.TIERED_COLD;
                } else {
                    return TieredState.TIERED_HOT;
                }
            }
        }
    }


}
