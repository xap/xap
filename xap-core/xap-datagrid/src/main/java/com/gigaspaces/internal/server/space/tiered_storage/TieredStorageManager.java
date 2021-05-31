package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.SAException;

public interface TieredStorageManager {

    boolean hasCacheRule(String typeName); // check if cache rule exist for a specific type

    boolean isTransient(String typeName); // check if a specific type is transient

    CachePredicate getCacheRule(String typeName); // get cache rule for a specific type

    TieredStorageTableConfig getTableConfig(String typeName);

    TimePredicate getRetentionRule(String typeName); // get retention rule for a specific type

    void setCacheRule(String typeName, CachePredicate newRule); // dynamically change rule

    InternalRDBMS getInternalStorage();

    TieredState getEntryTieredState(IEntryData entryData);

    TieredState guessEntryTieredState(String typeName);

    TemplateMatchTier guessTemplateTier(ITemplateHolder templateHolder);

    boolean isWarmStart();

    void initialize(String spaceName, String fullMemberName, SpaceTypeManager typeManager) throws SAException;

    // For the future when we would want to support warm layer
    //    CachePredicate getCacheRule(String typeName, String tier);
    //    Map<String,CachePredicate> getCacheRulesForTiers(String typeName);
}
