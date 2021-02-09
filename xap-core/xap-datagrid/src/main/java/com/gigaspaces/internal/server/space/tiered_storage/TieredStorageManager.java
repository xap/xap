package com.gigaspaces.internal.server.space.tiered_storage;

public interface TieredStorageManager {

    CachePredicate getCacheRule(String typeName); // get cache rule for a specific type

    TimePredicate getRetentionRule(String typeName); // get retention rule for a specific type

    void setCacheRule(String typeName, CachePredicate newRule); // dynamically change rule

    InternalRDBMS getInternalStorage();

    // For the future when we would want to support warm layer
    //    CachePredicate getCacheRule(String typeName, String tier);
    //    Map<String,CachePredicate> getCacheRulesForTiers(String typeName);
}
