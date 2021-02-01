package com.gigaspaces.internal.server.space.smart_cache;

public interface TieredStorageManager {

    CachePredicate getCacheRule(String typeName); // get cache rule for a specific type


    CachePredicate getRetentionRule(String typeName); // get retention rule for a specific type

    void setCacheRule(String typeName, CachePredicate newRule); // dynamically change rule

    // For the future when we would want to support warm layer
    //    CachePredicate getCacheRule(String typeName, String tier);
    //    Map<String,CachePredicate> getCacheRulesForTiers(String typeName);
}
