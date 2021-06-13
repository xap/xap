package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.MetricManager;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.sadapter.SAException;

import java.rmi.RemoteException;

public interface TieredStorageManager {

    boolean hasCacheRule(String typeName); // check if cache rule exist for a specific type

    boolean isTransient(String typeName); // check if a specific type is transient

    CachePredicate getCacheRule(String typeName); // get cache rule for a specific type

    TieredStorageTableConfig getTableConfig(String typeName);

    TimePredicate getRetentionRule(String typeName); // get retention rule for a specific type

    void setCacheRule(String typeName, CachePredicate newRule); // dynamically change rule

    InternalRDBMSManager getInternalStorage();

    TieredState getEntryTieredState(IEntryData entryData);

    TieredState guessEntryTieredState(String typeName);

    TemplateMatchTier guessTemplateTier(ITemplateHolder templateHolder);

    void initTieredStorageMetrics(SpaceImpl _spaceImpl, MetricManager metricManager);

    void close();

    void initialize(SpaceEngine engine) throws SAException, RemoteException;

    boolean RDBMSContainsData();

    // For the future when we would want to support warm layer
    //    CachePredicate getCacheRule(String typeName, String tier);
    //    Map<String,CachePredicate> getCacheRulesForTiers(String typeName);
}
