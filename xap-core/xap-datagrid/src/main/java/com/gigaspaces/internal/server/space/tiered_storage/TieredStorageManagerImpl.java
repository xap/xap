/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.Constants;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.sql.ReadQueryParser;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

public class TieredStorageManagerImpl implements TieredStorageManager {
    private Logger logger;
    private IDirectSpaceProxy spaceProxy;
    private TieredStorageConfig storageConfig;
    private ConcurrentHashMap<String, TimePredicate> retentionRules = new ConcurrentHashMap<>(); //TODO - tiered storage - lazy init retention rules
    private ConcurrentHashMap<String, CachePredicate> hotCacheRules = new ConcurrentHashMap<>();

    private InternalRDBMS internalDiskStorage;

    public TieredStorageManagerImpl() {

    }

    public TieredStorageManagerImpl(TieredStorageConfig storageConfig, InternalRDBMS internalDiskStorage, IDirectSpaceProxy proxy, String fullSpaceName) {
        this.logger = LoggerFactory.getLogger(Constants.TieredStorage.getLoggerName(fullSpaceName));
        this.internalDiskStorage = internalDiskStorage;
        this.storageConfig = storageConfig;
        this.spaceProxy = proxy;
    }

    @Override
    public boolean hasCacheRule(String typeName) {
        return storageConfig.getTables().get(typeName) != null;
    }

    @Override
    public boolean isTransient(String typeName) {
        return storageConfig.getTables().get(typeName) != null && storageConfig.getTables().get(typeName).isTransient();
    }

    @Override
    public CachePredicate getCacheRule(String typeName) {
        if (hasCacheRule(typeName)) {
            return hotCacheRules.computeIfAbsent(typeName, typeName1 -> createCacheRule(storageConfig.getTables().get(typeName1), spaceProxy));
        } else {
            return null;
        }
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
            logger.trace("No cache rule for type {}, EntryTieredState = TIERED_COLD", typeName);
            return TieredState.TIERED_COLD;
        }

        if (cacheRule.isTransient()) {
            logger.trace("Type {} is transient, EntryTieredState = TIERED_HOT", typeName);
            return TieredState.TIERED_HOT;
        }

        if (cacheRule.evaluate(entryData)) {
            logger.trace("Fits cache rule for type {}, EntryTieredState = TIERED_HOT_AND_COLD", typeName);
            return TieredState.TIERED_HOT_AND_COLD;
        }

        logger.trace("Doesn't Fit cache rule for type {}, EntryTieredState = TIERED_COLD", typeName);
        return TieredState.TIERED_COLD;
    }

    @Override
    public TieredState guessEntryTieredState(String typeName) {
        CachePredicate cacheRule = getCacheRule(typeName);
        if (cacheRule == null) {
            logger.trace("No cache rule for type {}, EntryTieredState = TIERED_COLD", typeName);
            return TieredState.TIERED_COLD;
        } else if (cacheRule.isTransient()) {
            logger.trace("Type {} is transient, EntryTieredState = TIERED_HOT", typeName);
            return TieredState.TIERED_HOT;
        } else {
            logger.trace("Has cache rule for type {}, EntryTieredState = TIERED_HOT_AND_COLD", typeName);
            return TieredState.TIERED_HOT_AND_COLD;
        }
    }

    @Override
    public TemplateMatchTier guessTemplateTier(ITemplateHolder templateHolder) { // TODO - tiered storage - return TemplateMatchTier, hot and cold
        String typeName = templateHolder.getServerTypeDesc().getTypeName();
        CachePredicate cacheRule = getCacheRule(typeName);
        if (cacheRule == null) {
            logger.trace("No cache rule for type {}, TemplateMatchTier = MATCH_COLD", typeName);
            return templateHolder.isEmptyTemplate() ? TemplateMatchTier.MATCH_HOT_AND_COLD : TemplateMatchTier.MATCH_COLD;
        } else {
            if (cacheRule.isTransient()) {
                logger.trace("Type {} is transient, TemplateMatchTier = MATCH_HOT", typeName);
                return TemplateMatchTier.MATCH_HOT;
            } else if (templateHolder.isIdQuery()) {
                logger.trace("Id query for type {}, TemplateMatchTier = MATCH_HOT_AND_COLD", typeName);
                return TemplateMatchTier.MATCH_HOT_AND_COLD;
            } else {
                TemplateMatchTier templateMatchTier = cacheRule.evaluate(templateHolder);
                logger.trace("Query for type {}, TemplateMatchTier = {}", typeName, templateMatchTier);
                return templateMatchTier;
            }
        }
    }


    private CachePredicate createCacheRule(TieredStorageTableConfig tableConfig, IDirectSpaceProxy proxy) {
        //TODO - validate transient has null criteria && period
        CachePredicate result = null;
        if (tableConfig.isTransient()) {
            result = Constants.TieredStorage.TRANSIENT_ALL_CACHE_PREDICATE;
        } else if (tableConfig.getTimeColumn() != null) {
            if (tableConfig.getPeriod() != null) {
                return new TimePredicate(tableConfig.getName(), tableConfig.getTimeColumn(), tableConfig.getPeriod(), tableConfig.isTransient());
            }
        } else if (tableConfig.getCriteria() != null) {
            if (tableConfig.getCriteria().equalsIgnoreCase(AllPredicate.ALL_KEY_WORD)) {
                result = new AllPredicate(tableConfig.isTransient());
            } else {
                ReadQueryParser parser = new ReadQueryParser();
                AbstractDMLQuery sqlQuery;
                try {
                    sqlQuery = parser.parseSqlQuery(new SQLQuery(tableConfig.getName(), tableConfig.getCriteria()), proxy);
                } catch (SQLException e) {
                    throw new RuntimeException("failed to parse criteria cache rule '" + tableConfig.getCriteria() + "'", e);
                }
                QueryTemplatePacket template = sqlQuery.getExpTree().getTemplate();
                HashMap<String, Range> ranges = template.getRanges();
                if (ranges.size() > 1) {
                    throw new IllegalArgumentException("currently only single range is supported");
                }
                Iterator<String> iterator = ranges.keySet().iterator();
                if (iterator.hasNext()) {
                    Range range = ranges.get(iterator.next());
                    result = new CriteriaRangePredicate(template.getTypeName(), range, tableConfig.isTransient());
                }
            }
        }

        if (result == null) {
            throw new IllegalStateException("Failed to create CachePredicate for " + tableConfig);
        }

        return result;
    }


}
