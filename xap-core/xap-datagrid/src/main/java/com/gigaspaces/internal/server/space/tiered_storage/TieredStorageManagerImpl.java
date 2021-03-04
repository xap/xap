package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.Constants;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.sql.ReadQueryParser;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TieredStorageManagerImpl implements TieredStorageManager {
    private IDirectSpaceProxy spaceProxy;
    private TieredStorageConfig storageConfig;
    private Map<String, TimePredicate> retentionRules = new HashMap<>();
    private Map<String, CachePredicate> hotCacheRules = new HashMap<>();

    private InternalRDBMS internalDiskStorage;

    public TieredStorageManagerImpl() {

    }

    public TieredStorageManagerImpl(TieredStorageConfig storageConfig, InternalRDBMS internalDiskStorage, IDirectSpaceProxy proxy) {
        this.internalDiskStorage = internalDiskStorage;
        this.storageConfig = storageConfig;
        this.spaceProxy = proxy;
    }

    @Override
    public CachePredicate getCacheRule(String typeName) {
        if (hotCacheRules.get(typeName) == null) {
            if (storageConfig.getTables().get(typeName) != null) {
                initCacheRule(storageConfig.getTables().get(typeName), spaceProxy);
                return hotCacheRules.get(typeName);
            } else {
                return null;
            }
        } else {
            return hotCacheRules.get(typeName);
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


    private void initCacheRule(TieredStorageTableConfig tableConfig, IDirectSpaceProxy proxy) {
        //TODO - validate transient has null criteria && period
        if (tableConfig.getTimeColumn() != null) {
            if (tableConfig.getPeriod() != null) {
                hotCacheRules.put(tableConfig.getName(), new TimePredicate(tableConfig.getName(), tableConfig.getTimeColumn(), tableConfig.getPeriod(), tableConfig.isTransient()));
            }

            if (tableConfig.getRetention() != null) {
                retentionRules.put(tableConfig.getName(), new TimePredicate(tableConfig.getName(), tableConfig.getTimeColumn(), tableConfig.getRetention(), tableConfig.isTransient()));
            }
        }

        if (tableConfig.getCriteria() != null) {
            if (tableConfig.getCriteria().equalsIgnoreCase(AllPredicate.ALL_KEY_WORD)) {
                hotCacheRules.put(tableConfig.getName(), new AllPredicate(tableConfig.isTransient()));
            } else {
                ReadQueryParser parser = new ReadQueryParser();
                AbstractDMLQuery sqlQuery = null;
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
                    hotCacheRules.put(template.getTypeName(), new CriteriaRangePredicate(template.getTypeName(), range, tableConfig.isTransient()));
                }

            }
        }

        if (tableConfig.isTransient()) {
            hotCacheRules.put(tableConfig.getName(), Constants.TieredStorage.TRANSIENT_ALL_CACHE_PREDICATE);
        }
    }


}
