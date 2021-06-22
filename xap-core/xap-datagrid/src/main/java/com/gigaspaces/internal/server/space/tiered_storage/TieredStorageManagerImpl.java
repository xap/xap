package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.*;
import com.j_spaces.core.Constants;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.cache.context.TieredState;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.sql.ReadQueryParser;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.range.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TieredStorageManagerImpl implements TieredStorageManager {
    private Logger logger;
    private TieredStorageConfig storageConfig;
    private boolean containsData;
    private ConcurrentHashMap<String, TimePredicate> retentionRules = new ConcurrentHashMap<>(); //TODO - tiered storage - lazy init retention rules
    private ConcurrentHashMap<String, CachePredicate> hotCacheRules = new ConcurrentHashMap<>();

    private InternalRDBMSManager internalDiskStorage;
    private InternalMetricRegistrator diskSizeRegistrator;
    private InternalMetricRegistrator operationsRegistrator;

    public TieredStorageManagerImpl(TieredStorageConfig storageConfig, InternalRDBMSManager internalDiskStorage, String fullSpaceName) {
        this.logger = LoggerFactory.getLogger(Constants.TieredStorage.getLoggerName(fullSpaceName));
        this.internalDiskStorage = internalDiskStorage;
        this.storageConfig = storageConfig;
    }

    @Override
    public boolean RDBMSContainsData() {
        return containsData;
    }

    @Override
    public void initialize(SpaceEngine engine) throws SAException, RemoteException {
        containsData = getInternalStorage().initialize(engine.getSpaceName(), engine.getFullSpaceName(), engine.getTypeManager(), engine.getSpaceImpl().isBackup());
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
            try{
                return hotCacheRules.computeIfAbsent(typeName, typeName1 -> createCacheRule(storageConfig.getTables().get(typeName1), internalDiskStorage.getTypeManager()));
            } catch (RuntimeException e){
                logger.error("failed to compute cache rule", e);
                throw e;
            }
        } else {
            return null;
        }
    }

    @Override
    public TieredStorageTableConfig getTableConfig(String typeName) {
        return storageConfig.getTables().get(typeName);
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
    public InternalRDBMSManager getInternalStorage() {
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

    public void initTieredStorageMetrics(SpaceImpl _spaceImpl, MetricManager metricManager){
    operationRegistratorInit(_spaceImpl, metricManager);
    diskSizeRegistratorInit(_spaceImpl, metricManager);
    }


    private void operationRegistratorInit(SpaceImpl _spaceImpl, MetricManager metricManager){
        Map<String, DynamicMetricTag> dynamicTags = new HashMap<>();
        dynamicTags.put("space_active", () -> {
            boolean active;
            try {
                active = _spaceImpl.isActive();
            } catch (RemoteException e) {
                active = false;
            }
            return active;
        });

        InternalMetricRegistrator registratorForPrimary = (InternalMetricRegistrator) metricManager.createRegistrator(MetricConstants.SPACE_METRIC_NAME, createTags(_spaceImpl), dynamicTags);

        registratorForPrimary.register( ("tiered-storage-read-tp"), getInternalStorage().getReadDisk());
        registratorForPrimary.register("tiered-storage-write-tp", getInternalStorage().getWriteDisk());
        this.operationsRegistrator = registratorForPrimary;
    }


    private void diskSizeRegistratorInit(SpaceImpl _spaceImpl, MetricManager metricManager){
        InternalMetricRegistrator registratorForAll = (InternalMetricRegistrator) metricManager.createRegistrator(MetricConstants.SPACE_METRIC_NAME, createTags(_spaceImpl));
        registratorForAll.register("disk-size",  new Gauge<Long>() {
            @Override
            public Long getValue()  {
                try {
                    return getInternalStorage().getDiskSize();
                }  catch (SAException | IOException e) {
                    logger.warn("failed to get disk size metric with exception: ", e);
                    return null;
                }
            }
        });
        this.diskSizeRegistrator = registratorForAll;
    }


    private Map<String, String>  createTags(SpaceImpl _spaceImpl){
        final String prefix = "metrics.";
        final Map<String, String> tags = new HashMap<>();
        for (Map.Entry<Object, Object> property : _spaceImpl.getCustomProperties().entrySet()) {
            String name = (String) property.getKey();
            if (name.startsWith(prefix))
                tags.put(name.substring(prefix.length()), (String) property.getValue());
        }
        tags.put("space_name", _spaceImpl.getName());
        tags.put("space_instance_id", _spaceImpl.getInstanceId());
        return tags;
    }

    @Override
    public TemplateMatchTier guessTemplateTier(ITemplateHolder templateHolder) { // TODO - tiered storage - return TemplateMatchTier, hot and cold
        String typeName = templateHolder.getServerTypeDesc().getTypeName();
        if(typeName.equals(Object.class.getTypeName())){
            logger.trace("Generic type {} = MATCH_HOT_AND_COLD", typeName);
            return TemplateMatchTier.MATCH_HOT_AND_COLD;
        }

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

    @Override
    public void close() {
        if (diskSizeRegistrator != null) {
            diskSizeRegistrator.clear();
        }
        if(operationsRegistrator != null) {
            operationsRegistrator.clear();
        }
        internalDiskStorage.shutDown();
    }


    private CachePredicate createCacheRule(TieredStorageTableConfig tableConfig, SpaceTypeManager typeManager) throws RuntimeException {
        CachePredicate result = null;
        if (tableConfig.isTransient()) {
            result = Constants.TieredStorage.TRANSIENT_ALL_CACHE_PREDICATE;
        } else if (tableConfig.getTimeColumn() != null) {
            if (tableConfig.getPeriod() != null) {
                return new TimePredicate(tableConfig.getName(), tableConfig.getTimeColumn(), tableConfig.getPeriod());
            }
        } else if (tableConfig.getCriteria() != null) {
            if (tableConfig.getCriteria().equalsIgnoreCase(AllPredicate.ALL_KEY_WORD)) {
                result = new AllPredicate(tableConfig.getName());
            } else {
                QueryTemplatePacket template = getQueryTemplatePacketFromCriteria(tableConfig, typeManager);
                HashMap<String, Range> ranges = template.getRanges();
                if (ranges.size() > 1) {
                    throw new IllegalArgumentException("currently only single range is supported");
                }
                Iterator<String> iterator = ranges.keySet().iterator();
                if (iterator.hasNext()) {
                    Range range = ranges.get(iterator.next());
                    result = new CriteriaRangePredicate(template.getTypeName(), range);
                }
            }
        }

        if (result == null) {
            throw new IllegalStateException("Failed to create CachePredicate for " + tableConfig);
        }

        return result;
    }


    /***
     * parses tje criteria string to QueryTemplatePacket
     * Note: uses v1 jdbc parser
     * @param tableConfig tiered storage table configuration
     * @param typeManager current space typeManager instance
     * @return QueryTemplatePacket representation of the criteria
     */
    private QueryTemplatePacket getQueryTemplatePacketFromCriteria(TieredStorageTableConfig tableConfig, SpaceTypeManager typeManager) throws RuntimeException{
        ReadQueryParser parser = new ReadQueryParser();
        AbstractDMLQuery sqlQuery;
        try {
            sqlQuery = parser.parseSqlQuery(new SQLQuery(tableConfig.getName(), tableConfig.getCriteria()), typeManager);
        } catch (SQLException e) {
            throw new RuntimeException("failed to parse criteria cache rule '" + tableConfig.getCriteria() + "'", e);
        }
        return sqlQuery.getExpTree().getTemplate();
    }
}
