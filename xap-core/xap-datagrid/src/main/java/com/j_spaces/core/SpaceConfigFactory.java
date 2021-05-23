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

package com.j_spaces.core;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.Constants.QueryProcessorInfo;
import com.j_spaces.core.Constants.SpaceProxy;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.exception.SpaceConfigurationException;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.core.filters.FiltersInfo;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.log.JProperties;
import com.j_spaces.sadapter.datasource.DataAdaptorIterator;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.StringTokenizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_SIZE_PROP;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_LRU;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_PROP;
import static com.j_spaces.core.Constants.Cluster.CLUSTER_CONFIG_URL_DEFAULT;
import static com.j_spaces.core.Constants.Cluster.CLUSTER_CONFIG_URL_PROP;
import static com.j_spaces.core.Constants.Cluster.IS_CLUSTER_SPACE_PROP;
import static com.j_spaces.core.Constants.DCache.CONFIG_NAME_PROP;
import static com.j_spaces.core.Constants.DCache.DCACHE_CONFIG_NAME_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_CLASS_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_CLASS_PROP;
import static com.j_spaces.core.Constants.DataAdapter.DATA_PROPERTIES;
import static com.j_spaces.core.Constants.DataAdapter.DATA_PROPERTIES_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_CLASS_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_CLASS_PROP;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_TTL_PROP;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_INHERITANCE_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_INHERITANCE_PROP;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_PARTIAL_UPDATE_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_PARTIAL_UPDATE_PROP;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_REMOVE_BY_ID_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_REMOVE_BY_ID_PROP;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_VERSION_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_VERSION_PROP;
import static com.j_spaces.core.Constants.DataAdapter.USAGE;
import static com.j_spaces.core.Constants.DataAdapter.USAGE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MAX_THREADS_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MAX_THREADS_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_ENABLED_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_MIN_THREADS_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MIN_THREADS_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFIER_RETRIES_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFIER_TTL_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_SERIALIZATION_TYPE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_SERIALIZATION_TYPE_PROP;
import static com.j_spaces.core.Constants.Filter.DEFAULT_FILTER_SECURITY_NAME;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_INTERVAL_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_INTERVAL_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_STALE_REPLICAS_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_IS_PRIVATE_PROP;
import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_ENABLED_PROP;
import static com.j_spaces.core.Constants.Schemas.SCHEMA_ELEMENT;
import static com.j_spaces.core.Constants.StorageAdapter.PERSISTENT_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.StorageAdapter.PERSISTENT_ENABLED_PROP;

@com.gigaspaces.api.InternalApi
public class SpaceConfigFactory {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONTAINER);

    public static SpaceConfig createSpaceConfig(Properties schemaProps,
                                                ClusterPolicy m_ClusterPolicy, String containerName, boolean isSchema, String fullSpaceSchemaLocation)
            throws IOException, SAXException, ParserConfigurationException, SpaceConfigurationException {

        // Initialize SpaceConfig properties
        SpaceConfig spaceConfig = new SpaceConfig(null, schemaProps, containerName, fullSpaceSchemaLocation);

        //retriveSpaceResourceFiles( schemaProps );

        //spaceConfig.m_spaceName = m_SpaceName;
        spaceConfig.setPrivate(
                Boolean.valueOf(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + LOOKUP_IS_PRIVATE_PROP,
                        Boolean.FALSE.toString())).booleanValue());//m_JSpaceAttr.m_isPrivate;
        //spaceConfig.state = getState();
        //spaceConfig.m_schema = DEFUALT_SCHEMA;

        String schemaName = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + SCHEMA_ELEMENT, null);
        spaceConfig.setSchemaName(schemaName);
        spaceConfig.setConnectionRetries(
                schemaProps.getProperty(SpaceProxy.OldRouter.RETRY_CONNECTION_FULL, SpaceProxy.OldRouter.RETRY_CONNECTION_DEFAULT));
        spaceConfig.setNotifyRetries(
                schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_NOTIFIER_TTL_PROP, ENGINE_NOTIFIER_RETRIES_DEFAULT));

        spaceConfig.setExpirationTimeInterval(
                schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_INTERVAL_PROP, String.valueOf(LM_EXPIRATION_TIME_INTERVAL_DEFAULT)));

        spaceConfig.setExpirationTimeRecentUpdate(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_RECENT_UPDATES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT)));

        spaceConfig.setExpirationTimeRecentDeletes(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_RECENT_DELETES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT)));

        spaceConfig.setExpirationStaleReplicas(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_STALE_REPLICAS_PROP,
                String.valueOf(LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT)));

        String engineMemoryUsageEnabled = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_ENABLED_PROP, ENGINE_MEMORY_USAGE_ENABLED_DEFAULT);
        String defaultEngineMemoryUsageEnabled = engineMemoryUsageEnabled;
        if (JProperties.isSystemProp(engineMemoryUsageEnabled))
            defaultEngineMemoryUsageEnabled = ENGINE_MEMORY_USAGE_ENABLED_DEFAULT;

        engineMemoryUsageEnabled = JProperties.getPropertyFromSystem(engineMemoryUsageEnabled, defaultEngineMemoryUsageEnabled);
        spaceConfig.setEngineMemoryUsageEnabled(engineMemoryUsageEnabled);

        spaceConfig.setEngineMemoryUsageHighPercentageRatio(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT));
        spaceConfig.setEngineMemoryUsageLowPercentageRatio(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT));
        spaceConfig.setEngineMemoryUsageWriteOnlyBlockPercentageRatio(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT));
        spaceConfig.setEngineMemoryWriteOnlyCheckPercentageRatio(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP, ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT));
        spaceConfig.setEngineMemoryUsageEvictionBatchSize(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP, ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT));
        spaceConfig.setEngineMemoryUsageRetryCount(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP, ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT));

        // Serialization type
        String serilType = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_SERIALIZATION_TYPE_PROP, ENGINE_SERIALIZATION_TYPE_DEFAULT);
        String defaultSerilType = serilType;
        //if there is system variable, then change value of default variable to "normal" value
        if (JProperties.isSystemProp(serilType))
            defaultSerilType = ENGINE_SERIALIZATION_TYPE_DEFAULT;
        //try to get system property for serialization type
        serilType = JProperties.getPropertyFromSystem(serilType, defaultSerilType);
        spaceConfig.setSerializationType(Integer.parseInt(serilType));

        spaceConfig.setEngineMinThreads(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MIN_THREADS_PROP, ENGINE_MIN_THREADS_DEFAULT));
        spaceConfig.setEngineMaxThreads(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + ENGINE_MAX_THREADS_PROP, ENGINE_MAX_THREADS_DEFAULT));

        //String saClassURL = JProperties.getProperty( m_SpaceName, STORAGE_ADAPTER_URL_PROP);
        //String saClassName = JProperties.getProperty( m_SpaceName, STORAGE_ADAPTER_CLASS_PROP, DEFAULT_STORAGE_ADAPTER_CLASS);

        // storage adapter attributes

        boolean isPersitent =
                Boolean.valueOf(schemaProps.getProperty(
                        Constants.SPACE_CONFIG_PREFIX + PERSISTENT_ENABLED_PROP, PERSISTENT_ENABLED_DEFAULT)).booleanValue();
        spaceConfig.setPersistent(isPersitent);


        //String numberSyncObjects = schemaProps.getProperty( Constants.SPACE_CONFIG_PREFIX + NUMBER_OF_SYNC_OBJECTS_PROP );
        //System.out.println( ">> numberSyncObjects=" + numberSyncObjects );
        spaceConfig.setMirrorServiceEnabled(Boolean.valueOf(
                schemaProps.getProperty(
                        Constants.SPACE_CONFIG_PREFIX + MIRROR_SERVICE_ENABLED_PROP,
                        MIRROR_SERVICE_ENABLED_DEFAULT)).booleanValue());

        // External Data Source
        //
        spaceConfig.setDataSourceClass(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        DATA_SOURCE_CLASS_PROP,
                DATA_SOURCE_CLASS_DEFAULT));

        spaceConfig.setDataClass(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        DATA_CLASS_PROP,
                DATA_CLASS_DEFAULT));
        spaceConfig.setQueryBuilderClass(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                Constants.DataAdapter.QUERY_BUILDER_PROP, Constants.DataAdapter.QUERY_BUILDER_PROP_DEFAULT));

        spaceConfig.setDataPropertiesFile(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        DATA_PROPERTIES,
                DATA_PROPERTIES_DEFAULT));

        spaceConfig.setUsage(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        USAGE,
                USAGE_DEFAULT));

        spaceConfig.setSupportsInheritanceEnabled(Boolean.parseBoolean(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        SUPPORTS_INHERITANCE_PROP,
                SUPPORTS_INHERITANCE_DEFAULT)));

        spaceConfig.setSupportsVersionEnabled(Boolean.parseBoolean(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        SUPPORTS_VERSION_PROP,
                SUPPORTS_VERSION_DEFAULT)));
        spaceConfig.setSupportsPartialUpdateEnabled(Boolean.parseBoolean(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        SUPPORTS_PARTIAL_UPDATE_PROP,
                SUPPORTS_PARTIAL_UPDATE_DEFAULT)));
        spaceConfig.setSupportsRemoveByIdEnabled(Boolean.parseBoolean(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        SUPPORTS_REMOVE_BY_ID_PROP,
                SUPPORTS_REMOVE_BY_ID_DEFAULT)));

        spaceConfig.setDataSourceSharedIteratorMode(Boolean.parseBoolean(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP,
                DATA_SOURCE_SHARE_ITERATOR_ENABLED_DEFAULT)));

        long leaseManagerExpirationTimeRecentDeletes = Long.parseLong(spaceConfig.getExpirationTimeRecentDeletes());
        long leaseManagerExpirationTimeRecentUpdates = Long.parseLong(spaceConfig.getExpirationTimeRecentUpdates());
        String dataSourceShareIteratorTTLDefault = DataAdaptorIterator.getDataSourceShareIteratorTTLDefault(leaseManagerExpirationTimeRecentDeletes, leaseManagerExpirationTimeRecentUpdates);

        spaceConfig.setDataSourceSharedIteratorTimeToLive(Long.parseLong(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX +
                        DATA_SOURCE_SHARE_ITERATOR_TTL_PROP,
                dataSourceShareIteratorTTLDefault)));

        //Query Processor ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        spaceConfig.setQPAutoCommit(Boolean.valueOf(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_AUTO_COMMIT_PROP, QueryProcessorInfo.QP_AUTO_COMMIT_DEFAULT)));
        spaceConfig.setQPParserCaseSensetivity(Boolean.valueOf(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_PROP, QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_DEFAULT)));
        spaceConfig.setQPTraceExecTime(Boolean.valueOf(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_TRACE_EXEC_TIME_PROP, QueryProcessorInfo.QP_TRACE_EXEC_TIME_DEFAULT)));
        spaceConfig.setQpTransactionTimeout(Integer.parseInt(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_PROP, QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_DEFAULT)));
        spaceConfig.setQpSpaceReadLeaseTime(Integer.parseInt(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_PROP, QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_DEFAULT)));
        spaceConfig.setQpSpaceWriteLeaseTime(Long.parseLong(schemaProps.getProperty(
                QueryProcessorInfo.QP_SPACE_WRITE_LEASE_PROP, QueryProcessorInfo.QP_SPACE_WRITE_LEASE_DEFAULT)));


        spaceConfig.setQpDateFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_DATE_FORMAT_PROP,
                QueryProcessorInfo.QP_DATE_FORMAT_DEFAULT));

        spaceConfig.setQpTimeFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_TIME_FORMAT_PROP,
                QueryProcessorInfo.QP_TIME_FORMAT_DEFAULT));

        spaceConfig.setQpDateTimeFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_DATETIME_FORMAT_PROP,
                QueryProcessorInfo.QP_DATETIME_FORMAT_DEFAULT));

        spaceConfig.setQpUtilDateFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_UTILDATE_FORMAT_PROP,
                QueryProcessorInfo.QP_UTILDATE_FORMAT_DEFAULT));

        spaceConfig.setQpSqlDateFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_SQLDATE_FORMAT_PROP,
                QueryProcessorInfo.QP_SQLDATE_FORMAT_DEFAULT));

        spaceConfig.setQpLocalDateTimeFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_LOCALDATETIME_FORMAT_PROP,
                QueryProcessorInfo.QP_LOCALDATETIME_FORMAT_DEFAULT));

        spaceConfig.setQpLocalTimeFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_LOCALTIME_FORMAT_PROP,
                QueryProcessorInfo.QP_LOCALTIME_FORMAT_DEFAULT));

        spaceConfig.setQpLocalDateFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_LOCALDATE_FORMAT_PROP,
                QueryProcessorInfo.QP_LOCALDATE_FORMAT_DEFAULT));

        spaceConfig.setQpSqlTimeFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_SQLTIME_FORMAT_PROP,
                QueryProcessorInfo.QP_SQLTIME_FORMAT_DEFAULT));

        spaceConfig.setQpTimestampFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_TIMESTAMP_FORMAT_PROP,
                QueryProcessorInfo.QP_TIMESTAMP_FORMAT_DEFAULT));

        spaceConfig.setQpInstantFormat(schemaProps.getProperty(
                Constants.SPACE_CONFIG_PREFIX + QueryProcessorInfo.QP_INSTANT_FORMAT_PROP,
                QueryProcessorInfo.QP_INSTANT_FORMAT_DEFAULT));
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


        spaceConfig.setCacheManagerSize(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + CACHE_MANAGER_SIZE_PROP, CACHE_MANAGER_SIZE_DEFAULT));
        final String defaultCachePolicyValue =
                isPersitent ? String.valueOf(CACHE_POLICY_LRU) :
                        String.valueOf(CACHE_POLICY_ALL_IN_CACHE);
        String cachePolicyPropStr = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + CACHE_POLICY_PROP, defaultCachePolicyValue);
        String defaultCachePolicyPropStr = cachePolicyPropStr;
        // if there is system variable, then change value of default variable to "normal" value
        if (JProperties.isSystemProp(cachePolicyPropStr))
            defaultCachePolicyPropStr = defaultCachePolicyValue;
        //try to get cache policy from system
        cachePolicyPropStr = JProperties.getPropertyFromSystem(cachePolicyPropStr, defaultCachePolicyPropStr);
        spaceConfig.setCachePolicy(cachePolicyPropStr);

        String clusterConfigURL = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + CLUSTER_CONFIG_URL_PROP, CLUSTER_CONFIG_URL_DEFAULT);
        String defaultClusterConfigURL = clusterConfigURL;
        //		 if there is system variable, then change value of default variable to "normal" value
        if (JProperties.isSystemProp(clusterConfigURL))
            defaultClusterConfigURL = CLUSTER_CONFIG_URL_DEFAULT;
        //try to get cluster config URL from system
        clusterConfigURL = JProperties.getPropertyFromSystem(clusterConfigURL, defaultClusterConfigURL);
        spaceConfig.setClusterConfigURL(clusterConfigURL);

        String clusterSpaceEnabledStr = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + IS_CLUSTER_SPACE_PROP, Boolean.FALSE.toString());
        String defaultClusterSpaceEnabledStr = clusterSpaceEnabledStr;
        if (JProperties.isSystemProp(clusterSpaceEnabledStr))
            defaultClusterSpaceEnabledStr = Boolean.FALSE.toString();
        clusterSpaceEnabledStr = JProperties.getPropertyFromSystem(clusterSpaceEnabledStr, defaultClusterSpaceEnabledStr);
        spaceConfig.setClustered(Boolean.valueOf(clusterSpaceEnabledStr).booleanValue());

        // Distributed Cache
        spaceConfig.setDCacheConfigName(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + CONFIG_NAME_PROP, DCACHE_CONFIG_NAME_DEFAULT));//, false );
        spaceConfig.setDCacheProperties(JProperties.loadConfigDCache(spaceConfig.getDCacheConfigName()));

        // build filter information
        int filterCounter = 0;
        String fnames = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + Constants.Filter.FILTER_NAMES_PROP, "");
        StringTokenizer st = new StringTokenizer(fnames, ",");
        spaceConfig.setFiltersInfo(new FiltersInfo[st.countTokens()]);
        while (st.hasMoreElements()) {
            String filterName = st.nextToken().trim();
            FiltersInfo info = new FiltersInfo();
            spaceConfig.setFilterInfoAt(info, filterCounter++);

            // get full filter information
            info.filterName = filterName;
            info.enabled = Boolean.valueOf(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + "filters." + filterName + ".enabled", "")).booleanValue();
            info.filterClassName = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + "filters." + filterName + ".class", "");
            info.paramURL = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + "filters." + filterName + ".url", "");

            String operationsCode = schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + "filters." + filterName + ".operation-code", "");
            StringTokenizer operStrToken = new StringTokenizer(operationsCode, ",");
            while (operStrToken.hasMoreTokens()) {
                int operCode = Integer.parseInt(operStrToken.nextToken().trim());
                switch (operCode) {
                    case FilterOperationCodes.AFTER_REMOVE:
                        info.afterRemove = true;
                        break;
                    case FilterOperationCodes.AFTER_WRITE:
                        info.afterWrite = true;
                        break;
                    case FilterOperationCodes.BEFORE_CLEAN_SPACE:
                        info.beforeClean = true;
                        break;
                    case FilterOperationCodes.BEFORE_NOTIFY:
                        info.beforeNotify = true;
                        break;
                    case FilterOperationCodes.BEFORE_READ:
                        info.beforeRead = true;
                        break;
                    case FilterOperationCodes.BEFORE_TAKE:
                        info.beforeTake = true;
                        break;
                    case FilterOperationCodes.BEFORE_WRITE:
                        info.beforeWrite = true;
                        break;

                    case FilterOperationCodes.BEFORE_GETADMIN:
                        info.beforeGetAdmin = true;
                        break;
                    case FilterOperationCodes.BEFORE_AUTHENTICATION:
                        info.beforeAuthentication = true;
                        break;
                    case FilterOperationCodes.BEFORE_UPDATE:
                        info.beforeUpdate = true;
                        break;
                    case FilterOperationCodes.AFTER_UPDATE:
                        info.afterUpdate = true;
                        break;

                }// switch()
            }// while operCode...

            if (filterName.equalsIgnoreCase(DEFAULT_FILTER_SECURITY_NAME))// && info.enabled )
            {
                String schemaRef = fullSpaceSchemaLocation != null ? " [" + fullSpaceSchemaLocation + "] " : " ";
                _logger.warn("The filter [" + DEFAULT_FILTER_SECURITY_NAME
                        + "] defined in the space schema file" + schemaRef + "is no longer in use since 7.0.1; Please remove it.");
            }
        }// while filters...


        spaceConfig.setJMSRmiPort(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + Constants.Jms.JMS_RMI_PORT_PROP, ""));
        spaceConfig.setJMSTopicNames(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + Constants.Jms.JMS_TOPIC_NAMES_PROP, ""));
        spaceConfig.setJMSQueueNames(schemaProps.getProperty(Constants.SPACE_CONFIG_PREFIX + Constants.Jms.JMS_QUEUE_NAMES_PROP, ""));


        // proxy
        spaceConfig.setProxyConnectionMode(schemaProps.getProperty(Constants.SpaceProxy.OldRouter.CONNECTION_MONITOR_FULL, Constants.SpaceProxy.OldRouter.CONNECTION_MONITOR_DEFAULT));
        spaceConfig.setProxyMonitorFrequency(Long.parseLong(schemaProps.getProperty(Constants.SpaceProxy.OldRouter.MONITOR_FREQUENCY_FULL, Constants.SpaceProxy.OldRouter.MONITOR_FREQUENCY_DEFAULT)));
        spaceConfig.setProxyDetectorFrequency(Long.parseLong(schemaProps.getProperty(Constants.SpaceProxy.OldRouter.DETECTOR_FREQUENCY_FULL, Constants.SpaceProxy.OldRouter.DETECTOR_FREQUENCY_DEFAULT)));
        spaceConfig.setProxyConnectionRetries(Integer.parseInt(schemaProps.getProperty(Constants.SpaceProxy.OldRouter.CONNECTION_RETRIES_FULL, Constants.SpaceProxy.OldRouter.CONNECTION_RETRIES_DEFAULT)));

        spaceConfig.putAll(schemaProps);

        return spaceConfig;

    }
    /*
     private static void retriveSpaceResourceFiles( Properties schemaProps ) throws IOException {
		 // Assure that the same place where the space config exist, you may find the
		 // <DefaultConfig>_DCache.xml and <DefaultConfig>_ClusteredJMS.xml.
		 // Otherwise create default files.


		 String homeDir = System.getProperty( JSPACE_HOME_SYS_PROP, JSPACE_HOME_SYS_DEFAULT );
		 String m_configDirectory = homeDir + File.separator + CONTAINER_CONFIG_DIRECTORY;

		//Check that the default <DefaultConfig>_DCache.xml config file exist.
		//otherwise create default config file.
		String dCacheConfigFile = m_configDirectory + File.separator + Constants.DCache.DCACHE_CONFIG_FILE_DEFAULT;
		if (!new File(dCacheConfigFile).isFile())//TODO make sure we wont write to disk with schemas
			JSpaceUtilities.retriveResource(DCACHE_TEMPLATE_FILE_NAME_DEFAULT, dCacheConfigFile);

		// check for dCache config file property, which exists in this specific space.
		String dCacheConfigName = schemaProps.getProperty( Constants.SPACE_CONFIG_PREFIX + CONFIG_NAME_PROP, DCACHE_CONFIG_NAME_DEFAULT);

		// create <dCacheConfig>_DCache.xml file if not exists
		dCacheConfigFile = m_configDirectory + File.separator + dCacheConfigName + Constants.DCache.FILE_SUFFIX_EXTENTION;
		if (!new File(dCacheConfigFile).isFile())
		{
			// retrieve from resource and copy xml file to the disk
			JSpaceUtilities.retriveResource(DCACHE_TEMPLATE_FILE_NAME_DEFAULT, dCacheConfigFile);
		}

		// update space-config properties
		//Properties spProp = JProperties.getSpaceProperties( spaceName );
		//if (spProp != null) {
		    schemaProps.setProperty(Constants.SPACE_CONFIG_PREFIX + CONFIG_NAME_PROP, dCacheConfigName);
		    schemaProps.setProperty(Constants.SPACE_CONFIG_PREFIX + CONFIG_FILE_URL_PROP, dCacheConfigFile);
		//}

		//JMS staff
		//Check that the default <DefaultConfig>_ClusteredJMS.xml config file exist.
		//otherwise create default config file.
//		String jmsConfigFile = m_configDirectory + File.separator + CLUSTERED_JMS_CONFIG_FILE_DEFAULT;
//		if (!new File(jmsConfigFile).isFile())
//			JSpaceUtilities.retriveResource(CLUSTERED_JMS_TEMPLATE_FILE_NAME_DEFAULT, jmsConfigFile);

		// check for dCache config file property, which exists in this specific space.
		String jmsConfigName = schemaProps.getProperty( Constants.SPACE_CONFIG_PREFIX + CLUSTERED_JMS_CONFIG_NAME_PROP, CLUSTERED_JMS_CONFIG_NAME_DEFAULT);

		InputStream jmsConfigInputStream = JSpaceUtilities.findDefaultConfigClusteredJMS();

		// create <jmsConfigName>_ClusteredJMS.xml file if not exists
		String jmsConfigFile = m_configDirectory + File.separator + jmsConfigName + CLUSTERED_JMS_FILE_SUFFIX_EXTENTION;
		//if file doesn't exist and this is not default config
		if( !new File( jmsConfigFile ).isFile() &&
				!Constants.Jms.CLUSTERED_JMS_CONFIG_NAME_DEFAULT.equals( jmsConfigName ) )	{
			// retrieve from resource and copy xml file to the disk
			JSpaceUtilities.retriveResource( jmsConfigInputStream, jmsConfigFile );
		}

		// update space-config properties
		//if( spProp != null ) {
		    schemaProps.setProperty(Constants.SPACE_CONFIG_PREFIX + CLUSTERED_JMS_CONFIG_NAME_PROP, jmsConfigFile);
		    //schemaProps.setProperty(Constants.SPACE_CONFIG_PREFIX + CLUSTERED_JMS_CONFIG_FILE_URL_PROP, jmsConfigFile);
		//}
	}
     */


    /**
     * Save content of SpaceAttributes instance into xml file( schema or configuration ).
     *
     * @param selFile         - In this file configuration will be saved
     * @param spaceName       - space name, relevant if value of isUseSchema is false
     * @param containerName   container owner name
     * @param spaceAttributes - attributes of space to be saved
     */
    public static void performSaveAs(File selFile,
                                     String spaceName, String containerName, JSpaceAttributes spaceAttributes) {
        try {
            //get InputStream for default schema
            InputStream schemaInputStream = ResourceLoader.findSpaceSchema(Constants.Schemas.DEFAULT_SCHEMA).getInputStream();
            //Write content of default schema to required file, if requierd add space name tag
            writeDOMToFile(schemaInputStream, selFile, spaceName);

            //replace content of selected file with current space content that saved in
            //spaceConfig instance
            SpaceImpl.setConfig(spaceName, containerName, spaceAttributes, selFile.getPath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void writeDOMToFile(InputStream is, File selFile, String spaceName) throws Exception {
        //replace the <space-name> tag with the actual space name
        //Obtaining a org.w3c.dom.Document from XML
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        /**
         * Parse the content of the given InputStream as an XML document and
         * return a new DOM Document object.
         **/
        Document doc = builder.parse(is);

        Element root = doc.getDocumentElement();
        writeToFile(selFile, root);
    }

    public static void writeDOMToFile(InputStream is, File selFile) throws Exception {

        //replace the <space-name> tag with the actual space name
        //Obtaining a org.w3c.dom.Document from XML
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        /**
         * Parse the content of the given InputStream as an XML document and
         * return a new DOM Document object.
         **/
        Document doc = builder.parse(is);

        Element root = doc.getDocumentElement();

        writeToFile(selFile, root);
    }

    private static void writeToFile(File selFile, Element root) throws IOException {

        FileOutputStream fos = new FileOutputStream(selFile);
        PrintStream ps = new PrintStream(fos);
        JSpaceUtilities.domWriter(root, ps, "");

        fos.flush();
        ps.flush();
        ps.close();
        fos.close();
    }
}
