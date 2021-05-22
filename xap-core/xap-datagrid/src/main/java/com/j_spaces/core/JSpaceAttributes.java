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

import com.gigaspaces.datasource.SQLDataProvider;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.server.space.SpaceInstanceConfig;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.SpaceCustomComponent;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.Constants.SpaceProxy;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.filters.FiltersInfo;
import com.j_spaces.sadapter.datasource.DataAdaptorIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.UnmarshalException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE;
import static com.j_spaces.core.Constants.CacheManager.FULL_CACHE_MANAGER_SIZE_PROP;
import static com.j_spaces.core.Constants.CacheManager.FULL_CACHE_POLICY_PROP;
import static com.j_spaces.core.Constants.Cluster.CLUSTER_CONFIG_URL_DEFAULT;
import static com.j_spaces.core.Constants.Cluster.FULL_CLUSTER_CONFIG_URL_PROP;
import static com.j_spaces.core.Constants.Cluster.FULL_IS_CLUSTER_SPACE_PROP;
import static com.j_spaces.core.Constants.Cluster.IS_CLUSTER_SPACE_DEFAULT;
import static com.j_spaces.core.Constants.DCache.DCACHE_CONFIG_NAME_DEFAULT;
import static com.j_spaces.core.Constants.DCache.FULL_DCACHE_CONFIG_NAME_PROP;
import static com.j_spaces.core.Constants.DataAdapter.DATA_CLASS_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_CLASS_PROP;
import static com.j_spaces.core.Constants.DataAdapter.DATA_PROPERTIES;
import static com.j_spaces.core.Constants.DataAdapter.DATA_PROPERTIES_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_CLASS_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_CLASS_PROP;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP;
import static com.j_spaces.core.Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_TTL_PROP;
import static com.j_spaces.core.Constants.DataAdapter.QUERY_BUILDER_PROP;
import static com.j_spaces.core.Constants.DataAdapter.QUERY_BUILDER_PROP_DEFAULT;
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
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_EXPLICIT_GC_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_MIN_THREADS_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFIER_RETRIES_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_SERIALIZATION_TYPE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_BLOBSTORE_ROCKSDB_ALLOW_DUPLICATE_UIDS;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MAX_THREADS_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_EXPLICIT_GC_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_ENABLED_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_MIN_THREADS_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_NOTIFIER_TTL_PROP;
import static com.j_spaces.core.Constants.Engine.FULL_ENGINE_SERIALIZATION_TYPE_PROP;
import static com.j_spaces.core.Constants.IS_SPACE_LOAD_ON_STARTUP;
import static com.j_spaces.core.Constants.Jms.FULL_JMS_QUEUE_NAMES_PROP;
import static com.j_spaces.core.Constants.Jms.FULL_JMS_RMI_PORT_PROP;
import static com.j_spaces.core.Constants.Jms.FULL_JMS_TOPIC_NAMES_PROP;
import static com.j_spaces.core.Constants.Jms.JMS_QUEUE_NAMES_DEFAULT;
import static com.j_spaces.core.Constants.Jms.JMS_RMI_PORT_DEFAULT;
import static com.j_spaces.core.Constants.Jms.JMS_TOPIC_NAMES_DEFAULT;
import static com.j_spaces.core.Constants.LeaderSelector.CONNECTION_TIMEOUT_DEFAULT;
import static com.j_spaces.core.Constants.LeaderSelector.FULL_LEADER_SELECTOR_CONNECTION_TIMEOUT;
import static com.j_spaces.core.Constants.LeaderSelector.FULL_LEADER_SELECTOR_RETRY_INTERVAL;
import static com.j_spaces.core.Constants.LeaderSelector.FULL_LEADER_SELECTOR_RETRY_TIMEOUT;
import static com.j_spaces.core.Constants.LeaderSelector.FULL_LEADER_SELECTOR_SESSION_TIMEOUT;
import static com.j_spaces.core.Constants.LeaderSelector.RETRY_INTERVAL_DEFAULT;
import static com.j_spaces.core.Constants.LeaderSelector.RETRY_TIMEOUT_DEFAULT;
import static com.j_spaces.core.Constants.LeaderSelector.SESSION_TIMEOUT_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.FULL_LM_EXPIRATION_TIME_INTERVAL_PROP;
import static com.j_spaces.core.Constants.LeaseManager.FULL_LM_EXPIRATION_TIME_RECENT_DELETES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.FULL_LM_EXPIRATION_TIME_RECENT_UPDATES_PROP;
import static com.j_spaces.core.Constants.LeaseManager.FULL_LM_EXPIRATION_TIME_STALE_REPLICAS_PROP;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_INTERVAL_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT;
import static com.j_spaces.core.Constants.LeaseManager.LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT;
import static com.j_spaces.core.Constants.LookupManager.FULL_LOOKUP_IS_PRIVATE_PROP;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_IS_PRIVATE_DEFAULT;
import static com.j_spaces.core.Constants.Mirror.FULL_MIRROR_SERVICE_ENABLED_PROP;
import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_AUTO_COMMIT_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_DATETIME_FORMAT_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_DATE_FORMAT_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_PARSER_CASE_SENSETIVITY_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_SPACE_READ_LEASE_TIME_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_SPACE_WRITE_LEASE_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_TIME_FORMAT_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_TRACE_EXEC_TIME_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.FULL_QP_TRANSACTION_TIMEOUT_PROP;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_AUTO_COMMIT_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_DATETIME_FORMAT_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_DATE_FORMAT_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_SPACE_WRITE_LEASE_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_TIME_FORMAT_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_TRACE_EXEC_TIME_DEFAULT;
import static com.j_spaces.core.Constants.QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_DEFAULT;
import static com.j_spaces.core.Constants.RemoteCode.FULL_REMOTE_CODE_MAX_CLASS_LOADERS_PROP;
import static com.j_spaces.core.Constants.RemoteCode.FULL_REMOTE_CODE_SUPPORT_CODE_CHANGE_PROP;
import static com.j_spaces.core.Constants.RemoteCode.REMOTE_CODE_MAX_CLASS_LOADERS_DEFAULT;
import static com.j_spaces.core.Constants.RemoteCode.REMOTE_CODE_SUPPORT_CODE_CHANGE_DEFAULT;
import static com.j_spaces.core.Constants.Schemas.FULL_SCHEMA_ELEMENT;
import static com.j_spaces.core.Constants.Space.FULL_SPACE_STATE;
import static com.j_spaces.core.Constants.StorageAdapter.FULL_STORAGE_PERSISTENT_ENABLED_PROP;
import static com.j_spaces.core.Constants.StorageAdapter.PERSISTENT_ENABLED_DEFAULT;

/**
 * JSpaceAttributes that contains all information per space.
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/

@com.gigaspaces.api.InternalApi
public class JSpaceAttributes
        extends Properties
        implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private static final int SERIAL_VERSION = 1;

    /**
     * Indicate if this configuration arrived from space MBean TODO This is temporary issue; remove
     * it after migrating to fully property-based configuration.
     */
    // public boolean isFromJMX = false;

    // Distributed Cache
    // public String m_dcacheConfigName;
    private JSpaceAttributes _dCacheProperties;
    private ClusterPolicy _clusterPolicy;
    private volatile SpaceClusterInfo _clusterInfo;
    private FiltersInfo[] _filtersInfo;


    private Properties _blobStoreProperties = new Properties();

    /**
     * If passed, we use this Properties object to overwrite space/container JProperties parsed
     * values.
     */
    private Properties _customProperties = new Properties();

    /**
     * Use in methods setClustered(boolean isClustered) and isClustered() instead of referring to
     * this deprecated class member. Should be used for read purposes only, not for update. Updating
     * is performing only by method setClustered(boolean isClustered)
     *
     * @deprecated
     */
    @Deprecated
    public boolean m_isClustered;

    /**
     * Default constructor.
     **/
    public JSpaceAttributes() {
    }

    public JSpaceAttributes(Properties prop) {
        super(prop);
    }

    /**
     * Initialize JSpace attributes.
     *
     * @param schemaName       The space/container schema name. If not null we use this schema xml
     *                         file as the space/container configuration template for this space.
     * @param customProperties if passed we use it to overwrite the space/container JProperties.
     **/
    public JSpaceAttributes(String schemaName, Properties customProperties,
                            boolean isLoadSpaceOnStartup) {
        this.setSchemaName(schemaName);
        this.setCustomProperties(customProperties);
        this.setLoadOnStartup(isLoadSpaceOnStartup);
    }

    /**
     * Initialize JSpace attributes.
     *
     * @param schemaName The space/container schema name. If not null we use this schema xml file as
     *                   the space/container configuration template for this space.
     **/
    public JSpaceAttributes(String schemaName, boolean isLoadSpaceOnStartup) {
        this.setSchemaName(schemaName);
        this.setLoadOnStartup(isLoadSpaceOnStartup);
    }

    /**
     * Initialize JSpace attributes.
     *
     * @param schemaName       The space/container schema name. If not null we use this schema xml
     *                         file as the space configuration template for this space.
     * @param clusterConfigURL URL to cluster configuration file.
     **/
    public JSpaceAttributes(String schemaName, String clusterConfigURL) {
        this.setSchemaName(schemaName);
        this.setClusterConfigURL(clusterConfigURL);
        if (clusterConfigURL != null)
            setClustered(true);
    }

    /**
     * Overrides method of super class. Checks value for null, only if value is not null appropriate
     * method of parent class will be called. In such way NullPointerException is prevented.
     *
     * @param key   the key to be placed into this property list.
     * @param value the value corresponding to <tt>key</tt>.
     */
    @Override
    public synchronized Object setProperty(String key, String value) {
        if (value != null) {
            return super.setProperty(key, value);
        }
        return null;
    }

    @Override
    public Object clone() {
        return super.clone();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("\n\t --- JSpaceAttributes ---\n");
        sb.append("\n\t DCache properties -\t ");
        StringUtils.appendProperties(sb, getDCacheProperties());
        sb.append('\n');
        sb.append("\n\t Cluster Info -\t");
        sb.append(getClusterInfo());
        sb.append('\n');
        sb.append("\n\t Filters configuration -\t");
        sb.append(Arrays.toString(getFiltersInfo()));
        sb.append('\n');
        sb.append("\n\t Custom properties -\t ");
        StringUtils.appendProperties(sb, getCustomProperties());
        sb.append('\n');
        sb.append("\n\t Properties\n\t ");
        StringUtils.appendProperties(sb, this);
        sb.append('\n');

        return sb.toString();
    }

    /**
     *
     *
     */
    public boolean isPersistent() {
        return Boolean.valueOf(getProperty(FULL_STORAGE_PERSISTENT_ENABLED_PROP,
                PERSISTENT_ENABLED_DEFAULT))
                .booleanValue();
    }

    /**
     * @param isPersistent
     */
    public void setPersistent(boolean isPersistent) {
        this.setProperty(FULL_STORAGE_PERSISTENT_ENABLED_PROP,
                String.valueOf(isPersistent));
    }

    public boolean isQPAutoCommit() {
        return Boolean.valueOf(getProperty(FULL_QP_AUTO_COMMIT_PROP,
                QP_AUTO_COMMIT_DEFAULT))
                .booleanValue();
    }

    /**
     * @param isQPAutoCommit
     */
    public void setQPAutoCommit(boolean isQPAutoCommit) {
        this.setProperty(FULL_QP_AUTO_COMMIT_PROP,
                String.valueOf(isQPAutoCommit));
    }

    /**
     *
     *
     */
    public boolean isQPParserCaseSensetivity() {
        return Boolean.valueOf(getProperty(FULL_QP_PARSER_CASE_SENSETIVITY_PROP,
                QP_PARSER_CASE_SENSETIVITY_DEFAULT))
                .booleanValue();
    }

    /**
     * @param isQPParserCaseSensetivity
     */
    public void setQPParserCaseSensetivity(boolean isQPParserCaseSensetivity) {
        this.setProperty(FULL_QP_PARSER_CASE_SENSETIVITY_PROP,
                String.valueOf(isQPParserCaseSensetivity));
    }

    /**
     *
     *
     */
    public boolean isQPTraceExecTime() {
        return Boolean.valueOf(getProperty(FULL_QP_TRACE_EXEC_TIME_PROP,
                QP_TRACE_EXEC_TIME_DEFAULT))
                .booleanValue();
    }

    /**
     * @param isQPTraceExecTime
     */
    public void setQPTraceExecTime(boolean isQPTraceExecTime) {
        this.setProperty(FULL_QP_TRACE_EXEC_TIME_PROP,
                String.valueOf(isQPTraceExecTime));
    }

    /**
     *
     *
     */
    public int getQpSpaceReadLeaseTime() {
        return Integer.parseInt(getProperty(FULL_QP_SPACE_READ_LEASE_TIME_PROP,
                QP_SPACE_READ_LEASE_TIME_DEFAULT));
    }

    /**
     * @param qpSpaceReadLeaseTime
     */
    public void setQpSpaceReadLeaseTime(int qpSpaceReadLeaseTime) {
        this.setProperty(FULL_QP_SPACE_READ_LEASE_TIME_PROP,
                String.valueOf(qpSpaceReadLeaseTime));
    }

    /**
     *
     *
     */
    public long getQpSpaceWriteLeaseTime() {
        return Long.parseLong(getProperty(FULL_QP_SPACE_WRITE_LEASE_PROP,
                QP_SPACE_WRITE_LEASE_DEFAULT));
    }

    /**
     * @param qpSpaceWriteLeaseTime
     */
    public void setQpSpaceWriteLeaseTime(long qpSpaceWriteLeaseTime) {
        this.setProperty(FULL_QP_SPACE_WRITE_LEASE_PROP,
                String.valueOf(qpSpaceWriteLeaseTime));
    }

    /**
     *
     *
     */
    public int getQpTransactionTimeout() {
        return Integer.parseInt(getProperty(FULL_QP_TRANSACTION_TIMEOUT_PROP,
                QP_TRANSACTION_TIMEOUT_DEFAULT));
    }

    /**
     * @param qpTransactionTimeout
     */
    public void setQpTransactionTimeout(int qpTransactionTimeout) {
        this.setProperty(FULL_QP_TRANSACTION_TIMEOUT_PROP,
                String.valueOf(qpTransactionTimeout));
    }

    /**
     *
     *
     */
    public String getQpDateFormat() {
        return getProperty(FULL_QP_DATE_FORMAT_PROP, QP_DATE_FORMAT_DEFAULT);
    }

    /**
     * @param qpDateFormat
     */
    public void setQpDateFormat(String qpDateFormat) {
        this.setProperty(FULL_QP_DATE_FORMAT_PROP, qpDateFormat);
    }

    /**
     *
     *
     */
    public String getQpDateTimeFormat() {
        return getProperty(FULL_QP_DATETIME_FORMAT_PROP,
                QP_DATETIME_FORMAT_DEFAULT);
    }

    /**
     * @param qpDateTimeFormat
     */
    public void setQpDateTimeFormat(String qpDateTimeFormat) {
        this.setProperty(FULL_QP_DATETIME_FORMAT_PROP, qpDateTimeFormat);
    }

    /**
     *
     *
     */
    public String getQpTimeFormat() {
        return getProperty(FULL_QP_TIME_FORMAT_PROP, QP_TIME_FORMAT_DEFAULT);
    }

    /**
     * @param qpTimeFormat
     */
    public void setQpTimeFormat(String qpTimeFormat) {
        this.setProperty(FULL_QP_TIME_FORMAT_PROP, qpTimeFormat);
    }

    // Engine setters and getters methods

    /**
     *
     */
    public String isEngineMemoryUsageEnabled() {
        return getProperty(FULL_ENGINE_MEMORY_USAGE_ENABLED_PROP,
                ENGINE_MEMORY_USAGE_ENABLED_DEFAULT);
    }

    /**
     * @param isMemoryUsageEnabled
     */
    public void setEngineMemoryUsageEnabled(String isMemoryUsageEnabled) {
        this.setProperty(FULL_ENGINE_MEMORY_USAGE_ENABLED_PROP,
                isMemoryUsageEnabled);
    }

    /**
     * @param highPercentageRatio
     */
    public void setEngineMemoryUsageHighPercentageRatio(
            String highPercentageRatio) {
        this.setProperty(FULL_ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP,
                highPercentageRatio);
    }

    /**
     *
     *
     */
    public String getEngineMemoryUsageHighPercentageRatio() {
        return getProperty(FULL_ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP,
                ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT);
    }

    /**
     * @param blockPercentageRatio
     */
    public void setEngineMemoryUsageWriteOnlyBlockPercentageRatio(
            String blockPercentageRatio) {
        this.setProperty(FULL_ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP,
                String.valueOf(blockPercentageRatio));
    }

    /**
     *
     *
     */
    public String getEngineMemoryUsageWriteOnlyBlockPercentageRatio() {
        return getProperty(FULL_ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP,
                ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT);
    }

    /**
     * @param writeOnlyCheckPercentageRatio
     */
    public void setEngineMemoryWriteOnlyCheckPercentageRatio(
            String writeOnlyCheckPercentageRatio) {
        this.setProperty(FULL_ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP,
                String.valueOf(writeOnlyCheckPercentageRatio));
    }

    /**
     *
     *
     *
     */
    public String getEngineMemoryWriteOnlyCheckPercentageRatio() {
        return getProperty(FULL_ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP,
                ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT);
    }

    /**
     * @param lowPercentageRatio
     */
    public void setEngineMemoryUsageLowPercentageRatio(String lowPercentageRatio) {
        this.setProperty(FULL_ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP,
                String.valueOf(lowPercentageRatio));
    }

    /**
     *
     *
     */
    public String getEngineMemoryUsageLowPercentageRatio() {
        return getProperty(FULL_ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP,
                ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT);
    }

    /**
     *
     *
     */
    public String isEngineMemoryExplicitGSEnabled() {
        return getProperty(FULL_ENGINE_MEMORY_EXPLICIT_GC_PROP,
                ENGINE_MEMORY_EXPLICIT_GC_DEFAULT);
    }

    /**
     * @param isMemoryExplicitGSEnabled
     */
    public void setEngineMemoryExplicitGSEnabled(
            String isMemoryExplicitGSEnabled) {
        this.setProperty(FULL_ENGINE_MEMORY_EXPLICIT_GC_PROP,
                isMemoryExplicitGSEnabled);
    }

    public String isEngineMemoryGCBeforeShortageEnabled() {
        return getProperty(FULL_ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP,
                ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_DEFAULT);
    }

    public void setEngineMemoryGCBeforeShortageEnabled(
            String gcBeforeShortage) {
        this.setProperty(FULL_ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP,
                gcBeforeShortage);
    }

    /**
     * * @param retryCount
     */
    public void setEngineMemoryUsageRetryCount(String retryCount) {
        this.setProperty(FULL_ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP, retryCount);
    }

    /**
     *
     *
     */
    public String getEngineMemoryUsageRetryCount() {
        return getProperty(FULL_ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP,
                ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT);
    }

    /**
     * @param evictionBatchSize
     */
    public void setEngineMemoryUsageEvictionBatchSize(String evictionBatchSize) {
        this.setProperty(FULL_ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP,
                evictionBatchSize);
    }

    /**
     *
     *
     */
    public String getEngineMemoryUsageEvictionBatchSize() {
        return getProperty(FULL_ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP,
                ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT);
    }

    /**
     * @param maxThreads
     */
    public void setEngineMaxThreads(String maxThreads) {
        this.setProperty(FULL_ENGINE_MAX_THREADS_PROP, maxThreads);
    }

    /**
     *
     *
     */
    public String getEngineMaxThreads() {
        return getProperty(FULL_ENGINE_MAX_THREADS_PROP,
                ENGINE_MAX_THREADS_DEFAULT);
    }

    /**
     * @param minThreads
     */
    public void setEngineMinThreads(String minThreads) {
        this.setProperty(FULL_ENGINE_MIN_THREADS_PROP, minThreads);
    }

    /**
     *
     *
     */
    public String getEngineMinThreads() {
        return getProperty(FULL_ENGINE_MIN_THREADS_PROP,
                ENGINE_MIN_THREADS_DEFAULT);
    }

    /**
     * @param dCacheConfigName
     */
    public void setDCacheConfigName(String dCacheConfigName) {
        this.setProperty(FULL_DCACHE_CONFIG_NAME_PROP, dCacheConfigName);
    }

    /**
     *
     *
     */
    public String getDCacheConfigName() {
        return getProperty(FULL_DCACHE_CONFIG_NAME_PROP,
                DCACHE_CONFIG_NAME_DEFAULT);
    }

    /**
     * @param notifyRetries
     */
    public void setNotifyRetries(String notifyRetries) {
        this.setProperty(FULL_ENGINE_NOTIFIER_TTL_PROP, notifyRetries);
    }

    /**
     *
     *
     */
    public String getNotifyRetries() {
        return getProperty(FULL_ENGINE_NOTIFIER_TTL_PROP,
                String.valueOf(ENGINE_NOTIFIER_RETRIES_DEFAULT));
    }

    public void setConnectionRetries(String connectionRetries) {
        this.setProperty(SpaceProxy.OldRouter.RETRY_CONNECTION_FULL, connectionRetries);
    }

    public String getConnectionRetries() {
        return getProperty(SpaceProxy.OldRouter.RETRY_CONNECTION_FULL,
                String.valueOf(SpaceProxy.OldRouter.RETRY_CONNECTION_DEFAULT));
    }

    public void setBlobstoreRocksDBAllowDuplicateUIDs(String isEnabled){
        this.setProperty(FULL_ENGINE_BLOBSTORE_ROCKSDB_ALLOW_DUPLICATE_UIDS, isEnabled);
    }

    /**
     * @param serializationType
     */
    public void setSerializationType(int serializationType) {
        this.setProperty(FULL_ENGINE_SERIALIZATION_TYPE_PROP,
                String.valueOf(serializationType));
    }

    /**
     *
     *
     */
    public int getSerializationType() {
        return Integer.parseInt(getProperty(FULL_ENGINE_SERIALIZATION_TYPE_PROP,
                String.valueOf(ENGINE_SERIALIZATION_TYPE_DEFAULT)));
    }

    /**
     *
     *
     */
    public boolean isPrivate() {
        return Boolean.valueOf(getProperty(FULL_LOOKUP_IS_PRIVATE_PROP,
                LOOKUP_IS_PRIVATE_DEFAULT))
                .booleanValue();
    }

    /**
     * @param isPrivate
     */
    public void setPrivate(boolean isPrivate) {
        this.setProperty(FULL_LOOKUP_IS_PRIVATE_PROP, String.valueOf(isPrivate));
    }

    /**
     * @param expirationTimeInterval
     */
    public void setExpirationTimeInterval(String expirationTimeInterval) {
        this.setProperty(FULL_LM_EXPIRATION_TIME_INTERVAL_PROP,
                expirationTimeInterval);
    }

    /**
     *
     *
     */
    public String getExpirationTimeInterval() {
        return getProperty(FULL_LM_EXPIRATION_TIME_INTERVAL_PROP,
                String.valueOf(LM_EXPIRATION_TIME_INTERVAL_DEFAULT));
    }

    public void setExpirationTimeRecentDeletes(
            String expirationTimeRecentDeletes) {
        this.setProperty(FULL_LM_EXPIRATION_TIME_RECENT_DELETES_PROP,
                expirationTimeRecentDeletes);
    }

    public String getExpirationTimeRecentDeletes() {
        return getProperty(FULL_LM_EXPIRATION_TIME_RECENT_DELETES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT));
    }

    public void setExpirationTimeRecentUpdate(String expirationTimeRecentUpdates) {
        this.setProperty(FULL_LM_EXPIRATION_TIME_RECENT_UPDATES_PROP,
                expirationTimeRecentUpdates);
    }

    public String getExpirationTimeRecentUpdates() {
        return getProperty(FULL_LM_EXPIRATION_TIME_RECENT_DELETES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT));
    }

    public void setExpirationStaleReplicas(String expirationStaleReplicas) {
        this.setProperty(FULL_LM_EXPIRATION_TIME_STALE_REPLICAS_PROP,
                expirationStaleReplicas);
    }

    public String getExpirationStaleReplicas() {
        return getProperty(FULL_LM_EXPIRATION_TIME_STALE_REPLICAS_PROP,
                String.valueOf(LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT));
    }

    /**
     * @param spaceState
     */
    public void setSpaceState(String spaceState) {
        setProperty(FULL_SPACE_STATE, spaceState);
    }

    /**
     *
     *
     */
    public String getSpaceState() {
        return getProperty(FULL_SPACE_STATE);
    }

    /**
     * @param schemaName
     */
    public void setSchemaName(String schemaName) {
        this.setProperty(FULL_SCHEMA_ELEMENT, schemaName);
    }

    /**
     *
     *
     */
    public String getSchemaName() {
        return getProperty(FULL_SCHEMA_ELEMENT, null);
    }

    /**
     * @param isLoadOnStartup
     */
    public void setLoadOnStartup(boolean isLoadOnStartup) {
        this.setProperty(IS_SPACE_LOAD_ON_STARTUP,
                String.valueOf(isLoadOnStartup));
    }

    /**
     *
     *
     */
    public boolean isLoadOnStartup() {
        return Boolean.valueOf(getProperty(IS_SPACE_LOAD_ON_STARTUP,
                Boolean.FALSE.toString()))
                .booleanValue();
    }

    /**
     * @param cacheManagerSize
     */
    public void setCacheManagerSize(String cacheManagerSize) {
        setProperty(FULL_CACHE_MANAGER_SIZE_PROP, cacheManagerSize);
    }

    /**
     * max cache size
     */
    public String getCacheManagerSize() {
        return getProperty(FULL_CACHE_MANAGER_SIZE_PROP,
                CACHE_MANAGER_SIZE_DEFAULT);
    }

    /**
     * @param cachePolicy
     */
    public void setCachePolicy(String cachePolicy) {
        setProperty(FULL_CACHE_POLICY_PROP, cachePolicy);
    }

    /**
     *
     *
     */
    public String getCachePolicy() {
        return getProperty(FULL_CACHE_POLICY_PROP,
                String.valueOf(CACHE_POLICY_ALL_IN_CACHE));
    }

    /**
     *
     *
     */
    public boolean isClustered() {
        return Boolean.valueOf(getProperty(FULL_IS_CLUSTER_SPACE_PROP,
                IS_CLUSTER_SPACE_DEFAULT))
                .booleanValue();
    }

    /**
     * @param isClustered
     */
    public void setClustered(boolean isClustered) {
        this.setProperty(FULL_IS_CLUSTER_SPACE_PROP,
                String.valueOf(isClustered));
        // temporary initialize deprecated class member
        m_isClustered = isClustered;
    }

    /**
     * @param clusterConfigURL
     */
    public void setClusterConfigURL(String clusterConfigURL) {
        this.setProperty(FULL_CLUSTER_CONFIG_URL_PROP, clusterConfigURL);
    }

    /**
     *
     *
     */
    public String getClusterConfigURL() {
        return getProperty(FULL_CLUSTER_CONFIG_URL_PROP,
                CLUSTER_CONFIG_URL_DEFAULT);
    }

    // JMS properties

    /**
     *
     */
    public void setJMSRmiPort(String rmiPort) {
        this.setProperty(FULL_JMS_RMI_PORT_PROP, rmiPort);
    }

    /**
     *
     *
     */
    public String getJMSRmiPort() {
        return getProperty(FULL_JMS_RMI_PORT_PROP, JMS_RMI_PORT_DEFAULT);
    }

    /**
     * @param topicNames
     */
    public void setJMSTopicNames(String topicNames) {
        this.setProperty(FULL_JMS_TOPIC_NAMES_PROP, topicNames);
    }

    /**
     *
     *
     */
    public String getJMSTopicNames() {
        return getProperty(FULL_JMS_TOPIC_NAMES_PROP, JMS_TOPIC_NAMES_DEFAULT);
    }

    /**
     * @param queueNames
     */
    public void setJMSQueueNames(String queueNames) {
        this.setProperty(FULL_JMS_QUEUE_NAMES_PROP, queueNames);
    }

    /**
     *
     *
     */
    public String getJMSQueueNames() {
        return getProperty(FULL_JMS_QUEUE_NAMES_PROP, JMS_QUEUE_NAMES_DEFAULT);
    }

    /**
     * @param customProperties
     */
    public void setCustomProperties(Properties customProperties) {
        _customProperties.clear();
        _customProperties.putAll(customProperties);
    }

    public Properties getCustomProperties() {
        return _customProperties;
    }

    /**
     * @param filtersInfo
     */
    public void setFiltersInfo(FiltersInfo[] filtersInfo) {
        _filtersInfo = filtersInfo;
    }

    /**
     * @param filterInfo
     * @param index
     */
    public void setFilterInfoAt(FiltersInfo filterInfo, int index) {
        _filtersInfo[index] = filterInfo;
    }

    public FiltersInfo[] getFiltersInfo() {
        return _filtersInfo;
    }

    /**
     * @param clusterPolicy
     */
    public void setClusterPolicy(ClusterPolicy clusterPolicy) {
        _clusterPolicy = clusterPolicy;
    }

    public ClusterPolicy getClusterPolicy() {
        return _clusterPolicy;
    }

    /**
     * @param dCacheProperties
     */
    public void setDCacheProperties(JSpaceAttributes dCacheProperties) {
        _dCacheProperties = dCacheProperties;
    }

    public JSpaceAttributes getDCacheProperties() {
        return _dCacheProperties;
    }

    /**
     * Is mirror service
     *
     * @return true if the space has a mirror service enabled, otherwise false returned
     */
    public boolean isMirrorServiceEnabled() {
        return Boolean.valueOf(getProperty(FULL_MIRROR_SERVICE_ENABLED_PROP,
                MIRROR_SERVICE_ENABLED_DEFAULT))
                .booleanValue();
    }

    /**
     * Set mirror service enabled.
     *
     * @param isMirrorServiceEnabled true if the space has a mirror service enabled, otherwise false
     *                               returned
     */
    public void setMirrorServiceEnabled(boolean isMirrorServiceEnabled) {
        this.setProperty(FULL_MIRROR_SERVICE_ENABLED_PROP,
                String.valueOf(isMirrorServiceEnabled));
    }

    public void setMaxClassLoaders(int maxClassLoaders) {
        this.setProperty(FULL_REMOTE_CODE_MAX_CLASS_LOADERS_PROP,
                String.valueOf(maxClassLoaders));
    }

    public String getMaxClassLoaders() {
        return getProperty(FULL_REMOTE_CODE_MAX_CLASS_LOADERS_PROP,
                REMOTE_CODE_MAX_CLASS_LOADERS_DEFAULT);
    }

    public void setSupportCodeChange(boolean enableTaskReloading) {
        this.setProperty(FULL_REMOTE_CODE_SUPPORT_CODE_CHANGE_PROP,
                String.valueOf(enableTaskReloading));
    }

    public String getSupportCodeChange() {
        return getProperty(FULL_REMOTE_CODE_SUPPORT_CODE_CHANGE_PROP,
                REMOTE_CODE_SUPPORT_CODE_CHANGE_DEFAULT);
    }

    /**
     *
     *
     */
    public String getDataSourceClass() {
        return getProperty(Constants.SPACE_CONFIG_PREFIX
                + DATA_SOURCE_CLASS_PROP, DATA_SOURCE_CLASS_DEFAULT);
    }

    public SpaceInstanceConfig getSpaceInstanceConfig() {
        return (SpaceInstanceConfig) getCustomProperties().get(Constants.Space.SPACE_CONFIG);
    }

    public Map<String, SpaceCustomComponent> getCustomComponents() {
        SpaceInstanceConfig config = getSpaceInstanceConfig();
        return config != null ? config.getCustomComponents() : null;
    }

    public SpaceDataSource getSpaceDataSourceInstance() {
        return (SpaceDataSource) getCustomProperties().get(Constants.DataAdapter.SPACE_DATA_SOURCE);
    }

    public String getSpaceDataSourceClassName() {
        return getCustomProperties().getProperty(Constants.SPACE_CONFIG_PREFIX + Constants.DataAdapter.SPACE_DATA_SOURCE_CLASS);
    }

    public SpaceSynchronizationEndpoint getSynchronizationEndpointInstance() {
        return (SpaceSynchronizationEndpoint) getCustomProperties().get(Constants.DataAdapter.SPACE_SYNC_ENDPOINT);
    }

    public String getSynchronizationEndpointClassName() {
        return getCustomProperties().getProperty(Constants.SPACE_CONFIG_PREFIX + Constants.DataAdapter.SPACE_SYNC_ENDPOINT_CLASS);
    }

    /**
     * @param className
     */
    public void setDataSourceClass(String className) {
        setProperty(Constants.SPACE_CONFIG_PREFIX + DATA_SOURCE_CLASS_PROP,
                className);
    }

    /**
     *
     *
     */
    public String getDataPropertiesFile() {
        return getProperty(Constants.SPACE_CONFIG_PREFIX + DATA_PROPERTIES,
                DATA_PROPERTIES_DEFAULT);
    }

    /**
     * @param propertiesFile
     */
    public void setDataPropertiesFile(String propertiesFile) {
        setProperty(Constants.SPACE_CONFIG_PREFIX + DATA_PROPERTIES,
                propertiesFile);
    }

    /**
     *
     *
     */
    public String getDataClass() {
        return getProperty(Constants.SPACE_CONFIG_PREFIX + DATA_CLASS_PROP,
                DATA_CLASS_DEFAULT);
    }

    /**
     *
     *
     */
    public String getQueryBuilderClass() {
        return getProperty(Constants.SPACE_CONFIG_PREFIX + QUERY_BUILDER_PROP,
                QUERY_BUILDER_PROP_DEFAULT);
    }

    /**
     * @param className
     */
    public void setQueryBuilderClass(String className) {
        setProperty(Constants.SPACE_CONFIG_PREFIX + QUERY_BUILDER_PROP,
                className);
    }

    /**
     * @param className
     */
    public void setDataClass(String className) {
        setProperty(Constants.SPACE_CONFIG_PREFIX + DATA_CLASS_PROP, className);
    }

    /**
     * usage
     */
    public String getUsage() {
        return getProperty(Constants.SPACE_CONFIG_PREFIX + USAGE, USAGE_DEFAULT);
    }

    /**
     * Set data source usage
     */
    public void setUsage(String usage) {
        this.setProperty(Constants.SPACE_CONFIG_PREFIX + USAGE, usage);
    }

    /**
     * supports-inheritance
     *
     * @return true if the data source support inheritance , otherwise false returned
     */
    public boolean isSupportsInheritanceEnabled() {
        return Boolean.valueOf(getProperty(Constants.SPACE_CONFIG_PREFIX
                + SUPPORTS_INHERITANCE_PROP, SUPPORTS_INHERITANCE_DEFAULT))
                .booleanValue();
    }

    /**
     * Set supports-inheritance
     *
     * @param isSupportsInheritanceEnabled true if the data source supports inheritance, otherwise
     *                                     false returned
     */
    public void setSupportsInheritanceEnabled(
            boolean isSupportsInheritanceEnabled) {
        this.setProperty(Constants.SPACE_CONFIG_PREFIX
                        + SUPPORTS_INHERITANCE_PROP,
                String.valueOf(isSupportsInheritanceEnabled));
    }

    /**
     * supports-version
     *
     * @return true if the data source support version, otherwise false returned
     */
    public boolean isSupportsVersionEnabled() {
        return Boolean.valueOf(getProperty(Constants.SPACE_CONFIG_PREFIX
                + SUPPORTS_VERSION_PROP, SUPPORTS_VERSION_DEFAULT))
                .booleanValue();
    }

    /**
     * Set supports-version
     *
     * @param isSupportsVersionEnabled true if the data source supports version, otherwise false
     *                                 returned
     */
    public void setSupportsVersionEnabled(boolean isSupportsVersionEnabled) {
        this.setProperty(Constants.SPACE_CONFIG_PREFIX + SUPPORTS_VERSION_PROP,
                String.valueOf(isSupportsVersionEnabled));
    }

    public boolean isSupportsPartialUpdateEnabled() {
        return Boolean.valueOf(getProperty(Constants.SPACE_CONFIG_PREFIX
                + SUPPORTS_PARTIAL_UPDATE_PROP, SUPPORTS_PARTIAL_UPDATE_DEFAULT))
                .booleanValue();
    }

    public void setSupportsPartialUpdateEnabled(
            boolean isSupportsPartialUpdateEnabled) {
        this.setProperty(Constants.SPACE_CONFIG_PREFIX
                        + SUPPORTS_PARTIAL_UPDATE_PROP,
                String.valueOf(isSupportsPartialUpdateEnabled));
    }

    public boolean isSupportsRemoveByIdEnabled() {
        return Boolean.valueOf(getProperty(Constants.SPACE_CONFIG_PREFIX
                + SUPPORTS_REMOVE_BY_ID_PROP, SUPPORTS_REMOVE_BY_ID_DEFAULT))
                .booleanValue();
    }

    public void setSupportsRemoveByIdEnabled(boolean isSupportsRemoveByIdEnabled) {
        this.setProperty(Constants.SPACE_CONFIG_PREFIX
                        + SUPPORTS_REMOVE_BY_ID_PROP,
                String.valueOf(isSupportsRemoveByIdEnabled));
    }

    /**
     * Gets shared iterator mode
     *
     * @return true if the data source should be wrapped with a shared iterator decorator which
     * optimize concurrent read access for {@link SQLDataProvider} data sources
     */
    public boolean getDataSourceSharedIteratorMode() {
        return Boolean.valueOf(getProperty(Constants.SPACE_CONFIG_PREFIX
                        + DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP,
                DATA_SOURCE_SHARE_ITERATOR_ENABLED_DEFAULT))
                .booleanValue();
    }

    /**
     * Set shared iterator mode
     *
     * @param enabled true if the data source should be wrapped with a shared iterator decorator
     *                which optimize concurrent read access for {@link SQLDataProvider} data
     *                sources
     */
    public void setDataSourceSharedIteratorMode(boolean enabled) {
        this.setProperty(Constants.SPACE_CONFIG_PREFIX
                        + DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP,
                String.valueOf(enabled));
    }

    /**
     * Get shared iterator time to live
     *
     * @return time to live in miliseconds of a shared data iterator, once it has expired the
     * iterator will no longer be shared by new requests
     */
    public long getDataSourceSharedIteratorTimeToLive() {
        String leaseManagerExpirationTimeRecentDeletes = getExpirationTimeRecentDeletes();
        String leaseManagerExpirationTimeRecentUpdates = getExpirationTimeRecentUpdates();
        String dataSourceShareIteratorTTLDefault = DataAdaptorIterator.getDataSourceShareIteratorTTLDefault(Long.parseLong(leaseManagerExpirationTimeRecentDeletes),
                Long.parseLong(leaseManagerExpirationTimeRecentUpdates));
        return Long.valueOf(getProperty(Constants.SPACE_CONFIG_PREFIX + DATA_SOURCE_SHARE_ITERATOR_TTL_PROP,
                dataSourceShareIteratorTTLDefault)).longValue();

    }

    /**
     * Set shared iterator time to live
     *
     * @param timeToLive time to live in miliseconds of a shared data iterator, once it has expired
     *                   the iterator will no longer be shared by new requests
     */
    public void setDataSourceSharedIteratorTimeToLive(long timeToLive) {
        this.setProperty(Constants.SPACE_CONFIG_PREFIX
                        + DATA_SOURCE_SHARE_ITERATOR_TTL_PROP,
                String.valueOf(timeToLive));
    }

    public String getProxyConnectionMode() {
        return getProperty(SpaceProxy.OldRouter.CONNECTION_MONITOR_FULL,
                SpaceProxy.OldRouter.CONNECTION_MONITOR_DEFAULT);
    }

    public void setProxyConnectionMode(String connectionMode) {
        setProperty(SpaceProxy.OldRouter.CONNECTION_MONITOR_FULL, connectionMode);
    }

    public long getProxyMonitorFrequency() {
        return Long.parseLong(getProperty(SpaceProxy.OldRouter.MONITOR_FREQUENCY_FULL,
                SpaceProxy.OldRouter.MONITOR_FREQUENCY_DEFAULT));
    }

    public void setProxyMonitorFrequency(long frequency) {
        setProperty(SpaceProxy.OldRouter.MONITOR_FREQUENCY_FULL,
                String.valueOf(frequency));
    }

    public long getProxyDetectorFrequency() {
        return Long.parseLong(getProperty(SpaceProxy.OldRouter.DETECTOR_FREQUENCY_FULL,
                SpaceProxy.OldRouter.DETECTOR_FREQUENCY_DEFAULT));
    }

    public void setProxyDetectorFrequency(long frequency) {
        setProperty(SpaceProxy.OldRouter.DETECTOR_FREQUENCY_FULL,
                String.valueOf(frequency));
    }

    public int getProxyConnectionRetries() {
        return Integer.parseInt(getProperty(SpaceProxy.OldRouter.CONNECTION_RETRIES_FULL,
                SpaceProxy.OldRouter.CONNECTION_RETRIES_DEFAULT));
    }

    public void setProxyConnectionRetries(int connectionRetries) {
        setProperty(SpaceProxy.OldRouter.CONNECTION_RETRIES_FULL,
                String.valueOf(connectionRetries));
    }

    /**
     * @deprecated Use {@link JSpaceAttributes#getBlobStoreProperties() instead all blobstore
     * properties is located inside.}
     */
    @Deprecated
    public int getBlobStoreCapacityGB() {
        String blobStoreCapacityGB = getProperty(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_CAPACITY_GB_PROP, "-1");
        return Integer.parseInt(blobStoreCapacityGB);
    }

    /**
     * @deprecated Use {@link JSpaceAttributes#getBlobStoreProperties() instead all blobstore
     * properties is located inside.}
     */
    @Deprecated
    public int getBlobStoreCacheCapacityMB() {
        String blobStoreCacheCapacityMB = getProperty(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_CACHE_CAPACITY_MB_PROP, "-1");
        return Integer.parseInt(blobStoreCacheCapacityMB);
    }

    /**
     * @deprecated Use {@link JSpaceAttributes#getBlobStoreProperties() instead all blobstore
     * properties is located inside.}
     */
    @Deprecated
    public String getBlobStoreDevices() {
        return getProperty(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_DEVICES_PROP);
    }

    @Deprecated
    public String getBlobStoreVolumeDir() {
        return getProperty(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_VOLUME_DIR_PROP);
    }

    /**
     * @deprecated Use {@link JSpaceAttributes#getBlobStoreProperties() instead all blobstore
     * properties is located inside.}
     */
    @Deprecated
    public String getBlobStoreDurabilityLevel() {
        return getProperty(Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_CACHE_DURABILITY_LEVEL_PROP);
    }

    /**
     * @deprecated Use {@link JSpaceAttributes#getBlobStoreProperties() instead all blobstore
     * properties is located inside.}
     */
    @Deprecated
    public boolean isBlobstorePersistent() {
        return Boolean.parseBoolean(getProperty(Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP, "false"));
    }

    /**
     * @deprecated Use {@link JSpaceAttributes#getBlobStoreProperties() instead all blobstore
     * properties is located inside.}
     */
    @Deprecated
    public int getBlobStoreCacheSize() {
        return Integer.parseInt(getProperty(Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP, CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT));
    }

    /**
     * Gets all blobstore properties from blobstore driver and the cache manager.}
     */
    public Properties getBlobStoreProperties() {
        return _blobStoreProperties;
    }

    public void setBlobStoreProperties(Properties blobstoreProperties) {
        this._blobStoreProperties.clear();
        this._blobStoreProperties.putAll(blobstoreProperties);
    }

    public int getZookeeperConnectionTimeout(){
        return Integer.parseInt(getSystemProperty(FULL_LEADER_SELECTOR_CONNECTION_TIMEOUT,
                getProperty(FULL_LEADER_SELECTOR_CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_DEFAULT)));
    }

    public void setZookeeperConnectionTimeout(long connectionTimeout){
        setProperty(FULL_LEADER_SELECTOR_CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }

    public int getZookeeperSessionTimeout(){
        return Integer.parseInt(getSystemProperty(FULL_LEADER_SELECTOR_SESSION_TIMEOUT,
                getProperty(FULL_LEADER_SELECTOR_SESSION_TIMEOUT, SESSION_TIMEOUT_DEFAULT)));
    }

    public void setZookeeperSessionTimeout(long sessionTimeout){
        setProperty(FULL_LEADER_SELECTOR_SESSION_TIMEOUT, String.valueOf(sessionTimeout));
    }

    public int getZookeeperRetryTimeout(){
        return Integer.parseInt(getSystemProperty(FULL_LEADER_SELECTOR_RETRY_TIMEOUT,
                getProperty(FULL_LEADER_SELECTOR_RETRY_TIMEOUT, RETRY_TIMEOUT_DEFAULT)));
    }

    public void setZookeeperRetryTimeout(long retryTimeout){
        setProperty(FULL_LEADER_SELECTOR_RETRY_TIMEOUT, String.valueOf(retryTimeout));
    }

    public int getZookeeperRetryInterval(){
        return Integer.parseInt(getSystemProperty(FULL_LEADER_SELECTOR_RETRY_INTERVAL,
                getProperty(FULL_LEADER_SELECTOR_RETRY_INTERVAL, RETRY_INTERVAL_DEFAULT)));
    }

    public void setZookeeperRetryInterval(long retryInterval){
        setProperty(FULL_LEADER_SELECTOR_RETRY_INTERVAL, String.valueOf(retryInterval));
    }

    private String getSystemProperty(String key, String def) {
        return System.getProperty(key, def);
    }

    /**
     * Clears all properties held, including defaults, custom properties, and dcache properties.
     */
    @Override
    public synchronized void clear() {
        super.clear();

        if (_customProperties != null)
            _customProperties.clear();

        if (_dCacheProperties != null)
            _dCacheProperties.clear();
    }

    public SpaceClusterInfo getClusterInfo() {
        return _clusterInfo;
    }

    public void setClusterInfo(SpaceClusterInfo newClusterInfo) {
        if (this._clusterInfo != newClusterInfo) {
            this._clusterInfo = newClusterInfo;
        }
    }

    public SpaceConfig cloneAndUpdate(ClusterTopology chunksMap) {
        SpaceConfig newSpaceConfig = (SpaceConfig) this.clone();
        newSpaceConfig.setClusterInfo(this._clusterInfo.cloneAndUpdate(chunksMap));
        return newSpaceConfig;
    }

    /* Bit map for serialization */
    private interface BitMap {
        byte _DCACHEPROPERTIES = 1 << 0;
        byte _CLUSTERPOLICY = 1 << 1;
        byte _FILTERSINFO = 1 << 2;
        byte _CUSTOMPROPERTIES = 1 << 3;
        byte M_ISCLUSTERED = 1 << 4;
        byte M_CLUSTERINFO = 1 << 5;
        byte _BLOBSTOREPROPERTIES = 1 << 6;
    }

    private byte buildFlags(PlatformLogicalVersion version) {
        byte flags = 0;
        if (_dCacheProperties != null)
            flags |= BitMap._DCACHEPROPERTIES;
        if (_clusterPolicy != null)
            flags |= BitMap._CLUSTERPOLICY;
        if (_filtersInfo != null)
            flags |= BitMap._FILTERSINFO;
        if (_customProperties != null)
            flags |= BitMap._CUSTOMPROPERTIES;
        if (m_isClustered)
            flags |= BitMap.M_ISCLUSTERED;
        if (_blobStoreProperties != null && version.greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            flags |= BitMap._BLOBSTOREPROPERTIES;
        return flags;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        int version = in.readInt();

        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Class ["
                    + getClass().getName()
                    + "] received version ["
                    + version
                    + "] does not match local version ["
                    + SERIAL_VERSION
                    + "]. Please sure you are using the same version on both ends.");

        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            Object key = in.readObject();
            Object value = in.readObject();
            put(key, value);
        }
        final byte flags = in.readByte();

        if (flags == 0)
            return;

        if ((flags & BitMap._DCACHEPROPERTIES) != 0) {
            _dCacheProperties = (JSpaceAttributes) in.readObject();
        }

        if ((flags & BitMap._CLUSTERPOLICY) != 0) {
            _clusterPolicy = new ClusterPolicy();
            _clusterPolicy.readExternal(in);
        }

        if ((flags & BitMap._FILTERSINFO) != 0)
            _filtersInfo = (FiltersInfo[]) in.readObject();

        if ((flags & BitMap._CUSTOMPROPERTIES) != 0)
            _customProperties = (Properties) in.readObject();

        m_isClustered = ((flags & BitMap.M_ISCLUSTERED) != 0);

        if ((flags & BitMap.M_CLUSTERINFO) != 0) {
            _clusterInfo = new SpaceClusterInfo();
            _clusterInfo.readExternal(in);
        }

        if ((flags & BitMap._BLOBSTOREPROPERTIES) != 0)
            _blobStoreProperties = (Properties) in.readObject();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        final boolean serializeClusterInfo = _clusterInfo != null && version.greaterOrEquals(PlatformLogicalVersion.v11_0_0);
        out.writeInt(SERIAL_VERSION);

        // Write out length, count of elements and then the key/value objects
        out.writeInt(size());

        for (Map.Entry entry : this.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }

        byte flags = buildFlags(version);
        if (serializeClusterInfo)
            flags |= BitMap.M_CLUSTERINFO;
        out.writeByte(flags);

        if (flags == 0)
            return;

        if (_dCacheProperties != null) {
            out.writeObject(_dCacheProperties);
        }

        if (_clusterPolicy != null) {
            _clusterPolicy.writeExternal(out);
        }

        if (_filtersInfo != null) {
            out.writeObject(_filtersInfo);
        }

        if (_customProperties != null) {
            out.writeObject(_customProperties);
        }

        if (serializeClusterInfo)
            _clusterInfo.writeExternal(out);

        if (version.greaterOrEquals(PlatformLogicalVersion.v11_0_0) && _blobStoreProperties != null) {
            out.writeObject(_blobStoreProperties);
        }
    }
}
