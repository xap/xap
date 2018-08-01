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

import com.gigaspaces.client.transaction.ITransactionManagerProvider.TransactionManagerType;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;

/**
 * This class contains all constants of GigaSpaces Platform. Use the following property convention:
 * [interface name]_[MY_PROPERTY_NAME]_PROP
 *
 * Default property value: [interface name]_[MY_PROPERTY_NAME]_DEFAULT
 *
 * @author Igor Goldenberg
 * @version 3.2
 */
public interface Constants {
    String SPACE_CONFIG_PREFIX = "space-config.";
    String IS_SPACE_LOAD_ON_STARTUP = "load-on-startup";

    public interface System {
        String SYSTEM_GS_POLICY = "policy/policy.all";
    }

    public interface Schemas {
        String SCHEMA_ELEMENT = "schema";
        String FULL_SCHEMA_ELEMENT = SPACE_CONFIG_PREFIX + SCHEMA_ELEMENT;
        String SCHEMAS_FOLDER = "schemas";
        String DEFAULT_SCHEMA = "default";
        String PERSISTENT_SCHEMA = "persistent";
        String MIRROR_SCHEMA = "mirror";
        String CACHE_SCHEMA = "cache";
        String JAVASPACE_SCHEMA = "javaspace";

        String SPACE_SCHEMA_FILE_SUFFIX = "-space-schema.xml";
        String CONTAINER_SCHEMA_FILE_SUFFIX = "-container-schema.xml";

        String SCHEMA_FILE_PATH = "schemaFilePath";
    }

    public interface Container {
        String PREFIX = "com.j_spaces.core.container.";

        String CONTAINER_CONFIG_DIRECTORY = "config";

        String CONTAINER_CONFIG_FILE_SUFFIX = "-config.xml";

        String CONTAINER_NAME_PROP = PREFIX + "name";

        String CONTAINER_SHUTDOWN_HOOK_PROP = PREFIX + "shutdown_hook";
        String CONTAINER_SHUTDOWN_HOOK_PROP_DEFAULT = "true";

        /**
         * Embedded Mahalo Jini Transaction Manager
         */
        String CONTAINER_EMBEDDED_MAHALO_ENABLED_PROP = PREFIX + "embedded-services.mahalo.start-embedded-mahalo";
        String CONTAINER_EMBEDDED_MAHALO_ENABLED_DEFAULT = "false";
    }

    public interface Mirror {
        String MIRROR_SERVICE_PREFIX = "mirror-service.";

        String MIRROR_SERVICE_ENABLED_PROP = MIRROR_SERVICE_PREFIX + "enabled";
        String MIRROR_SERVICE_ENABLED_DEFAULT = "false";
        String FULL_MIRROR_SERVICE_ENABLED_PROP =
                SPACE_CONFIG_PREFIX + MIRROR_SERVICE_ENABLED_PROP;

        // Operation grouping tag values
        String MIRROR_SERVICE_OPERATION_GROUPING_TAG = MIRROR_SERVICE_PREFIX + "operation-grouping";
        String FULL_MIRROR_SERVICE_OPERATION_GROUPING_TAG = SPACE_CONFIG_PREFIX + MIRROR_SERVICE_OPERATION_GROUPING_TAG;
        String MIRROR_IMPORT_DIRECTORY_TAG = MIRROR_SERVICE_PREFIX + "import-directory";
        String FULL_MIRROR_IMPORT_DIRECTORY_TAG = SPACE_CONFIG_PREFIX + MIRROR_IMPORT_DIRECTORY_TAG;

        String GROUP_BY_SPACE_TRANSACTION_TAG_VALUE = "group-by-space-transaction";
        String GROUP_BY_REPLICATION_BULK_TAG_VALUE = "group-by-replication-bulk";
        String MIRROR_SERVICE_OPERATION_GROUPING_DEFAULT_VALUE = GROUP_BY_REPLICATION_BULK_TAG_VALUE;

        String MIRROR_SERVICE_CLUSTER_NAME = MIRROR_SERVICE_PREFIX + "cluster.name";
        String FULL_MIRROR_SERVICE_CLUSTER_NAME = SPACE_CONFIG_PREFIX + MIRROR_SERVICE_CLUSTER_NAME;

        String MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT = MIRROR_SERVICE_PREFIX + "cluster.partitions";
        String FULL_MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT = SPACE_CONFIG_PREFIX + MIRROR_SERVICE_CLUSTER_PARTITIONS_COUNT;

        String MIRROR_SERVICE_CLUSTER_BACKUPS_PER_PARTITION = MIRROR_SERVICE_PREFIX + "cluster.backups-per-partition";
        String FULL_MIRROR_SERVICE_CLUSTER_BACKUPS_PER_PARTITION = SPACE_CONFIG_PREFIX + MIRROR_SERVICE_CLUSTER_BACKUPS_PER_PARTITION;

        String MIRROR_DISTRIBUTED_TRANSACTION_PROCESSING_PARAMETERS = MIRROR_SERVICE_PREFIX + "distributed-transaction-processing.";
        String MIRROR_DISTRIBUTED_TRANSACTION_TIMEOUT = MIRROR_DISTRIBUTED_TRANSACTION_PROCESSING_PARAMETERS + "wait-timeout";
        String FULL_MIRROR_DISTRIBUTED_TRANSACTION_TIMEOUT = SPACE_CONFIG_PREFIX + MIRROR_DISTRIBUTED_TRANSACTION_TIMEOUT;
        String MIRROR_DISTRIBUTED_TRANSACTION_WAIT_FOR_OPERATIONS = MIRROR_DISTRIBUTED_TRANSACTION_PROCESSING_PARAMETERS + "wait-for-operations";
        String FULL_MIRROR_DISTRIBUTED_TRANSACTION_WAIT_FOR_OPERATIONS = SPACE_CONFIG_PREFIX + MIRROR_DISTRIBUTED_TRANSACTION_WAIT_FOR_OPERATIONS;
        String MIRROR_DISTRIBUTED_TRANSACTION_MONITOR_PENDING_OPERATIONS_MEMORY = MIRROR_DISTRIBUTED_TRANSACTION_PROCESSING_PARAMETERS + "monitor-pending-operations-memory";
    }

    public interface Space {
        String FULL_SPACE_STATE = SPACE_CONFIG_PREFIX + "space_state";
        String SPACE_CONFIG = "space-config";
    }

    public interface QueryProcessorInfo {
        String PREFIX = "QueryProcessor.";

        String QP_SPACE_READ_LEASE_TIME = "space_read_lease_time";
        String QP_SPACE_READ_LEASE_TIME_DEFAULT = String.valueOf(0);
        String QP_SPACE_READ_LEASE_TIME_PROP = PREFIX + QP_SPACE_READ_LEASE_TIME;
        String FULL_QP_SPACE_READ_LEASE_TIME_PROP = SPACE_CONFIG_PREFIX + QP_SPACE_READ_LEASE_TIME_PROP;


        String QP_SPACE_WRITE_LEASE = "space_write_lease";
        long lease = 9223372036854775807L;
        String QP_SPACE_WRITE_LEASE_DEFAULT = String.valueOf(lease);
        String QP_SPACE_WRITE_LEASE_PROP = PREFIX + QP_SPACE_WRITE_LEASE;
        String FULL_QP_SPACE_WRITE_LEASE_PROP = SPACE_CONFIG_PREFIX + QP_SPACE_WRITE_LEASE_PROP;


        String QP_TRANSACTION_TIMEOUT = "transaction_timeout";
        String QP_TRANSACTION_TIMEOUT_DEFAULT = String.valueOf(30000);
        String QP_TRANSACTION_TIMEOUT_PROP = PREFIX + QP_TRANSACTION_TIMEOUT;
        String FULL_QP_TRANSACTION_TIMEOUT_PROP = SPACE_CONFIG_PREFIX + QP_TRANSACTION_TIMEOUT_PROP;

        // JDBC Transaction Configuration
        String QP_TRANSACTION_TYPE = "gs.tx_manager_type";
        String QP_TRANSACTION_TYPE_DISTRIBUTED = TransactionManagerType.DISTRIBUTED.getNameInConfiguration();
        String QP_TRANSACTION_TYPE_DEFAULT = QP_TRANSACTION_TYPE_DISTRIBUTED;
        String QP_LOOKUP_TRANSACTION_NAME = "gs.lookup_tx.name";
        String QP_LOOKUP_TRANSACTION_NAME_DEFAULT = null;
        String QP_LOOKUP_TRANSACTION_TIMEOUT = "gs.lookup_tx.timeout";
        long QP_LOOKUP_TRANSACTION_TIMEOUT_DEFAULT = 3000;
        String QP_LOOKUP_TRANSACTION_GROUPS = "gs.lookup_tx.groups";
        String QP_LOOKUP_TRANSACTION_LOCATORS = "gs.lookup_tx.locators";
        String QP_LOOKUP_TRANSACTION_LOCATORS_DEFAULT = null;

        String QP_TRACE_EXEC_TIME = "trace_exec_time";
        String QP_TRACE_EXEC_TIME_DEFAULT = String.valueOf(false);
        String QP_TRACE_EXEC_TIME_PROP = PREFIX + QP_TRACE_EXEC_TIME;
        String FULL_QP_TRACE_EXEC_TIME_PROP = SPACE_CONFIG_PREFIX + QP_TRACE_EXEC_TIME_PROP;


        String QP_PARSER_CASE_SENSETIVITY = "parser_case_sensetivity";
        String QP_PARSER_CASE_SENSETIVITY_DEFAULT = String.valueOf(true);
        String QP_PARSER_CASE_SENSETIVITY_PROP = PREFIX + QP_PARSER_CASE_SENSETIVITY;
        String FULL_QP_PARSER_CASE_SENSETIVITY_PROP = SPACE_CONFIG_PREFIX + QP_PARSER_CASE_SENSETIVITY_PROP;

        String QP_AUTO_COMMIT = "auto_commit";
        String QP_AUTO_COMMIT_DEFAULT = String.valueOf(true);
        String QP_AUTO_COMMIT_PROP = PREFIX + QP_AUTO_COMMIT;
        String FULL_QP_AUTO_COMMIT_PROP = SPACE_CONFIG_PREFIX + QP_AUTO_COMMIT_PROP;

        String QP_DATE_FORMAT = "date_format";
        String QP_DATE_FORMAT_DEFAULT = "yyyy-MM-dd";
        String QP_DATE_FORMAT_PROP = PREFIX + QP_DATE_FORMAT;
        String FULL_QP_DATE_FORMAT_PROP = SPACE_CONFIG_PREFIX + QP_DATE_FORMAT_PROP;

        String QP_DATETIME_FORMAT = "datetime_format";
        String QP_DATETIME_FORMAT_DEFAULT = "yyyy-MM-dd hh:mm:ss";
        String QP_DATETIME_FORMAT_PROP = PREFIX + QP_DATETIME_FORMAT;
        String FULL_QP_DATETIME_FORMAT_PROP = SPACE_CONFIG_PREFIX + QP_DATETIME_FORMAT_PROP;

        String QP_TIME_FORMAT = "time_format";
        String QP_TIME_FORMAT_DEFAULT = "hh:mm:ss";
        String QP_TIME_FORMAT_PROP = PREFIX + QP_TIME_FORMAT;
        String FULL_QP_TIME_FORMAT_PROP = SPACE_CONFIG_PREFIX + QP_TIME_FORMAT_PROP;
    }

    public interface LookupManager {
        String MANUFACTURE = "GigaSpaces Technologies Ltd.";

        String VENDOR = "GigaSpaces";

        String PUBLIC = "public";// public

        // group
        // of
        // lookup
        // service

        String PUBLIC_GROUP = "";// public

        // lookup
        // group

        String NONE_GROUP = "none";// NO_GROUPS

        String ALL_GROUP = "all";// ALL_GROUPS

        String LOOKUP_ENABLED_DEFAULT = "true";

        String START_EMBEDDED_LOOKUP_DEFAULT = "true";

        String LOOKUP_ENABLED_PROP = Container.PREFIX
                + "directory_services.jini_lus.enabled";

        String START_EMBEDDED_LOOKUP_PROP = Container.PREFIX + "directory_services" +
                ".jini_lus.start-embedded-lus";

        String LOOKUP_GROUP_PROP = Container.PREFIX
                + "directory_services.jini_lus.groups";

        String LOOKUP_UNICAST_ENABLED_DEFAULT = "false";

        String LOOKUP_UNICAST_ENABLED_PROP = Container.PREFIX
                + "directory_services.jini_lus.unicast_discovery.enabled";

        String LOOKUP_UNICAST_URL_DEFAULT = "";

        String LOOKUP_UNICAST_URL_PROP = Container.PREFIX
                + "directory_services.jini_lus.unicast_discovery.lus_host";

        String LOOKUP_JNDI_ENABLED_DEFAULT = "false";

        String LOOKUP_JNDI_ENABLED_PROP = Container.PREFIX
                + "directory_services.jndi.enabled";

        String LOOKUP_JNDI_URL_DEFAULT = "localhost:10098";

        String LOOKUP_JNDI_URL_PROP = Container.PREFIX
                + "directory_services.jndi.url";

        String LOOKUP_JNDI_PUBLIC_URL_PROP = Container.PREFIX
                + "directory_services.jndi.public.url";

        String LOOKUP_JMS_ENABLED_DEFAULT = "false";

        String LOOKUP_JMS_ENABLED_PROP = Container.PREFIX
                + "directory_services.jms_services.enabled";

        String LOOKUP_JMS_INTERNAL_ENABLED_DEFAULT = "false";

        String LOOKUP_JMS_INTERNAL_ENABLED_PROP = Container.PREFIX
                + "directory_services.jms_services.internal-jndi.internal-jndi-enabled";

        //In case we bind the jms objects into an external jndi registry (such
        // as JBoss JNDI reg implementation) we use the default jndi.properties file
        //for the Context details of the external jndi service to be used.
        String LOOKUP_JMS_EXT_ENABLED_DEFAULT = "false";

        String LOOKUP_JMS_EXT_ENABLED_PROP = Container.PREFIX
                + "directory_services.jms_services.ext-jndi.ext-jndi-enabled";

        String LOOKUP_IS_PRIVATE_PROP = "isPrivate";
        String FULL_LOOKUP_IS_PRIVATE_PROP = SPACE_CONFIG_PREFIX + LOOKUP_IS_PRIVATE_PROP;
        String LOOKUP_IS_PRIVATE_DEFAULT = Boolean.FALSE.toString();
    }

    public interface Jms {
        String JMS_DELIMITER = ";";
        String JMS_CONFIG_FILE_NAME = "jms-config.xml";
        //e.g. "<GS-Root>/config/jms"
        String JMS_CONFIG_DIRECTORY = "config/jms/";

        /**
         * prop names for the jms container section in the xml file
         */
        String JMS_RMI_PORT_PROP = "jms.connections.rmi-port";
        String JMS_RMI_PORT_DEFAULT = "10098";

        String JMS_TOPIC_NAMES_PROP = "jms.administrated-destinations.topics.topic-names";
        String JMS_QUEUE_NAMES_PROP = "jms.administrated-destinations.queues.queue-names";

        String FULL_JMS_QUEUE_NAMES_PROP = SPACE_CONFIG_PREFIX + JMS_QUEUE_NAMES_PROP;
        String FULL_JMS_RMI_PORT_PROP = SPACE_CONFIG_PREFIX + JMS_RMI_PORT_PROP;
        String FULL_JMS_TOPIC_NAMES_PROP = SPACE_CONFIG_PREFIX + JMS_TOPIC_NAMES_PROP;

        //default queues/topics which are bonded to the jndi registry
        String JMS_QUEUE_NAMES_DEFAULT = "MyQueue,TempQueue";
        String JMS_TOPIC_NAMES_DEFAULT = "MyTopic,TempTopic";

        //jms names used in jndi looksup etc.
        String JMS_JMS_NAME = "jms";
        String JMS_DESTINATIONS_NAME = "destinations";
        String JMS_CON_FAC_NAME = "GSConnectionFactoryImpl";
        String JMS_TOPIC_CON_FAC_NAME = "GSTopicConnectionFactoryImpl";
        String JMS_XATOPIC_CON_FAC_NAME = "GSXATopicConnectionFactoryImpl";
        String JMS_QUEUE_CON_FAC_NAME = "GSQueueConnectionFactoryImpl";
        String JMS_XAQUEUE_CON_FAC_NAME = "GSXAQueueConnectionFactoryImpl";
    }

    public interface Management {
        String JMX_MBEAN_DESCRIPTORS_CONTAINER = "ContainerMBeanDescriptors.xml";
        String JMX_MBEAN_DESCRIPTORS_JAVASPACE = "JavaSpaceMBeanDescriptors.xml";
        String JMX_MBEAN_DESCRIPTORS_JAVASPACE_EXT = "JavaSpaceExtMBeanDescriptors.xml";
    }

    public interface Engine {
        /* start properties default values */
        int NOTIFIER_TIME_LIMIT = 2000;

        long UPDATE_NO_LEASE = 0;

        String ENGINE_MIN_THREADS_PROP = "engine.min_threads";
        String ENGINE_MIN_THREADS_DEFAULT = "1";
        String FULL_ENGINE_MIN_THREADS_PROP = SPACE_CONFIG_PREFIX + ENGINE_MIN_THREADS_PROP;

        String ENGINE_MAX_THREADS_PROP = "engine.max_threads";
        String ENGINE_MAX_THREADS_DEFAULT = "64";
        String FULL_ENGINE_MAX_THREADS_PROP = SPACE_CONFIG_PREFIX + ENGINE_MAX_THREADS_PROP;

        String ENGINE_NOTIFY_MIN_THREADS_DEFAULT = ENGINE_MIN_THREADS_DEFAULT;

        String ENGINE_NOTIFY_MAX_THREADS_DEFAULT = ENGINE_MAX_THREADS_DEFAULT;

        String ENGINE_NOTIFY_MIN_THREADS_PROP = "engine.notify_min_threads";

        String ENGINE_NOTIFY_MAX_THREADS_PROP = "engine.notify_max_threads";

        String ENGINE_THREADS_HIGHER_PRIORITY_PROP = "engine.threads_higher_priority";

        String ENGINE_DIRTY_READ_DEFAULT = "false";

        String ENGINE_DIRTY_READ_PROP = "engine.dirty_read";

        String ENGINE_LOCAL_CACHE_MODE_DEFAULT = "false";

        String ENGINE_LOCAL_CACHE_MODE_PROP = "engine.local_cache_mode";

        String ENGINE_MEMORY_USAGE_ENABLED_DEFAULT = "true";
        String ENGINE_MEMORY_USAGE_ENABLED_PRIMARY_ONLY = "primary-only";

        String ENGINE_MEMORY_USAGE_ENABLED_PROP = "engine.memory_usage.enabled";
        String FULL_ENGINE_MEMORY_USAGE_ENABLED_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_ENABLED_PROP;

        String ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT = "95";

        String ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP = "engine.memory_usage.high_watermark_percentage";
        String FULL_ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP;

        String ENGINE_MEMORY_USAGE_SYNCHRONUS_EVICTION_WATERMARK_DEFAULT = "0";

        String ENGINE_MEMORY_USAGE_SYNCHRONUS_EVICTION_WATERMARK_PROP = "engine.memory_usage.synchronous_eviction_watermark_percentage";

        String ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT = "85";

        String ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP = "engine.memory_usage.write_only_block_percentage";
        String FULL_ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP;

        String ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT = "76";

        String ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP = "engine.memory_usage.write_only_check_percentage";
        String FULL_ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP;

        String ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT = "75";

        String ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP = "engine.memory_usage.low_watermark_percentage";
        String FULL_ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP;

        String ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT = "500";

        String ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP = "engine.memory_usage.eviction_batch_size";
        String FULL_ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP;

        String ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT = "5";


        String ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP = "engine.memory_usage.retry_count";
        String FULL_ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP;

        String ENGINE_MEMORY_USAGE_RETRY_YIELD_DEFAULT = "50";
        String ENGINE_MEMORY_USAGE_RETRY_YIELD_PROP = "engine.memory_usage.retry_yield_time";

        String ENGINE_MEMORY_EXPLICIT_GC_DEFAULT = "false";

        String ENGINE_MEMORY_EXPLICIT_GC_PROP = "engine.memory_usage.explicit-gc";
        String FULL_ENGINE_MEMORY_EXPLICIT_GC_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_EXPLICIT_GC_PROP;

        String ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_DEFAULT = "true";

        String ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP = "engine.memory_usage.gc-before-shortage";
        String FULL_ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP = SPACE_CONFIG_PREFIX + ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP;

        String ENGINE_MEMORY_EXPLICIT_LEASE_REAPER_DEFAULT = "true";

        String ENGINE_MEMORY_EXPLICIT_LEASE_REAPER_PROP = "engine.memory_usage.explicit-lease-reaper";

        String ENGINE_SERIALIZATION_TYPE_DEFAULT = String.valueOf(StorageType.OBJECT.getCode());

        String ENGINE_SERIALIZATION_TYPE_PROP = "serialization-type";
        String FULL_ENGINE_SERIALIZATION_TYPE_PROP =
                SPACE_CONFIG_PREFIX + ENGINE_SERIALIZATION_TYPE_PROP;

        String ENGINE_NOTIFIER_RETRIES_DEFAULT = "3";

        String ENGINE_NOTIFIER_TTL_PROP = "notifier-retries";
        String FULL_ENGINE_NOTIFIER_TTL_PROP = SPACE_CONFIG_PREFIX + ENGINE_NOTIFIER_TTL_PROP;

        String ENGINE_REGULAR_EXPRESSIONS_CACHE_SIZE_PROP = "engine.extended-match.regular-expressions-cache-size";

        String ENGINE_REGULAR_EXPRESSIONS_CACHE_SIZE_DEFAULT = "300";

        String ENGINE_INSERT_SHORT_LEASE_RETRY_PROP = "engine.insert_short_lease_retry";

        String ENGINE_INSERT_SHORT_LEASE_RETRY_DEFAULT = "3";

        //fifo threads
        String ENGINE_FIFO_NOTIFY_THREADS_PROP = "engine.fifo_notify_threads";
        String ENGINE_FIFO_NOTIFY_THREADS_DEFAULT = "0";

        String ENGINE_FIFO_REGULAR_THREADS_PROP = "engine.fifo_regular_threads";
        String ENGINE_FIFO_REGULAR_THREADS_DEFAULT = "4";

        String ENGINE_NON_BLOCKING_READ_PROP = "engine.non_blocking_read";
        String ENGINE_NON_BLOCKING_READ_DEFAULT = "true";

        String ENGINE_CLEAN_UNUSED_EMBEDDED_GLOBAL_XTNS_PROP = "engine.clean_unused_embedded_global_xtns";
        String ENGINE_CLEAN_UNUSED_EMBEDDED_GLOBAL_XTNS_DEFAULT = "true";

        String ENGINE_QUERY_RESULT_SIZE_LIMIT = "engine.query.result.size.limit";
        String ENGINE_QUERY_RESULT_SIZE_LIMIT_DEFAULT = "0";

        String ENGINE_QUERY_RESULT_SIZE_LIMIT_MEMORY_CHECK_BATCH_SIZE = "engine.query.result.size.limit.memory.check.batch.size";
        String ENGINE_QUERY_RESULT_SIZE_LIMIT_MEMORY_CHECK_BATCH_SIZE_DEFAULT = "0";

    }

    public interface Replication {
        String GATEWAY_PROXY = "gateway-proxy";
    }

    public interface ReplicationFilter {
        /**
         * Allows to inject an actual instance of {@link com.j_spaces.core.cluster.ReplicationFilterProvider}
         * providing the ability to inject actual instnaces of both input and output replication
         * filters.
         */
        String REPLICATION_FILTER_PROVIDER = "replication-filter-provider";
    }

    public interface Filter {
        String FILTERS_TAG_NAME = "filters";
        String FILTER_NAMES_TAG_NAME = "filter-names";

        String FILTER_PROVIDERS = "filter-providers";

        String FILTER_NAMES_PROP = FILTERS_TAG_NAME + "." + FILTER_NAMES_TAG_NAME;

        String FILTER_ACTIVE_WHEN_BACKUP = "active-when-backup";

        String FILTER_SHUTDOWN_ON_INIT_FAILURE = "shutdown-space-on-init-failure";

        String FILTER_ENABLED_TAG_NAME = "enabled";

        String FILTER_CLASS_TAG_NAME = "class";
        String FILTER_OPERATION_CODE_TAG_NAME = "operation-code";
        String FILTER_URL_TAG_NAME = "url";
        String FILTER_PRIORITY_TAG_NAME = "priority";

        String FILTER_PASS_ENTRY = "pass-filter-entry";

        @Deprecated //TODO remove in next major release after 7.0
                String DEFAULT_FILTER_SECURITY_NAME = "DefaultSecurityFilter";
        String DEFAULT_ACTIVE_WHEN_BACKUP = String.valueOf(true);
        String DEFAULT_SHUTDOWN_ON_INIT_FAILURE = String.valueOf(false);
        String DEFAULT_FILTER_ENABLE_VALUE = String.valueOf(true);
        String DEFAULT_OPERATION_CODE_VALUE = "";
        String DEFAULT_PASS_ENTRY = String.valueOf(true);
    }

    public interface SqlFunction {
        String USER_SQL_FUNCTION = "user-sql-function";
    }

    public interface RemoteCode {
        String REMOTE_CODE = "remote-code";
        String REMOTE_CODE_MAX_CLASS_LOADERS = REMOTE_CODE + ".max-class-loaders";
        String REMOTE_CODE_MAX_CLASS_LOADERS_DEFAULT = "3";
        String FULL_REMOTE_CODE_MAX_CLASS_LOADERS_PROP = SPACE_CONFIG_PREFIX + REMOTE_CODE_MAX_CLASS_LOADERS;

        String REMOTE_CODE_SUPPORT_CODE_CHANGE = REMOTE_CODE + ".support.code.change";
        String REMOTE_CODE_SUPPORT_CODE_CHANGE_DEFAULT = "true";
        String FULL_REMOTE_CODE_SUPPORT_CODE_CHANGE_PROP = SPACE_CONFIG_PREFIX + REMOTE_CODE_SUPPORT_CODE_CHANGE;
    }

    public interface LeaseManager {
        long LM_EXPIRATION_TIME_INTERVAL_DEFAULT = 10 * 1000;    // 10 sec.

        long LM_BACKUP_EXPIRATION_DELAY_DEFAULT = 5 * 60 * 1000;    // 5 min.

        long LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT = 60 * 1000 * 5;    // 5 minutes

        long LM_EXPIRATION_TIME_PENDING_ANSWERS_DEFAULT = 60 * 1000;    // 1 minutes

        long LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT = 1000 * 60 * 3;    // 3 minutes

        long LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT = 1000 * 60 * 3; // 3 minutes

        long LM_EXPIRATION_TIME_RECENT_DELETES_CHECK_DEFAULT = 5 * 1000; // 5 seconds

        long LM_EXPIRATION_TIME_RECENT_UPDATES_CHECK_DEFAULT = 5 * 1000; // 5 seconds

        long LM_CHECK_TIME_MARKERS_REPOSITORY_DEFAULT = 1000 * 5; // 5 seconds

        long LM_EXPIRATION_TIME_FIFOENTRY_XTNINFO = 1000 * 60 * 1; // 1 minutes

        int LM_EXPIRATION_TIME_UNUSED_TXN_DEFAULT = 5000;

        int LM_SEGMEENTS_PER_EXPIRATION_CELL_DEFAULT = 2;

        boolean LM_DISABLE_ENTRIES_LEASES_DEFAULT = false;

        String LM_EXPIRATION_TIME_UNUSED_TXN_PROP = "lease_manager.expiration_unused_txns";

        String LM_DISABLE_ENTRIES_LEASES_PROP = "lease_manager.disable_entries_leases";

        String LM_EXPIRATION_TIME_STALE_REPLICAS_PROP = "lease_manager.expiration_stale_replicas";
        String FULL_LM_EXPIRATION_TIME_STALE_REPLICAS_PROP = SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_STALE_REPLICAS_PROP;

        String LM_EXPIRATION_TIME_RECENT_DELETES_PROP = "lease_manager.expiration_time_recent_deletes";
        String FULL_LM_EXPIRATION_TIME_RECENT_DELETES_PROP = SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_RECENT_DELETES_PROP;

        String LM_EXPIRATION_TIME_RECENT_UPDATES_PROP = "lease_manager.expiration_time_recent_updates";
        String FULL_LM_EXPIRATION_TIME_RECENT_UPDATES_PROP = SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_RECENT_UPDATES_PROP;

        String LM_EXPIRATION_TIME_INTERVAL_PROP = "lease_manager.expiration_time_interval";
        String FULL_LM_EXPIRATION_TIME_INTERVAL_PROP = SPACE_CONFIG_PREFIX + LM_EXPIRATION_TIME_INTERVAL_PROP;
        String LM_BACKUP_EXPIRATION_DELAY_PROP = "lease_manager.backup_leases_expiration_delay";
        String LM_SEGMEENTS_PER_EXPIRATION_CELL_PROP = "lease_manager.segments_per_expiration_cell";
    }

    public interface SystemTime {
        String SYSTEM_TIME_PROVIDER_PROP = "com.j_spaces.kernel.time-provider";
    }

    public interface LRMIStubHandler {

        String PROTOCOL_RMI = "RMI";
        String PROTOCOL_NIO = "NIO";

        //default values
        String LRMI_DEFAULT_STUB_HANDLER_CLASS = com.gigaspaces.internal.lrmi.stubs.LRMIStubHandlerImpl.class.getName();

        String LRMI_DEFAULT_PROTOCOL = PROTOCOL_NIO;
    }

    public interface StorageAdapter {
        String SPACE_PERSISTENT_PREFIX = "persistent.";

        String XARESOURCE_TIMEOUT = "xaresource_timeout";

        String PERSISTENT_ENABLED = "enabled";
        String PERSISTENT_ENABLED_PROP = SPACE_PERSISTENT_PREFIX + PERSISTENT_ENABLED;
        String PERSISTENT_ENABLED_DEFAULT = Boolean.FALSE.toString();
        String FULL_STORAGE_PERSISTENT_ENABLED_PROP = SPACE_CONFIG_PREFIX + PERSISTENT_ENABLED_PROP;
        String DIRECT_PERSISTENCY_LAST_PRIMARY_STATE_PATH_PROP = "com.gs.DirectPersistencyLastPrimaryStatePath";
    }

    public interface DataAdapter {
        String DATA_ADAPTER_PREFIX = "external-data-source.";

        String DATA_SOURCE = DATA_ADAPTER_PREFIX
                + "data-source";

        String SPACE_DATA_SOURCE = DATA_ADAPTER_PREFIX
                + "space-data-source";

        String SPACE_DATA_SOURCE_CLASS = DATA_ADAPTER_PREFIX
                + "space-data-source-class";

        String SPACE_SYNC_ENDPOINT = DATA_ADAPTER_PREFIX
                + "space-sync-endpoint";

        String SPACE_SYNC_ENDPOINT_CLASS = DATA_ADAPTER_PREFIX
                + "space-sync-endpoint-class";

        String DATA_SOURCE_CLASS_PROP = DATA_ADAPTER_PREFIX
                + "data-source-class";

        String DATA_SOURCE_CLASS_DEFAULT = "org.openspaces.persistency.hibernate.DefaultHibernateExternalDataSource";

        String DATA_CLASS_PROP = DATA_ADAPTER_PREFIX
                + "data-class";
        String DATA_CLASS_DEFAULT = Object.class.getName();

        String QUERY_BUILDER_PROP = DATA_ADAPTER_PREFIX
                + "query-builder-class";

        String QUERY_BUILDER_PROP_DEFAULT = DefaultSQLQueryBuilder.class.getName();

        String USAGE = DATA_ADAPTER_PREFIX
                + "usage";

        String USAGE_DEFAULT = "read-write";

        String SUPPORTS_INHERITANCE_PROP = DATA_ADAPTER_PREFIX
                + "supports-inheritance";
        String SUPPORTS_INHERITANCE_DEFAULT = "true";

        String SUPPORTS_VERSION_PROP = DATA_ADAPTER_PREFIX
                + "supports-version";
        String SUPPORTS_VERSION_DEFAULT = "false";
        String SUPPORTS_PARTIAL_UPDATE_PROP = DATA_ADAPTER_PREFIX
                + "supports-partial-update";
        String SUPPORTS_PARTIAL_UPDATE_DEFAULT = "false";
        String SUPPORTS_REMOVE_BY_ID_PROP = DATA_ADAPTER_PREFIX
                + "supports-remove-by-id";
        String SUPPORTS_REMOVE_BY_ID_DEFAULT = "false";

        // Data properties file name
        String DATA_PROPERTIES = DATA_ADAPTER_PREFIX
                + "init-properties-file";

        String DATA_PROPERTIES_DEFAULT = "";

        String SHARED_ITERATOR_PREFIX = DATA_ADAPTER_PREFIX
                + "shared-iterator.";

        // Use share iterator implementation
        String DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP = SHARED_ITERATOR_PREFIX
                + "enabled";

        String DATA_SOURCE_SHARE_ITERATOR_ENABLED_DEFAULT = "true";

        String DATA_SOURCE_SHARE_ITERATOR_TTL_PROP = SHARED_ITERATOR_PREFIX
                + "time-to-live";
    }

    public interface CacheManager {
        //cache policy types
        int CACHE_POLICY_LRU = 0;
        int CACHE_POLICY_ALL_IN_CACHE = 1;
        int CACHE_POLICY_PLUGGED_EVICTION = 2;
        int CACHE_POLICY_BLOB_STORE = 3;

        int MAX_CACHE_POLICY_VALUE = CACHE_POLICY_BLOB_STORE;

        String CACHE_MANAGER_SIZE_DEFAULT = "100000";

        String CACHE_MANAGER_SIZE_PROP = "engine.cache_size";
        String FULL_CACHE_MANAGER_SIZE_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_SIZE_PROP;

        String CACHE_POLICY_PROP = "engine.cache_policy";
        String FULL_CACHE_POLICY_PROP = SPACE_CONFIG_PREFIX + CACHE_POLICY_PROP;

        String CACHE_MANAGER_EVICTION_STRATEGY_CLASS_PROP = "engine.eviction_strategy";
        String CACHE_MANAGER_EVICTION_STRATEGY_PROP = "engine.eviction_strategy_instance";

        String CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_CLASS_PROP = "engine.blobstore_storage_handler";

        String CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_PROP = "engine.blobstore_storage_handler_instance";

        String BLOBSTORE_ENABLE_UNSAFE_ENDPOINT_PROP = "blobstore.enable_unsafe_sync_endpoint";
        String FULL_BLOBSTORE_ENABLE_UNSAFE_ENDPOINT_PROP = SPACE_CONFIG_PREFIX + BLOBSTORE_ENABLE_UNSAFE_ENDPOINT_PROP;

        String CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP = "engine.blobstore_persistent";
        String FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP;

        String CACHE_MANAGER_BLOBSTORE_CACHE_FILTER_QUERIES_PROP = "engine.blobstore_cache_filter_queries";
        String FULL_CACHE_MANAGER_BLOBSTORE_CACHE_FILTER_QUERIES_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_BLOBSTORE_CACHE_FILTER_QUERIES_PROP;

        String CACHE_MANAGER_USE_BLOBSTORE_BULKS_PROP = "engine.blobstore_use_bulks";
        String FULL_CACHE_MANAGER_USE_BLOBSTORE_BULKS_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_USE_BLOBSTORE_BULKS_PROP;

        String CACHE_MANAGER_USE_BLOBSTORE_CLEAR_OPTIMIZATION_PROP = "engine.blobstore_use_clear_optimization";
        String FULL_CACHE_MANAGER_USE_BLOBSTORE_CLEAR_OPTIMIZATION_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_USE_BLOBSTORE_CLEAR_OPTIMIZATION_PROP;

        String CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP = "engine.blobstore_cache_size";
        String FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP;

        String CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT = "10000";

        String CACHE_MANAGER_BLOBSTORE_PREFETCH_MIN_THREADS_PROP = "engine.blobstore_prefetch_min_threads";
        String FULL_CACHE_MANAGER_BLOBSTORE_PREFETCH_MIN_THREADS_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_BLOBSTORE_PREFETCH_MIN_THREADS_PROP;
        int CACHE_MANAGER_BLOBSTORE_PREFETCH_MIN_THREADS_DEFAULT = 0;

        String CACHE_MANAGER_BLOBSTORE_PREFETCH_MAX_THREADS_PROP = "engine.blobstore_prefetch_max_threads";
        String FULL_CACHE_MANAGER_BLOBSTORE_PREFETCH_MAX_THREADS_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_BLOBSTORE_PREFETCH_MAX_THREADS_PROP;
        int CACHE_MANAGER_BLOBSTORE_PREFETCH_MAX_THREADS_DEFAULT = 16;

        String CACHE_MANAGER_BOLBSTORE_USE_PREFETCH_PROP = "engine.blobstore_prefetch";
        String FULL_CACHE_MANAGER_BOLBSTORE_USE_PREFETCH_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_BOLBSTORE_USE_PREFETCH_PROP;

        String CACHE_MANAGER_BLOBSTORE_DEVICES_PROP = "blobStoreDevices";
        String CACHE_MANAGER_BLOBSTORE_CAPACITY_GB_PROP = "blobStoreCapacityGB";
        String CACHE_MANAGER_BLOBSTORE_CAPACITY_GB_DEFAULT = "200";
        String CACHE_MANAGER_BLOBSTORE_CACHE_CAPACITY_MB_PROP = "blobStoreCacheSizeMB";
        String CACHE_MANAGER_BLOBSTORE_CACHE_CAPACITY_MB_DEFAULT = "100";
        String CACHE_MANAGER_BLOBSTORE_CACHE_DURABILITY_LEVEL_PROP = "blobStoreDurabilityLevel";
        String CACHE_MANAGER_BLOBSTORE_VOLUME_DIR_PROP = "blobStoreVolumeDir";

        String BLOBSTORE_MEMORY_USAGE_WRITE_ONLY_BLOCK_PRECENTAGE_DEFAULT = "80";

        String BLOBSTORE_OFF_HEAP_MIN_DIFF_TO_ALLOCATE_PROP = "blobstore.off-heap.update_threshold";
        String BLOBSTORE_OFF_HEAP_MIN_DIFF_TO_ALLOCATE_DEFAULT_VALUE = "50B";

        /* initial % from LRU-cache to load */
        String CACHE_MANAGER_INITIAL_LOAD_DEFAULT = "50";

        String CACHE_MANAGER_INITIAL_LOAD_PROP = "engine.initial_load";
        String FULL_CACHE_MANAGER_INITIAL_LOAD_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_INITIAL_LOAD_PROP;

        String CACHE_MANAGER_INITIAL_LOAD_CLASS_PROP = "engine.initial_load_class";

        String CACHE_MANAGER_MIN_EXTENDED_INDEX_ACTIVATION_DEFAULT = "1";

        String CACHE_MANAGER_MIN_EXTENDED_INDEX_ACTIVATION_PROP = "engine.extended-match.min_ext_index_activation_size";

        int PERSISTENT_GC_INTERVAL_DEFAULT = 5 * 60 * 1000;

        String PERSISTENT_GC_INTERVAL_PROP = LeaseManager.LM_EXPIRATION_TIME_INTERVAL_PROP;

        /**
         * when the actual # of objects in the lru is small, we can save the overhead of touch.
         * (supported only for ConcurrentLruEvictionStrategy) this is the percentage under which to
         * touching is performed. 0 means always touch, 100 means no touch at all
         */
        String CACHE_MANAGER_LRU_TOUCH_THRESHOLD_PROP = "engine.lruTouchThreshold";

        /**
         * when the actual # of objects in the LRU is small, we can save the overhead of touch.
         * (supported only for ConcurrentLruEvictionStrategy) this is the percentage under which to
         * touching is performed. 0 means always touch, 100 means no touch at all
         */
        String CACHE_MANAGER_LRU_TOUCH_THRESHOLD_DEFAULT = "50";
        /**
         * size of evictable locks table
         */
        String CACHE_MANAGER_EVICTABLE_LOCKS_SIZE_PROP = "engine.EvictableLocksSize";

        /**
         * default size of evictable locks table
         */
        String CACHE_MANAGER_EVICTABLE_LOCKS_SIZE_DEFAULT = "2000";

        String CACHE_MANAGER_USE_ECONOMY_HASHMAP_PROP = "engine.use_economy_hashmap";

        String CACHE_MANAGER_USE_ECONOMY_HASHMAP_DEFAULT = "false";

        String CACHE_MANAGER_PARTIAL_UPDATE_REPLICATION_PROP = "engine.partial_update_replication";

        String CACHE_MANAGER_PARTIAL_UPDATE_REPLICATION_DEFAULT = "true";

        int CACHE_MANAGER_TIMEBASED_EVICTION_MEMORY_TIME_DEFAULT = 24 * 60 * 60 * 1000;
        String CACHE_MANAGER_TIMEBASED_EVICTION_MEMORY_TIME_PROP = "engine.time_based_eviction_memory_time";

        int CACHE_MANAGER_TIMEBASED_EVICTION_REINSERTED_MEMORY_TIME_DEFAULT = 0;
        String CACHE_MANAGER_TIMEBASED_EVICTION_REINSERTED_MEMORY_TIME_PROP = "engine.time_based_eviction_reinserted_memory_time";

        int CACHE_MANAGER_SYNC_LIST_THREAD_INTERVAL_TIME_DEFAULT = 500;
        String CACHE_MANAGER_SYNC_LIST_THREAD_INTERVAL_TIME_PROP = "engine.sync_list_thread_interval_time";

        int CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_DEFAULT = 100000;
        String CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_PROP = "engine.sync_list_max_size_in_memory";
        String FULL_CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_PROP = SPACE_CONFIG_PREFIX + CACHE_MANAGER_SYNC_LIST_MAX_SIZE_IN_MEMORY_PROP;

        String CACHE_MANAGER_FORCE_ID_INDEX_PROP = "engine.cache.force-space-id-index-if-equal";
    }

    public interface DCache {
        String SPACE_SUFFIX = "_DCache";

        String FILE_SUFFIX_EXTENTION = SPACE_SUFFIX + ".xml";

        String DCACHE_CONFIG_NAME_DEFAULT = "DefaultConfig";
        String DCACHE_CONFIG_FILE_DEFAULT = DCACHE_CONFIG_NAME_DEFAULT + FILE_SUFFIX_EXTENTION;

        //String DCACHE_TEMPLATE_FILE_NAME_DEFAULT = "/dcache-space-template.xml";

        String DIST_CACHE_PREFIX = "dist-cache.";

        //String CONFIG_FILE_URL_PROP               = DIST_CACHE_PREFIX + "config-url";

        String CONFIG_NAME_PROP = DIST_CACHE_PREFIX + "config-name";
        String FULL_DCACHE_CONFIG_NAME_PROP = SPACE_CONFIG_PREFIX + CONFIG_NAME_PROP;

        String UPDATE_MODE_PROP = DIST_CACHE_PREFIX + "update-mode";
        String UPDATE_MODE_DEFUALT = Integer.toString(SpaceURL.UPDATE_MODE_PULL);

        String STORAGE_TYPE_PROP = DIST_CACHE_PREFIX + "storage-type";
        String STORAGE_TYPE_DEFAULT = "reference";

        String VERSIONED_PROP = DIST_CACHE_PREFIX + "versioned";
        String VERSIONED_DEFUALT = "false";

        String MAX_OBJECT_TIMEOUT_PROP = DIST_CACHE_PREFIX + "max-object-timeout";

        String RETRY_CONNECTIONS_PROP = DIST_CACHE_PREFIX + "retry-connections";

        String DELAY_BETWEEN_RETRIES_PROP = DIST_CACHE_PREFIX + "delay-between-retries";

        String EVICTION_STRATEGY_PROP = DIST_CACHE_PREFIX + "eviction-strategy";
        String EVICTION_STRATEGY_DEFUALT = "com.j_spaces.map.eviction.FIFOEvictionStrategy";

        String PUT_FIRST_PROP = DIST_CACHE_PREFIX + "put-first";
        String PUT_FIRST_DEFUALT = "true";

        String COMPRESSION_PROP = DIST_CACHE_PREFIX + "compression";
        String COMPRESSION_DEFUALT = "0"; // 0 = NONE
    }

    public interface Cluster {
        // system property
        String IS_CLUSTER_SPACE_PROP = "cluster.enabled";
        String FULL_IS_CLUSTER_SPACE_PROP = SPACE_CONFIG_PREFIX + IS_CLUSTER_SPACE_PROP;
        String IS_CLUSTER_SPACE_DEFAULT = Boolean.FALSE.toString();

        String CLUSTER_CONFIG_URL_PROP = "cluster.config-url";
        String FULL_CLUSTER_CONFIG_URL_PROP = SPACE_CONFIG_PREFIX + CLUSTER_CONFIG_URL_PROP;
        String CLUSTER_CONFIG_URL_DEFAULT = "none";
    }

    public interface WorkerManager {
        String WORKER_PREFIX = "workers.";
        String IS_INTERRUPT_THREADS_ON_SHUTDOWN = WORKER_PREFIX + "interrupt";
        String WORKER_NAMES_PROP = WORKER_PREFIX + "worker-names";
        String WORKER_ENABLED = "enabled";
        String WORKER_ACTIVE_WHEN_BACKUP = "active-when-backup";
        String WORKER_CLASSNAME = "class-name";
        String WORKER_SHUTDOWN_ON_INIT_FAILURE = "shutdown-space-on-init-failure";
        String WORKER_INSTANCES = "instances";
        String WORKER_ARG = "arg";
        String WORKER_DESCRIPTION = "description";

        // default values
        String DEFAULT_WORKER_ENABLED = "true";
        String DEFAULT_WORKER_ACTIVE_WHEN_BACKUP = "true";
        String DEFAULT_WORKER_SHUTDOWN_ON_INIT_FAILURE = "false";
        String DEFAULT_WORKER_INSTANCES = "1";
    }

    public interface Statistics {
        String STATISTICS_FILTER_NAME = "Statistics";

        long DEFAULT_PERIOD = 10000;
    }

    public interface SpaceProxy {
        String PREFIX = SPACE_CONFIG_PREFIX + "proxy.";

        public interface Events {
            String PREFIX = SpaceProxy.PREFIX + "events.";

            String REGISTRATION_DURATION = PREFIX + "registration-duration";
            long REGISTRATION_DURATION_DEFAULT = 30 * 1000;
            String REGISTRATION_RENEW_INTERVAL = PREFIX + "registration-renew-interval";
            long REGISTRATION_RENEW_INTERVAL_DEFAULT = 10 * 1000;
        }

        public interface Router {
            String PREFIX = SpaceProxy.PREFIX + "router.";
            String ACTIVE_SERVER_LOOKUP_TIMEOUT = PREFIX + "active-server-lookup-timeout";
            long ACTIVE_SERVER_LOOKUP_TIMEOUT_DEFAULT = 20000;
            String ACTIVE_SERVER_LOOKUP_SAMPLING_INTERVAL = PREFIX + "active-server-lookup-sampling-interval";
            long ACTIVE_SERVER_LOOKUP_SAMPLING_INTERVAL_DEFAULT = 100;
            String THREAD_POOL_SIZE = PREFIX + "threadpool-size";
            String LOAD_BALANCER_TYPE = PREFIX + "load-balancer-type";
        }

        public interface OldRouter {
            String PREFIX = "proxy-settings.";

            String RETRY_CONNECTION = "retries";
            String RETRY_CONNECTION_DEFAULT = "10";
            String RETRY_CONNECTION_FULL = SPACE_CONFIG_PREFIX + RETRY_CONNECTION;

            String CONNECTION_MONITOR = PREFIX + "connection-monitor";
            String CONNECTION_MONITOR_DEFAULT = "all";
            String CONNECTION_MONITOR_FULL = SPACE_CONFIG_PREFIX + CONNECTION_MONITOR;

            String MONITOR_FREQUENCY = PREFIX + "ping-frequency";
            String MONITOR_FREQUENCY_DEFAULT = java.lang.System.getProperty(
                    SystemProperties.LIVENESS_MONITOR_FREQUENCY,
                    String.valueOf(SystemProperties.LIVENESS_MONITOR_FREQUENCY_DEFAULT));
            String MONITOR_FREQUENCY_FULL = SPACE_CONFIG_PREFIX + MONITOR_FREQUENCY;

            String DETECTOR_FREQUENCY = PREFIX + "lookup-frequency";
            String DETECTOR_FREQUENCY_DEFAULT = java.lang.System.getProperty(SystemProperties.LIVENESS_DETECTOR_FREQUENCY,
                    String.valueOf(SystemProperties.LIVENESS_DETECTOR_FREQUENCY_DEFAULT));
            String DETECTOR_FREQUENCY_FULL = SPACE_CONFIG_PREFIX + DETECTOR_FREQUENCY;

            // The number of retries to reconnect with the space/spaces in case that all of the relevant targets are  not available
            String CONNECTION_RETRIES = PREFIX + "connection-retries";
            String CONNECTION_RETRIES_DEFAULT = "10";
            String CONNECTION_RETRIES_FULL = SPACE_CONFIG_PREFIX + CONNECTION_RETRIES;
        }
    }

    public interface Security {
        String PREFIX = "security.";
        String USER_DETAILS = PREFIX + "userDetails";
        String CREDENTIALS_PROVIDER = PREFIX + "credentials-provider";
        String USERNAME = PREFIX + "username";
        String PASSWORD = PREFIX + "password";
    }

    public interface DirectPersistency {
        String DIRECT_PERSISTENCY_ATTRIBURE_STORE_PROP = "directPersistency.attribute-store";
        String FULL_DIRECT_PERSISTENCY_ATTRIBURE_STORE_PROP = SPACE_CONFIG_PREFIX + DIRECT_PERSISTENCY_ATTRIBURE_STORE_PROP;

        public interface ZOOKEEPER {
            String ATTRIBUET_STORE_HANDLER_CLASS_NAME = "org.openspaces.zookeeper.attribute_store.ZooKeeperAttributeStore";
        }
    }

    public interface LeaderSelector {
        String LEADER_SELECTOR_HANDLER_CLASS_NAME = "org.openspaces.zookeeper.leader_selector.ZooKeeperBasedLeaderSelectorHandler";

        String PREFIX = "leader-election.zookeeper.";
        String SESSION_TIMEOUT = PREFIX + "session-timeout";
        String SESSION_TIMEOUT_DEFAULT = "8000";
        String FULL_LEADER_SELECTOR_SESSION_TIMEOUT = SPACE_CONFIG_PREFIX + SESSION_TIMEOUT;
        String CONNECTION_TIMEOUT = PREFIX + "connection-timeout";
        String CONNECTION_TIMEOUT_DEFAULT = "5000";
        String FULL_LEADER_SELECTOR_CONNECTION_TIMEOUT = SPACE_CONFIG_PREFIX + CONNECTION_TIMEOUT;
        String RETRY_TIMEOUT = PREFIX + "retry-timeout";
        String RETRY_TIMEOUT_DEFAULT = String.valueOf(Integer.MAX_VALUE);
        String FULL_LEADER_SELECTOR_RETRY_TIMEOUT = SPACE_CONFIG_PREFIX + RETRY_TIMEOUT;
        String RETRY_INTERVAL = PREFIX + "retry-interval";
        String RETRY_INTERVAL_DEFAULT = "100";
        String FULL_LEADER_SELECTOR_RETRY_INTERVAL = SPACE_CONFIG_PREFIX + RETRY_INTERVAL;

    }

}
