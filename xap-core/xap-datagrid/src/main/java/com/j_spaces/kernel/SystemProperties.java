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


package com.j_spaces.kernel;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.client.iterator.SpaceIteratorType;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.Constants;
import com.j_spaces.core.client.UpdateModifiers;

import java.util.HashMap;
import java.util.Map;

/**
 * This Class includes system properties used by the product.
 */

public class SystemProperties extends CommonSystemProperties {
    /* One way */
    /**
     * @deprecated Use {@link UpdateModifiers#NO_RETURN_VALUE} instead
     */
    @Deprecated
    public final static String ONE_WAY_WRITE = "com.gs.onewaywrite";
    /**
     * @deprecated Use {@link UpdateModifiers#NO_RETURN_VALUE} instead
     */
    @Deprecated
    public final static String ONE_WAY_UPDATE = "com.gs.onewayupdate";
    /**
     * @deprecated
     */
    @Deprecated
    public final static String ONE_WAY_CLEAR = "com.gs.onewayclear";
    /**
     * @deprecated
     */
    @Deprecated
    public final static String ONE_WAY_NOTIFY = "com.gs.onewaynotify";

    /* Security */
    /**
     * The system property key identifying a secured service; default false
     */
    public final static String SECURITY_ENABLED = "com.gs.security.enabled";

    /**
     * The system property key identifying the security properties file location
     */
    public static final String SECURITY_PROPERTIES_FILE = "com.gs.security.properties-file";

    public static final String SECURITY_DISABLE_TRANSACTION_AUTHENTICATION = "com.gs.security.disable-commit-abort-authentication";

    /* JINI */
    public final static String JINI_LUS_GROUPS = SystemInfo.LOOKUP_GROUPS_SYS_PROP;

    public final static String JINI_LUS_LOCATORS = SystemInfo.LOOKUP_LOCATORS_SYS_PROP;
    public final static String JINI_LUS_LOCATORS_DEFAULT = "";

    /* XML */
    public final static String CLUSTER_XML_DEBUG = "com.gs.clusterXML.debug";

    public final static String URL = "com.gs.url";

    /**
     * Determines the FIFO notify queue size.
     */
    public final static String NOTIFY_FIFO_QUEUE = "com.gs.fifo_notify.queue";

    /**
     * if true, when JMS clients use transacted sessions the JMS transactions will use the Mahalo
     * Jini transaction manager, which expects the manager to be started.
     */
    public final static String JMS_USE_MAHALO_PROP = "com.gs.jms.use_mahalo";

    /**
     * When set to false, the XAResource will not throw an error when a non existing or already
     * rolled back transaction is rolled back.
     *
     * @see javax.transaction.xa.XAResource#rollback(javax.transaction.xa.Xid) Default: true
     */
    public final static String FAIL_ON_INVALID_ROLLBACK = "com.gs.xa.failOnInvalidRollback";

    /**
     * if -Dcom.gs.XMLEnvCheck=true we flush info about the Jaxp environment and print it to a file
     * called GS_JAXP_EnvironmentCheck.xml in current directory.
     */
    public final static String JAXP_ENV_DEBUG_REPORT = "com.gs.XMLEnvCheck";

    /**
     * If true it will register the jms administrated objects in the rmi registry
     **/
    public final static String JMS_LOOKUP_ENABLED = "com.gs.jms.enabled";

    public final static String JMS_FACTORY = "com.gs.jms.factory-class";

    /**
     * When it is necessary to ensure that DGC clean calls for unreachable remote references are
     * delivered in a timely fashion, the value of this property represents the maximum interval (in
     * milliseconds) that the RMI runtime will allow between garbage collections of the local heap.
     * The default value of 60000 milliseconds (60 seconds) will be set by GS to 1 hour (36000000
     * milliseconds) if it was not been set.
     */
    public final static String RMI_CLIENT_GC_INTERVAL = "sun.rmi.dgc.client.gcInterval";

    /**
     * When it is necessary to ensure that unreachable remote objects are unexported and garbage
     * collected in a timely fashion, the value of this property represents the maximum interval (in
     * milliseconds) that the RMI runtime will allow between garbage collections of the local heap.
     * The default value of 60000 milliseconds (60 seconds) will be set by GS to 1 hour (36000000
     * milliseconds) if it was not been set.
     */
    public final static String RMI_SERVER_GC_INTERVAL = "sun.rmi.dgc.server.gcInterval";
    public final static String RMI_GC_DEFAULT_INTERVAL = "36000000";

    /**
     * Watchdog parameters
     */
    public final static String WATCHDOG_REQUEST_TIMEOUT = "com.gs.transport_protocol.lrmi.request_timeout";
    public final static String WATCHDOG_LISTENING_TIMEOUT = "com.gs.transport_protocol.lrmi.listening_timeout";
    public final static String WATCHDOG_IDLE_CONNECTION_TIMEOUT = "com.gs.transport_protocol.lrmi.idle_connection_timeout";
    public final static String WATCHDOG_INSPECT_TIMEOUT = "com.gs.transport_protocol.lrmi.inspect_timeout";
    public final static String WATCHDOG_INSPECT_RESPONSE_TIMEOUT = "com.gs.transport_protocol.lrmi.inspect_response_timeout";
    public final static String WATCHDOG_DISABLE_RESPONSE_WATCH = "com.gs.transport_protocol.lrmi.response_watch.disable";
    // Resolution in percents
    public final static String WATCHDOG_TIMEOUT_RESOLUTION = "com.gs.transport_protocol.lrmi.timeout_resolution";
    /**
     * Watchdog parameters default values
     */
    // Default timeout in seconds
    public final static String WATCHDOG_LISTENING_TIMEOUT_DEFAULT = "5m";
    public final static String WATCHDOG_REQUEST_TIMEOUT_DEFAULT = "30s";
    public final static String WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT = "15m";
    // Resolution in percents
    public final static String WATCHDOG_TIMEOUT_RESOLUTION_DEFAULT = "10";

    public final static long LRMI_THREADPOOL_IDLE_TIMEOUT_DEFAULT = 5 * 60 * 1000; // 5 min

    /**
     * LRMI number of read selector threads. @see #LRMI_READ_SELECTOR_THREADS_DEFAULT
     */
    public final static String LRMI_READ_SELECTOR_THREADS = "com.gs.transport_protocol.lrmi.selector.threads";
    /**
     * LRMI number of read selector threads default = 4. @see #LRMI_READ_SELECTOR_THREADS
     */
    public final static int LRMI_READ_SELECTOR_THREADS_DEFAULT = 4;

    public static final int LRMI_SYSTEM_PRIORITY_QUEUE_CAPACITY_DEFAULT = Integer.MAX_VALUE;

    public static final int LRMI_SYSTEM_PRIORITY_THREAD_IDLE_TIMEOUT = 60000;

    public static final int LRMI_SYSTEM_PRIORITY_MIN_THREADS_DEFAULT = 1;

    public static final int LRMI_SYSTEM_PRIORITY_MAX_THREADS_DEFAULT = 8;

    public static final int LRMI_CUSTOM_QUEUE_CAPACITY_DEFAULT = Integer.MAX_VALUE;

    public static final int LRMI_CUSTOM_THREAD_IDLE_TIMEOUT = 300000;

    public static final int LRMI_CUSTOM_MIN_THREADS_DEFAULT = 1;

    public static final int LRMI_CUSTOM_MAX_THREADS_DEFAULT = 128;


    public final static String LRMI_USE_SECURE_RADNDOM = "com.gs.transport_protocol.lrme.use_secure_random";
    public final static String LRMI_USE_ASYNC_CONNECT = "com.gs.transport_protocol.lrmi.use_async_connect";

    /**
     * Provide a custom network mapper (Full class name).
     */
    public final static String LRMI_NETWORK_MAPPER = "com.gs.transport_protocol.lrmi.network-mapper";

    /**
     * The location of the mapping file which is used by the default network mapper
     */
    public final static String LRMI_NETWORK_MAPPING_FILE = "com.gs.transport_protocol.lrmi.network-mapping-file";

    /**
     * The timeout on lrmi socket connect
     *
     * @see #LRMI_CONNECT_TIMEOUT_DEFAULT
     */
    public final static String LRMI_CONNECT_TIMEOUT = "com.gs.transport_protocol.lrmi.connect_timeout";
    /**
     * Default connect timeout in seconds is 30 sec
     *
     * @see #LRMI_CONNECT_TIMEOUT
     */
    public final static String LRMI_CONNECT_TIMEOUT_DEFAULT = "30s";

    /**
     * read() call on Socket will block for only this amount of time (in milliseconds),
     * or indefinite (timeout=0) by default.
     * @since 15.5
     */
    public static final String LRMI_READ_BLOCK_TIMEOUT = "com.gs.transport_protocol.lrmi.read-block-timeout";

    /**
     * Sets the maximum queue length for incoming connection indications.
     */
    public final static String LRMI_ACCEPT_BACKLOG = "com.gs.transport_protocol.lrmi.accpet-backlog";

    /**
     * Set the TCP Send Buffer size (SO_SNDBUF)
     */
    public final static String LRMI_TCP_SEND_BUFFER = "com.gs.transport_protocol.lrmi.tcp-send-buffer-size";

    /**
     * Set the TCP receive Buffer size (SO_RCVBUF)
     */
    public final static String LRMI_TCP_RECEIVE_BUFFER = "com.gs.transport_protocol.lrmi.tcp-receive-buffer-size";

    /**
     * Set the maximum used buffer size that may be cached for communication
     */
    public final static String LRMI_MAX_CACHED_BUFFER_SIZE = "com.gs.transport_protocol.lrmi.cache.buffer-size";

    /**
     * Default value for {@link #LRMI_MAX_CACHED_BUFFER_SIZE}
     */
    public final static int LRMI_MAX_CACHED_BUFFER_SIZE_DEFAULT = 50 * 1024 * 1024; //50megabytes

    /**
     * Set the ratio that if the currently used buffer size * current cached buffer size is below,
     * the cached buffer expunge threshold counter will be increases
     */
    public final static String LRMI_CACHED_BUFFER_EXPUNGE_RATIO = "com.gs.transport_protocol.lrmi.cache.buffer-expunge-ratio";

    /**
     * Default value for {@link #LRMI_CACHED_BUFFER_EXPUNGE_RATIO}
     */
    public final static double LRMI_CACHED_BUFFER_EXPUNGE_RATIO_DEFAULT = 0.5;
    /**
     * Set the number of time the cached buffer has failed the expunge ratio test after which the
     * cached buffer is expunged.
     */
    public final static String LRMI_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD = "com.gs.transport_protocol.lrmi.cache.buffer-expunge-times-threshold";

    /**
     * Default value for {@link #LRMI_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD}
     */
    public final static int LRMI_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD_DEFAULT = 20;

    /**
     * The default size to maximum queue length for incoming connection indications.
     */
    public final static int LRMI_ACCEPT_BACKLOG_DEFUALT = 1024;

    /**
     * Set the TCP keep alive mode (SO_KEEPALIVE)
     */
    public final static String LRMI_TCP_KEEP_ALIVE = "com.gs.transport_protocol.lrmi.tcp-keep-alive";

    /**
     * Set the TCP no delay mode (TCP_NODELAY)
     */
    public final static String LRMI_TCP_NO_DELAY = "com.gs.transport_protocol.lrmi.tcp-no-delay";

    /**
     * TCP keep alive mode default (true)
     *
     * @see #LRMI_TCP_KEEP_ALIVE
     */
    public final static boolean LRMI_TCP_KEEP_ALIVE_DEFAULT = true;
    /**
     * TCP no delay mode default (true)
     *
     * @see #LRMI_TCP_NO_DELAY
     */
    public final static boolean LRMI_TCP_NO_DELAY_DEFAULT = true;

    /**
     * Set the TCP traffic class (SO_TRAFFIC_CLASS)
     */
    public final static String LRMI_TCP_TRAFFIC_CLASS = "com.gs.transport_protocol.lrmi.tcp-traffic-class";

    /**
     * Set to true in order to enable simple request/response remote class loading for all purposes, set to false to disable
     * it. Defaults to false
     */
    public final static String LRMI_SIMPLE_CLASSLOADING = "com.gs.transport_protocol.lrmi.simple-classloading";
    public final static String LRMI_SIMPLE_CLASSLOADING_DEFAULT = "false";

    /**
     * Set the space iterator type, allowed values: CURSOR, PREFETCH_UIDS
     * it. Defaults to CURSOR
     */
    public final static String SPACE_ITERATOR_TYPE = "com.gs.iterator.type";
    public final static String SPACE_ITERATOR_TYPE_DEFAULT = SpaceIteratorType.CURSOR.toString();

    /**
     * Set to true in order to enable lrmi class loading for all purposes, set to false to disable
     * it. Defaults to true
     */
    public final static String LRMI_CLASSLOADING = "com.gs.transport_protocol.lrmi.classloading";
    public final static String LRMI_CLASSLOADING_DEFAULT = "true";

    /**
     * Set to true in order to enable lrmi class loading for importing classes, set to false to
     * disable it. Defaults to true
     */
    public final static String LRMI_CLASSLOADING_IMPORT = LRMI_CLASSLOADING + ".import";
    public final static String LRMI_CLASSLOADING_IMPORT_DEFAULT = "true";

    /**
     * Set to true in order to enable lrmi class loading for exporting classes, set to false to
     * disable it. Defaults to true
     */
    public final static String LRMI_CLASSLOADING_EXPORT = LRMI_CLASSLOADING + ".export";
    public final static String LRMI_CLASSLOADING_EXPORT_DEFAULT = "true";

    public final static String LRMI_NETWORK_FILTER_FACTORY = "com.gs.lrmi.filter.factory";

    public final static String LRMI_NETWORK_FILTER_FACTORY_ADDRESS_MATCHERS_FILE = "com.gs.lrmi.filter.address-matchers-file";

    public final static String LRMI_NETWORK_FILTER_UNWRAP_BUFFER_ALLOCATION_STRATEGY = "com.gs.lrmi.filter.unrap.buffer.allocation.strategy";

    /**
     * Retry to connect, this is a property is use for workaround for a bug in the JVM see (IBM JVM
     * bugid:IZ19325)
     */
    public final static String LRMI_SELECTOR_BUG_CONNECT_RETRY = "com.gs.lrmi.connect.selector.retry";

    public final static String LRMI_RESOURCE_WARN_THRESHOLD_FACTOR = "com.gs.lrmi.resources.warn-threshold-factor";


    /**
     * https://insightedge.atlassian.net/browse/GS-12365
     */
    public final static String LRMI_ASYNC_THROW_RESOURCE_NOT_AVAILABLE = "com.gs.transport_protocol.lrmi.fail-async-on-max-connection-exceeded";
    public final static boolean LRMI_ASYNC_THROW_RESOURCE_NOT_AVAILABLE_DEFAULT = false;

    /**
     * XAP-13299 -- allow adjustment of the tmp selector pool size.
     */
    public final static String LRMI_TMP_SELECTOR_POOL_SIZE = "com.gs.lrmi.tmp.selector.pool.size";
    public final static String LRMI_TMP_SELECTOR_POOL_WAIT_TIMEOUT = "com.gs.lrmi.tmp.selector.pool.wait.timeout";


    /**
     * When enabled the server will perform validation if incoming communication is of valid
     * protocol, this is not backward compatible with client of version prior to 9.0.x
     */
    public final static String LRMI_PROTOCOL_VALIDATION_DISABLED = "com.gs.transport_protocol.lrmi.protocol-validation-disabled";

    public final static String SERIALIZE_USING_EXTERNALIZABLE = "com.gs.transport_protocol.lrmi.serialize-using-externalizable";

    /**
     * Set the maximum used buffer size that may be cached for storage type serialization
     */
    public final static String STORAGE_TYPE_SERIALIZATION_MAX_CACHED_BUFFER_SIZE = "com.gs.client.storage-type-serialization.cache.buffer-size";

    /**
     * Default value for {@link #STORAGE_TYPE_SERIALIZATION_MAX_CACHED_BUFFER_SIZE}
     */
    public final static int STORAGE_TYPE_SERIALIZATION_MAX_CACHED_BUFFER_SIZE_DEFAULT = 64 * 1024 * 1024; //64megabytes

    /**
     * Set the ratio that if the currently used buffer size * current cached buffer size is below,
     * the cached buffer expunge threshold counter will be increases
     */
    public final static String STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_RATIO = "com.gs.client.storage-type-serialization.cache.buffer-expunge-ratio";

    /**
     * Default value for {@link #STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_RATIO}
     */
    public final static double STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_RATIO_DEFAULT = 0.5;
    /**
     * Set the number of time the cached buffer has failed the expunge ratio test after which the
     * cached buffer is expunged.
     */
    public final static String STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD = "com.gs.client.storage-type-serialization.cache.buffer-expunge-times-threshold";

    /**
     * Default value for {@link #STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD}
     */
    public final static int STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD_DEFAULT = 20;

    /**
     * Set the maximum pool memory size of buffers that may be cached for storage type
     * serialization
     */
    public final static String STORAGE_TYPE_SERIALIZATION_MAX_POOL_MEMORY_SIZE = "com.gs.client.storage-type-serialization.cache.max-pool-memory-size";

    /**
     * Set the maximum pool number of buffers that may be cached for storage type serialization
     */
    public final static String STORAGE_TYPE_SERIALIZATION_MAX_POOL_RESOURCE_COUNT_SIZE = "com.gs.client.storage-type-serialization.cache.max-pool-resource-count";

    public final static String STORAGE_TYPE_SERIALIZATION_ZIP_COMPRESSION_LEVEL = "com.gs.client.storage-type-serialization.zip.compression-level";

    public final static String STORAGE_TYPE_SERIALIZATION_ZIP_IDEMPOTENT = "com.gs.client.storage-type-serialization.zip.idempotent";

    public static final String AES_PASSPHRASE = "com.gs.property-storage.aes.passphrase";
    public static final String AES_PASSPHRASE_PATH = "com.gs.property-storage.aes.passphrase-path";
    public static final String AES_KEY_LENGTH = "com.gs.property-storage.aes.key-length";
    public static final String AES_MAC_ALGORITHM = "com.gs.property-storage.aes.mac.algorithm";
    public static final String AES_MAC_KEY_LENGTH = "com.gs.property-storage.aes.mac.key-length";
    public static final String AES_GCM_TAG_LENGTH = "com.gs.property-storage.aes.gcm.tag-length";
    public static final String AES_GCM_IV_LENGTH = "com.gs.property-storage.aes.gcm.iv-length";
    public static final String AES_CBC_IV_LENGTH = "com.gs.property-storage.aes.cbc.iv-length";
    public static final String AES_CBC_PADDING = "com.gs.property-storage.aes.cbc.padding";


    //    -Dcom.gigaspaces.lrmi.nio.filters.SSLFilterFactory
    //    -Dcom.gs.lrmi.filter.security.keystore=keystore.ks
    //    -Dcom.gs.lrmi.filter.security.password=password

    public final static String IS_CLUSTER_ENABLED = "com.gs.cluster.cluster_enabled";
    public final static String IS_CLUSTER_ENABLED_DEFAULT = Constants.Cluster.IS_CLUSTER_SPACE_DEFAULT;

    public final static String CLUSTER_CONFIG_URL = "com.gs.cluster.config-url";
    public final static String CLUSTER_CONFIG_URL_DEFAULT = Constants.Cluster.CLUSTER_CONFIG_URL_DEFAULT;

    public final static String IS_FILTER_STATISTICS_ENABLED = "com.gs.filters.statistics.enabled";
    public final static String IS_FILTER_STATISTICS_ENABLED_DEFAULT = Boolean.TRUE.toString();

    public final static String SERIALIZATION_TYPE = "com.gs.serialization";
    public final static String SERIALIZATION_TYPE_DEFAULT = Constants.Engine.ENGINE_SERIALIZATION_TYPE_DEFAULT;

    public final static String TYPE_CHECKSUM_VALIDATION = "com.gs.type.checksum-validation";
    public final static String TYPE_CHECKSUM_VALIDATION_DEFAULT = "true";

    public final static String TYPE_DELETE_ON_DROP = "com.gs.type.delete-on-drop";

    public final static String GS_PROTOCOL = "com.gs.protocol";
    public final static String GS_PROTOCOL_DEFAULT = Constants.LRMIStubHandler.LRMI_DEFAULT_PROTOCOL;

    public final static String ENGINE_CACHE_POLICY = "com.gs.engine.cache_policy";
    public final static String ENGINE_CACHE_POLICY_DEFAULT = String.valueOf(Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE);

    public final static String MEMORY_USAGE_ENABLED = "com.gs.memory_usage_enabled";
    public final static String MEMORY_USAGE_ENABLED_DEFAULT = Constants.Engine.ENGINE_MEMORY_USAGE_ENABLED_DEFAULT;


    public final static String CLUSTER_CACHE_LOADER_EXTERNAL_DATA_SOURCE = "com.gs.cluster.cache-loader.external-data-source";
    public final static String CLUSTER_CACHE_LOADER_CENTRAL_DATA_SOURCE = "com.gs.cluster.cache-loader.central-data-source";

    public final static String DOWNLOAD_HOST = "com.gs.downloadhost";
    public final static String DOWNLOAD_HOST_DEFAULT_VALUE = "localhost:9010";

    public final static String CONTAINER_SHUTDOWN_HOOK = "com.gs.shutdown_hook";
    public final static String CONTAINER_SHUTDOWN_HOOK_DEFAULT_VALUE = Constants.Container.CONTAINER_SHUTDOWN_HOOK_PROP_DEFAULT;

    public final static String LOOKUP_JNDI_URL = "com.gs.jndi.url";
    public final static String LOOKUP_JNDI_URL_DEFAULT = Constants.LookupManager.LOOKUP_JNDI_URL_DEFAULT;

    public final static String START_EMBEDDED_LOOKUP_DEFAULT = Constants.LookupManager.START_EMBEDDED_LOOKUP_DEFAULT;

    public final static String START_EMBEDDED_MAHALO = "com.gs.start-embedded-mahalo";
    public final static String START_EMBEDDED_MAHALO_DEFAULT = Constants.Container.CONTAINER_EMBEDDED_MAHALO_ENABLED_DEFAULT;

    public final static String LOOKUP_UNICAST_ENABLED = "com.gs.jini_lus.unicast_discovery.enabled";
    public final static String LOOKUP_UNICAST_ENABLED_DEFAULT = Constants.LookupManager.LOOKUP_UNICAST_ENABLED_DEFAULT;


    public final static String CREATE_HEAP_DUMP_ON_MEMORY_SHORTAGE = "com.gs.memory.create-heap-dump-on-memory-shortage";
    public final static String MAX_HEAPS_ON_MEMORY_SHORTAGE = "com.gs.memory.max-heaps-on-memory-shortage";
    public final static String HEAPS_ON_MEMORY_SHORTAGE_QUIET_PERIOD = "com.gs.memory.heaps-on-memory-shortage-quiet-period";

    public final static String JMX_REMOTE_BASE_PORT = "com.gs.jmx.remote.base-port";

    /**
     * Default value of JMX supporting
     */

    public final static String JMX_ENABLED_DEFAULT_VALUE = JMX_ENABLED_DEFAULT_BOOLEAN_VALUE.toString();

    /**
     * Replication secure restart property
     */
    public final static String SPACE_STARTUP_STATE_ENABLED = "com.gs.cluster.replication.secure-restart";

    /**
     * System variable for look&feel class definition
     */
    public final static String UI_ENABLED = "com.gs.ops-ui.enabled";

    /**
     * System variable for enabling recording of all metrics tables to hsqldb
     */
    public final static String
        RECORDING_OF_ALL_METRICS_TO_HSQLDB_ENABLED = "com.gs.hsqldb.all-metrics-recording.enabled";

    /**
     * System variable for look&feel class definition
     */
    public final static String LOOK_AND_FEEL_CLASS_NAME = "com.gs.ui.laf.classname";

    /**
     * System variable for maximum size of thread pool that used by UI for redirection Jini events
     */
    public final static String UI_THREAD_POOL_MAX_SIZE = "com.gs.ui.threadpool.maxsize";

    /**
     * System variable for minimum size of thread pool that used by UI for redirection Jini events
     */
    public final static String UI_THREAD_POOL_MIN_SIZE = "com.gs.ui.threadpool.minsize";

    /**
     * System variable for minimum size of scheduled thread pool that used by UI
     */
    public final static String UI_SCHEDULED_THREAD_POOL_MIN_SIZE = "com.gs.ui.scheduledthreadpool.minsize";

    /**
     * System variable for keep alive time of thread pool that used by UI for redirection Jini
     * events
     */
    public final static String UI_THREAD_POOL_KEEP_ALIVE_TIME = "com.gs.ui.threadpool.keepalivetime";

    public final static String UI_THREAD_POOL_MIN_SIZE_DEFAULT_VALUE = String.valueOf(1);
    public final static String UI_SCHEDULED_THREAD_POOL_MIN_SIZE_DEFAULT_VALUE =
            String.valueOf(2);
    public final static String UI_THREAD_POOL_MAX_SIZE_DEFAULT_VALUE = String.valueOf(64);
    public final static String UI_THREAD_POOL_KEEP_ALIVE_TIME_DEFAULT_VALUE = String.valueOf(60);//seconds

    public final static String UI_ORDERED_THREAD_POOL_SIZE = "com.gs.ui.orderedthreadpool.size";

    public final static String JINI_GROUP_USE_DEFINED_ONLY = "com.gs.jini.useDefinedGroupOnly";

    /**
     * System property to define the frequency in which liveness of 'live' members in a cluster is
     * monitored; Default 10000 ms
     */
    public final static String LIVENESS_MONITOR_FREQUENCY = "com.gs.cluster.livenessMonitorFrequency";

    /**
     * Defines the frequency in which liveness of 'live' members in a cluster is monitored; Default
     * 10000 ms
     */
    public final static long LIVENESS_MONITOR_FREQUENCY_DEFAULT = 10000L;

    /**
     * System property to define the frequency in which liveness of members in a cluster is
     * detected; Default 5000 ms
     */
    public final static String LIVENESS_DETECTOR_FREQUENCY = "com.gs.cluster.livenessDetectorFrequency";

    /**
     * Defines the frequency in which liveness of members in a cluster is detected; Default 5000 ms
     */
    public final static long LIVENESS_DETECTOR_FREQUENCY_DEFAULT = 5000L;

    /**
     * disables the duplication filtering mechanism used to avoid double processing of packets after
     * recovery.
     */
    public final static String CANCEL_DUPLICATE_PACKET_FILTER = "com.gs.replication.disable-duplicate-filtering";
    public final static String CANCEL_DUPLICATE_PACKET_FILTER_DEFAULT = "false";

    /**
     * Maximum size (bytes) of buffer sent over the network, each buffer sent by the LRMI layer is
     * broken into smaller buffers according to the this value.
     *
     * @see #MAX_LRMI_BUFFER_SIZE_DEFAULT
     */
    public final static String MAX_LRMI_BUFFER_SIZE = "com.gs.lrmi.maxBufferSize";

    /**
     * The default value set as the LRMI maximum buffer size, 64k.
     *
     * @see #MAX_LRMI_BUFFER_SIZE
     */
    public final static int MAX_LRMI_BUFFER_SIZE_DEFAULT = 64 * 1024;

    /**
     * Maximum time in milliseconds that a message to be sent to the other side can delayed in the
     * system queue before a warning to the log is issue.
     *
     * @see #WRITE_DELAY_BEFORE_WARN_DEFAULT
     */
    public final static String WRITE_DELAY_BEFORE_WARN = "com.gs.lrmi.maxWriteDelayBeforeWarn";

    /**
     * The default value set as the LRMI maxWriteDelayBeforeWarn.
     *
     * @see #WRITE_DELAY_BEFORE_WARN_DEFAULT
     */
    public final static int WRITE_DELAY_BEFORE_WARN_DEFAULT = 30 * 1000;


    /**
     * The default value set to true.
     *
     * @see #LRMI_FAIL_TO_GET_CLASS_BYTES_FROM_ACTIVE_CONNECTION
     */
    public final static boolean LRMI_FAIL_TO_GET_CLASS_BYTES_FROM_ACTIVE_CONNECTION_DEFAULT = true;

    /**
     * Determine if class bytes should retrieved from active connection when failed to retrieve from
     * the parent class loader connection.
     */
    public final static String LRMI_FAIL_TO_GET_CLASS_BYTES_FROM_ACTIVE_CONNECTION = "com.gs.lrmi.failToGetClassBytesFromActiveConnection";


    /**
     * Number of segments used by the engine stored lists. Two different segments can be accessed
     * concurrently by different threads, therefore number of segments defines how concurrent the
     * engine structures are.
     */
    public final static String ENGINE_STORED_LIST_SEGMENTS = "com.gs.engine.storedListSegments";

    /**
     * Default number of segments in engine stored list is 5*Runtime.getRuntime().availableProcessors().
     * It gives the probability of 0.7 that there will be no collisions between the threads we add 1
     * since #core is generaly 2**n (n=1,2,3...) so an even number will yield better distribution in
     * case of constant thread numbers If set to 0 - the default is used.
     */
    public final static int ENGINE_STORED_LIST_SEGMENTS_DEFAULT = 5 * Runtime.getRuntime().availableProcessors() + 1;

    /**
     * Number of locks used by the engine stored lists. Reducing the  number of locks will reduce
     * the  memory footprint for each object index
     */
    public final static String ENGINE_STORED_LIST_LOCKS = "com.gs.engine.storedListLocks";

    /**
     * Default number of index locks in engine If set to 0 - each index has its own lock
     */
    public final static int ENGINE_STORED_LIST_LOCKS_DEFAULT = 10 * Runtime.getRuntime().availableProcessors();


    /**
     * Number of segments used by the concurrent server-based lru . Two different segments can be
     * accessed concurrently by different threads, therefore number of segments defines how
     * concurrent the engine structures are.
     */
    public final static String ENGINE_LRU_SEGMENTS = "com.gs.engine.lruSegments";

    /**
     * Default number of segments in lru is 5*Runtime.getRuntime().availableProcessors(). we add 1
     * since #core is generaly 2**n (n=1,2,3...) so an even number will yield better distribution in
     * case of constant thread numbers If set to 0 - the default is used.
     */
    public final static int ENGINE_LRU_SEGMENTS_DEFAULT = 5 * Runtime.getRuntime().availableProcessors() + 1;

    /**
     * are the before/after remove filters general and not only for lease cancel/expiration
     */
    @Deprecated
    public static final String ENGINE_GENERAL_PURPOSE_REMOVE_FILTERS = "com.gs.engine.general_purpose_remove_filters";

    public static final String BROWSER_LAF_IS_CROSS_PLATFORM = "com.gs.browser.laf.isCross";

    public static final String CONTAINER_NAME = "com.gs.browser.containername";

    /**
     * Interval in miliseconds to log database recovery process, default is 10 seconds.
     */
    public static final String CACHE_MANAGER_RECOVER_INTERVAL_LOG = "com.gs.cacheManager.logRecoveryInterval";

    public static final long CACHE_MANAGER_RECOVER_INTERVAL_DEFAULT = 10000;

    /**
     * Specified whether to disable database recovery logging, default is true.
     */
    public static final String CACHE_MANAGER_LOG_RECOVER_PROCESS = "com.gs.cacheManager.logRecovery";


    /**
     * protect embedded indexes by clonning values.
     */
    public static final String CACHE_MANAGER_EMBEDDED_INDEX_PROTECTION = "com.gs.cacheManager.EmbeddedIndexProtection";

    public static final String CACHE_MANAGER_EMBEDDED_INDEX_PROTECTION_DEFAULT = "true";


    /**
     * Number of segments used in creating CHM for entries- uids, indices, off-heap internalcache .
     */
    public final static String CACHE_MANAGER_HASHMAP_SEGMENTS = "com.gs.cacheManager.hashmapSegments";

    /**
     * Default Number of segments used in creating CHM for entries- uids, indices, off-heap
     * internalcache.
     */
    public final static int CACHE_MANAGER_HASHMAP_SEGMENTS_DEFAULT = 64;

    /**
     * The timeout that a caller to the lease manager reaper force cycle is ready to wait for the
     * cycle to be completed
     */
    public final static String LEASE_MANAGER_FORCE_REAPER_CYCLE_TIME_TO_WAIT = "com.gs.leaseManager.forceReaperCycleTimeToWait";

    /**
     * The default timeout that a caller to the lease manager reaper force cycle is ready to wait
     * for the cycle to be completed
     */
    public static final long LEASE_MANAGER_FORCE_REAPER_CYCLE_TIME_TO_WAIT_DEFAULT = 60000;

    /**
     * Indication for if the QueryCache inner implementation will be bounded or not
     */
    public final static String ENABLE_BOUNDED_QUERY_CACHE = "com.gs.queryCache.bounded.enable";

    /**
     * The default indication for if the QueryCache inner implementation will be bounded or not
     */
    public final static String ENABLE_BOUNDED_QUERY_CACHE_DEFAULT = "true";

    /**
     * The bounded query cache size
     */
    public final static String BOUNDED_QUERY_CACHE_SIZE = "com.gs.queryCache.cacheSize";

    /**
     * The default bounded query cache size
     */
    public final static long BOUNDED_QUERY_CACHE_SIZE_DEFAULT = 1000L;

    /**
     * disable quiesce mode- false means quiesce command will be rejected
     */
    public final static String DISABLE_QUIESCE_MODE = "com.gs.engine.disableQuiesceMode";

    public final static String CLEAR_REST_JETTY_FILES = "com.gs.clear-rest-jetty-files.enabled";


    public static final boolean ENABLE_DYNAMIC_LOCATORS_DEFAULT = false;
    public static final String DYNAMIC_LOCATORS_MAX_INIT_DELAY = "com.gs.jini_lus.locators.dynamic.max_delay_before_discovery";
    public static final long DYNAMIC_LOCATORS_MAX_INIT_DELAY_DEFAULT = 10000l;

    public static final String JCONSOLE_INTERVAL = "com.gs.jconsole.interval";

    public static final String PERSISTENCY_STORE_LOG_DIRECTORY = "com.gs.persistency.logDirectory";

    public static final String PERSISTENCY_STORE_DELETE_CURRENT_ON_DESTROY = "com.gs.persistency.delete_current_on_destroy";

    public static final String LICENSE_KEY = "com.gs.licensekey";

    public static final String ENV_REPORT = "com.gs.env.report";
    public static final String DB_CONTAINER_NAME = "com.gs.container.name";
    public static final String DB_SPACE_NAME = "com.gs.space.name";
    public static final String CLUSTER_XML_SCHEMA_VALIDATION = "com.gs.xmlschema.validation";
    public static final String CLUSTER_XML_SCHEMA_VALIDATION_DEFAULT = "true";

    public static final String REQUIRED_CONSISTENCY_LEVEL = "com.gs.replication.required_consistency_level";
    public static final int REQUIRED_CONSISTENCY_LEVEL_DEFAULT = 1;

    public static final String REPLICATION_BLOBSTORE_SYNC_LIST_BATCH_SIZE = "com.gs.replication.blobstore.sync_list_batch_size";
    public static final int REPLICATION_BLOBSTORE_SYNC_LIST_BATCH_SIZE_DEFAULT = 15000;

    public static final String USE_BLOBSTORE_EMBEDDED_SYNC_LIST = "com.gs.blobstore.use_embedded_sync_list";
    public static final String USE_BLOBSTORE_EMBEDDED_SYNC_LIST_DEFAULT = "true";

    public static final String REPLICATION_USE_BLOBSTORE_SYNC_LIST = "com.gs.replication.blobstore.use_sync_list";
    public static final String REPLICATION_USE_BLOBSTORE_SYNC_LIST_DEFAULT = "true";

    public static final String REPLICATION_USE_BACKUP_BLOBSTORE_BULKS = "com.gs.replication.blobstore.use_backup_bulks";
    public static final String REPLICATION_USE_BACKUP_BLOBSTORE_BULKS_DEFAULT = "true";

    public static final String DIRECT_PERSISTENCY_RECOVER_RETRIES = "com.gs.direct_persistency.recover_retries";
    public static final int DIRECT_PERSISTENCY_RECOVER_RETRIES_DEFAULT = 10;

    public static final String BLOCKING_CLIENT_CONNECT = "com.gs.blocking.client.client.connect";
    public static final String BLOCKING_CLIENT_CONNECT_DEFAULT = "false";

    public static final int EXTRA_BACKUP_SPACE_RESOLUTION_RETRIES_DEFAULT = 15;
    public static final String EXTRA_BACKUP_SPACE_RESOLUTION_RETRIES = "com.gs.cluster.extra_backup_space_resolution_retries";

    // provide workaround for GS-12201
    public final static long CACHE_CONTEXT_CLOSE_MAX_WAIT_DEFAULT = -1;
    public final static String CACHE_CONTEXT_CLOSE_MAX_WAIT = "com.gs.cache_context_close_max_wait";

    public final static String JMX_REMOTE_AUTHENTICATE = "com.sun.management.jmxremote.authenticate";

    //stuck 2PC prepare times
    public final static String CACHE_STUCK_2PC_BASIC_TIME = "com.gs.cache_stuck_xtns_basic_time";
    public final static long CACHE_STUCK_2PC_BASIC_TIME_DEFAULT = 3 * 60 * 1000;
    public final static String CACHE_STUCK_2PC_EXTENTION_TIME = "com.gs.cache_stuck_xtns_extention_time";
    public final static long CACHE_STUCK_2PC_EXTENTION_TIME_DEFAULT = 1 * 60 * 1000;

    public final static String HANDLE_EXTRA_STATELESS_INSTANCES_RETRIES = "com.gs.handle-extra-stateless-instances.retries";
    public final static int HANDLE_EXTRA_STATELESS_INSTANCES_RETRIES_DEFAULT = 60;

    public static final String MANAGER_REST_JETTY_CONFIG = "com.gs.manager.rest.jetty.config";
    public static final String MANAGER_REST_SSL_KEYSTORE_PATH = "com.gs.manager.rest.ssl.keystore-path";
    public static final String MANAGER_REST_SSL_KEYSTORE_PASSWORD = "com.gs.manager.rest.ssl.keystore-password";
    public static final String MANAGER_REST_REQUESTS_MAX_RUNNING = "com.gs.manager.rest.max-running-requests";
    public static final String MANAGER_REST_REQUESTS_MAX_COMPLETED = "com.gs.manager.rest.max-completed-requests";
    public static final String MANAGER_REST_PLUGINS_PATH = "com.gs.manager.rest.plugins.path";

    public static final String SPARK_REST_REQUEST_TIMEOUT = "com.gs.manager.rest.spark.request-timeout";
    public static final String SPARK_REST_REQUEST_TIMEOUT_DEFAULT = "10000";

    public static final String MANAGER_ZOOKEEPER_DISCOVERY_PORT = "com.gs.manager.zookeeper.discovery.port";
    public static final String MANAGER_ZOOKEEPER_LEADER_ELECTION_PORT = "com.gs.manager.zookeeper.leader-election.port";

    public static final String MANAGER_ADMIN_WITH_STATISTICS_HISTORY = "com.gs.admin.with-stats-history";
    public static final String MANAGER_ADMIN_DISCOVER_UN_MANAGED_SPACES = "com.gs.admin.discover-un-managed-spaces";

    public static final String QUERY_PROCESSOR_CLUSTER_LOOKUP_TIMEOUT = "com.gs.queryProcessor.cluster-lookup-timeout";
    public static final long QUERY_PROCESSOR_CLUSTER_LOOKUP_TIMEOUT_DEFAULT = 15000; //in milliseconds

    public static final String JETTY_LAUNCHER_REQUEST_HEADER_SIZE = "com.gs.jetty-launcher.request-header-size";

    //GS-13424 java 9 support: VisualVM is not part of JDK 9
    public static final String VISUAL_VM_LAUNCH_FILE_PATH = "com.gs.visualvm.binary";

    public static final String CLASS_BINARY_STORAGE_ADAPTER = "com.gs.class-binary-storage-adapter.";

    private static final Map<String, String> defaultValues = initDefaultValues();

    private static Map<String, String> initDefaultValues() {
        Map<String, String> result = new HashMap<>(30);

        //fill map with default values, keys are system variable names
        result.put(WATCHDOG_IDLE_CONNECTION_TIMEOUT, WATCHDOG_IDLE_CONNECTION_TIMEOUT_DEFAULT);
        result.put(WATCHDOG_LISTENING_TIMEOUT, WATCHDOG_LISTENING_TIMEOUT_DEFAULT);
        result.put(WATCHDOG_REQUEST_TIMEOUT, WATCHDOG_REQUEST_TIMEOUT_DEFAULT);
        result.put(WATCHDOG_TIMEOUT_RESOLUTION, WATCHDOG_TIMEOUT_RESOLUTION_DEFAULT);
        result.put(DOWNLOAD_HOST, DOWNLOAD_HOST_DEFAULT_VALUE);
        result.put(CONTAINER_SHUTDOWN_HOOK, CONTAINER_SHUTDOWN_HOOK_DEFAULT_VALUE);
        result.put(JINI_LUS_LOCATORS, JINI_LUS_LOCATORS_DEFAULT);
        result.put(LOOKUP_JNDI_URL, LOOKUP_JNDI_URL_DEFAULT);
        result.put(START_EMBEDDED_LOOKUP, START_EMBEDDED_LOOKUP_DEFAULT);
        result.put(START_EMBEDDED_MAHALO, START_EMBEDDED_MAHALO_DEFAULT);
        result.put(LOOKUP_UNICAST_ENABLED, LOOKUP_UNICAST_ENABLED_DEFAULT);

        result.put(IS_CLUSTER_ENABLED, IS_CLUSTER_ENABLED_DEFAULT);

        result.put(CLUSTER_CONFIG_URL, CLUSTER_CONFIG_URL_DEFAULT);
        result.put(IS_FILTER_STATISTICS_ENABLED, IS_FILTER_STATISTICS_ENABLED_DEFAULT);
        result.put(SERIALIZATION_TYPE, SERIALIZATION_TYPE_DEFAULT);
        result.put(GS_PROTOCOL, GS_PROTOCOL_DEFAULT);
        result.put(ENGINE_CACHE_POLICY, ENGINE_CACHE_POLICY_DEFAULT);
        result.put(MEMORY_USAGE_ENABLED, MEMORY_USAGE_ENABLED_DEFAULT);
        result.put(CANCEL_DUPLICATE_PACKET_FILTER, CANCEL_DUPLICATE_PACKET_FILTER_DEFAULT);

        result.put(ENABLE_DYNAMIC_LOCATORS, Boolean.toString(ENABLE_DYNAMIC_LOCATORS_DEFAULT));
        result.put(DYNAMIC_LOCATORS_MAX_INIT_DELAY, Long.toString(DYNAMIC_LOCATORS_MAX_INIT_DELAY_DEFAULT));
        result.put(REQUIRED_CONSISTENCY_LEVEL, String.valueOf(REQUIRED_CONSISTENCY_LEVEL_DEFAULT));
        result.put(BLOCKING_CLIENT_CONNECT, BLOCKING_CLIENT_CONNECT_DEFAULT);

        return result;
    }

    public static String getSystemVariableDefaultValue(String name) {
        return defaultValues.get(name);
    }

    public static String setSystemProperty(String key, String value) {
        return value != null ? System.setProperty(key, value) : System.clearProperty(key);
    }

    public static boolean getBoolean(String name, boolean defaultValue) {
        String property = System.getProperty(name);
        return property == null ? defaultValue : Boolean.parseBoolean(property);
    }
}
