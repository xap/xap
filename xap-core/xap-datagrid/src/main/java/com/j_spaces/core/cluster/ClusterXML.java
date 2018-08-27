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

/*
 * @(#)ClusterXML.java 1.0   25/12/2001  11:52AM
 */

package com.j_spaces.core.cluster;

import com.gigaspaces.cluster.activeelection.core.ActiveElectionConfig;
import com.gigaspaces.cluster.loadbalance.LoadBalancingPolicy;
import com.gigaspaces.cluster.loadbalance.LoadBalancingPolicy.BroadcastCondition;
import com.gigaspaces.cluster.replication.ConsistencyLevel;
import com.gigaspaces.cluster.replication.MirrorServiceConfig;
import com.gigaspaces.cluster.replication.ReplicationTransmissionPolicy;
import com.gigaspaces.cluster.replication.sync.SyncReplPolicy;
import com.gigaspaces.internal.cluster.node.impl.config.MultiBucketReplicationPolicy;
import com.gigaspaces.internal.io.XmlUtils;
import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.Constants.Mirror;
import com.j_spaces.core.CreateException;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.client.SpaceURLParser;
import com.j_spaces.core.cluster.ReplicationPolicy.ReplicationPolicyDescription;
import com.j_spaces.core.exception.ClusterConfigurationException;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.XPathProperties;
import com.j_spaces.kernel.log.JProperties;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class ClusterXML {

    // ---------------------  START OF SCHEMA -------------------------------------//

    //check depends system property if need to validate XML versus XML schema
    final static private boolean validXMLSchema = Boolean.parseBoolean(System.getProperty(
            SystemProperties.CLUSTER_XML_SCHEMA_VALIDATION,
            SystemProperties.CLUSTER_XML_SCHEMA_VALIDATION_DEFAULT));

    final static public String CLUSTER_MEMBER_URL_PROTOCOL_PREFIX = "com.gs.cluster.url-protocol-prefix";
    /**
     * By default we use jini: with multicast protocol as prefix to each of the cluster members
     * created dynamically. If the -Dcom.gs.cluster.url-protocol-prefix is passed we use this prefix
     * instead. e.g. when defining -Dcom.gs.cluster.url-protocol-prefix=rmi://pc-gershon:10098/ will
     * have this protocol prefix for all the cluster member urls
     */
    final static public String CLUSTER_MEMBER_URL_PROTOCOL_PREFIX_DEF = "jini://*/";

    /**
     * if -Dcom.gs.clusterXML.debug=true we flash to system out the content of the cluster members
     * XML file, the cluster schema XSL file and the resulted transformed cluster config DOM
     * document, after xalan parsing.
     */
    final static private boolean m_debugMode = Boolean.getBoolean(SystemProperties.CLUSTER_XML_DEBUG);
    /**
     * This tag is located in the XXX-cluster.xml (the static cluster members file) file, which is
     * used in conjunction with the XXX-cluster-schema.xsl and which points to its name. e.g.
     * <cluster-schema-name>LB</cluster-schema-name> means that this cluster xml file is using the
     * cluster schema xsl file name: LB-cluster-schema.xsl
     */
    final static public String CLUSTER_SCHEMA_NAME_TAG = "cluster-schema-name";
    final static public String CLUSTER_SCHEMA_DESCRIPTION_TAG = "description";

    /**
     * having such file suffix indicates that we use a static list of cluster members defined in
     * this xml file. In that file, we will have <schema-name> attribute which will reference to the
     * cluster topology xsl file to be used with the members xml to transformate and produce final
     * version of cluster config document.
     */
    final static public String CLUSTER_XML_FILE_SUFFIX = "-cluster.xml";
    final static public String CLUSTER_XML_DYNAMIC_MEMBERS_TAG = "dynamic-members-generation";

    final static public String CLUSTER_SCHEMA_XSL_FILE_SUFFIX = "-cluster-schema.xsl";

    final static public String CLUSTER_SCHEMA_NAME_ASYNC_REPLICATED = "async_replicated";
    final static public String CLUSTER_SCHEMA_NAME_ASYNC_REPLICATED_SYNC2BACKUP = "async-repl-sync2backup";
    final static public String CLUSTER_SCHEMA_NAME_SYNC_REPLICATED = "sync_replicated";
    final static public String CLUSTER_SCHEMA_NAME_PRIMARY_BACKUP = "primary_backup";
    final static public String CLUSTER_SCHEMA_NAME_PARTITIONED = "partitioned";
    final static public String CLUSTER_SCHEMA_NAME_PARTITIONED_SYNC2BACKUP = "partitioned-sync2backup";

    //final static public String CLUSTER_SCHEMA_NAME_ONECLICK = "oneclick";

    // ---------------------  END OF SCHEMA -------------------------------------//

    // name of tag in cluster xml file
    final static public String CLUSTER_CONFIG_TAG = "cluster-config";
    final static public String GROUP_TAG = "group";
    final static public String GROUPS_TAG = "groups";
    final static public String GROUPS_MEMBERS = "group-members";
    final static public String GROUP_NAME_TAG = "group-name";
    final static public String MEMBER_TAG = "member";
    final static public String PARAM_TAG = "param";
    final static public String PARAM_NAME_TAG = "param-name";
    final static public String PARAM_VALUE_TAG = "param-value";
    final static public String MEMBER_NAME_TAG = "member-name";
    final static public String MEMBER_URL_TAG = "member-url";
    final static public String FAIL_OVER_POLICY_TAG = "fail-over-policy";
    final static public String FAIL_OVER_FIND_TIMEOUT_TAG = "fail-over-find-timeout";
    final static public String FAIL_OVER_FAILBACK_TAG = "fail-back";

    /**
     * active election manager tags
     */
    final static public String ACTIVE_ELECTION_TAG = "active-election";
    final static public String ACTIVE_ELECTION_RETRY_COUNT_TAG = "connection-retries";
    final static public String ACTIVE_ELECTION_YIELD_TIME_TAG = "yield-time";
    final static public String ACTIVE_ELECTION_RESOLUTION_TIMEOUT_TAG = "resolution-timeout";

    /**
     * active election fault detector
     */
    final static public String ACTIVE_ELECTION_FD_TAG = "fault-detector";
    final static public String ACTIVE_ELECTION_FD_INVOCATION_DELAY_TAG = "invocation-delay";
    final static public String ACTIVE_ELECTION_FD_RETRY_COUNT_TAG = "retry-count";
    final static public String ACTIVE_ELECTION_FD_RETRY_TIMEOUT_TAG = "retry-timeout";

    /**
     * fail over tags
     */
    final static public String FAIL_OVER_BACKUP_MEMBER_TAG = "backup-member";
    final static public String FAIL_OVER_BACKUP_MEMBERS_TAG = "backup-members";
    final static public String FAIL_OVER_BACKUP_MEMBERS_ONLY_TAG = "backup-members-only";
    final static public String FAIL_OVER_BACKUP_MEMBER_ONLY_TAG = "backup-member-only";
    final static public String FAIL_OVER_SOURCE_MEMBER_TAG = "source-member";
    final static public String POLICY_TYPE_TAG = "policy-type";
    final static public String BROADCAST_CONDITION_TAG = "broadcast-condition";
    final static public String CLUSTER_MEMBERS_TAG = "cluster-members";
    final static public String CLUSTER_NAME_TAG = "cluster-name";

    /**
     * mirror service tags
     */
    final static public String MIRROR_SERVICE_TAG = "mirror-service";
    final static public String MIRROR_SERVICE_URL_TAG = "url";
    final static public String MIRROR_SERVICE_BULK_SIZE_TAG = "bulk-size";
    final static public String MIRROR_SERVICE_INTERVAL_MILLIS_TAG = "interval-millis";
    final static public String MIRROR_SERVICE_INTERVAL_OPERS_TAG = "interval-opers";
    final static public String MIRROR_SERVICE_REDO_LOG_CAPACITY_EXCEEDED_TAG = "on-redo-log-capacity-exceeded";
    final static public String MIRROR_SERVICE_REDO_LOG_CAPACITY_TAG = "redo-log-capacity";
    final static public String MIRROR_SERVICE_SUPPORTS_PARTIAL_UPDATE_TAG = "supports-partial-update";
    final static public String MIRROR_SERVICE_SUPPORTS_CHANGE_TAG = "change-support";

    final static public String MIRROR_SERVICE_CHANGE_SUPPORT_NONE_VALUE = "none";

    /**
     * mirror service default values
     */
    final static public String MIRROR_SERVICE_ENABLED_DEFAULT_VALUE = String.valueOf(false);
    final static public String MIRROR_SERVICE_URL_DEFAULT_VALUE =
            "jini://*/mirror-service_container/mirror-service";
    final static public String MIRROR_SERVICE_BULK_SIZE_DEFAULT_VALUE = String.valueOf(100);
    final static public String MIRROR_SERVICE_INTERVAL_MILLIS_DEFAULT_VALUE = String.valueOf(2000);
    final static public String MIRROR_SERVICE_INTERVAL_OPERS_DEFAULT_VALUE = String.valueOf(100);


    final static public String PROXY_BROADCAST_THREADPOOL_MIN_SIZE_DEFAULT_VALUE = String.valueOf(4);
    final static public String PROXY_BROADCAST_THREADPOOL_MAX_SIZE_DEFAULT_VALUE = String.valueOf(64);


    /**
     * cluster-wise cache-loader tags
     */
    final static public String CACHE_LOADER_TAG = "cache-loader";
    /**
     * true if cluster holds at least one external data-source; default: false. when true,
     * replication transfers meta-data; otherwise transfers only uid.
     */
    final static public String CACHE_LOADER_EXTERNAL_DATA_SOURCE = "external-data-source";
    /**
     * true if cluster interacts with a central-data-source; default: false. when true, CacheAdapter
     * ignores operations arriving from replication.
     */
    final static public String CACHE_LOADER_CENTRAL_DATA_SOURCE = "central-data-source";


    //cache loader default values
    final static public String CACHE_LOADER_EXTERNAL_DATA_SOURCE_DEFAULT_VALUE =
            "${" + SystemProperties.CLUSTER_CACHE_LOADER_EXTERNAL_DATA_SOURCE + "}";
    final static public String CACHE_LOADER_CENTRAL_DATA_SOURCE_DEFAULT_VALUE =
            "${" + SystemProperties.CLUSTER_CACHE_LOADER_CENTRAL_DATA_SOURCE + "}";


    // async replication xml tags
    final static public String ASYNC_REPLICATION_TAG = "async-replication";
    final static public String REPL_POLICY_TAG = "repl-policy";
    final static public String REPL_SYNC_ON_COMMIT_TAG = "sync-on-commit";
    final static public String REPL_SYNC_ON_COMMIT_TIMEOUT_TAG = "sync-on-commit-timeout";
    final static public String REPL_NOTIFY_TEMPLATE_TAG = "replicate-notify-templates";
    final static public String REPL_TRIGGER_NOTIFY_TEMPLATES_TAG = "trigger-notify-templates";
    final static public String REPL_LEASE_EXPIRATIONS_TAG = "replicate-lease-expirations";
    final static public String REPL_CHUNK_SIZE_TAG = "repl-chunk-size";
    final static public String REPL_INTERVAL_MILLIS_TAG = "repl-interval-millis";
    final static public String REPL_INTERVAL_OPERS_TAG = "repl-interval-opers";
    final static public String REPL_FIND_TIMEOUT_TAG = "repl-find-timeout";
    final static public String REPL_FIND_REPORT_INTERVAL_TAG = "repl-find-report-interval";
    final static public String REPL_MEMORY_RECOVERY_TAG = "recovery";
    final static public String REPL_ORIGINAL_STATE_TAG = "repl-original-state";
    final static public String REPL_NETWORK_COMPRESSION_TAG = "repl-network-compression";
    final static public String REPL_REDO_LOG_CAPACITY_TAG = MIRROR_SERVICE_REDO_LOG_CAPACITY_TAG;
    final static public String REPL_REDO_LOG_MEMORY_CAPACITY_TAG = "redo-log-memory-capacity";
    final static public String REPL_REDO_LOG_COMPACTION_TAG = "redo-log-compaction";
    final static public String REPL_REDO_LOG_BACKLOG_WEIGHT_POLICY = "backlog-weight-policy";
    final static public String REPL_REDO_LOG_RECOVERY_CAPACITY_TAG = "redo-log-recovery-capacity";
    final static public String REPL_REDO_LOG_LOCALVIEW_CAPACITY_TAG = "redo-log-local-view-capacity";
    final static public String REPL_REDO_LOG_LOCALVIEW_RECOVERY_CAPACITY_TAG = "redo-log-local-view-recovery-capacity";
    final static public String REPL_REDO_LOG_DURABLE_NOTIFICATION_CAPACITY_TAG = "redo-log-durable-notification-capacity";
    final static public String REPL_LOCALVIEW_MAX_DISCONNECTION_TIME_TAG = "local-view-max-disconnection-time";
    final static public String REPL_DURABLE_NOTIFICATION_MAX_DISCONNECTION_TIME_TAG = "durable-notification-max-disconnection-time";
    final static public String REPL_REDO_LOG_CAPACITY_EXCEEDED_TAG = MIRROR_SERVICE_REDO_LOG_CAPACITY_EXCEEDED_TAG;
    final static public String REPL_TOLERATE_MISSING_PACKETS_TAG = "on-missing-packets";
    final static public String REPL_ON_CONFLICTING_PACKETS_TAG = "on-conflicting-packets";
    final static public String REPL_FULL_TAKE_TAG = "repl-full-take";
    final static public String RECOVERY_CHUNK_SIZE_TAG = "recovery-chunk-size";
    public final static String RECOVERY_THREAD_POOL_SIZE = "recovery-thread-pool-size";
    final static public String REPL_ONE_PHASE_COMMIT_TAG = "repl-one-phase-commit";
    final static public String CONNECTION_MONITOR_THREAD_POOL_SIZE = "connection-monitor-thread-pool-size";

    final static public String RELIABLE_ASYNC_REPL_TAG = "reliable";
    final static public String RELIABLE_ASYNC_STATE_NOTIFY_INTERVAL_TAG = "reliable-async-completion-notifier-interval";
    final static public String RELIABLE_ASYNC_STATE_NOTIFY_PACKETS_TAG = "reliable-async-completion-notifier-packets-threshold";
    final static public String REPL_ASYNC_CHANNEL_SHUTDOWN_TIMEOUT_TAG = "async-channel-shutdown-timeout";


    final static public String REPL_FIND_REPORT_INTERVAL_DEFAULT_VALUE = String.valueOf(30000);
    final static public String REPL_REDO_LOG_CAPACITY_DEFAULT_VALUE = String.valueOf(-1);
    final static public String RECOVERY_CHUNK_SIZE_DEFAULT_VALUE = String.valueOf(200);
    public final static String RECOVERY_THREAD_POOL_SIZE_DEFAULT_VALUE = "4";

    final static public String REPL_PESRISTENT_BLOBSTORE_REDO_LOG_CAPACITY_DEFAULT_VALUE = "1000000";
    final static public String REPL_PESRISTENT_BLOBSTORE_MEMORY_REDO_LOG_CAPACITY_DEFAULT_VALUE = "400000";

    // replication filters tags
    final static public String REPL_FILTERS_TAG = "repl-filters";
    final static public String REPL_INPUT_FILTER_CLASSNAME_TAG = "input-filter-className";
    final static public String REPL_INPUT_FILTER_PARAM_URL_TAG = "input-filter-paramUrl";
    final static public String REPL_OUTPUT_FILTER_CLASSNAME_TAG = "output-filter-className";
    final static public String REPL_OUTPUT_FILTER_PARAM_URL_TAG = "output-filter-paramUrl";
    final static public String REPL_ACTIVE_WHEN_BACKUP_TAG = "active-when-backup";
    final static public String REPL_SHUTDOWN_SPACE_ON_INIT_FAILURE_TAG = "shutdown-space-on-init-failure";

    final static public String REPL_ACTIVE_WHEN_BACKUP_DEFAULT = "true";

    final static public String REPL_SHUTDOWN_SPACE_ON_INIT_FAILURE_DEFAULT = "false";

    // member memory recovery
    final static public String REPL_MEMBER_RECOVERY_TAG = "repl-recovery";
    final static public String ENABLED_TAG = "enabled";
    final static public String REPL_SOURCE_MEMBER_URL_TAG = "source-member-name";

    // load balancing tags
    final static public String LOAD_BAL_TAG = "load-bal-policy";
    final static public String APPLY_OWNERSHIP_TAG = "apply-ownership";
    final static public String DISABLE_PARALLEL_SCATTERING_TAG = "disable-parallel-scattering";
    final static public String PROXY_BROADCAST_THREADPOOL_MIN_SIZE_TAG = "proxy-broadcast-threadpool-min-size";
    final static public String PROXY_BROADCAST_THREADPOOL_MAX_SIZE_TAG = "proxy-broadcast-threadpool-max-size";
    final static public String NOTIFY_RECOVERY_TAG = "notify-recovery";
    final static public String NOTIFY_RECOVERY_DEFAULT_VALUE = String.valueOf(true);

    final static public String WRITE_TAG = "write";
    final static public String READ_TAG = "read";
    final static public String TAKE_TAG = "take";
    final static public String NOTIFY_TAG = "notify";
    final static public String DEFAULT_TAG = "default";


    /**
     * sync/async replication
     */
    final static public String REPL_RECEIVER_ACK_POSTFIX = "-rec-ack";

    // async/sync
    final static public String REPLICATION_MODE_TAG = "replication-mode";
    final static public String PERMITTED_OPERATIONS_TAG = "permitted-operations";

    // sync replication tags
    final static public String COMMUNICATION_MODE_TAG = "communication-mode";
    final static public String SYNC_REPL_UNICAST_TAG = "unicast";
    final static public String SYNC_REPL_MULTICAST_TAG = "multicast";
    final static public String SYNC_REPLICATION_TAG = "sync-replication";
    final static public String MIN_WORK_THREADS_TAG = "min-work-threads";
    final static public String MAX_WORK_THREADS_TAG = "max-work-threads";
    final static public String TODO_QUEUE_TIMEOUT_TAG = "todo-queue-timeout";
    final static public String HOLD_TXN_LOCK_TAG = "hold-txn-lock";
    final static public String MULTIPLE_OPERS_CHUNK_SIZE = "multiple-opers-chunk-size";
    final static public String THROTTLE_WHEN_INACTIVE_TAG = "throttle-when-inactive";
    final static public String MAX_THROTTLE_TP_WHEN_INACTIVE_TAG = "max-throttle-tp-when-inactive";
    final static public String MIN_THROTTLE_WHEN_INACTIVE_TAG = "min-throttle-tp-when-active";
    final static public String TARGET_CONSUME_TIMEOUT_TAG = "target-consume-timeout";
    final static public String CONSISTENCY_LEVEL_TAG = "consistency-level";

    // replication mode tags
    final static public String REPLICATION_PROCESSING_TYPE = "processing-type";

    // replication multi bucket tags
    final static public String REPLICATION_MULTI_BUCKET_CONFIG = "multi-bucket-processing";
    final static public String REPLICATION_MULTI_BUCKET_COUNT = "bucket-count";
    final static public String REPLICATION_MULTI_BUCKET_BATCH_PARALLEL_FACTOR = "batch-parallel-factor";
    final static public String REPLICATION_MULTI_BUCKET_BATCH_PARALLEL_THRESHOLD = "batch-parallel-threshold";

    final static public String SWAP_REDOLOG_CONFIG = "swap-redo-log";
    final static public String SWAP_REDOLOG_FLUSH_BUFFER_PACKET_COUNT = "flush-buffer-packet-count";
    final static public String SWAP_REDOLOG_FETCH_BUFFER_PACKET_COUNT = "fetch-buffer-packet-count";
    final static public String SWAP_REDOLOG_SEGMENT_SIZE = "segment-size";
    final static public String SWAP_REDOLOG_MAX_SCAN_LENGTH = "max-scan-length";
    final static public String SWAP_REDOLOG_MAX_OPEN_CURSORS = "max-open-cursors";
    final static public String SWAP_REDOLOG_WRITER_BUFFER_SIZE = "writer-buffer-size";

    final static public String IP_GROUP_TAG = "ip-group";
    final static public String PORT_TAG = "port";
    final static public String TTL_TAG = "ttl";
    final static public String TTL_DEFAULT_VALUE = String.valueOf(4);
    final static public String ADAPTIVE_MULTICAST_TAG = "adaptive";

    final static public String HOLD_TXN_LOCK_DEFAULT_VALUE = String.valueOf(false);

    // name of polices
    final static public String FAIL_IN_GROUP = "fail-in-group";
    final static public String FAIL_TO_BACKUP = "fail-to-backup";
    final static public String FAIL_TO_ALTERNATE = "fail-to-alternate";
    final static public String FULL_REPLICATION = "full-replication";
    final static public String PARTIAL_REPLICATION = "partial-replication";
    final static private List<String> FAIL_OVER_POLICIES = CollectionUtils.toUnmodifiableList(FAIL_IN_GROUP, FAIL_TO_BACKUP, FAIL_TO_ALTERNATE);

    // dCache configuration
    final static public String DCACHE_TAG = "dist-cache";
    final static public String DCACHE_CONFIG_NAME_TAG = "config-name";

    //JMS configuration
    final static public String JMS_TAG = "jms";
    final static public String JMS_CONFIG_NAME_TAG = "config-name";

    // this trans-policy need for dynamic clustering support
    //final static String TRANSMISSION_POLICY_TEMPLATE="/transmission-policy-template.xml";

    // constant of first first available recovery member
    public static final String FIRST_AVAILABLE_MEMBER = "First available member";

    /**
     * Constants used for JAXP 1.2
     */
    static final String JAXP_SCHEMA_LANGUAGE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
    static final String W3C_XML_SCHEMA = "http://www.w3.org/2001/XMLSchema";
    static final String JAXP_SCHEMA_LOCATION =
            "http://apache.org/xml/properties/schema/external-noNamespaceSchemaLocation";

    // cluster object
    private ClusterPolicy clusterPolicy;
    private String _clusterMemberName;
    private String[] _clusterMemberNames;
    private Map<String, ClusterPolicy> _clusterPolicies; // key - memberName, value - ClusterPolicy

    // root document object of cluster xml file
    private Document m_rootDoc;

    // source cluster config file
    //(NOTE THAT THIS CAN BE THE _clusterSchemaName IF CLUSTER SCHEMA US USED)
    private String clusterConfigFile;

    /**
     * the spaceURL of the space
     */
    private SpaceURL _spaceURL;

    private String _clusterSchema;
    private boolean generateMembersDynamically = true;

    private StringBuilder clusterConfigDebugOutput;

    static private TransformerFactory tFactory;
    static private Transformer transformer;

    //broadcast conditions
    //default value
    public static final String BC_BROADCAST_IF_NULL_VALUES = "broadcast-if-null-values";
    public static final String BC_BROADCAST_UNCONDITIONAL = "unconditional";
    public static final String BC_BROADCAST_DISABLED = "broadcast-disabled";

    // new broadcast condition values
    final static public String BC_BROADCAST_IF_ROUTING_INDEX_IS_NULL = "routing-index-is-null";
    final static public String BC_BROADCAST_ALWAYS = "always";
    final static public String BC_BROADCAST_NEVER = "never";

    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG);

    /**
     * This constructor is used for both cases: when cluster parameters are passed viw SpaceFinder
     * and for the case when cluster URL is passed.
     */

    public ClusterXML(SpaceURL spaceURL, String clusterConfigUrl, String spaceName)
            throws IOException, SAXException, ParserConfigurationException, CreateException {
        _spaceURL = spaceURL;

        // starting to create cluster policy
        String clusterSchemaName = null;
        if (spaceURL != null)
            clusterSchemaName = spaceURL.getClusterSchema();

        //if we have cluster_schema attribute as part of the passed SpaceURL
        //we use different ClusterXML constructor
        if (!JSpaceUtilities.isEmpty(clusterSchemaName)) {
            int numOfPrimaryMembers = 0;         //default == 0
            int numOfBackupMembersPerPrimary = 0;//default == 0
            String totalMembers = spaceURL.getProperty(SpaceURL.CLUSTER_TOTAL_MEMBERS);
            // check if the ?total_members contains also the number Of Backup Members Per Primary
            //(which is the next value after the ",");
            String[] members = totalMembers.trim().split(",");//we do not except spaces in the _totalMembers
            numOfPrimaryMembers = Integer.parseInt(members[0]);
            if (members.length > 1) {
                numOfBackupMembersPerPrimary = Integer.parseInt(members[1]);
            }

            init(numOfPrimaryMembers, numOfBackupMembersPerPrimary, clusterSchemaName,
                    spaceName, null,//"default",//TODO which dcache config file name to use ???
                    null,//"default"//TODO which JMS config file name to use ???
                    spaceURL.getProperty(SpaceURL.GROUPS),
                    spaceURL);
        } else {
            init(clusterConfigUrl);
        }
    }


    /**
     * SEMI-DYNAMIC CLUSTERING ONLY !!! =================================== Constructor- Used when
     * the cluster is setuped using the SpaceFinder and The cluster_schema etc. attributes are
     * passed as well. In this case we do not load any cluster xml file from the resource bundle but
     * we build a Dom tree in the memory using the inputs from the SpaceFinder. Then, together with
     * the cluster schema XSL file (which is loaded from the resource bundle, using the
     * cluster_schema name) we execute the XSLT parser and get the Cluster config dom tree, used as
     * input in the ClusterXML existed logic.
     *
     * @param _totalMembers      stands for the SpaceURL primary instances part of the
     *                           ?total_numbers option
     * @param _backupMembers     stands for the SpaceURL backups part of the ?total_numbers option,
     *                           total_members={number of primary instances, number of backup
     *                           instances per primary} In this example the value is 4,2 which means
     *                           that this cluster contains up to 4 primary instances each
     *                           containing 2 backup instances.
     * @param _clusterSchemaName stands for the SpaceURL ?cluster_schema option
     * @param _clusterName       stands for the SpaceURL space/container ?schema option
     * @see <code>com.j_spaces.kernel.JSpaceUtilities.buildClusterXMLDom()</code>
     */
    public ClusterXML(int _totalMembers, int _backupMembers, String _clusterSchemaName
            , String _clusterName, String _distCacheConfigName, String _jmsConfigName, String _groups)
            throws IOException, SAXException, ParserConfigurationException, CreateException {
        init(_totalMembers, _backupMembers, _clusterSchemaName
                , _clusterName, _distCacheConfigName, _jmsConfigName, _groups, null);
    } // Constructor

    private void init(int _totalMembers, int _backupMembers, String _clusterSchemaName,
                      String _clusterName, String _distCacheConfigName,
                      String _jmsConfigName, String _groups, SpaceURL spaceURL)
            throws IOException, CreateException {

        _spaceURL = spaceURL;
        this._clusterSchema = _clusterSchemaName;

        //TODO later support XSD schema validation to every produced/used XSL
        //// Obtaining a org.w3c.dom.Document from XML
        //DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        //// check depends system property if need to validate XML versus XML schema
        //if ( validXMLSchema.equalsIgnoreCase("true") )//TODO remove useClusterSchemas later and support XSD again
        //{
        //factory.setValidating(true);
        //factory.setAttribute(JAXP_SCHEMA_LANGUAGE, W3C_XML_SCHEMA);
        //factory.setAttribute(JAXP_SCHEMA_LOCATION, getClass().getResource( VALID_SCHEMA_FILE ).toString() );
        //}
        //DocumentBuilder builder = factory.newDocumentBuilder();
        //// set error handler
        //builder.setErrorHandler( new DefaultErrorHandler() );

        //if true,- we need to use cluster schema and XSLT
        //useClusterSchemas = true;
        generateMembersDynamically = true;
        try {
            //use the cluster schema name to find the cluster schema xsl file.
            InputStream clusterXSLPolicy = null;
            try {
                clusterXSLPolicy = ResourceLoader.findClusterXSLSchema(_clusterSchemaName);
                if (clusterXSLPolicy == null)
                    throw new CreateException("Failed to load a cluster using a cluster schema. " +
                            "Failed to load the < " + _clusterSchemaName + " > cluster xsl schema.");

                if (isClusterXMLInDebugMode()) {
                    printClusterConfigDebug(clusterXSLPolicy, null, null, null,
                            " Using Semi-Dynamic Cluster " + (spaceURL != null ? "\nand SpaceURL:" + spaceURL.getURL() : "\nand cluster schema: " + _clusterSchemaName));
                }
            } catch (Exception ex) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, ex.toString(), ex);
                }
                throw new CreateException("Failed to load a cluster using a cluster schema. " +
                        "Failed to load the < " + _clusterSchemaName + " > cluster xsl schema.", ex);
            }

            /**
             * build a Dom element in memory, of the cluster xml file (to be
             * input to the XSLT parser) according to the inputs.
             */
            Document clusterXMLDomElement = JSpaceUtilities.buildClusterXMLDom(_totalMembers, _backupMembers, _clusterSchemaName
                    , _clusterName, _distCacheConfigName, _jmsConfigName, _groups);

            if (isClusterXMLInDebugMode()) {
                //use the XSLT transformation package to output the DOM tree we just created
                //Use the static TransformerFactory.newInstance() method to instantiate
                // a TransformerFactory. The javax.xml.transform.TransformerFactory
                // system property setting determines the actual class to instantiate --
                // org.apache.xalan.transformer.TransformerImpl.
                if (tFactory == null)
                    tFactory = TransformerFactory.newInstance();
                if (transformer == null)
                    transformer = tFactory.newTransformer();
                printClusterConfigDebug(null, (Element) clusterXMLDomElement.getFirstChild(), null, transformer, null);
                //need to bring it again since the stream was already considered closed for forther calls.
                clusterXSLPolicy = ResourceLoader.findClusterXSLSchema(_clusterSchemaName);
            }

            m_rootDoc = JSpaceUtilities.convertToClusterConfiguration(clusterXMLDomElement, clusterXSLPolicy);

            // convert the DOM tree values from System Properties if defined
            // It is done BEFORE XPATH resolving is ran so the XPath has the highest override priority.
            JSpaceUtilities.convertDOMTreeFromSystemProperty(m_rootDoc);

            if (_spaceURL != null) {
                try {
                    JSpaceUtilities.overrideClusterConfigWithXPath(_spaceURL.getCustomProperties(), m_rootDoc);
                } catch (Exception e) {
                    if (_logger.isLoggable(Level.SEVERE)) {
                        _logger.log(Level.SEVERE, "Failed to override the Cluster config using the XPATH passed through the custom properties. Cause:  " + e.toString(), e);
                    }
                }
            }

            // source cluster config file
            clusterConfigFile = _clusterSchemaName;
            if (isClusterXMLInDebugMode()) {
                printClusterConfigDebug(null, null, m_rootDoc, transformer, null);
            }
            // removes all white spaces between XML tags
            JSpaceUtilities.normalize(m_rootDoc);
        } catch (TransformerConfigurationException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.toString(), e.getException());
            }
            throw new CreateException("Failed to create Transformer instance. Failed to load a cluster using a cluster schema. ", e.getException());
        } catch (TransformerException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.toString(), e.getException());
            }
            throw new CreateException("Failed to load a cluster using a cluster schema. ", e.getException());
        } catch (ParserConfigurationException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.toString(), e);
            }
            throw new CreateException("Failed to load a cluster using a cluster schema. ", e);
        } catch (SAXException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.toString(), e.getException());
            }
            throw new CreateException("Failed to load a cluster using a cluster schema because of validation errors. ", e);
        }

        checkXSLAvailability();
    } // init()

    /**
     * This method check if transformation may be performed without any cutting of the member names.
     * This happens with JDK 1.4 ( well known bug ).
     */
    private void checkXSLAvailability() throws ClusterConfigurationException {
        /**
         * 19.12.05 Gershon - a patch for 1.4 xalan bug.
         * In case its JDK 1.4 and after the Transformation we see that the
         * member-name does not contain.
         */
        if (System.getProperty("java.runtime.version").indexOf("1.4") > -1) {
            String[] membersName = getClusterMemberNames();
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("cluster members: " + Arrays.asList(membersName));
            }
            for (int i = 0; i < membersName.length; i++) {
                String name = membersName[i];
                validateMemberName(name);
            }

			/*check validation of group members names, under
               <groups> -> <group> -> <group-name> ->
	           <group-members> -> <member> -> <member-name>
			 */
            String[] groupMembersArray = getGroupMemberNames();
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("cluster Group members: " + Arrays.asList(groupMembersArray));
            }
            for (int i = 0; i < groupMembersArray.length; i++) {
                validateMemberName(groupMembersArray[i]);
            }
        }
    }

    /**
     * check member name according to its usual format: "spaceName_contaner:spaceName" if name is
     * not suitable to this format (indicates that we have a transformation bug), then
     * ClusterConfigurationException thrown
     */
    private void validateMemberName(String memberName) throws ClusterConfigurationException {
        if (memberName.indexOf(":") == -1) {
            String commandLine = "\"-Xbootclasspath/p:%XML_JARS%\""; //windows
            if (!System.getProperty("file.separator").equals("\\"))
                commandLine = "\"-Xbootclasspath/p:${XML_JARS}\""; //unix

            throw new ClusterConfigurationException("The used JDK 1.4.x default Xalan implementation, does not support proper xsl transformations.\n" +
                    " Please use the Xalan package located under <GigaSpaces Root Directory>/lib/xml" +
                    "\n and add the following to the Java command line: " +
                    commandLine);
        }
    }

    /**
     * Constructor.
     *
     * @param clusterConfigURLs The URLs is a (white space separated) list of URLs that indicate the
     *                          location of the cluster configuration file.
     **/
    public ClusterXML(String clusterConfigURLs)
            throws IOException, SAXException, ParserConfigurationException, CreateException {
        init(clusterConfigURLs);
    }


    private void init(String clusterConfigURLs)
            throws IOException, SAXException, ParserConfigurationException, CreateException {
        String clusterConfigResourceStr = null;//url path of the cluster-config.xml file to be parsed.
        String singleURL;
        Document staticClusterXmlDoc = null;
        //check if the clusterConfigURLs contains more than 1 url
        //getting the first non failable URL
        for (StringTokenizer urlsStrTokenizer = new StringTokenizer(clusterConfigURLs, ",");
             urlsStrTokenizer.hasMoreTokens(); ) {
            try {
                clusterConfigFile = singleURL = urlsStrTokenizer.nextToken().trim();
                //url built from several urls, will be handled later inside a for.
                if (new File(singleURL).isFile()) {
                    try {
                        /**
                         * build a Dom element in memory, of the cluster xml file (to be
                         * input to the XSLT parser) according to the inputs.
                         */
                        staticClusterXmlDoc = XmlUtils.getDocumentBuilder().parse(singleURL);
                        _clusterSchema = ClusterXML.getNodeValueIfExists(staticClusterXmlDoc, ClusterXML.CLUSTER_SCHEMA_NAME_TAG);
                        String groupsElement = ClusterXML.getNodeValueIfExists(staticClusterXmlDoc, ClusterXML.GROUPS_TAG);
                        if (_clusterSchema != null) {
                            if (groupsElement != null) {
                                throw new ClusterConfigurationException("Failed to load static cluster members configuration file: "
                                        + singleURL + ". \nThe file should not include <groups> element, " +
                                        "since it contains <" + ClusterXML.CLUSTER_SCHEMA_NAME_TAG + "> element.");
                            }

                            //use the cluster schema name to find the cluster schema xsl file.
                            InputStream clusterXSLPolicy = ResourceLoader.findClusterXSLSchema(_clusterSchema);
                            m_rootDoc = JSpaceUtilities.convertToClusterConfiguration(staticClusterXmlDoc, clusterXSLPolicy);

                            // Convert the DOM tree values from System Properties if defined
                            // It is done BEFORE XPATH resolving is ran so the XPath has the highest override priority.
                            JSpaceUtilities.convertDOMTreeFromSystemProperty(m_rootDoc);

                            if (_spaceURL != null) {
                                try {
                                    JSpaceUtilities.overrideClusterConfigWithXPath(_spaceURL.getCustomProperties(), m_rootDoc);
                                } catch (Exception e) {
                                    if (_logger.isLoggable(Level.SEVERE)) {
                                        _logger.log(Level.SEVERE, "Failed to override the Cluster config using the XPATH passed through the custom properties. Cause:  " + e.toString(), e);
                                    }
                                }
                            }

                            //if debug==true --> flush the xsl schema, cluster members Dom and final cluster config root Dom --> to sys out and log
                            if (isClusterXMLInDebugMode()) {
                                //use the XSLT transformation package to output the DOM tree we just created
                                //Use the static TransformerFactory.newInstance() method to instantiate
                                // a TransformerFactory. The javax.xml.transform.TransformerFactory
                                // system property setting determines the actual class to instantiate --
                                // org.apache.xalan.transformer.TransformerImpl.
                                try {
                                    if (tFactory == null)
                                        tFactory = TransformerFactory.newInstance();
                                    if (transformer == null)
                                        transformer = tFactory.newTransformer();
                                } catch (TransformerConfigurationException e) {
                                    if (_logger.isLoggable(Level.SEVERE)) {
                                        _logger.log(Level.SEVERE, e.toString(), e.getException());
                                    }
                                }
                                printClusterConfigDebug(null, (Element) staticClusterXmlDoc.getFirstChild(), m_rootDoc,
                                        transformer, "load a cluster using a cluster schema < " + _clusterSchema + " > " +
                                                "and a static cluster members xml file < " + singleURL + " >");
                            }
                            // source cluster config file
                            clusterConfigFile = _clusterSchema;

                            // removes all white spaces between XML tags
                            JSpaceUtilities.normalize(m_rootDoc);
                            break;
                        }
                        //if no <cluster-schema-name> exists
                        else {
                            //if no <cluster-schema-name> exists AND no <groups> element then, a its corrupted static members file
                            if (groupsElement == null) {
                                String missingSchemaMsg = "Could not find the <cluster-schema-name> tag for cluster members xml file: " + singleURL;
                                //if the _clusterXSLSchema is missing, it means no schema were ever passed by the client
                                //then we throw exception back to user. (we do NOT
                                //load any default cluster XSL Schema files)
                                throw new ClusterConfigurationException(missingSchemaMsg);
                            }
                            /**
                             * NO USE OF CLUSTER XSL schemas AT ALL.
                             * USING FULL CLUSTER CONFIG FILE --> NO SCHEMAS --> LEGACY
                             * When using cluster schema and cluster XML from the disk (not from SpaceFinder).
                             */
                            m_rootDoc = XmlUtils.getDocumentBuilder(validXMLSchema).parse(singleURL);
                            //convert the DOM tree values from System Properties if defined
                            JSpaceUtilities.convertDOMTreeFromSystemProperty(m_rootDoc);

                            if (_spaceURL != null) {
                                try {
                                    JSpaceUtilities.overrideClusterConfigWithXPath(_spaceURL.getCustomProperties(), m_rootDoc);
                                } catch (Exception e) {
                                    if (_logger.isLoggable(Level.SEVERE)) {
                                        _logger.log(Level.SEVERE, "Failed to override the Cluster config using the XPATH passed through the custom properties. Cause:  " + e.toString(), e);
                                    }
                                }
                            }

                            //if debug==true --> flush the final cluster config root Dom --> to sys out and log
                            if (isClusterXMLInDebugMode()) {
                                try {
                                    if (tFactory == null)
                                        tFactory = TransformerFactory.newInstance();
                                    if (transformer == null)
                                        transformer = tFactory.newTransformer();
                                } catch (TransformerConfigurationException e) {
                                    if (_logger.isLoggable(Level.SEVERE)) {
                                        _logger.log(Level.SEVERE, e.toString(), e.getException());
                                    }
                                }
                                printClusterConfigDebug(null, null, m_rootDoc, transformer,
                                        " Using static cluster config file " + singleURL);
                            }
                            break;
                        }
                    } catch (Throwable ex) {
                        if (_logger.isLoggable(Level.SEVERE)) {
                            _logger.log(Level.SEVERE, ex.toString(), ex);
                        }
                        throw new ClusterConfigurationException(ex.toString(), ex);
                    }
                }
                //trying to look for the file in classpath using resource bundle
                //if it starts with /config/ notation and not as a file
                else {
                    String clusterSchemaXMLFileName = singleURL.substring(singleURL.lastIndexOf('/') + 1);
//					e.g. /config/LB-hashbased or http://localhost:port/LB-hashbased-cluster-config.xml
                    boolean httpDownloadable = (singleURL.startsWith("http://") ? true : false);
                    if (singleURL.startsWith("/config/") || httpDownloadable) {
                        //check only the case without http://
                        if (!httpDownloadable) {
                            /** we check if it is a full cluster config --> e.g. /config/LB-hashbased-cluster-config.xml **/
                            int clusterSuffix = singleURL.lastIndexOf("-cluster.xml");
                            int clusterConfigSuffix = singleURL.lastIndexOf("-cluster-config.xml");
                            if (clusterConfigSuffix > 0) {
                                clusterSchemaXMLFileName = singleURL.substring(0, clusterConfigSuffix);
                                //lets search for /config/LB-hashbased
                                URL clusterConfigResourceURL = ResourceLoader.getResourceURL(singleURL);
                                if (clusterConfigResourceURL == null) {
                                    // lets give it another try with /config/LB-hashbased-cluster-config.xml
                                    clusterConfigResourceURL = ResourceLoader.getResourceURL(clusterSchemaXMLFileName);
                                }
                                //found
                                if (clusterConfigResourceURL != null) {
                                    clusterConfigResourceStr = clusterConfigResourceURL.toExternalForm();
                                } else {
                                    throw new ClusterConfigurationException("Failed to load a static cluster config file < " + singleURL + " > ");
                                    //throw error since was not found in classpath as /config/LB-hashbased-cluster-config.xml nor /config/LB-hashbased
                                }
                                //check

                            }
                            /** we check if it is a static cluster members xml --> e.g. config/LB-hashbased-cluster.xml **/
                            else if (clusterSuffix > 0) {
                                clusterSchemaXMLFileName = singleURL.substring(0, clusterSuffix);
                                //lets search for /config/LB-hashbased
                                URL clusterConfigResourceURL = ResourceLoader.getResourceURL(singleURL);
                                if (clusterConfigResourceURL == null) {
                                    // lets give it another try with /config/LB-hashbased-cluster.xml
                                    clusterConfigResourceURL = ResourceLoader.getResourceURL(clusterSchemaXMLFileName);
                                }
                                //found
                                if (clusterConfigResourceURL != null) {
                                    clusterConfigResourceStr = clusterConfigResourceURL.toExternalForm();
                                } else {
                                    throw new ClusterConfigurationException("Failed to load a static cluster members file < " + singleURL + " > ");
                                }
                            } else {
                                /** last try with /config/<singleURL>-cluster.xml . we add teh suffix -cluster.xml **/
                                URL clusterMembersResourceURL = ResourceLoader.getResourceURL(singleURL + "-cluster.xml");
                                //found
                                if (clusterMembersResourceURL != null) {
                                    clusterConfigResourceStr = clusterMembersResourceURL.toExternalForm();
                                } else {
                                    //nothing was found - we throw exception
                                    throw new ClusterConfigurationException("Failed to load a static cluster members file < " + singleURL + "-cluster.xml >");
                                }
                            }
                        }
                        //handle case of a http:// url
                        else {
                            clusterConfigResourceStr = singleURL;
                        }
                        try {
                            /** parse the xml and check inside if its a static members xml OR a full static cluster config xml **/
                            staticClusterXmlDoc = XmlUtils.getDocumentBuilder().parse(clusterConfigResourceStr);
                            _clusterSchema = ClusterXML.getNodeValueIfExists(staticClusterXmlDoc, ClusterXML.CLUSTER_SCHEMA_NAME_TAG);
                            String groupsElement = ClusterXML.getNodeValueIfExists(staticClusterXmlDoc, ClusterXML.GROUPS_TAG);
                            if (_clusterSchema != null) {
                                if (groupsElement != null) {
                                    throw new ClusterConfigurationException("Failed to load static cluster members configuration file: "
                                            + clusterConfigResourceStr + ". \nThe file should not include <groups> element, " +
                                            "since it contains <" + ClusterXML.CLUSTER_SCHEMA_NAME_TAG + "> element.");
                                }
                                //               use the cluster schema name to find the cluster schema xsl file.
                                InputStream clusterXSLPolicy = ResourceLoader.findClusterXSLSchema(_clusterSchema);
                                m_rootDoc = JSpaceUtilities.convertToClusterConfiguration(staticClusterXmlDoc, clusterXSLPolicy);

                                if (_spaceURL != null) {
                                    try {
                                        JSpaceUtilities.overrideClusterConfigWithXPath(_spaceURL.getCustomProperties(), m_rootDoc);
                                    } catch (Exception e) {
                                        if (_logger.isLoggable(Level.SEVERE)) {
                                            _logger.log(Level.SEVERE, "Failed to override the Cluster config using the XPATH passed through the custom properties. Cause:  " + e.toString(), e);
                                        }
                                    }
                                }
                                // convert the DOM tree values from System Properties if defined
                                JSpaceUtilities.convertDOMTreeFromSystemProperty(m_rootDoc);

                                if (_spaceURL != null) {
                                    try {
                                        JSpaceUtilities.overrideClusterConfigWithXPath(_spaceURL.getCustomProperties(), m_rootDoc);
                                    } catch (Exception e) {
                                        if (_logger.isLoggable(Level.SEVERE)) {
                                            _logger.log(Level.SEVERE, "Failed to override the Cluster config using the XPATH passed through the custom properties. Cause:  " + e.toString(), e);
                                        }
                                    }
                                }

                                //if debug==true --> flush the xsl schema, cluster members Dom and final cluster config root Dom --> to sys out and log
                                if (isClusterXMLInDebugMode()) {
                                    try {
                                        if (tFactory == null)
                                            tFactory = TransformerFactory.newInstance();
                                        if (transformer == null)
                                            transformer = tFactory.newTransformer();
                                    } catch (TransformerConfigurationException e) {
                                        if (_logger.isLoggable(Level.SEVERE)) {
                                            _logger.log(Level.SEVERE, e.toString(), e.getException());
                                        }
                                    }
                                    printClusterConfigDebug(null, staticClusterXmlDoc.getDocumentElement(), m_rootDoc,
                                            transformer, "load a cluster using a cluster schema < " + _clusterSchema + " > " +
                                                    "and a static cluster members xml file < " + clusterConfigResourceStr + " >");
                                }
                                // source cluster config file
                                clusterConfigFile = _clusterSchema;

                                // removes all white spaces between XML tags
                                JSpaceUtilities.normalize(m_rootDoc);
                                break;
                            } else {
                                //if no <cluster-schema-name> exists AND no <groups> element --> its corrupted static members file
                                if (groupsElement == null) {
                                    String missingSchemaMsg = "Could not find the <cluster-schema-name> tag for cluster members xml file: " + singleURL;
                                    //if the _clusterXSLSchema is missing, it means no schema were ever passed by the client
                                    //then we throw exception back to user. (we do NOT
                                    //load any default cluster XSL Schema files)
                                    throw new ClusterConfigurationException(missingSchemaMsg);
                                }
                                /**
                                 * NO USE OF CLUSTER XSL schemas AT ALL.
                                 * USING FULL CLUSTER CONFIG FILE --> NO SCHEMAS --> LEGACY
                                 * When using cluster schema and cluster XML from the disk (not from SpaceFinder).
                                 */
                                if (validXMLSchema)
                                    m_rootDoc = XmlUtils.getDocumentBuilder(validXMLSchema).parse(clusterConfigResourceStr);
                                else
                                    m_rootDoc = XmlUtils.getDocumentBuilder(validXMLSchema).parse(singleURL);

                                //convert the DOM tree values from System Properties if defined
                                JSpaceUtilities.convertDOMTreeFromSystemProperty(m_rootDoc);

                                if (_spaceURL != null) {
                                    try {
                                        JSpaceUtilities.overrideClusterConfigWithXPath(_spaceURL.getCustomProperties(), m_rootDoc);
                                    } catch (Exception e) {
                                        if (_logger.isLoggable(Level.SEVERE)) {
                                            _logger.log(Level.SEVERE, "Failed to override the Cluster config using the XPATH passed through the custom properties. Cause:  " + e.toString(), e);
                                        }
                                    }
                                }

                                //if debug==true --> flush the final cluster config root Dom --> to sys out and log
                                if (isClusterXMLInDebugMode()) {
                                    try {
                                        if (tFactory == null)
                                            tFactory = TransformerFactory.newInstance();
                                        if (transformer == null)
                                            transformer = tFactory.newTransformer();
                                    } catch (TransformerConfigurationException e) {
                                        if (_logger.isLoggable(Level.SEVERE)) {
                                            _logger.log(Level.SEVERE, e.toString(), e.getException());
                                        }
                                    }
                                    printClusterConfigDebug(null, null, m_rootDoc, transformer,
                                            " Using static cluster config file " + singleURL);
                                }
                                break;
                            }
                        } catch (Throwable ex) {
                            if (_logger.isLoggable(Level.SEVERE)) {
                                _logger.log(Level.SEVERE, ex.toString(), ex);
                            }
                            throw new ClusterConfigurationException(ex.toString(), ex);
                        }
                        //System.out.println("ClusterXML: About to use the static cluster XML file with cluster schema: " + clusterConfigResourceStr + " \n");
                    } else {
                        throw new ClusterConfigurationException("Failed to load static cluster members configuration file: \n"
                                + singleURL + ". The file path is invalid.");
                    }
                }//else /config/
            }//end try
            catch (ClusterConfigurationException ex) {
                if (!urlsStrTokenizer.hasMoreTokens())
                    throw ex;
                else {
                    if (_logger.isLoggable(Level.SEVERE)) {
                        _logger.log(Level.SEVERE, "ClusterXML: " + ex.toString(), ex);
                    }
                }
            }
        }//for url's
        //removes all white spaces between XML tags
        JSpaceUtilities.normalize(m_rootDoc);

        checkXSLAvailability();
    }//init()


    /**
     * This constructor should receive a root document object that represent the cluster xml file.
     */
    public ClusterXML(Document doc, String clusterConfigFile) {
        this.m_rootDoc = doc;
        this.clusterConfigFile = clusterConfigFile;
        JSpaceUtilities.normalize(m_rootDoc);
    }

    /**
     * Return <code>true</code> if used cluster schema, otherwise <code>false</code>.
     */
    public boolean useClusterSchema() {
        return (_clusterSchema != null ? true : false);
    }

    public boolean isGenerateMembersDynamically() {
        return generateMembersDynamically;
    }

    /**
     * Return cluster schema name.
     *
     * @return If used return <code>String</code> value, otherwise <code>null</code>
     */
    public String getClusterSchemaName() {
        return _clusterSchema;
    }

    /**
     * Return the <code>String</code> array objects that contain all cluster member names.
     */
    public String[] getClusterMemberNames() {
        // get cluster-members node
        NodeList cml = m_rootDoc.getElementsByTagName(CLUSTER_MEMBERS_TAG);
        Element clusMemElem = (Element) cml.item(0);
        NodeList memNodeList = clusMemElem.getElementsByTagName(MEMBER_NAME_TAG);

        int membersCount = memNodeList.getLength();
        _clusterMemberNames = new String[membersCount];
        for (int i = 0; i < membersCount; i++) {
            _clusterMemberNames[i] = memNodeList.item(i).getFirstChild().getNodeValue().trim();
        }

        return _clusterMemberNames;
    }

    /**
     * Return the <code>String</code> array objects that contain all cluster member names for all
     * groups.
     */
    public String[] getGroupMemberNames() {
        // get cluster-members node
        NodeList groupMembersTagList = m_rootDoc.getElementsByTagName(GROUPS_MEMBERS);
        Element memberElement = (Element) groupMembersTagList.item(0);

        NodeList memNodeList = memberElement.getElementsByTagName(MEMBER_NAME_TAG);

        int membersCount = memNodeList.getLength();

        String[] clusterMemberNames = new String[membersCount];


        for (int index = 0; index < membersCount; index++) {
            clusterMemberNames[index] =
                    memNodeList.item(index).getFirstChild().getNodeValue().trim();
        }

        return clusterMemberNames;
    }

    /**
     * Creating cluster policy object.
     *
     * @param clusterMemberName Cluster member name for creating
     */
    public ClusterPolicy createClusterPolicy(String clusterMemberName)
            throws CreateException {
        long time1 = System.currentTimeMillis();
        // Prepare cluster policies for all cluster members
        if (_clusterPolicies == null) {
            _clusterMemberNames = getClusterMemberNames();
            if (_clusterMemberNames != null && _clusterMemberNames.length > 0) {
                _clusterPolicies = new HashMap<String, ClusterPolicy>();
                ClusterPolicy[] tempPolicy = new ClusterPolicy[_clusterMemberNames.length];
                // INFO: The array index for _clusterMemberNames and tempPolicy is corresponding
                for (int i = 0; i < _clusterMemberNames.length; i++)
                    tempPolicy[i] = createClusterPolicyInternal(_clusterMemberNames[i]);

                // Collect one replication policy from each replication group
                ArrayList<ReplicationPolicy> selectedReplPolicies = new ArrayList<ReplicationPolicy>(_clusterMemberNames.length);
                List<String> selectedReplGroupNames = new ArrayList<String>(_clusterMemberNames.length);
                for (int i = 0; i < tempPolicy.length; i++)
                    if (tempPolicy[i].m_ReplicationPolicy != null &&
                            !selectedReplGroupNames.contains(
                                    tempPolicy[i].m_ReplicationPolicy.m_ReplicationGroupName)) {
                        selectedReplPolicies.add(tempPolicy[i].m_ReplicationPolicy);
                        selectedReplGroupNames.add(tempPolicy[i].m_ReplicationPolicy.m_ReplicationGroupName);
                    }

                // if no replication policies, discard of this list
                if (selectedReplPolicies.isEmpty())
                    selectedReplPolicies = null;
                else
                    selectedReplPolicies.trimToSize();

                // Set the collection of replication policies for each cluster policy
                for (int i = 0; i < _clusterMemberNames.length; i++) {
                    tempPolicy[i].m_ReplicationGroups = selectedReplPolicies;
                    _clusterPolicies.put(_clusterMemberNames[i], tempPolicy[i]);
                }
            }
        }

        ClusterPolicy result = _clusterPolicies.get(clusterMemberName);
        // anyway, if required cluster policy don't exists, try to create it
        if (result == null)
            result = createClusterPolicyInternal(clusterMemberName);

        long time2 = System.currentTimeMillis();

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("Creation of ClusterPolicy instance for \"" + clusterMemberName +
                    "\" cluster member took " + (time2 - time1) + " msec.");
        }

        return result;
    }

    private ClusterPolicy createClusterPolicyInternal(String clusterMemberName)
            throws CreateException {
        _clusterMemberName = clusterMemberName.intern();

        // init cluster policy object
        clusterPolicy = new ClusterPolicy();
        clusterPolicy.m_ClusterSchemaName = _clusterSchema == null ? clusterConfigFile : clusterConfigFile;

        // replication policy description hashtable
        Hashtable<String, ReplicationPolicy.ReplicationPolicyDescription> replPolicyDescTable =
                new Hashtable<String, ReplicationPolicy.ReplicationPolicyDescription>();

        // set cluster name and cluster member-name
        clusterPolicy.m_ClusterName = getNodeValueIfExists(m_rootDoc.getDocumentElement(), CLUSTER_NAME_TAG);
        clusterPolicy.m_ClusterGroupMember = _clusterMemberName;

        String notifyRecovery = getNodeValueIfExists(m_rootDoc.getDocumentElement(), NOTIFY_RECOVERY_TAG);
        if (notifyRecovery != null)
            clusterPolicy.m_NotifyRecovery = JSpaceUtilities.parseBooleanTag(NOTIFY_RECOVERY_TAG, notifyRecovery);

        // set DCache config in cluster policy for backward compatibility
        JSpaceAttributes spaceAttr = (JSpaceAttributes) JProperties.getSpaceProperties(_clusterMemberName);
        if (spaceAttr != null) {
            clusterPolicy.m_DCacheConfigName = spaceAttr.getDCacheConfigName();
            clusterPolicy.m_DCacheAttributes = spaceAttr.getDCacheProperties();
        }

		/*
		 * fetch cache-loader xml values
		 */
        clusterPolicy.m_CacheLoaderConfig = new ClusterPolicy.CacheLoaderConfig();    //init with defaults

        String cacheLoader_externalDataSource = getNodeValueIfExists(m_rootDoc.getDocumentElement(), CACHE_LOADER_EXTERNAL_DATA_SOURCE);
        if (cacheLoader_externalDataSource != null)
            clusterPolicy.m_CacheLoaderConfig.externalDataSource = JSpaceUtilities.parseBooleanTag(CACHE_LOADER_EXTERNAL_DATA_SOURCE, cacheLoader_externalDataSource, clusterPolicy.m_CacheLoaderConfig.externalDataSourceDefault());

        String cacheLoader_centralDataSource = getNodeValueIfExists(m_rootDoc.getDocumentElement(), CACHE_LOADER_CENTRAL_DATA_SOURCE);
        if (cacheLoader_centralDataSource != null)
            clusterPolicy.m_CacheLoaderConfig.centralDataSource = JSpaceUtilities.parseBooleanTag(CACHE_LOADER_CENTRAL_DATA_SOURCE, cacheLoader_centralDataSource, clusterPolicy.m_CacheLoaderConfig.centralDataSourceDefault());

        // create cluster member properties
        createClusterMemberProperties(m_rootDoc);

        // get groups node
        NodeList gl = m_rootDoc.getElementsByTagName(GROUP_TAG);

        // Checks if mirror service is needed and configure correctly.
        final boolean isMirrorEnabled = checkMirrorEnabled();

        if (gl.getLength() == 0)
            throw new CreateException("<group> tag not found in " + clusterConfigFile + " cluster config file.");

        for (int i = 0; i < gl.getLength(); i++) {
            String activeGroupName = null;

            // get members node list
            Element groupElem = (Element) gl.item(i);
            NodeList ml = groupElem.getElementsByTagName(MEMBER_NAME_TAG);

            for (int j = 0; j < ml.getLength(); j++) {
                Node mn = ml.item(j).getFirstChild();
                String memberName = mn.getNodeValue();

                // check if my member name exists in this group
                if (memberName.equals(_clusterMemberName)) {
                    // get group name where this member belong to
                    activeGroupName = getNodeValueIfExists(groupElem, GROUP_NAME_TAG);
                    if (activeGroupName == null)
                        throw new CreateException("<group-name> tag not found in " + clusterConfigFile + " cluster config file.");
                    activeGroupName = activeGroupName.intern();

                    // ************ CREATES GROUP MEMBERS AND GROUP MEMBERS URL ~~~~~~~~~~~~ //
                    ArrayList<String> groupMemberNames = new ArrayList<String>();
                    ArrayList<SpaceURL> groupMemberURL = new ArrayList<SpaceURL>();
                    Vector<String> memNameList = new Vector<String>();

                    for (int h = 0; h < ml.getLength(); h++) {
                        Node memNode = ml.item(h);
                        memNameList.add(memNode.getFirstChild().getNodeValue().trim().intern());
                    }

                    // get cluster-members node
                    NodeList cml = m_rootDoc.getElementsByTagName(CLUSTER_MEMBERS_TAG);
                    if (ml.getLength() == 0)
                        throw new CreateException("<cluster-members> tag not found in " + clusterConfigFile + " cluster config file.");

                    Element clusMemElem = (Element) cml.item(0);
                    NodeList memNodeList = clusMemElem.getElementsByTagName(MEMBER_NAME_TAG);
                    int memNodeListSize = memNodeList.getLength();
                    for (int v = 0; v < memNodeListSize; v++) {
                        String memName = memNodeList.item(v).getFirstChild().getNodeValue().trim().intern();

                        int memListSize = memNameList.size();
                        String memNameStr;
                        // comparing the members names
                        for (int h = 0; h < memListSize; h++) {
                            memNameStr = memNameList.elementAt(h);
                            // if true getting member url
                            if (memName.equalsIgnoreCase(memNameStr)) {
                                Node sib = memNodeList.item(v).getNextSibling();
                                groupMemberNames.add(memNameStr.intern()); // add to member name group
                                Node memberNameURLNode = sib.getFirstChild();
                                if (memberNameURLNode == null)
                                    //if the number of members does not match.
                                    throw new CreateException("The member-name-url is NULL: <" + memName + "> in \"cluster-members\" section.Check " + clusterConfigFile + " cluster config file.");

                                //We append the SpaceURLValidator.IGNORE_VALIDATION to each of the member url's since
                                //no need for validation processing for such internal calls. It is redundant overhead.
                                String memberURL = memberNameURLNode.getNodeValue().trim();
                                String memberURLWithoutValidation = SpaceUrlUtils.setPropertyInUrl(memberURL, SpaceURL.IGNORE_VALIDATION, "true", false);

                                try {
                                    Properties custProps = null;
                                    if (_spaceURL != null) {
                                        custProps = _spaceURL.getCustomProperties();
                                    }
                                    Properties memberCustomProperties = removeRedundantProperties(custProps);
                                    groupMemberURL.add(SpaceURLParser.parseURL(
                                            memberURLWithoutValidation, memberCustomProperties)); // add member name URL
                                } catch (MalformedURLException e) {
                                    throw new RuntimeException(new ClusterConfigurationException("Failed to parse cluster member url: " + memberURLWithoutValidation, e));
                                }

                                // if repl policy already exists, so all member for repl group already created
                                if (clusterPolicy.m_ReplicationPolicy == null) {
                                    // create replication policy description table
                                    ReplicationPolicy.ReplicationPolicyDescription replDesc =
                                            createReplDescPolicy(memName, (Element) ml.item(h).getParentNode(), memNameList);
                                    replPolicyDescTable.put(memName, replDesc);
                                }// if replPolicy == null...
                            }
                        } // for int h...
                    } // for int v...

                    if (clusterPolicy.m_ReplicationPolicy == null) {
                        HashSet<String> targetMembers = new HashSet<String>();
                        HashSet<String> disabledMembers = new HashSet<String>();
                        // check if transmission policy disable for member and nobody transmit to him
                        for (Map.Entry<String, ReplicationPolicy.ReplicationPolicyDescription> replPolicyEntry :
                                replPolicyDescTable.entrySet()) {
                            String descMemberName = replPolicyEntry.getKey();
                            ReplicationPolicy.ReplicationPolicyDescription replDesc = replPolicyEntry.getValue();

                            // if for some member not defined transmission policy it mean, that can't be situation
                            // when no member doesn't transmit to member with disabled transmission policy
                            if (replDesc.replTransmissionPolicies == null) {
                                disabledMembers.clear();
                                break;
                            }

                            ArrayList<String> grpMem = (ArrayList<String>) groupMemberNames.clone();
                            grpMem.remove(descMemberName);
                            int transmPolisySize = replDesc.replTransmissionPolicies.size();
                            for (int w = 0; w < transmPolisySize; w++) {
                                ReplicationTransmissionPolicy trPolicy =
                                        replDesc.replTransmissionPolicies.get(w);

                                // build matrix for disabled and transmission target members
                                if (!trPolicy.m_DisableTransmission)
                                    targetMembers.add(trPolicy.m_TargetSpace);
                                else {
                                    grpMem.remove(trPolicy.m_TargetSpace);
                                    disabledMembers.add(trPolicy.m_TargetSpace);
                                }
                            }

                            int grMemSize = grpMem.size();
                            // complete target member that doesn't have transmission
                            for (int k = 0; k < grMemSize; k++) {
                                targetMembers.add(grpMem.get(k));
                                targetMembers.add(descMemberName);
                            }
                        }

                        // check if no member doesn't transmit to member with disabled transmission policy
                        String[] disMem = disabledMembers.toArray(new String[disabledMembers.size()]);
                        for (int f = 0; f < disMem.length; f++)
                            if (!targetMembers.contains(disMem[f]))
                                throw new CreateException("IllegalReplicationDefinitionError: This member: " + disMem[f] + " is not transmitting and not receiving replication from any other member in replication group. Check out the replication transmission matrix.");

                    }// if ReplicationPolicy != null

                    // check if active member-name defined in cluster-members section
                    if (!groupMemberNames.contains(_clusterMemberName))
                        throw new CreateException("This member-name: <" + _clusterMemberName + "> not defined in \"cluster-members\" section.Check " + clusterConfigFile + " cluster config file.");

                    int memNameListSize = memNameList.size();
                    // check existent and definition group members in cluster member section
                    for (int x = 0; x < memNameListSize; x++) {
                        if (!groupMemberNames.contains(memNameList.elementAt(x))) {
                            if (_logger.isLoggable(Level.INFO)) {
                                _logger.info("WARNING: This member-name: <" + memNameList.elementAt(x) + "> not defined in \"cluster-members\" section. Check " + clusterConfigFile + " cluster config file.");
                            }
                        }
                    }
                    // ************ END OF CREATING GROUP MEMBERS AND GROUP MEMBERS URL ~~~~~~~~~~~~ //

                    //********* S T A R T I N G     L O A D B A L A N C I N G    P O L I C Y    I N I T  **********//
                    if (clusterPolicy.m_LoadBalancingPolicy != null &&
                            groupElem.getElementsByTagName(LOAD_BAL_TAG).getLength() > 0)
                        throw new CreateException("This member: <" + _clusterMemberName + "> can be defined only in one LoadBalancing group.The member was already defined in <" + clusterPolicy.m_LoadBalancingPolicy.m_GroupName + "> LoadBalancing group.");


                    if (clusterPolicy.m_LoadBalancingPolicy != null &&
                            groupElem.getElementsByTagName(FAIL_OVER_POLICY_TAG).getLength() > 0)
                        throw new CreateException("This member: <" + _clusterMemberName + "> can't be defined in different LoadBalancing and FailOver groups.Check definition of <" + clusterPolicy.m_LoadBalancingPolicy.m_GroupName + "> and <" + activeGroupName + "> groups.");


                    if (clusterPolicy.m_LoadBalancingPolicy == null)
                        clusterPolicy.m_LoadBalancingPolicy =
                                createIfExistsLoadBalancingPolicy(groupElem,
                                        activeGroupName,
                                        groupMemberNames,
                                        groupMemberURL);

                    //********* E N D    L O A D B A L A N C I N G    P O L I C Y    I N I T  **********//


                    //********* S T A R T I N G     R E P L I C A T I O N    P O L I C Y    I N I T  **********//
                    if (clusterPolicy.m_ReplicationPolicy != null &&
                            groupElem.getElementsByTagName(REPL_POLICY_TAG).getLength() > 0)
                        throw new CreateException("This member: <" + _clusterMemberName + "> can be defined only in one Replication group.The member was already defined in <" + clusterPolicy.m_ReplicationPolicy.m_ReplicationGroupName + "> Replication group.");

                    if (clusterPolicy.m_ReplicationPolicy == null)
                        clusterPolicy.m_ReplicationPolicy =
                                createIfExistsReplicationPolicy(groupElem, activeGroupName,
                                        groupMemberNames, groupMemberURL, replPolicyDescTable, isMirrorEnabled);

                    //********* E N D     R E P L I C A T I O N    P O L I C Y    I N I T  **********//

                    //********* S T A R T I N G    F A I L   O V E R   P O L I C Y    I N I T  **********//
                    if (clusterPolicy.m_FailOverPolicy != null &&
                            groupElem.getElementsByTagName(FAIL_OVER_POLICY_TAG).getLength() > 0)
                        throw new CreateException("This member: <" + _clusterMemberName + "> can be defined only in one FailOver group.The member was already defined in <" + clusterPolicy.m_FailOverPolicy.failOverGroupName + "> FailOver group.");

                    if (clusterPolicy.m_FailOverPolicy != null &&
                            groupElem.getElementsByTagName(LOAD_BAL_TAG).getLength() > 0)
                        throw new CreateException("This member: <" + _clusterMemberName + "> can't be defined in different FailOver and LoadBalancing groups.Check definition of <" + clusterPolicy.m_FailOverPolicy.failOverGroupName + "> and <" + activeGroupName + "> groups.");

                    // check if fail over policy defined also in this group
                    if (clusterPolicy.m_FailOverPolicy == null) {
                        // keep all failOver policies, for satisfy alternatate recursion construction
                        Hashtable<String, FailOverPolicy> FOPolicies = new Hashtable<String, FailOverPolicy>();
                        clusterPolicy.m_FailOverPolicy = createIfExistsFailOverPolicy(groupElem, activeGroupName, groupMemberNames, groupMemberURL, FOPolicies);

                        // if defined only FailOver without LoadBalancing,
                        // build default "local-space" LoadBalacing policy
                        if (clusterPolicy.m_FailOverPolicy != null && clusterPolicy.m_LoadBalancingPolicy == null)
                            clusterPolicy.m_LoadBalancingPolicy = createDefaultLoadBalacingPolicy(activeGroupName, groupMemberNames, groupMemberURL);
                    }
                    //********* E N D    F A I L   O V E R   P O L I C Y    I N I T  **********//

                    break;
                } // if ( mn.getNodeValue().equals( _clusterMemberName ) )
            } // for ... member list

            // break the cycle when fail over policy or replication policy was discovered
            if (clusterPolicy.m_LoadBalancingPolicy != null && clusterPolicy.m_FailOverPolicy != null && clusterPolicy.m_ReplicationPolicy != null)
                break;
        } // for groups ...


        if (clusterPolicy.m_LoadBalancingPolicy == null && clusterPolicy.m_FailOverPolicy == null && clusterPolicy.m_ReplicationPolicy == null)
            throw new CreateException("An attempt to create CLUSTERED SPACE failed. This member: " + _clusterMemberName + " must be defined at least in one group member in " + clusterConfigFile + " CLUSTER CONFIG XML file.");


        return clusterPolicy;
    }


    /**
     * Checks if mirror service is needed and cofigure correctly.
     *
     * @return <code>true</code> if mirror is enabled.
     * @throws IllegalArgumentException if mirror is enabled but the element is missing.
     */
    private boolean checkMirrorEnabled() {
        boolean isMirrorEnabled = false;
        if (_spaceURL != null) {
            isMirrorEnabled = Boolean.parseBoolean(_spaceURL.getProperty(SpaceURL.MIRROR,
                    Mirror.MIRROR_SERVICE_ENABLED_DEFAULT));
        }

        Element mirrorService = getFirstMatchElement(m_rootDoc.getDocumentElement(), MIRROR_SERVICE_TAG);
        // Checks if mirror service is needed but the mirror serivce doesn't exists in the cluster schema.
        if (isMirrorEnabled && mirrorService == null) {
            throw new IllegalArgumentException(
                    new ClusterConfigurationException("Mirror service is set in the Space URL, " +
                            "but the cluster schema used does not support a mirror service."));
        }

        if (mirrorService != null) {
            isMirrorEnabled |= JSpaceUtilities.parseBooleanTag(ENABLED_TAG,
                    getNodeValueIfExists(mirrorService, ENABLED_TAG));
        }

        return isMirrorEnabled;
    }

    /**
     * In this method properties that are not relevant for cluster member are removed For example:
     * com.j_spaces.core.container.directory_services.jndi.url
     */
    private Properties removeRedundantProperties(Properties props) {
        if (props == null) {
            return null;
        }

        Properties clonedProps = (Properties) props.clone();
        clonedProps.remove(XPathProperties.CONTAINER_JNDI_URL);

        return clonedProps;
    }

    /**
     * create replication policy.
     */
    @SuppressWarnings("deprecation")
    private ReplicationPolicy createIfExistsReplicationPolicy(Element groupElem, String activeGroupName,
                                                              ArrayList<String> groupMemberNames, ArrayList<SpaceURL> groupMemberURLList,
                                                              Hashtable<String, ReplicationPolicyDescription> replPolicyDescTable, boolean isMirrorEnabled)
            throws CreateException {
        ReplicationPolicy replPolicy;
        NodeList repNodeList = groupElem.getElementsByTagName(REPL_POLICY_TAG);
        if (repNodeList.getLength() <= 0)
            return null;

        Element replPolicyNode = (Element) repNodeList.item(0);

        // check for existence <policy-type> tag
        NodeList nl = replPolicyNode.getElementsByTagName(POLICY_TYPE_TAG);
        if (nl.getLength() == 0)
            throw new CreateException("<policy-type> tag not found under <repl-policy> tag.Check " + clusterConfigFile + " cluster config file.");

        // create replication policy
        clusterPolicy.m_Replicated = true;
        replPolicy = new ReplicationPolicy(clusterPolicy.m_ClusterName,
                activeGroupName,
                (List<String>) groupMemberNames.clone(),
                (List<SpaceURL>) groupMemberURLList.clone(),
                _clusterMemberName,
                replPolicyDescTable,
                new SyncReplPolicy(_clusterMemberName),
                new MultiBucketReplicationPolicy(),
                new SwapBacklogConfig());

        /*************************** G E N E R A L REPLICATION  CONFIG ********************/

        /** create mirror-service configuration if exists */
        if (isMirrorEnabled)
            createMirrorServiceConfig(replPolicy);

        // REPLICATION_MODE - if sync - true, otherwise false (async)
        String replicationMode = getNodeValueIfExists(replPolicyNode, REPLICATION_MODE_TAG);
        if (replicationMode != null) {
            //if ( replicationMode.startsWith( ReplicationPolicy.SYNC_REPLICATION_MODE ) )
            replPolicy.m_IsSyncReplicationEnabled =
                    calculateSyncReplicationValue(replicationMode);

            /** set one way if repl-mode name ends with "-rec-ack" */
            replPolicy.isOneWayReplication =
                    calculateOneWayReplicationValue(replicationMode);
        }

        replPolicy.m_ReplicationMode = replicationMode;

        String permittedOperations = getNodeValueIfExists(replPolicyNode, PERMITTED_OPERATIONS_TAG);
        List<ReplicationOperationType> opers;
        if (permittedOperations != null) {
            opers = new ArrayList<ReplicationOperationType>();
            StringTokenizer st = new StringTokenizer(permittedOperations, ",", false);
            while (st.hasMoreTokens()) {
                opers.add(ReplicationOperationType.valueOf(st.nextToken().trim().toUpperCase()));
            }
        } else {
            opers = Arrays.asList(ReplicationOperationType.values());
        }

        replPolicy.setPermittedOperations(opers);


        // REPL_POLICY_TAG
        String policyType = getNodeValueIfExists(replPolicyNode, POLICY_TYPE_TAG);
        if (policyType.equalsIgnoreCase(FULL_REPLICATION))
            replPolicy.m_PolicyType = ReplicationPolicy.FULL_REPLICATION;
        else if (policyType.equalsIgnoreCase(PARTIAL_REPLICATION))
            replPolicy.m_PolicyType = ReplicationPolicy.PARTIAL_REPLICATION;
        else
            throw new CreateException("Unknown replication policy type. Check <policy-type> tag value under <repl-policy> tag.Check " +
                    clusterConfigFile + " cluster config file.");

        // REPL_MEMORY_RECOVERY_TAG
        String value = getNodeValueIfExists(replPolicyNode, REPL_MEMORY_RECOVERY_TAG);
        if (value != null) {
            replPolicy.m_Recovery = JSpaceUtilities.parseBooleanTag(REPL_MEMORY_RECOVERY_TAG, value);
        }

        // REPL_REDO_LOG_CAPACITY_TAG - max redoLog capacity
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_CAPACITY_TAG);
        if (value != null)
            replPolicy.setMaxRedoLogCapacity(Long.parseLong(value));

        // REPL_REDO_LOG_MEMORY_CAPACITY_TAG - max redoLog memory capacity
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_MEMORY_CAPACITY_TAG);
        if (value != null)
            replPolicy.setMaxRedoLogMemoryCapacity(Long.parseLong(value));

        // REPL_REDO_LOG_COMPACTION_TAG - max redoLog memory capacity
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_COMPACTION_TAG);
        if (value != null)
            replPolicy.setRedoLogCompaction(RedoLogCompaction.parse(value));

        // REPL_REDO_LOG_BACKLOG_WEIGHT_POLICY - policy of backlog wight
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_BACKLOG_WEIGHT_POLICY);
        if (value != null)
            replPolicy.setBacklogWeightPolicy(value);

        //local view redo log capacity
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_LOCALVIEW_CAPACITY_TAG);
        if (value != null) {
            if (value.toLowerCase().trim().equals("memory"))
                replPolicy.setLocalViewMaxRedologCapacity(null);
            else
                replPolicy.setLocalViewMaxRedologCapacity(Long.parseLong(value));
        }

        value = getNodeValueIfExists(replPolicyNode, REPL_LOCALVIEW_MAX_DISCONNECTION_TIME_TAG);
        if (value != null) {
            replPolicy.setLocalViewMaxDisconnectionTime(Long.parseLong(value));
        }

        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_LOCALVIEW_RECOVERY_CAPACITY_TAG);
        if (value != null) {
            if (value.toLowerCase().trim().equals("memory"))
                replPolicy.setLocalViewMaxRedologRecoveryCapacity(null);
            else
                replPolicy.setLocalViewMaxRedologRecoveryCapacity(Long.parseLong(value));
        }

        //notification redo log capacity
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_DURABLE_NOTIFICATION_CAPACITY_TAG);
        if (value != null) {
            if (value.toLowerCase().trim().equals("memory"))
                replPolicy.setDurableNotificationMaxRedologCapacity(null);
            else
                replPolicy.setDurableNotificationMaxRedologCapacity(Long.parseLong(value));
        }

        value = getNodeValueIfExists(replPolicyNode, REPL_DURABLE_NOTIFICATION_MAX_DISCONNECTION_TIME_TAG);
        if (value != null)
            replPolicy.setDurableNotificationMaxDisconnectionTime(Long.parseLong(value));

        // REPL_REDO_LOG_RECOVERY_CAPACITY_TAG - max redoLog capacity during recovery
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_RECOVERY_CAPACITY_TAG);
        if (value != null)
            replPolicy.setMaxRedoLogRecoveryCapacity(Long.parseLong(value));

        // REPL_REDO_LOG_CAPACITY_EXCEEDED_TAG - redoLog capacity exceeded
        value = getNodeValueIfExists(replPolicyNode, REPL_REDO_LOG_CAPACITY_EXCEEDED_TAG);
        if (value != null)
            replPolicy.setOnRedoLogCapacityExceeded(parseRedoLogCapacityExceededPolicy(value));

        // REPL_TOLERATE_MISSING_PACKETS_TAG - tolerate capacity exceeded
        value = getNodeValueIfExists(replPolicyNode, REPL_TOLERATE_MISSING_PACKETS_TAG);
        if (value != null)
            replPolicy.setOnMissingPackets(parseMissingPacketsPolicy(value));

        // REPL_ON_CONFLICTING_PACKETS_TAG
        value = getNodeValueIfExists(replPolicyNode, REPL_ON_CONFLICTING_PACKETS_TAG);
        if (value != null)
            replPolicy.setConflictingOperationPolicy(ConflictingOperationPolicy.parseConflictingPacketsPolicy(value));

        // REPL_FULL_TAKE_TAG - replicate full take
        value = getNodeValueIfExists(replPolicyNode, REPL_FULL_TAKE_TAG);
        if (value != null)
            replPolicy.setReplicateFullTake(Boolean.parseBoolean(value));

        value = getNodeValueIfExists(replPolicyNode, REPL_ONE_PHASE_COMMIT_TAG);
        if (value != null)
            replPolicy.setReplicateOnePhaseCommit(Boolean.parseBoolean(value));

        value = getNodeValueIfExists(replPolicyNode, CONNECTION_MONITOR_THREAD_POOL_SIZE);
        if (value != null)
            replPolicy.setConnectionMonitorThreadPoolSize(Integer.parseInt(value));

        // RECOVERY_CHUNK_SIZE_TAG - recovery chunk size
        value = getNodeValueIfExists(replPolicyNode, RECOVERY_CHUNK_SIZE_TAG);
        if (value != null)
            replPolicy.setRecoveryChunkSize(Integer.parseInt(value));

        // RECOVERY_THREAD_POOL_SIZE_TAG - recovery thread pool size
        value = getNodeValueIfExists(replPolicyNode, RECOVERY_THREAD_POOL_SIZE);
        if (value != null)
            replPolicy.setRecoveryThreadPoolSize(Integer.parseInt(value));

        // REPLICATION_OLD_MODE - operate using old replication
        value = getNodeValueIfExists(replPolicyNode, REPLICATION_PROCESSING_TYPE);
        if (value != null) {
            replPolicy.setProcessingType(parseProcessingType(value));
            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "Replication processing type set to: " + value);
        }

        /*************************** MULTI BUCKET REPLICATION CONFIG ********************/
        // MULTI BUCKET REPLICATION
        NodeList multiBucketNL = replPolicyNode.getElementsByTagName(REPLICATION_MULTI_BUCKET_CONFIG);
        if (multiBucketNL.getLength() > 0) {
            Element multiBucketNode = (Element) multiBucketNL.item(0);

            // REPLICATION_MULTI_BUCKET_COUNT
            value = getNodeValueIfExists(multiBucketNode, REPLICATION_MULTI_BUCKET_COUNT);
            if (value != null)
                replPolicy.getMultiBucketReplicationPolicy().setBucketsCount(Short.parseShort(value));

            value = getNodeValueIfExists(multiBucketNode, REPLICATION_MULTI_BUCKET_BATCH_PARALLEL_FACTOR);
            if (StringUtils.hasText(value) && !value.equals("default"))
                replPolicy.getMultiBucketReplicationPolicy().setBatchParallelFactor(Integer.parseInt(value));

            value = getNodeValueIfExists(multiBucketNode, REPLICATION_MULTI_BUCKET_BATCH_PARALLEL_THRESHOLD);
            if (value != null)
                replPolicy.getMultiBucketReplicationPolicy().setBatchParallelThreshold(Integer.parseInt(value));
        }//MULTI BUCKET REPLICATION
        /*************************** SWAP REDO LOG CONFIG ********************/
        // SWAP REDO LOG
        NodeList swapRedoLogNL = replPolicyNode.getElementsByTagName(SWAP_REDOLOG_CONFIG);
        if (swapRedoLogNL.getLength() > 0) {
            Element swapRedologNode = (Element) swapRedoLogNL.item(0);

            value = getNodeValueIfExists(swapRedologNode, SWAP_REDOLOG_FLUSH_BUFFER_PACKET_COUNT);
            if (value != null)
                replPolicy.getSwapRedologPolicy().setFlushBufferPacketsCount(Integer.parseInt(value));

            value = getNodeValueIfExists(swapRedologNode, SWAP_REDOLOG_FETCH_BUFFER_PACKET_COUNT);
            if (value != null)
                replPolicy.getSwapRedologPolicy().setFetchBufferPacketsCount(Integer.parseInt(value));

            value = getNodeValueIfExists(swapRedologNode, SWAP_REDOLOG_SEGMENT_SIZE);
            if (value != null)
                replPolicy.getSwapRedologPolicy().setSegmentSize(Long.parseLong(value));

            value = getNodeValueIfExists(swapRedologNode, SWAP_REDOLOG_MAX_SCAN_LENGTH);
            if (value != null)
                replPolicy.getSwapRedologPolicy().setMaxScanLength(Integer.parseInt(value));

            value = getNodeValueIfExists(swapRedologNode, SWAP_REDOLOG_MAX_OPEN_CURSORS);
            if (value != null)
                replPolicy.getSwapRedologPolicy().setMaxOpenCursors(Integer.parseInt(value));

            value = getNodeValueIfExists(swapRedologNode, SWAP_REDOLOG_WRITER_BUFFER_SIZE);
            if (value != null)
                replPolicy.getSwapRedologPolicy().setWriterBufferSize(Integer.parseInt(value));
        }//SWAP REDO LOG


        // REP_NOTIFY_TEMPLATE_TAG
        value = getNodeValueIfExists(replPolicyNode, REPL_NOTIFY_TEMPLATE_TAG);
        if (value != null)
            replPolicy.m_ReplicateNotifyTemplates = JSpaceUtilities.parseBooleanTag(REPL_NOTIFY_TEMPLATE_TAG, value);

        // TRIGGER_NOTIFY_TEMPLATES_TAG
        value = getNodeValueIfExists(replPolicyNode, REPL_TRIGGER_NOTIFY_TEMPLATES_TAG);
        if (value != null)
            replPolicy.m_TriggerNotifyTemplates = JSpaceUtilities.parseBooleanTag(REPL_TRIGGER_NOTIFY_TEMPLATES_TAG, value);

        // REP_LEASE_EXPIRATIONS_TAG
        value = getNodeValueIfExists(replPolicyNode, REPL_LEASE_EXPIRATIONS_TAG);
        if (value != null)
            replPolicy.setReplicateLeaseExpirations(JSpaceUtilities.parseBooleanTag(REPL_LEASE_EXPIRATIONS_TAG, value));


        // REPL_FIND_TIMEOUT
        value = getNodeValueIfExists(replPolicyNode, REPL_FIND_TIMEOUT_TAG);
        if (value != null)
            replPolicy.m_SpaceFinderTimeout = Long.parseLong(value);

        //REPL_FIND_REPORT_INTERVAL
        value = getNodeValueIfExists(replPolicyNode, REPL_FIND_REPORT_INTERVAL_TAG);
        if (value != null)
            replPolicy.m_SpaceFinderReportInterval = Long.parseLong(value);

        // REPL_ORIGINAL_STATE_TAG
        value = getNodeValueIfExists(replPolicyNode, REPL_ORIGINAL_STATE_TAG);
        if (value != null)
            replPolicy.setReplicatedOriginalState(JSpaceUtilities.parseBooleanTag(REPL_ORIGINAL_STATE_TAG, value));


        /*************************** ASYNC REPLICATION CONFIG ********************/

        // ASYNC_REPLICATION_TAG
        NodeList asyncNL = replPolicyNode.getElementsByTagName(ASYNC_REPLICATION_TAG);
        if (asyncNL.getLength() > 0) {
            Element asyncReplElem = (Element) asyncNL.item(0);

            // REPL_CHUNK_SIZE_TAG
            value = getNodeValueIfExists(asyncReplElem, REPL_CHUNK_SIZE_TAG);
            if (value != null)
                replPolicy.m_ReplicationChunkSize = Integer.parseInt(value);

            // REPL_INTERVAL_MILLIS_TAG
            value = getNodeValueIfExists(asyncReplElem, REPL_INTERVAL_MILLIS_TAG);
            if (value != null)
                replPolicy.m_ReplicationIntervalMillis = Long.parseLong(value);

            // REPL_INTERVAL_OPERS
            value = getNodeValueIfExists(asyncReplElem, REPL_INTERVAL_OPERS_TAG);
            if (value != null)
                replPolicy.m_ReplicationIntervalOperations = Integer.parseInt(value);

            // REPL_NETWORK_COMPRESSION_TAG
            value = getNodeValueIfExists(asyncReplElem, REPL_NETWORK_COMPRESSION_TAG);
            if (value != null)
                replPolicy.m_ReplicationNetworkCompression = JSpaceUtilities.parseBooleanTag(REPL_NETWORK_COMPRESSION_TAG, value);

            // REPL_SYNC_ON_COMMIT_TAG
            value = getNodeValueIfExists(asyncReplElem, REPL_SYNC_ON_COMMIT_TAG);
            if (value != null)
                replPolicy.m_SyncOnCommit = JSpaceUtilities.parseBooleanTag(REPL_SYNC_ON_COMMIT_TAG, value);

            // REPL_SYNC_ON_COMMIT_TIMEOUT_TAG
            value = getNodeValueIfExists(asyncReplElem, REPL_SYNC_ON_COMMIT_TIMEOUT_TAG);
            if (value != null)
                replPolicy.m_SyncOnCommitTimeout = Long.parseLong(value);

            // RELIABLE_ASYNC_REPL_TAG
            value = getNodeValueIfExists(asyncReplElem, RELIABLE_ASYNC_REPL_TAG);
            if (value != null)
                replPolicy.setReliableAsyncRepl(JSpaceUtilities.parseBooleanTag(RELIABLE_ASYNC_REPL_TAG, value));


            // REPL_SHUTDOWN_TIMEOUT_TAG
            value = getNodeValueIfExists(asyncReplElem, REPL_ASYNC_CHANNEL_SHUTDOWN_TIMEOUT_TAG);
            if (value != null)
                replPolicy.setAsyncChannelShutdownTimeout(Long.parseLong(value));

            value = getNodeValueIfExists(asyncReplElem, RELIABLE_ASYNC_STATE_NOTIFY_INTERVAL_TAG);
            if (value != null)
                replPolicy.setReliableAsyncCompletionNotifierInterval(Long.parseLong(value));

            value = getNodeValueIfExists(asyncReplElem, RELIABLE_ASYNC_STATE_NOTIFY_PACKETS_TAG);
            if (value != null)
                replPolicy.setReliableAsyncCompletionNotifierPacketsThreshold(Long.parseLong(value));


        }// ASYNC_REPLICATION_TAG


        /*************************** SYNC REPLICATION CONFIG ********************/
        // SYNC_REPLICATION_TAG
        NodeList syncNL = replPolicyNode.getElementsByTagName(SYNC_REPLICATION_TAG);
        if (syncNL.getLength() > 0) {
            // sync replication
            Element syncReplElem = (Element) replPolicyNode.getElementsByTagName(SYNC_REPLICATION_TAG).item(0);

            /*************************** GENERAL SYNC REPLICATION CONFIG ********************/

            // TODO_QUEUE_TIMEOUT_TAG
            value = getNodeValueIfExists(syncReplElem, TODO_QUEUE_TIMEOUT_TAG);
            if (value != null)
                replPolicy.m_SyncReplPolicy.setTodoQueueTimeout(Long.parseLong(value));

            // HOLD_TXN_LOCK_TAG
            value = getNodeValueIfExists(syncReplElem, HOLD_TXN_LOCK_TAG);
            if (value != null)
                replPolicy.m_SyncReplPolicy.setHoldTxnLockUntilSyncReplication(JSpaceUtilities.parseBooleanTag(HOLD_TXN_LOCK_TAG, value));

            // MULTIPLE_OPERS_CHUNK_SIZE
            value = getNodeValueIfExists(syncReplElem, MULTIPLE_OPERS_CHUNK_SIZE);
            if (value != null)
                replPolicy.m_SyncReplPolicy.setMultipleOperationChunkSize(Integer.parseInt(value));

            // THROTTLE WHEN IN ACTIVE
            value = getNodeValueIfExists(syncReplElem, THROTTLE_WHEN_INACTIVE_TAG);
            if (value != null)
                replPolicy.m_SyncReplPolicy.setThrottleWhenInactive(Boolean.parseBoolean(value));

            // MAX_THROTTLE_WHEN_INACTIVE_TAG
            value = getNodeValueIfExists(syncReplElem, MAX_THROTTLE_TP_WHEN_INACTIVE_TAG);
            if (value != null)
                replPolicy.m_SyncReplPolicy.setMaxThrottleTPWhenInactive(Integer.parseInt(value));

            // MIN_THROTTLE_WHEN_INACTIVE_TAG
            value = getNodeValueIfExists(syncReplElem, MIN_THROTTLE_WHEN_INACTIVE_TAG);
            if (value != null)
                replPolicy.m_SyncReplPolicy.setMinThrottleTPWhenActive(Integer.parseInt(value));

            value = getNodeValueIfExists(syncReplElem, TARGET_CONSUME_TIMEOUT_TAG);
            if (value != null)
                replPolicy.m_SyncReplPolicy.setTargetConsumeTimeout(Long.parseLong(value));

            value = getNodeValueIfExists(syncReplElem, CONSISTENCY_LEVEL_TAG);
            if (value != null) {
                if (value.toUpperCase().equals("QUOROM")) //Fixes Typo - convert QUOROM to QUORUM
                    value = ConsistencyLevel.QUORUM.toString();
                replPolicy.m_SyncReplPolicy.setConsistencyLevel(Enum.valueOf(ConsistencyLevel.class, value.toUpperCase()));
            }

            /*************************** UNICAST SYNC REPLICATION CONFIG ********************/
            // sync replication
            NodeList unicastSyncNL = syncReplElem.getElementsByTagName(SYNC_REPL_UNICAST_TAG);
            if (unicastSyncNL.getLength() > 0) {
                Element unicastSyncElem = (Element) unicastSyncNL.item(0);

                // UNICAST MIN_WORK_THREADS_TAG
                value = getNodeValueIfExists(unicastSyncElem, MIN_WORK_THREADS_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setUnicastMinThreadPoolSize(Integer.parseInt(value));

                // UNICAST MAX_WORK_THREADS_TAG
                value = getNodeValueIfExists(unicastSyncElem, MAX_WORK_THREADS_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setUnicastMaxThreadPoolSize(Integer.parseInt(value));
            }// unicast

            /*************************** MULTICAST SYNC REPLICATION CONFIG ********************/
            // sync replication
            NodeList multicastSyncNL = syncReplElem.getElementsByTagName(SYNC_REPL_MULTICAST_TAG);
            if (multicastSyncNL.getLength() > 0) {
                Element multicastElem = (Element) multicastSyncNL.item(0);

                // IP_GROUP_TAG
                value = getNodeValueIfExists(multicastElem, IP_GROUP_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setMulticastIpGroup(value);

                // PORT_TAG
                value = getNodeValueIfExists(multicastElem, PORT_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setMulticastPort(Integer.parseInt(value));

                // TTL_TAG
                value = getNodeValueIfExists(multicastElem, TTL_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setMulticastTTL(Integer.parseInt(value));

                // MULTICAST MIN_WORK_THREADS_TAG
                value = getNodeValueIfExists(multicastElem, MIN_WORK_THREADS_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setMulticastMinThreadPoolSize(Integer.parseInt(value));

                // MULTICAST MAX_WORK_THREADS_TAG
                value = getNodeValueIfExists(multicastElem, MAX_WORK_THREADS_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setMulticastMaxThreadPoolSize(Integer.parseInt(value));

                // ADAPTIVE_MULTICAST_TAG
                value = getNodeValueIfExists(multicastElem, ADAPTIVE_MULTICAST_TAG);
                if (value != null)
                    replPolicy.m_SyncReplPolicy.setAdaptiveMulticast(JSpaceUtilities.parseBooleanTag(ADAPTIVE_MULTICAST_TAG, value));
            }
        }

        return replPolicy;
    }

    private ReplicationProcessingType parseProcessingType(String value) {
        if (ReplicationPolicy.GLOBAL_ORDER_MODE.equalsIgnoreCase(value))
            return ReplicationProcessingType.GLOBAL_ORDER;
        if (ReplicationPolicy.MULTI_BUCKET_MODE.equalsIgnoreCase(value))
            return ReplicationProcessingType.MULTIPLE_BUCKETS;
        if (ReplicationPolicy.MULTI_SOURCE_MODE.equalsIgnoreCase(value))
            return ReplicationProcessingType.MULTIPLE_SOURCES;

        throw new IllegalArgumentException("illegal processing type policy, can be either " + ReplicationPolicy.GLOBAL_ORDER_MODE + ", " + ReplicationPolicy.MULTI_SOURCE_MODE + " or " + ReplicationPolicy.MULTI_BUCKET_MODE);
    }

    private MissingPacketsPolicy parseMissingPacketsPolicy(String value) {
        if (ReplicationPolicy.RECOVER_MODE.equalsIgnoreCase(value))
            return MissingPacketsPolicy.RECOVER;
        if (ReplicationPolicy.IGNORE_MODE.equalsIgnoreCase(value))
            return MissingPacketsPolicy.IGNORE;

        throw new IllegalArgumentException("illegal missing packets policy, can be either " + ReplicationPolicy.RECOVER_MODE + " or " + ReplicationPolicy.IGNORE_MODE);
    }


    private RedoLogCapacityExceededPolicy parseRedoLogCapacityExceededPolicy(
            String value) {
        if (ReplicationPolicy.BLOCK_OPERATIONS_MODE.equalsIgnoreCase(value))
            return RedoLogCapacityExceededPolicy.BLOCK_OPERATIONS;
        if (ReplicationPolicy.DROP_OLDEST_MODE.equalsIgnoreCase(value))
            return RedoLogCapacityExceededPolicy.DROP_OLDEST;

        throw new IllegalArgumentException("illegal redo log capacity exceeded policy, can be either " + ReplicationPolicy.BLOCK_OPERATIONS_MODE + " or " + ReplicationPolicy.DROP_OLDEST_MODE);
    }


    /**
     * create mirror service config
     */
    private void createMirrorServiceConfig(ReplicationPolicy replPolicy) {
		/*
		 * read MIRROR_SERVICE_TAG from this replication group, if not defined tries to read from
		 * common mirror service definition for all repl-group
		 **/
        Element mirrorService = getFirstMatchElement(m_rootDoc.getDocumentElement(), MIRROR_SERVICE_TAG);
        String elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_URL_TAG);
        if (elemValue == null)
            throw new IllegalArgumentException(new ClusterConfigurationException("Mirror service enabled for replication group: " +
                    replPolicy.m_ReplicationGroupName + ", but mirror url is missing. Check " + clusterConfigFile + " cluster config file."));

        MirrorServiceConfig mirrorConfig = new MirrorServiceConfig();
        try {
            Properties custProps = null;
            if (_spaceURL != null) {
                custProps = _spaceURL.getCustomProperties();
            }
            Properties memberCustomProperties = removeRedundantProperties(custProps);

            mirrorConfig.serviceURL = SpaceURLParser.parseURL(elemValue, memberCustomProperties);
        } catch (MalformedURLException e) {
            throw new RuntimeException(new ClusterConfigurationException("Failed to parse mirror-service finder URL: " + mirrorConfig.serviceURL, e));
        }

        if (_spaceURL != null) {
            String jiniGroup = _spaceURL.getProperty(SpaceURL.GROUPS);
            if (jiniGroup != null)
                mirrorConfig.serviceURL.setProperty(SpaceURL.GROUPS, jiniGroup);
        }

        elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_BULK_SIZE_TAG);
        if (elemValue != null)
            mirrorConfig.bulkSize = Integer.parseInt(elemValue);

        elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_INTERVAL_MILLIS_TAG);
        if (elemValue != null)
            mirrorConfig.intervalMillis = Long.parseLong(elemValue);

        elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_INTERVAL_OPERS_TAG);
        if (elemValue != null)
            mirrorConfig.intervalOpers = Integer.parseInt(elemValue);

        elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_REDO_LOG_CAPACITY_EXCEEDED_TAG);
        if (elemValue != null)
            mirrorConfig.onRedoLogCapacityExceeded = parseRedoLogCapacityExceededPolicy(elemValue);

        elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_REDO_LOG_CAPACITY_TAG);
        if (elemValue != null)
            mirrorConfig.maxRedoLogCapacity = Long.parseLong(elemValue);

        elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_SUPPORTS_PARTIAL_UPDATE_TAG);
        if (elemValue != null)
            mirrorConfig.supportsPartialUpdate = Boolean.parseBoolean(elemValue);

        elemValue = getNodeValueIfExists(mirrorService, MIRROR_SERVICE_SUPPORTS_CHANGE_TAG);
        if (elemValue != null) {
            if (elemValue.equals(MIRROR_SERVICE_CHANGE_SUPPORT_NONE_VALUE))
                mirrorConfig.supportedChangeOperations = null;
            else {
                String[] split = elemValue.split(",");
                HashSet<String> operations = new HashSet<String>();
                for (String operation : split)
                    operations.add(operation.trim());

                mirrorConfig.supportedChangeOperations = operations;
            }
        }

        replPolicy.setMirrorServiceConfig(mirrorConfig);
        replPolicy.m_ReplicationGroupMembersNames.add(mirrorConfig.serviceURL.getMemberName());
        replPolicy.m_ReplicationGroupMembersURLs.add(mirrorConfig.serviceURL);
        mirrorConfig.memberName = mirrorConfig.serviceURL.getMemberName();
    }// isMirror

    /**
     * @return active election configuration
     */
    private ActiveElectionConfig createActiveElectConfig(Element failOverElem) {
        Element electElem = getFirstMatchElement(failOverElem, ACTIVE_ELECTION_TAG);
        if (electElem == null)
            return null;

        ActiveElectionConfig electConfig = new ActiveElectionConfig();
        String retryCount = getNodeValueIfExists(electElem, ACTIVE_ELECTION_RETRY_COUNT_TAG, String.valueOf(ActiveElectionConfig.DEFAULT_RETRY_COUNT));
        String yieldTime = getNodeValueIfExists(electElem, ACTIVE_ELECTION_YIELD_TIME_TAG, String.valueOf(ActiveElectionConfig.DEFAULT_YIELD_TIME));
        String resolutionTimeout = getNodeValueIfExists(electElem, ACTIVE_ELECTION_YIELD_TIME_TAG);

        electConfig.setRetryConnection(Integer.parseInt(retryCount));
        electConfig.setYieldTime(Long.parseLong(yieldTime));
        if (resolutionTimeout != null)
            electConfig.setResolutionTimeout(Long.parseLong(resolutionTimeout));

        Element fdElem = getFirstMatchElement(electElem, ACTIVE_ELECTION_FD_TAG);
        if (fdElem == null)
            return electConfig;
	   
	   /* fault-detection config */
        String fdInvDelay = getNodeValueIfExists(fdElem, ACTIVE_ELECTION_FD_INVOCATION_DELAY_TAG, String.valueOf(ActiveElectionConfig.DEFAULT_FAULTDETECTOR_INVOCATION_DELAY));
        String fdRetryCount = getNodeValueIfExists(fdElem, ACTIVE_ELECTION_FD_RETRY_COUNT_TAG, String.valueOf(ActiveElectionConfig.DEFAULT_FAULTDETECTOR_RETRY_COUNT));
        String fdRetryTimeout = getNodeValueIfExists(fdElem, ACTIVE_ELECTION_FD_RETRY_TIMEOUT_TAG, String.valueOf(ActiveElectionConfig.DEFAULT_FAULTDETECTOR_RETRY_TIMEOUT));

        electConfig.setFaultDetectorInvocationDelay(Long.parseLong(fdInvDelay));
        electConfig.setFaultDetectorRetryCount(Integer.parseInt(fdRetryCount));
        electConfig.setFaultDetectorRetryTimeout(Long.parseLong(fdRetryTimeout));

        return electConfig;
    }

    // create loadBalancing policy for specific group
    private FailOverPolicy createIfExistsFailOverPolicy(Element groupElem,
                                                        String activeGroupName,
                                                        ArrayList<String> groupMemberNames,
                                                        ArrayList<SpaceURL> groupMemberURLList,
                                                        Hashtable<String, FailOverPolicy> FOPolicies)
            throws CreateException {
        FailOverPolicy failOverPolicy = null;

        NodeList fp = groupElem.getElementsByTagName(FAIL_OVER_POLICY_TAG);
        if (fp.getLength() > 0) {
            // check whether policy-type tag exists
            Element fpElem = (Element) fp.item(0);

            // set fail over members group
            failOverPolicy = new FailOverPolicy();

            failOverPolicy.setActiveElectionConfig(createActiveElectConfig(fpElem));

            failOverPolicy.failOverGroupName = activeGroupName;
            failOverPolicy.failOverGroupMembersNames = (List<String>) groupMemberNames.clone();
            failOverPolicy.failOverGroupMembersURLs = (List<SpaceURL>) groupMemberURLList.clone();

            // keep all failOver policies, for satisfy alternatate recursion construction
            FOPolicies.put(activeGroupName, failOverPolicy);

            // FAIL_OVER_FIND_TIMEOUT_TAG
            String value = getNodeValueIfExists(fpElem, FAIL_OVER_FIND_TIMEOUT_TAG);
            if (value != null)
                failOverPolicy.spaceFinderTimeout = Long.parseLong(value);

            value = getNodeValueIfExists(fpElem, FAIL_OVER_FAILBACK_TAG);
            if (value != null)
                failOverPolicy.setFailBackEnabled(JSpaceUtilities.parseBooleanTag(FAIL_OVER_FAILBACK_TAG, value));


            // creates failover policy description
            failOverPolicy.m_WriteFOPolicy = createFailOverDescPolicy(fpElem, WRITE_TAG, activeGroupName, groupMemberNames);
            failOverPolicy.m_ReadFOPolicy = createFailOverDescPolicy(fpElem, READ_TAG, activeGroupName, groupMemberNames);
            failOverPolicy.m_TakeFOPolicy = createFailOverDescPolicy(fpElem, TAKE_TAG, activeGroupName, groupMemberNames);
            failOverPolicy.m_NotifyFOPolicy = createFailOverDescPolicy(fpElem, NOTIFY_TAG, activeGroupName, groupMemberNames);
            failOverPolicy.m_DefaultFOPolicy = createFailOverDescPolicy(fpElem, DEFAULT_TAG, activeGroupName, groupMemberNames);

            // throws exception if default failover policy not defined or not all operation defined in LB policy
            if (failOverPolicy.m_DefaultFOPolicy == null &&
                    (failOverPolicy.m_WriteFOPolicy == null ||
                            failOverPolicy.m_ReadFOPolicy == null ||
                            failOverPolicy.m_TakeFOPolicy == null ||
                            failOverPolicy.m_NotifyFOPolicy == null))
                throw new CreateException("Default FailOver Policy not defined in <" + activeGroupName + "> group. Check " + clusterConfigFile + " CLUSTER CONFIG XML file.");


            failOverPolicy.buildElectionGroups(_clusterMemberName);
        }

        return failOverPolicy;
    }

    // create failover policy description
    private FailOverPolicy.FailOverPolicyDescription createFailOverDescPolicy(Element failOverElem,
                                                                              String operationName, String groupName, List<String> groupMemberNames)
            throws CreateException {
        FailOverPolicy.FailOverPolicyDescription polDesc = null;

        NodeList operTypeNL = failOverElem.getElementsByTagName(operationName);
        if (operTypeNL.getLength() > 0) {
            polDesc = new FailOverPolicy.FailOverPolicyDescription();
            Element operElem = (Element) operTypeNL.item(0);
            String policyType = getNodeValueIfExists(operElem, POLICY_TYPE_TAG);

            if (policyType == null)
                throw new CreateException("\"policy-type\" tag or value not exists for [" +
                        operationName + "] operation under " + FAIL_OVER_POLICY_TAG + " tag.");

            // check if policy correct
            for (int i = 0; i < FAIL_OVER_POLICIES.size(); i++) {
                if (policyType.equalsIgnoreCase(FAIL_OVER_POLICIES.get(i)))
                    polDesc.m_PolicyType = i;
            }

            if (polDesc.m_PolicyType == -1)
                throw new CreateException("IllegalFailOverDefinitionError: Unknown failOver policy type: " + policyType + ".Check definition of " + groupName + " group in " + clusterConfigFile + " CLUSTER CONFIG XML file.");


            if (policyType.equalsIgnoreCase(FAIL_TO_BACKUP)) {
                // build backup members
                NodeList nlBackupMem = failOverElem.getElementsByTagName(FAIL_OVER_BACKUP_MEMBERS_TAG);
                if (nlBackupMem.getLength() <= 0)
                    throw new CreateException("IllegalFailOverDefinitionError: At lease one backup member should be defined in " + "\"" + FAIL_TO_BACKUP + "\" policy. Check definition of " + groupName + " group in " + clusterConfigFile + " CLUSTER CONFIG XML file.");

                // build backup member information for every member
                polDesc.m_BackupMemberNames = new HashMap<String, List<String>>();
                Element backupElem = (Element) nlBackupMem.item(0);
                NodeList nlMembers = backupElem.getElementsByTagName(MEMBER_TAG);

                if (nlMembers.getLength() <= 0)
                    throw new CreateException("IllegalFailOverDefinitionError: At lease one backup member should be defined in " + "\"" + FAIL_TO_BACKUP + "\" policy. Check definition of " + groupName + " group in " + clusterConfigFile + " CLUSTER CONFIG XML file.");

                for (int i = 0; i < nlMembers.getLength(); i++) {
                    List<String> backupMemList = new ArrayList<String>();
                    Element memberElem = (Element) nlMembers.item(i);
                    String memberName = getNodeValueIfExists(memberElem, FAIL_OVER_SOURCE_MEMBER_TAG);
                    NodeList nlBackupMembers = memberElem.getElementsByTagName(FAIL_OVER_BACKUP_MEMBER_TAG);

                    // build backupMembers hashtable
                    for (int j = 0; j < nlBackupMembers.getLength(); j++) {
                        String backupMemberName = nlBackupMembers.item(j).getFirstChild().getNodeValue();
                        if (!groupMemberNames.contains(backupMemberName))
                            throw new CreateException("IllegalFailOverDefinitionError: " + backupMemberName + " backup member not part of " + groupName + " group. Check " + clusterConfigFile + " CLUSTER CONFIG XML file.");

                        // check if source and backup not the same member
                        if (backupMemberName.equalsIgnoreCase(memberName))
                            throw new CreateException("IllegalFailOverDefinitionError: source member: " + memberName + " == " + backupMemberName + " backup member. Check definition of " + groupName + " group in " + clusterConfigFile + " CLUSTER CONFIG XML file.");

                        if (!backupMemList.contains(backupMemberName))
                            backupMemList.add(backupMemberName);
                    }// for j...

                    if (!groupMemberNames.contains(memberName))
                        throw new CreateException("IllegalFailOverDefinitionError: " + memberName + " source backup member not part of " + groupName + " group. Check " + clusterConfigFile + " CLUSTER CONFIG XML file.");

                    polDesc.m_BackupMemberNames.put(memberName, backupMemList);
                }// for i...

                // build backup-only info
                polDesc.m_BackupOnly = new ArrayList<String>();
                NodeList nlBackupOnly = failOverElem.getElementsByTagName(FAIL_OVER_BACKUP_MEMBERS_ONLY_TAG);
                if (nlBackupOnly.getLength() > 0) {
                    Element backupOnlyElem = (Element) nlBackupOnly.item(0);
                    nlMembers = backupOnlyElem.getElementsByTagName(FAIL_OVER_BACKUP_MEMBER_ONLY_TAG);
                    for (int i = 0; i < nlMembers.getLength(); i++) {
                        String backupOnlyMember = nlMembers.item(i).getFirstChild().getNodeValue();

                        // check if backup-only-member member of this group
                        if (!groupMemberNames.contains(backupOnlyMember))
                            throw new CreateException("IllegalFailOverDefinitionError: " + backupOnlyMember + " backup only member not part of " + groupName + " failOver group. Check " + clusterConfigFile + " CLUSTER CONFIG XML file.");

                        // check if backup-only-member defined like a source member in backup list
                        if (polDesc.m_BackupMemberNames.get(backupOnlyMember) != null)
                            throw new CreateException("IllegalFailOverDefinitionError: " + backupOnlyMember + " source member can't be defined as backup only member. Check " + clusterConfigFile + " CLUSTER CONFIG XML file.");

                        // prevent backup-only-member duplication
                        if (!polDesc.m_BackupOnly.contains(backupOnlyMember))
                            polDesc.m_BackupOnly.add(backupOnlyMember);
                    }
                }// if nlBackupOnly.getLength() > 0
            }// if ( policyType.equalsIgnoreCase( FAIL_TO_BACKUP ) )
        }// if operTypeNL.getLength() > 0

        return polDesc;
    }

    // create loadBalancing policy for specific group
    private LoadBalancingPolicy createIfExistsLoadBalancingPolicy(Element groupElem,
                                                                  String activeGroupName,
                                                                  ArrayList<String> groupMemberNames,
                                                                  ArrayList<SpaceURL> groupMemberURLList)
            throws CreateException {
        LoadBalancingPolicy loadBalPolicy = null;

        NodeList loadBalNodeList = groupElem.getElementsByTagName(LOAD_BAL_TAG);
        if (loadBalNodeList.getLength() > 0) {
            Element lbElem = (Element) loadBalNodeList.item(0);

            // create load balancing policy
            loadBalPolicy = new LoadBalancingPolicy();
            loadBalPolicy.m_GroupName = activeGroupName;
            loadBalPolicy.loadBalanceGroupMembersNames = (List<String>) groupMemberNames.clone();
            loadBalPolicy.loadBalanceGroupMembersURLs = (List<SpaceURL>) groupMemberURLList.clone();


            String applyOwnershipStr = getNodeValueIfExists(lbElem, APPLY_OWNERSHIP_TAG);
            //get apply ownership value
            loadBalPolicy.m_ApplyOwnership = applyOwnershipStr != null ? JSpaceUtilities.parseBooleanTag(APPLY_OWNERSHIP_TAG, applyOwnershipStr) : false;

            String disaleParallelScatteringStr = getNodeValueIfExists(lbElem, DISABLE_PARALLEL_SCATTERING_TAG);
            //get disableParallelScattering value
            loadBalPolicy.m_DisableParallelScattering = disaleParallelScatteringStr != null ? JSpaceUtilities.parseBooleanTag(DISABLE_PARALLEL_SCATTERING_TAG, disaleParallelScatteringStr) : false;

            String minValue = getNodeValueIfExists(lbElem, PROXY_BROADCAST_THREADPOOL_MIN_SIZE_TAG);
            if (minValue != null)
                loadBalPolicy.m_broadcastThreadpoolMinSize = Integer.parseInt(minValue);

            String maxValue = getNodeValueIfExists(lbElem, PROXY_BROADCAST_THREADPOOL_MAX_SIZE_TAG);
            if (maxValue != null)
                loadBalPolicy.m_broadcastThreadpoolMaxSize = Integer.parseInt(maxValue);

            // create load balancing description
            loadBalPolicy.m_WriteOperationsPolicy = createLoadBalancingDescPolicy(lbElem, WRITE_TAG);
            loadBalPolicy.m_ReadOperationsPolicy = createLoadBalancingDescPolicy(lbElem, READ_TAG);
            loadBalPolicy.m_TakeOperationsPolicy = createLoadBalancingDescPolicy(lbElem, TAKE_TAG);
            loadBalPolicy.m_NotifyOperationsPolicy = createLoadBalancingDescPolicy(lbElem, NOTIFY_TAG);
            loadBalPolicy.m_DefaultPolicy = createLoadBalancingDescPolicy(lbElem, DEFAULT_TAG);

            // throws exception if default load balancing policy not defined or not all operation defined in LB policy
            if (loadBalPolicy.m_DefaultPolicy == null &&
                    (loadBalPolicy.m_WriteOperationsPolicy == null ||
                            loadBalPolicy.m_ReadOperationsPolicy == null ||
                            loadBalPolicy.m_TakeOperationsPolicy == null ||
                            loadBalPolicy.m_NotifyOperationsPolicy == null))
                throw new CreateException("Default LoadBalancing Policy not defined in group <" + activeGroupName + ">. Check " + clusterConfigFile + " CLUSTER CONFIG XML file.");
        }

        return loadBalPolicy;
    }


    // create loadbalancing description
    private LoadBalancingPolicy.LoadBalancingPolicyDescription createLoadBalancingDescPolicy(Element loadBalanceElem, String operationName)
            throws CreateException {
        Properties prop = new Properties();
        LoadBalancingPolicy.LoadBalancingPolicyDescription loadBalanceDesc = null;

        NodeList operTypeNL = loadBalanceElem.getElementsByTagName(operationName);
        if (operTypeNL.getLength() > 0) {
            Element operElem = (Element) operTypeNL.item(0);
            String policyType = getNodeValueIfExists(operElem, POLICY_TYPE_TAG);

            if (policyType == null)
                throw new CreateException("\"policy-type\" tag or value not exists for [" +
                        operationName + "] operation under " + LOAD_BAL_TAG + " tag.");


            String broadcastCondition = getNodeValueIfExists(operElem, BROADCAST_CONDITION_TAG);

            // get param-name and param-value
            NodeList paramNL = operElem.getElementsByTagName(PARAM_TAG);
            for (int z = 0; z < paramNL.getLength(); z++) {
                String paramName = getNodeValueIfExists((Element) paramNL.item(z), PARAM_NAME_TAG);
                String paramValue = getNodeValueIfExists((Element) paramNL.item(z), PARAM_VALUE_TAG);

                if (paramName == null)
                    throw new CreateException("\"param-name\" tag or value not exists for " +
                            operationName + " operation name under " + LOAD_BAL_TAG + " tag. Check " + clusterConfigFile + " cluster config file.");

                if (paramValue == null)
                    throw new CreateException("\"param-value\" tag or value not exists for " +
                            operationName + " operation name under " + LOAD_BAL_TAG + " tag. Check " + clusterConfigFile + " cluster config file.");

                // set param property
                prop.setProperty(paramName, paramValue);
            } // for

            // init loadBalacingDescription
            loadBalanceDesc = new LoadBalancingPolicy.LoadBalancingPolicyDescription();
            loadBalanceDesc.m_PolicyType = policyType;
            loadBalanceDesc.m_Properties = (prop.size() > 0) ? prop : null;
            loadBalanceDesc.setBroadcastConditionDescription(broadcastCondition);
        } // if operation exists...

        return loadBalanceDesc;
    }


    // creates cluster member properties
    private void createClusterMemberProperties(Document parentNode)
            throws CreateException {
        // get cluster-members node
        NodeList cml = parentNode.getElementsByTagName(CLUSTER_MEMBERS_TAG);
        if (cml.getLength() == 0)
            throw new CreateException("<cluster-members> tag not found in " + clusterConfigFile + " cluster config file.");

        // creating HashMap of properties
        clusterPolicy.m_ClusterMembersProperties = new HashMap<String, Properties>();
        clusterPolicy.m_AllClusterMemberList = new ArrayList<String>();

        Element clusMemElem = (Element) cml.item(0);
        NodeList memberNodeList = clusMemElem.getElementsByTagName(MEMBER_TAG);

        // iterate between member nodes
        for (int i = 0; i < memberNodeList.getLength(); i++) {
            Properties prop = new Properties();
            String memberName = getNodeValueIfExists((Element) memberNodeList.item(i), MEMBER_NAME_TAG);
            clusterPolicy.m_AllClusterMemberList.add(memberName);

            // get param-name and param-value
            NodeList paramNL = ((Element) memberNodeList.item(i)).getElementsByTagName(PARAM_TAG);
            for (int z = 0; z < paramNL.getLength(); z++) {
                String paramName = getNodeValueIfExists((Element) paramNL.item(z), PARAM_NAME_TAG);
                String paramValue = getNodeValueIfExists((Element) paramNL.item(z), PARAM_VALUE_TAG);

                if (paramName == null)
                    throw new CreateException("\"param-name\" tag or value not exists for " +
                            memberName + " member name. Check " + clusterConfigFile + " cluster config file.");

                if (paramValue == null)
                    throw new CreateException("\"param-value\" tag or value not exists for " +
                            memberName + " member name. Check " + clusterConfigFile + " cluster config file.");

                // set param property
                prop.setProperty(paramName, paramValue);
            } // for ParamNL

            // set cluster member policy
            if (paramNL.getLength() > 0)
                clusterPolicy.m_ClusterMembersProperties.put(memberName, prop);
        } // for memberNodeList
    }

    // returns the first matched element by element name
    static public Element getFirstMatchElement(Element elem, String elemName) {
        NodeList nl = elem.getElementsByTagName(elemName);

        if (nl.getLength() > 0)
            return (Element) nl.item(0);

        return null;
    }

    // return the node value
    static public String getNodeValueIfExists(Element parentNode, String nodeName) {
        String value = null;
        NodeList nl = parentNode.getElementsByTagName(nodeName);

        if (nl.getLength() > 0) {
            Node child = nl.item(0).getFirstChild();
            if (child != null)
                value = child.getNodeValue().trim();
        }

        return System.getProperty(getFullPath(parentNode, nodeName), value);
    }

    static private String getFullPath(Element parentNode, String nodeName) {
        StringBuilder stringBuilder = new StringBuilder();
        buildPath(stringBuilder, parentNode);
        stringBuilder.append('.').append(nodeName);
        return stringBuilder.toString();
    }


    static private void buildPath(StringBuilder result, Node node) {
        if (!(node.getParentNode() instanceof Element)) {
            result.append(node.getNodeName());
            return;
        }
        buildPath(result, node.getParentNode());
        result.append('.').append(node.getNodeName());
    }

    /* return node value if exists otherwise returns a default value */
    static public String getNodeValueIfExists(Element parentNode, String nodeName, String defaultValue) {
        String value = getNodeValueIfExists(parentNode, nodeName);

        return value != null ? value : defaultValue;
    }

    //return the node value
    static public String getNodeValueIfExists(Document doc, String nodeName) {
        String value = null;
        NodeList nl = doc.getElementsByTagName(nodeName);

        if (nl.getLength() > 0) {
            Node child = nl.item(0).getFirstChild();
            if (child != null)
                value = child.getNodeValue().trim();
        }

        return value;
    }

    public static boolean calculateOneWayReplicationValue(String replicationMode) {
        return replicationMode.endsWith(REPL_RECEIVER_ACK_POSTFIX);
    }

    public static boolean calculateSyncReplicationValue(String replicationMode) {

        return replicationMode.startsWith(ReplicationPolicy.SYNC_REPLICATION_MODE);
    }

    public static String calculateReplicationMode(boolean isSyncReplication, boolean isOneWayReplication) {
        String returnedVal = null;
        if (isSyncReplication) {
            if (isOneWayReplication)
                returnedVal = ReplicationPolicy.SYNC_REC_ACK_REPLICATION_MODE;
            else
                returnedVal = ReplicationPolicy.SYNC_REPLICATION_MODE;
        } else {
            returnedVal = ReplicationPolicy.ASYNC_REPLICATION_MODE;
        }

        return returnedVal;
    }

    /**
     * Create default load balancing policy, if defined in XML file FailOver without LoadBalancing.
     **/
    private LoadBalancingPolicy createDefaultLoadBalacingPolicy(String activeGroupName,
                                                                List<String> groupMemberNames,
                                                                List<SpaceURL> groupMemberURL) {
        LoadBalancingPolicy loadBalPolicy = new LoadBalancingPolicy();
        loadBalPolicy.m_GroupName = activeGroupName;
        loadBalPolicy.loadBalanceGroupMembersNames = groupMemberNames;
        loadBalPolicy.loadBalanceGroupMembersURLs = groupMemberURL;
        loadBalPolicy.m_DefaultPolicy = new LoadBalancingPolicy.LoadBalancingPolicyDescription();
        loadBalPolicy.m_DefaultPolicy.setBroadcastCondition(BroadcastCondition.ROUTING_INDEX_IS_NULL);
        loadBalPolicy.m_DefaultPolicy.m_PolicyType = "local-space";

        return loadBalPolicy;
    }


    /**
     * Creates replication policy description for every repl group member.
     *
     * @param sourceMemberName Name of member for which creating ReplicationPolicyDescription
     * @param groupMemberNode  Member XML Element
     * @return ReplicationPolicy.ReplicationPolicyDescription
     */
    public ReplicationPolicy.ReplicationPolicyDescription createReplDescPolicy(String sourceMemberName, Element groupMemberNode, List<String> groupMemList)
            throws CreateException {
        Element replPolicyElem = null;
        ReplicationPolicy.ReplicationPolicyDescription replDescPolicy = new ReplicationPolicy.ReplicationPolicyDescription();

        // get <group> tag for this member
        Element groupElem = (Element) groupMemberNode.getParentNode().getParentNode();
        NodeList replPolicyNL = groupElem.getElementsByTagName(REPL_POLICY_TAG);

        // if replication-policy tag not found, replication not defined in cluster ( group level )
        if (replPolicyNL.getLength() == 0)
            return replDescPolicy;

        // replication policy tag <repl-policy> tag
        replPolicyElem = (Element) replPolicyNL.item(0);

        // build member replication filters
        NodeList replFilterNL = groupMemberNode.getElementsByTagName(REPL_FILTERS_TAG);
        if (replFilterNL.getLength() > 0) {
            Element replFilterElem = (Element) replFilterNL.item(0);
            replDescPolicy.inputReplicationFilterClassName = getNodeValueIfExists(replFilterElem, REPL_INPUT_FILTER_CLASSNAME_TAG);
            replDescPolicy.inputReplicationFilterParamUrl = getNodeValueIfExists(replFilterElem, REPL_INPUT_FILTER_PARAM_URL_TAG);
            replDescPolicy.outputReplicationFilterClassName = getNodeValueIfExists(replFilterElem, REPL_OUTPUT_FILTER_CLASSNAME_TAG);
            replDescPolicy.outputReplicationFilterParamUrl = getNodeValueIfExists(replFilterElem, REPL_OUTPUT_FILTER_PARAM_URL_TAG);

            String tmp = getNodeValueIfExists(replFilterElem, REPL_ACTIVE_WHEN_BACKUP_TAG);
            // set the default value
            if (tmp == null || tmp.length() == 0)
                tmp = REPL_ACTIVE_WHEN_BACKUP_DEFAULT;

            replDescPolicy.activeWhenBackup = JSpaceUtilities.parseBooleanTag(REPL_ACTIVE_WHEN_BACKUP_TAG, tmp);

            tmp = getNodeValueIfExists(replFilterElem, REPL_SHUTDOWN_SPACE_ON_INIT_FAILURE_TAG);
            // set the default value
            if (tmp == null || tmp.length() == 0)
                tmp = REPL_SHUTDOWN_SPACE_ON_INIT_FAILURE_DEFAULT;

            replDescPolicy.shutdownSpaceOnInitFailure = JSpaceUtilities.parseBooleanTag(REPL_SHUTDOWN_SPACE_ON_INIT_FAILURE_TAG, tmp);
        }// if replFilterNL ...

        // if for member not defined recovery, set memory-recovery boolean of group into this member
        NodeList replRecoveryNL = groupMemberNode.getElementsByTagName(REPL_MEMBER_RECOVERY_TAG);
        if (replRecoveryNL.getLength() > 0) {
            if (replPolicyElem == null)
                throw new CreateException("IllegalReplicationDefinitionError: Wrong memory recovery " + sourceMemberName + " definition. <" + REPL_POLICY_TAG + "> tag not found. Check " + clusterConfigFile + " cluster config file.");

            Element replRecoveryElem = (Element) replRecoveryNL.item(0);
            String recovery = getNodeValueIfExists(replRecoveryElem, ENABLED_TAG);
            replDescPolicy.memberRecovery = JSpaceUtilities.parseBooleanTag(ENABLED_TAG, recovery);
            String sourceMember = getNodeValueIfExists(replRecoveryElem, REPL_SOURCE_MEMBER_URL_TAG);

            // not insert to replDescPolicy if member = FIRST_AVAILABLE_MEMBER
            if (sourceMember != null && !sourceMember.equals(FIRST_AVAILABLE_MEMBER))
                replDescPolicy.sourceMemberRecovery = sourceMember;

            // recovery source member
            if (replDescPolicy.sourceMemberRecovery != null) {
                // check that member doesn't not recovery from it self
                if (replDescPolicy.sourceMemberRecovery.equalsIgnoreCase(sourceMemberName))
                    throw new CreateException("IllegalReplicationDefinitionError: " + sourceMemberName + " member: Wrong recovery source member: " + replDescPolicy.sourceMemberRecovery + ". The member: " + sourceMemberName + " can not recovery from it self. Check " + clusterConfigFile + " cluster config file.");

                // check if target member, member of replication group
                if (!groupMemList.contains(replDescPolicy.sourceMemberRecovery) && !replDescPolicy.sourceMemberRecovery.equalsIgnoreCase(FIRST_AVAILABLE_MEMBER))
                    throw new CreateException("IllegalReplicationDefinitionError: " + sourceMemberName + " member: Unknown recovery source member: " + replDescPolicy.sourceMemberRecovery + ". The source member is not in replication group. Check " + clusterConfigFile + " cluster config file.");
            }// if ...
        }// if replRecoveryNL ...
        else {
            // get memory recovery status of repl-group
            String recovery = getNodeValueIfExists(replPolicyElem, REPL_MEMORY_RECOVERY_TAG);
            replDescPolicy.memberRecovery = JSpaceUtilities.parseBooleanTag(REPL_MEMORY_RECOVERY_TAG, recovery);
        }

        return replDescPolicy;
    }/* createReplDescPolicy() */

    /**
     * return root <code>Document</code> of parsed XML file.
     */
    public Document getXMLRootDocument() {
        return m_rootDoc;
    }

    /**
     * returns current cluster config file.
     */
    public String getClusterFileURL() {
        return clusterConfigFile;
    }


    /**
     * Find desired member in replication group. Return <code>org.w3c.dom.Element</code> of
     * <code>sourceMember</code> in the found replication group. For example the desired member is :
     * "pc-igor:sp2" <groups> <group> <group-name>repl-group</group-name> <group-members> <member>
     * <member-name>pc-igor:sp1</member-name> </member> <member> <-- This Element will return
     * <member-name>pc-igor:sp2</member-name> </member> </group-members> </group> </groups>
     *
     * @param sourceMember The member to find in XML file.
     * @return Element Return <code>org.w3c.dom.Element</code> of <code>sourceMember</code>, or
     * <code>null</code> if not found.
     **/
    private Element findMemberInReplGroup(String sourceMember) {
        // get groups node
        NodeList gl = m_rootDoc.getElementsByTagName(GROUP_TAG);

        for (int i = 0; i < gl.getLength(); i++) {
            // find desired members in replication group
            Element groupElem = (Element) gl.item(i);
            NodeList repNodeList = groupElem.getElementsByTagName(REPL_POLICY_TAG);
            if (repNodeList.getLength() > 0) {
                NodeList ml = groupElem.getElementsByTagName(MEMBER_NAME_TAG);

                for (int j = 0; j < ml.getLength(); j++) {
                    Node memNode = ml.item(j).getFirstChild();

                    // check if source member exists in this repl group
                    if (memNode.getNodeValue().equals(sourceMember)) {
                        return (Element) ml.item(j).getParentNode();
                    }
                }// for j ...
            }
        }// for i ...

        // if we reach here the source member not found in none of the repl group
        return null;
    }


    /**
     * Build new member tag. Add the new member tag to the group where <code>ownerMemberName</code>
     * located and to <cluster-members> tag.
     *
     * @param ownerMemberName  Owner group member.
     * @param targetMemberName The new member name.
     * @param targetMemberURL  Member URL.
     * @throws ClusterException Throws if the <code>ownerMemberName</code> not found in no
     *                          replication group.
     * @see #addMember(String, String, String)
     * @see #findMemberInReplGroup(String)
     **/
    private void addMemberToReplGroup(String ownerMemberName, String targetMemberName, String targetMemberURL)
            throws ClusterException {
        Element memElem = findMemberInReplGroup(ownerMemberName);
        if (memElem == null)
            throw new ClusterException(ownerMemberName + " couldn't found in no replication group.");

        // create and append new xml member tags to the group
        Element memberElem = m_rootDoc.createElement(MEMBER_TAG);
        Element memberNameElem = m_rootDoc.createElement(MEMBER_NAME_TAG);
        Text textNode = m_rootDoc.createTextNode(targetMemberName);

        // connect xml tag with main group tag
        Element groupMemNode = (Element) memElem.getParentNode();
        groupMemNode.appendChild(memberElem);
        memberElem.appendChild(memberNameElem).appendChild(textNode);

        // clone memberElem for appending to the cluster-members tag
        Node rootClusMemNode = m_rootDoc.getElementsByTagName(CLUSTER_MEMBERS_TAG).item(0);
        Node clusMemElem = memberElem.cloneNode(true);
        Element memberElemURL = m_rootDoc.createElement(MEMBER_URL_TAG);
        Text textURL = m_rootDoc.createTextNode(targetMemberURL);
        rootClusMemNode.appendChild(clusMemElem).appendChild(memberElemURL).appendChild(textURL);

		/* TODO DYMANIC MEMBER TRANSMISSION MATRIX SUPPORT
	try
	{
	  // add disabled transmission matrix for all members, ownerMemberName
	  URL transURL = getClass().getResource( TRANSMISSION_POLICY_TEMPLATE );

	  // Obtaining a org.w3c.dom.Document from XML
	  DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	  DocumentBuilder builder = factory.newDocumentBuilder();
	  Document transDoc = builder.parse( transURL.openStream() );
	  Element transElem = transDoc.getDocumentElement();


	  Element[] memElemArr = getGroupMembersElements( groupMemNode );
	  for( int i = 0; i < memElemArr.length; i++  )
	  {
	    Element transElemClone = (Element)transElem.cloneNode(true);
		Element tarElem = (Element)transElemClone.getElementsByTagName( REPL_TARGET_MEMBER_TAG ).item(0);

		System.out.println("Target Elem: " + tarElem.getFirstChild().getNodeValue());

		tarElem.getFirstChild().setNodeValue(targetMemberName);

		// add transmission matrix
		memElemArr[i].appendChild( transElemClone );
	  }
	}catch( Exception ex )
	{
	  ex.printStackTrace();
	  System.exit(1);
	}
		 */
    }

    /**
     * Remove member tag from replication group and <cluster-member> tag.
     *
     * @param targetMemberName The target member to remove.
     * @throws ClusterException Throws if <code>targetMember</code> not found in no replication
     *                          group.
     **/
    private void removeMemberFromReplGroup(String targetMemberName)
            throws ClusterException {
        Element tarMemElem = findMemberInReplGroup(targetMemberName);
        if (tarMemElem == null)
            throw new ClusterException(targetMemberName + " couldn't found in no replication group.");

        // remove targetMemeber from parent
        Node parentNode = tarMemElem.getParentNode();
        parentNode.removeChild(tarMemElem);

        // also remove the member from cluster-members tag
        Element clusMemElem = (Element) m_rootDoc.getElementsByTagName(CLUSTER_MEMBERS_TAG).item(0);
        NodeList memNodeList = clusMemElem.getElementsByTagName(MEMBER_NAME_TAG);
        for (int v = 0; v < memNodeList.getLength(); v++) {
            String memName = memNodeList.item(v).getFirstChild().getNodeValue().trim();
            if (memName.equalsIgnoreCase(targetMemberName)) {
                // remove the target member from <cluster-members> tag
                Node memElem = memNodeList.item(v).getParentNode();
                clusMemElem.removeChild(memElem);

                break;
            }
        }
    }


    /**
     * Add <code>targetMember</code> member in the same replication group of
     * <code>ownerMemberName</code> member and update Cluster XML file.
     *
     * @param ownerMemberName  The owner member.
     * @param targetMemberName The target member to add.
     * @param targetMemberURL  The target member URL.
     * @throws ClusterException Throws if <code>ownderMemberName</code> not found in no replication
     *                          group, or failed to update cluster xml file.
     **/
    public void addMember(String ownerMemberName, String targetMemberName, String targetMemberURL)
            throws ClusterException {
        // add member to repl group
        addMemberToReplGroup(ownerMemberName, targetMemberName, targetMemberURL);

        try {
            // save <code>clusterConfigFile</code> XML file
            saveFile();
        } catch (FileNotFoundException ex) {
            throw new ClusterException("Failed to add " + targetMemberName +
                    " member to " + clusterConfigFile + ". " + ex.toString(), ex);
        }
    }


    /**
     * Remove <code>targetMemberName</code> member from replication group and update Cluster XML
     * file.
     *
     * @param targetMemberName The member to remove from XML file.
     * @throws ClusterException Throws if the <code>targetMemberName</code> not found in no
     *                          replication group or failed to update Cluster XML file.
     **/
    public void removeMember(String targetMemberName)
            throws ClusterException {
        // remove member from first repl Group
        removeMemberFromReplGroup(targetMemberName);

        try {
            // save <code>clusterConfigFile</code> XML file
            saveFile();
        } catch (FileNotFoundException ex) {
            throw new ClusterException("Failed to remove " + targetMemberName +
                    " member from " + clusterConfigFile + ". " + ex.toString(), ex);
        }
    }


    /**
     * Change member info in replication group and update xml file.
     *
     * @param sourceMember    Source member name.
     * @param targetMember    Target name
     * @param targetMemberURL Target URL
     * @throws ClusterException Throws if <code>sourceMember</code> not found in XML file.
     * @see #addMember(String, String, String)
     * @see #removeMember(String)
     */
    public void changeMember(String sourceMember, String targetMember, String targetMemberURL)
            throws ClusterException {
        // add to group
        addMember(sourceMember, targetMember, targetMemberURL);

        // remove sourceMember from group
        removeMember(sourceMember);
    }


    /**
     * Save the current DOM Tree structure into <code>clusterConfigFile</code> XML file.
     *
     * @throws FileNotFoundException Throws if <code>clusterConfigFile</code> not found.
     **/
    private void saveFile()
            throws FileNotFoundException {
        // creating new xxx.xml file
        PrintStream psStream = new PrintStream(new FileOutputStream(clusterConfigFile));
        JSpaceUtilities.domWriter(m_rootDoc.getDocumentElement(), psStream, "");
        psStream.close();
    }


    private void printClusterConfigDebug(InputStream _clusterXSLPolicy, Element _membersXMLDomElement,
                                         Document _finalClusterConfigDomElement, Transformer _transformer, String _originMsg) {
        try {
            if (clusterConfigDebugOutput == null)
                clusterConfigDebugOutput = new StringBuilder();
            if (_originMsg != null) {
                clusterConfigDebugOutput.append("\n\n==============================================================================================\n");
                clusterConfigDebugOutput.append("Cluster Configuration: " + _originMsg +
                        "\n---------------------------------------------------------------------------------------- \n");
                if (!doEnvDump && _logger.isLoggable(Level.INFO)) {
                    _logger.info(clusterConfigDebugOutput.toString());
                }
            }
            if (_clusterXSLPolicy != null) {
                clusterConfigDebugOutput.append("\nCluster XSL: " +
                        "\n---------------------------------------------------------------------------------------- \n");
                StringBuilder sb = new StringBuilder();
                String line = null;
                BufferedReader in = new BufferedReader(new InputStreamReader(_clusterXSLPolicy));
                while ((line = in.readLine()) != null) {
                    sb.append(line).append('\n');
                }
                String xslPolicy = sb.toString();
                clusterConfigDebugOutput.append(xslPolicy);
                clusterConfigDebugOutput.append("\n ---------------------------------------------------------------------------------------- \n");
                if (!doEnvDump && _logger.isLoggable(Level.INFO)) {
                    _logger.info(xslPolicy +
                            "\n ---------------------------------------------------------------------------------------- \n");
                }
            }

            if (_membersXMLDomElement != null) {
                //FOR FLUSHING the xml to sys out for observation in debug
                clusterConfigDebugOutput.append("\nCluster members XML Document:" +
                        "\n---------------------------------------------------------------------------------------- \n");
                if (!doEnvDump && _logger.isLoggable(Level.INFO)) {
                    _logger.info("\nCluster members XML Document:" +
                            "\n ---------------------------------------------------------------------------------------- \n");
                }
                //use the XSLT transformation package to output the DOM tree we just created
                //Create an empty DOMResult for the Result.
                DOMResult domResult = new DOMResult();
                _transformer.transform(new DOMSource(_membersXMLDomElement), domResult);
                //Instantiate an Xalan XML serializer and use it to serialize the output DOM to System.out
                // using a default output format.
                ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                JSpaceUtilities.domWriter(domResult.getNode(), new PrintStream(byteArray), "");
                byteArray.flush();
                byteArray.close();
                String output = new String(byteArray.toByteArray());
                clusterConfigDebugOutput.append(output);
                clusterConfigDebugOutput.append("\n ---------------------------------------------------------------------------------------- \n");
                if (!doEnvDump && _logger.isLoggable(Level.INFO)) {
                    _logger.info(output +
                            "\n ---------------------------------------------------------------------------------------- \n");
                }
            }

            if (_finalClusterConfigDomElement != null) {
                //FOR FLUSHING the xml to sys out for observation in debug
                clusterConfigDebugOutput.append("\nTransformed Cluster Config XML Document:" +
                        "\n---------------------------------------------------------------------------------------- \n");
                if (!doEnvDump) {
                    if (_logger.isLoggable(Level.INFO)) {
                        _logger.info("\nTransformed Cluster Config XML Document:" +
                                "\n ---------------------------------------------------------------------------------------- \n");
                    }
                }
                //use the XSLT transformation package to output the DOM tree we just created
                //Create an empty DOMResult for the Result.
                DOMResult domResult = new DOMResult();
                _transformer.transform(new DOMSource(_finalClusterConfigDomElement), domResult);
                //Instantiate an Xalan XML serializer and use it to serialize the output DOM to System.out
                // using a default output format.
                ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                JSpaceUtilities.domWriter(domResult.getNode(), new PrintStream(byteArray), "");
                byteArray.flush();
                byteArray.close();
                String output = new String(byteArray.toByteArray());
                clusterConfigDebugOutput.append(output);
                clusterConfigDebugOutput.append("\n ---------------------------------------------------------------------------------------- \n");
                if (!doEnvDump) {
                    if (_logger.isLoggable(Level.INFO)) {
                        _logger.info(output +
                                "\n ---------------------------------------------------------------------------------------- \n");
                    }
                }
            }
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, "failed to print cluster configuration debug output.", e);
            }
        }
    }

    /**
     * @return Returns the clusterConfigDebugOutput of the Cluster Configuration structure if debug
     * mode is turned on.
     */
    public StringBuilder getClusterConfigDebugOutput() {
        return clusterConfigDebugOutput;
    }

    private static boolean doEnvDump = Boolean.getBoolean(SystemProperties.ENV_REPORT);

    /**
     * @return true if -Dcom.gs.clusterXML.debug=true AND -Dcom.gs.env.report=false Since if
     * -Dcom.gs.env.dump=true we any way will dump all configurations incld. teh cluster config so
     * we prevent duplicated output.
     */
    private boolean isClusterXMLInDebugMode() {
        if (doEnvDump || m_debugMode)
            return true;
        return false;
    }

    public static boolean supportsBackup(String clusterSchema) {
        return clusterSchema.equalsIgnoreCase(CLUSTER_SCHEMA_NAME_PARTITIONED) ||
                clusterSchema.equalsIgnoreCase(CLUSTER_SCHEMA_NAME_PARTITIONED_SYNC2BACKUP) ||
                clusterSchema.equalsIgnoreCase(CLUSTER_SCHEMA_NAME_PRIMARY_BACKUP);
    }

    public static List<String> getFailOverPolicies() {
        return FAIL_OVER_POLICIES;
    }
} // end of class
