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


package com.j_spaces.core.client;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionState;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.internal.utils.CollectionUtils;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.LookupManager;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.XPathProperties;
import com.j_spaces.lookup.entry.ClusterGroup;
import com.j_spaces.lookup.entry.ClusterName;
import com.j_spaces.lookup.entry.ContainerName;
import com.j_spaces.lookup.entry.GenericEntry;
import com.j_spaces.lookup.entry.State;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.lookup.entry.Name;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@Deprecated

public class SpaceURL extends Properties implements Externalizable {
    private static final long serialVersionUID = 1L;

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE_URL);

    /**
     * Holds a list of all the non serialized custom properties to the will be excluded during
     * serialization. This is due to the fact that within this custom properties we pass actual
     * instnaces (of Filter provider for example) which are not serializable (and we only need it
     * for this local embedded Space).
     */
    private static final String[] NON_SERIALIZED_CUSTOM_PROPS = {
            Constants.SqlFunction.USER_SQL_FUNCTION,
            Constants.Space.SPACE_CONFIG,
            Constants.Filter.FILTER_PROVIDERS,
            Constants.DataAdapter.DATA_SOURCE,
            Constants.ReplicationFilter.REPLICATION_FILTER_PROVIDER,
            Constants.Security.USER_DETAILS,
            Constants.Security.CREDENTIALS_PROVIDER,
            Constants.CacheManager.CACHE_MANAGER_EVICTION_STRATEGY_PROP,
            Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_PROP,
            Constants.DataAdapter.SPACE_DATA_SOURCE,
            Constants.DataAdapter.SPACE_SYNC_ENDPOINT,
            Constants.DirectPersistency.DIRECT_PERSISTENCY_ATTRIBURE_STORE_PROP};

    /**
     * Represents <code>*</code> symbol.
     **/
    public final static String ANY = "*";

    /**
     * jini:// protocol.
     **/
    public final static String JINI_PROTOCOL = "jini:";

    /**
     * jini:// protocol.
     **/
    public final static String FILE_PROTOCOL = "file:";

    /**
     * rmi:// protocol.
     **/
    public final static String RMI_PROTOCOL = "rmi:";

    /**
     * java:// protocol.
     **/
    public final static String EMBEDDED_SPACE_PROTOCOL = "java:";

    /**
     * Contains a set of actual space protocols.
     */
    private final static Set<String> AVAILABLE_PROTOCOLS = CollectionUtils.toUnmodifiableSet(
            JINI_PROTOCOL, RMI_PROTOCOL, EMBEDDED_SPACE_PROTOCOL, FILE_PROTOCOL);


    /**
     * <pre>
     * <code>jini://</code> multicast support.
     * LookupService group to find Container or Space using multicast jini:// protocol.
     * This attribute will be ignored in unicast protocol jini://lookuphost/containerName/JavaSpaces,
     *
     * Example:
     * jini://<code>*</code>/containerName/?groups=fenix,pantera
     * jini://<code>*</code>/containerName/spaceName?groups=grid
     * Default: public
     * </pre>
     **/
    public final static String GROUPS = "groups";

    /**
     * <pre>
     * <code>jini://</code> unicast support.
     * The comma delimited Jini Locators list.
     * This list will be used in the LookupLocators unicast registration for the container/space.
     * When creating a space using SpaceFinder:
     * SpaceFinder.find("/./Space?groups=g1,g2,g3&locators=h1:port,h2:port,h3:port).
     * The locators property value will override the <lus_host> container schema value.
     * When client discovers a space:
     * The locators URL property should be used to perform unicast LUS discovery.
     * To enable both multicast and unicast discovery the following Space URL should be used.
     * SpaceFinder.find("jini:////Space?groups=g1,g2,g3&locators=h1:port,h2:port,h3:port).
     * </pre>
     **/
    public final static String LOCATORS = "locators";

    /**
     * <pre>
     * <code>jini://</code> multicast support.
     * LookupService attribute to find Space using multicast jini:// protocol in specified cluster.
     * This attribute will be ignored in unicast protocol jini://lookuphost/containerName/JavaSpaces,
     *
     * Example:
     * jini://<code>*</code>/containerName/JavaSpaces?clustername=myClusterTest
     *
     * Find JavaSpaces in <code>myClusterTest</code>, doesn't matter which container name.
     * jini://<code>*</code>/<code>*</code>/JavaSpaces?clustername=myClusterTest
     *
     * Find any space in <code>myClusterTest</code>, doesn't matter which container name and space
     * name.
     * jini://<code>*</code>/<code>*</code>/<code>*</code>?clustername=myClusterTest
     *
     * Default: null
     * </pre>
     **/
    public final static String CLUSTER_NAME = "clustername";

    /**
     * <pre>
     * <code>jini://</code> multicast support.
     * Clustered group to find Container or Space using multicast jini:// protocol.
     * This attribute will be ignored in unicast protocol jini://lookuphost/containerName/JavaSpaces,
     *
     * Example:
     * jini://<code>*</code>/containerName/?clustergroup=fenix,pantera
     * jini://<code>*</code>/containerName/JavaSpaces?clustergroup=test
     * Default: null
     * </pre>
     **/
    public final static String CLUSTER_GROUP = "clustergroup";

    /**
     * <pre>
     * <code>jini://</code> multicast support.
     * The maximum timeout in ms to find Container or Space using multicast jini:// protocol.
     * This attribute will be ignored in unicast protocol jini://lookuphost/containerName/JavaSpaces,
     *
     * Example:
     * jini://<code>*</code>/containerName/?timeout=5000
     * jini://<code>*</code>/containerName/JavaSpaces?timeout=10000
     * Default: 5000 ms
     * </pre>
     **/
    public final static String TIMEOUT = "timeout";

    /**
     * Controls the interval (in millis) at which the finder polls for Spaces. Default to {@link
     * #DEFAULT_INTERVAL_TIMEOUT} which is <code>100</code> milliseconds.
     */
    public final static String INTERVAL_TIMEOUT = "intervalTimeout";

    /**
     * The default timeout interval (in millis).
     *
     * @see #INTERVAL_TIMEOUT
     */
    public final static long DEFAULT_INTERVAL_TIMEOUT = LookupFinder.DEFAULT_INTERVAL_TIMEOUT;

    /**
     * <pre>
     * if <code>true</code> FIFO mode enabled.
     * Example: jini://localhost:10098/containerName/JavaSpaces?fifo
     * Default: false
     * </pre>
     *
     * @deprecated use {@link FifoSupport} instead.
     **/
    @Deprecated
    public final static String FIFO_MODE = "fifo";

    /**
     * <pre>
     * java:// protocol support.
     * if <code>true</code> a new Space will be created,
     * if requested space already exists in container create operation will be ignored.
     * Example: java://localhost:10098/containerName/JavaSpaces?create
     *
     * Note that when using the ?create option with java:// protocol, the system
     * will create a container, space and use the default space configuration schema file
     * (default-schema.xml)
     * same as using for example:
     * Example: java://localhost:10098/containerName/JavaSpaces?create?schema=default
     *
     * if <code>false</code> no new space will be created. The system will return the
     * space proxy which has same container name. In this case we do NOT match on the SpaceURL
     * objects
     * equals.
     *
     * Default: false
     * </pre>
     **/
    public final static String CREATE = "create";

    /**
     * <pre>
     * Initializes local cache.
     * Example: jini://localhost:10098/containerName/JavaSpaces?useLocalCache
     * Default: not initialized
     * </pre>
     **/
    public final static String USE_LOCAL_CACHE = "useLocalCache";

    /**
     * <pre>
     * DCache update modes:
     * UPDATE_MODE_PULL = 1
     * UPDATE_MODE_PUSH = 2
     *
     * Example: jini://localhost:10098/containerName/JavaSpaces?useLocalCache&updateMode=1
     *
     * Default: UPDATE_MODE_PULL.
     * </pre>
     **/
    public final static String LOCAL_CACHE_UPDATE_MODE = "updateMode";

    /**
     * @see com.gigaspaces.internal.client.cache.localcache.LocalCacheContainer
     */
    public final static int UPDATE_MODE_NONE = 0;

    /**
     * <pre>
     * In the UPDATE_MODE_PUSH policy,
     * the cache will be updated with the updated value.
     * </pre>
     *
     * @see com.gigaspaces.internal.client.cache.localcache.LocalCacheContainer
     **/
    public final static int UPDATE_MODE_PULL = 1;

    /**
     * <pre>
     * In the UPDATE_MODE_PUSH policy, the updated value is simply marked as invalidated.
     * Any attempt to retrieve an object from the cache,
     * will enforce a reload of the updated value from the master cache.
     * </pre>
     *
     * @see com.gigaspaces.internal.client.cache.localcache.LocalCacheContainer
     **/
    public final static int UPDATE_MODE_PUSH = 2;

    /**
     * <pre>
     * Local cache storage type:
     * LOCAL_CACHE_STORE_REFERENCE
     * LOCAL_CACHE_STORE_SHALLOW_COPY
     * </pre>
     */
    public final static String LOCAL_CACHE_STORAGE_TYPE = "storageType";

    /**
     * <pre>
     * Local cache will store a reference to the user's object.
     * </pre>
     */
    public final static String LOCAL_CACHE_STORE_REFERENCE = "reference";
    /**
     * <pre>
     * Local cache will store a shallow copy of the user's object.
     * </pre>
     */
    public final static String LOCAL_CACHE_STORE_SHALLOW_COPY = "shallowCopy";

    /**
     * <pre>
     * if <code>false</code> Optimistic Locking is disabled.
     * Example: jini://localhost:10098/containerName/JavaSpaces?versioned
     * Default: false
     * </pre>
     **/
    public final static String VERSIONED = "versioned";

    /**
     * <pre>
     * if <code>false</code> SpaceFinder will not initialize Security Manager.
     * Example: jini://localhost:10098/containerName/JavaSpaces?securityManager
     * Default: true
     * </pre>
     **/
    public final static String SECURITY_MANAGER = "securityManager";

    /**
     * <pre>
     * When setting this URL property it will allow the space to connect to the Mirror
     * service to push its data and operations for asynchronous persistency.
     * Example: /./JavaSpace?cluster_schema=sync_replicated&mirror
     * Default: no mirror connection
     * </pre>
     **/
    public final static String MIRROR = "mirror";

    /**
     * <pre>
     * When setting this URL property a view will be created according to the query.
     * The views value can include a list of queries separated by ";".
     * Query structure: Class-Name:SQL-Where-clause
     * The SQL-Where-clause should keep the {@link com.j_spaces.core.client.view.View} rules.
     * Notice: views are only supported in conjuction with useLocalCache.
     * Example: /./JavaSpace?useLocalCache&views={MyEntry:fieldA=3;UserEntry:fieldB=3 AND fieldC=2}
     * Default: no views
     * </pre>
     *
     * @see com.j_spaces.core.client.view.View
     **/
    public final static String VIEWS = "views";

    /**
     * <pre>
     * When setting this URL property a secured space will be created. This property is implicit
     * when providing
     * user details.
     * </pre>
     */
    public final static String SECURED = "secured";

    /**
     * <pre>
     * Using the schema flag, the requested space schema name will be loaded/parsed
     * while creating an embedded space. If the space already has configuration file
     * then the requested schema will not be applied and the that file exist, it will
     * overwrite the default configuration defined by the schema.
     *
     * Note that when using the option ?create with java:// protocol, the system
     * will create a container, space and use the default space configuration schema file
     * (default-schema.xml)
     * </pre>
     */
    public final static String SCHEMA_NAME = Constants.Schemas.SCHEMA_ELEMENT;//"schema"

    /**
     * Default schema name, will be used if schema is not defined.
     *
     * @see #SCHEMA_NAME
     */
    public final static String DEFAULT_SCHEMA_NAME = Constants.Schemas.DEFAULT_SCHEMA;//"default";

    /**
     * The cluster schema XSL file name to be used to setup a cluster config on the fly in memory.
     * If the ?cluster_schema option is passed e.g. ?cluster_schema=sync_replication the system will
     * use the sync_replication-cluster-schema.xsl together with a cluster Dom which will be built
     * using user's inputs on regards # of members, # of backup members etc.
     */
    public final static String CLUSTER_SCHEMA = "cluster_schema";

    /**
     * <pre>
     * The total_members attribute in the space url stands for
     * the total number of space members within the space cluster.
     * The number is used to create the list of members participating in the cluster on the fly
     * based on the space name convention.
     * This pattern is used to avoid the need for creating a cluster topology file.
     * The number of actual running space instances can vary dynamically between 1<=total_members.
     *
     * Example:
     * SpaceFinder.find(/./mySpace?cluster_schema=partitioned&total_members=4&id=1);
     *
     * In case it is used in Partitioned Space (when adding backup to each partition).
     * In partitioned space each instance contains different segment of the total information.
     * That information may be lost once a space is brought down or fails.
     * To  ensure the high availability of each partition a backup instance is set per partition.
     * Setting up a partitioned space with backup instances will be done using the following
     * command:
     * For Member1:
     * SpaceFinder.find(/./mySpace?cluster_schema=partitioned&total_members=4,2&id=1);
     * For Member 1 backup 1:
     * SpaceFinder.find(/./mySpace?cluster_schema=partitioned&total_members=4,2&id=2&backup_id=1);
     * For Member 2 backup 2:
     * SpaceFinder.find(/./mySpace?cluster_schema=partitioned&total_members=4,2&id=2&backup_id=2);
     *
     * The format of the
     * total_members={number of primary instances, number of backup instances per primary}
     * In this example the value is 4,2 which means that this cluster contains
     * up to 4 primary instances each containing 2 backup instances.
     * The backup_id is used to define whether the instance is a backup instance or not.
     * If this attribute is not defined the instance will be considered a primary instance.
     * The container name will be translated in this case to [space name]_container[id]_[backup_id].
     * In this case it will be expanded to mySpace_container1_1
     * </pre>
     */
    public final static String CLUSTER_TOTAL_MEMBERS = "total_members";

    /**
     * Used in case of Partitioned Cache (when adding backup to each partition) The backup_id is
     * used to define whether the instance is a backup instance or not. If this attribute is not
     * defined the instance will be considered a primary space instance.
     *
     * The container name will be translated in this case to [space name]_container[id]_[backup_id].
     * In this case it will be expanded to mySpace_container1_1.
     */
    public final static String CLUSTER_BACKUP_ID = "backup_id";

    /**
     * The id attribute is used to distinguish between cache instances in this cluster.
     */
    public final static String CLUSTER_MEMBER_ID = "id";

    /**
     * If properties file name is passed, it means loading the schema and then loading the requested
     * [prop-file-name].properties file which contains the values to override the schema
     * space/container/cluster configuration values that are defined in the schema files.
     *
     * Another benefit of using the ?properties option is when we want to load system properties
     * while VM starts or set SpaceURL attributes. See /config/gs.properties file as a reference.
     */
    public final static String PROPERTIES_FILE_NAME = "properties";


    /**
     * This system property can be used when space started using {@link
     * com.sun.jini.start.ServiceStarter ServiceStarter}.
     */
    final public static String CACHE_URL_PROP = "gs.space.url";

    /**
     * When using properties, one can set the SpaceURL attributes which we break into properties
     * with gs.space.url.arg.<attribute-name> where attribute name = cluster_schema | schema |
     * total_members etc. The space url args should be in a lower case.
     */
    final public static String PROPERTIES_SPACE_URL_ARG = "gs.space.url.arg";

    /**
     * When using properties, one can set the SpaceURL attributes line which we break into
     * properties with gs.space.url.arg_line. The value will be a lower cased url e.g.
     * gs.space.url.arg_line=schema=persistent&id=2&total_members=3&cluster_schema=sync-replicated
     */
    final public static String PROPERTIES_SPACE_URL_ARGLINE = "gs.space.url.arg_line";

    /**
     * If ignore validation is passed, it means the space url not need to be valid. The class
     * SpaceUrlValidation will ignore and wouldn't check the Url. The default is false : to check
     * the url validation
     */
    final public static String IGNORE_VALIDATION = "ignoreValidation";

    /**
     * <pre>
     * Container key which holds the container name taken from the space url string
     * </pre>
     **/
    public final static String CONTAINER = "container";

    /**
     * <pre>
     * Container name key which holds the GenericEntry Container for service lookup
     * @see com.j_spaces.lookup.entry.ContainerName
     * </pre>
     **/
    final static String LOOKUP_CONTAINER_NAME = "containername";

    /**
     * <pre>
     * The state attribute is used to define specified space state.
     * Example: jini://<code>*</code>/<code>*</code>/<code>*</code>?clustername=cluster&clustergroup=group&state=started
     * @see com.j_spaces.core.ISpaceState
     * </pre>
     **/
    public final static String STATE = "state";

    /**
     * <pre>
     * A STATE parameter value,
     * means that the space state is started.
     * </pre>
     *
     * @see com.j_spaces.core.ISpaceState
     **/
    public final static String STATE_STARTED = "started";

    /**
     * <pre>
     * A STATE parameter value,
     * means that the space state is stopped.
     * </pre>
     *
     * @see com.j_spaces.core.ISpaceState
     **/
    public final static String STATE_STOPPED = "stopped";

    /**
     * <pre>
     * Service name key which holds the GenericEntry Name for service lookup.
     * Might be space or container name for example.
     * @see net.jini.lookup.entry.Name
     * </pre>
     **/
    final static String LOOKUP_SERVICE_NAME = "servicename";

    /**
     * <pre>
     * Service name key which holds the GenericEntry {@link com.gigaspaces.cluster.activeelection.core.ActiveElectionState}
     * for service lookup
     * @see com.gigaspaces.cluster.activeelection.core.ActiveElectionState
     * </pre>
     **/
    final static String LOOKUP_ACTIVE_ELECTION_STATE = "activeelectionState";

    /**
     * <pre>
     * Space name
     * </pre>
     **/
    public final static String SPACE = "space";

    /**
     * <pre>
     * RMI Registry host and port or Jini any host (*)
     * </pre>
     **/
    public final static String HOST_NAME = "host";

    /**
     * <pre>
     * The machine hostname where space is running in
     * </pre>
     **/
    public final static String MACHINE_HOSTNAME = "machineHostname";

    /**
     * <pre>
     * Protocol name
     * </pre>
     **/
    public final static String PROTOCOL_NAME = "protocol";

    /**
     * <pre>
     * Space Url name
     * </pre>
     **/
    final static String URL_NAME = "url";

    /**
     * <pre>
     * Member name (containername:spacename)
     * </pre>
     **/
    final static String MEMBER_NAME = "membername";

    /**
     * Gets all the available space url attributes keys in lower case E.g. create, schema,
     * cluster_schema. NoWriteLease, fifo etc.
     */
    final static Set<String> AVAILABLE_URL_ATTRIBUTES = initSpaceUrlAttributes();
    final static String[] BOOLEAN_URL_ATTRIBUTES = new String[]{
            FIFO_MODE,
            VERSIONED,
            USE_LOCAL_CACHE,
            //NO_WRITE_LEASE,
            CREATE,
            IGNORE_VALIDATION,
            MIRROR,
            SECURED};

    private static Set<String> initSpaceUrlAttributes() {
        Set<String> result = new HashSet<String>();
        result.add(CLUSTER_BACKUP_ID.toLowerCase());
        result.add(CLUSTER_MEMBER_ID.toLowerCase());
        result.add(CLUSTER_GROUP.toLowerCase());
        result.add(CLUSTER_NAME.toLowerCase());
        result.add(CLUSTER_SCHEMA.toLowerCase());
        result.add(CLUSTER_TOTAL_MEMBERS.toLowerCase());
        result.add(SCHEMA_NAME.toLowerCase());
        result.add(GROUPS.toLowerCase());
        result.add(FIFO_MODE.toLowerCase());
        result.add(LOCAL_CACHE_UPDATE_MODE.toLowerCase());
        result.add(VERSIONED.toLowerCase());
        result.add(USE_LOCAL_CACHE.toLowerCase());
        result.add(TIMEOUT.toLowerCase());
        result.add(SECURITY_MANAGER.toLowerCase());
        //result.add(NO_WRITE_LEASE.toLowerCase());
        result.add(CREATE.toLowerCase());
        result.add(PROPERTIES_FILE_NAME.toLowerCase());
        result.add(IGNORE_VALIDATION.toLowerCase());
        result.add(LOCATORS.toLowerCase());
        result.add(STATE.toLowerCase());
        result.add(MIRROR.toLowerCase());
        result.add(VIEWS.toLowerCase());
        result.add(SECURED.toLowerCase());
        //result.add(SPACE.toLowerCase());
        //result.add(CONTAINER.toLowerCase());
        //result.add(PROTOCOL_NAME.toLowerCase());
        return result;
    }

    private final static Map<String, Entry> LOOKUP_ATTRIBUTES = initLookupAttributes();

    private static Map<String, Entry> initLookupAttributes() {
        Map<String, Entry> result = new HashMap<String, Entry>();
        result.put(SpaceURL.LOOKUP_CONTAINER_NAME, new ContainerName());
        result.put(SpaceURL.CLUSTER_NAME, new ClusterName());
        result.put(SpaceURL.CLUSTER_GROUP, new ClusterGroup());
        result.put(SpaceURL.STATE, new State());
        result.put(SpaceURL.LOOKUP_SERVICE_NAME, new Name());
        result.put(SpaceURL.LOOKUP_ACTIVE_ELECTION_STATE, new ActiveElectionState());
        return result;
    }

    /**
     * Gets a list of all the available space url attributes keys.
     *
     * @return List of all the available space url attributes keys in lower case E.g. create,
     * schema, cluster_schema. NoWriteLease, fifo etc.
     */
    public static Set<String> getSpaceUrlAttributes() {
        return AVAILABLE_URL_ATTRIBUTES;
    }

    public static boolean isUrlAttribute(String name) {
        return AVAILABLE_URL_ATTRIBUTES.contains(name.toLowerCase());
    }

    public static boolean isBooleanUrlAttribute(String name) {
        for (String BOOLEAN_URL_ATTRIBUTE : BOOLEAN_URL_ATTRIBUTES) {
            if (BOOLEAN_URL_ATTRIBUTE.equalsIgnoreCase(name))
                return true;
        }
        return false;
    }

    private Map<String, Entry> _lookupAttributes;
    private Properties _customProperties;

    /**
     * constructor.
     */
    public SpaceURL() {
    }// constructor()

    @Override
    public String toString() {
        if (_logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            if (!super.isEmpty())
                sb.append("\n\t space url attributes:");
            for (Enumeration keys = super.keys(); keys.hasMoreElements(); ) {
                String propKey = (String) keys.nextElement();
                //String propValue = super.getProperty(propKey);
                Object propValue = super.get(propKey);

                sb.append("\n\t\t Key [ ").append(propKey).append(" ] Value [ ").append(propValue).append(" ]");
            }
            sb.append("\n");
            _logger.debug(sb.toString());
            return sb.toString();
        } else {
            return getProperty(URL_NAME);
        }
    }

    @Override
    public synchronized boolean equals(Object obj) {
        if (obj instanceof SpaceURL) {
            SpaceURL other = (SpaceURL) obj;
            //check if space and container names, schema and cluster schema are equal
            //the rest of attributes are not taken into account while testing equality
            return JSpaceUtilities.isStringEquals(this.getContainerName(), other.getContainerName()) &&
                    JSpaceUtilities.isStringEquals(this.getSpaceName(), other.getSpaceName()) &&
                    JSpaceUtilities.isStringEquals(this.getSchema(), other.getSchema()) &&
                    JSpaceUtilities.isStringEquals(this.getClusterSchema(), other.getClusterSchema()) &&
                    isJiniGroupsEqual(other);
        }

        return false;
    }

    @Override
    public synchronized int hashCode() {
        //check if space and container names, schema and cluster schema are equal
        //the rest of attributes are not taken into account while testing euality

        int hashCode = 0;
        String cont = getContainerName();
        if (cont != null)
            hashCode = hashCode ^ cont.hashCode();
        String space = getSpaceName();
        if (space != null)
            hashCode = hashCode ^ space.hashCode();
        String schema = getSchema();
        if (schema != null)
            hashCode = hashCode ^ schema.hashCode();
        String clusterSchema = getClusterSchema();
        if (clusterSchema != null)
            hashCode = hashCode ^ clusterSchema.hashCode();

        return hashCode;
    }

    /**
     * deep clones this SpaceURL instance - makes a distinct copy of each of the SpaceURL fields,
     * and underlying tables. Basically overrides {@link Properties#clone()} method, which creates
     * only a shallow copy.
     *
     * @return a distinct copy of this SpaceURL (including all object references).
     * @see IOUtils#deepClone(Object)
     */
    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public synchronized SpaceURL clone() {
        return (SpaceURL) IOUtils.deepClone(this);
    }

    @Override
    public synchronized boolean containsKey(Object key) {
        if (super.containsKey(key))
            return true;
        if (key instanceof String)
            return super.containsKey(((String) key).toLowerCase());
        return false;
    }

    @Override
    public Object put(Object key, Object value) {
        if (key instanceof String)
            key = ((String) key).intern();
        if (value instanceof String)
            value = ((String) value).intern();
        return super.put(key, value);
    }

    @Override
    public String getProperty(String key) {
        String retVal = super.getProperty(key);
        if (retVal != null) {
            return retVal;
        }
        return super.getProperty(key.toLowerCase());
    }

    /**
     * An overwrite method to set a SpaceURL property. This method adds to the super functionality
     * and intended to be the only method needs to be used to set SpaceURL properties (do not use
     * SpaceURL.concatAttrIfNotExist()). - It updates the space url string with the passed key and
     * appends it to the url string in case such does not exist. - If the passed key is one of the
     * generic lookup attributes, used for services lookup, it will update its values as well. - It
     * is also a chaining method which means you can call it several times and the set properties
     * will be appended and set appropriately into the current SpaceURL object.
     */
    @Override
    public SpaceURL setProperty(String key, String value) {
        final String keyLower = key.toLowerCase();
        //first we update the property itself.
        super.setProperty(keyLower, value);

        if (keyLower.equals(URL_NAME))
            return this;

        if (AVAILABLE_URL_ATTRIBUTES.contains(keyLower)) {
            super.setProperty(URL_NAME, SpaceUrlUtils.setPropertyInUrl(getProperty(URL_NAME), key, value, true));
            setLookupAttribute(keyLower, value);
        } else if (keyLower.equals(HOST_NAME))
            updateHostName(value);

        return this;//for chaining method reasons
    }

    @Override
    public synchronized Object remove(Object key) {
        return key instanceof String ? removeProperty((String) key) : super.remove(key);
    }

    private Object removeProperty(String key) {
        final String keyLower = key.toLowerCase();
        Object result = super.remove(keyLower);
        if (result != null) {
            if (AVAILABLE_URL_ATTRIBUTES.contains(keyLower)) {
                super.setProperty(URL_NAME, SpaceUrlUtils.deletePropertyFromUrl(getProperty(URL_NAME), key));
                if (LOOKUP_ATTRIBUTES.containsKey(keyLower))
                    getGenericLookupAttributes().remove(keyLower);
            }
        }
        return result;
    }

    private void updateHostName(String value) {
        getCustomProperties().setProperty(XPathProperties.CONTAINER_JNDI_URL, value);
        String url = getProperty(URL_NAME);
        int lastSearchIndex = url.indexOf('?');
        if (lastSearchIndex == -1)
            lastSearchIndex = url.length() - 1;

        //use here length of "java:" as fromIndex in order to reach
        //host url delimeter if exists
        int hostURLDelimeterIndex = url.lastIndexOf(":");
        //if hist url delimeter exists ":"
        if (hostURLDelimeterIndex > 0 && hostURLDelimeterIndex < lastSearchIndex) {
            int lastHostURLIndex = url.indexOf("/", hostURLDelimeterIndex);
            int firstHostURLIndex = url.substring(0, lastHostURLIndex - 1).lastIndexOf("/");
            if (firstHostURLIndex > 0 && lastHostURLIndex > firstHostURLIndex) {
                String oldValue = url.substring(firstHostURLIndex + 1, lastHostURLIndex);
                url = url.replaceFirst(oldValue, value);
                super.setProperty(URL_NAME, url);
            }
        }
    }

    /**
     * Gets URL.
     *
     * @return entire SpaceFinder URL
     **/
    public String getURL() {
        return getProperty(URL_NAME);
    }

    /**
     * Returns SpaceFinder protocol.
     *
     * @return SpaceFinder protocol.
     */
    public String getProtocol() {
        return getProperty(PROTOCOL_NAME);
    }

    /**
     * Returns true if the protocol is JINI, false otherwise.
     *
     * @since 9.0.1
     */
    public boolean isJiniProtocol() {
        return StringUtils.equalsIgnoreCase(getProtocol(), JINI_PROTOCOL);
    }

    public boolean isFileProtocol() {
        return StringUtils.equalsIgnoreCase(getProtocol(), FILE_PROTOCOL);
    }

    /**
     * Returns true if the protocol is RMI, false otherwise.
     *
     * @since 9.0.1
     */
    public boolean isRmiProtocol() {
        return StringUtils.equalsIgnoreCase(getProtocol(), RMI_PROTOCOL);
    }

    /**
     * Returns true if the protocol is EMBEDDED_SPACE, false otherwise.
     *
     * @since 9.0.1
     */
    public boolean isEmbeddedProtocol() {
        return StringUtils.equalsIgnoreCase(getProtocol(), EMBEDDED_SPACE_PROTOCOL);
    }

    /**
     * Returns true if the protocol is remote (JINI or RMI), false otherwise.
     *
     * @since 9.0.1
     */
    public boolean isRemoteProtocol() {
        return isJiniProtocol() || isRmiProtocol() || isFileProtocol();
    }

    /**
     * @return the member name - containerName:spaceName
     */
    public String getMemberName() {
        return getProperty(MEMBER_NAME);
    }

    /**
     * Gets URL host.
     *
     * @return URL host, or <code>*</code> if hostname is not defined
     **/
    public String getHost() {
        final String host = getProperty(HOST_NAME);
        return host.equals(ANY) ? null : host;
    }

    /**
     * Get container name.
     *
     * @return container name, or <code>*</code> if container name is not defined
     **/
    public String getContainerName() {
        return getProperty(CONTAINER, ANY);
    }

    /**
     * Sets container name in SpaceURL and in the ContainerName GenericEntry
     *
     * @param containerName The name of the container.
     **/
    public void setContainerName(String containerName) {
        setProperty(CONTAINER, containerName);
        //set also the GenericEntry ContainerName
        if (!containerName.equals(ANY))
            setLookupAttribute(LOOKUP_CONTAINER_NAME, containerName);
    }

    /**
     * Gets Space name.
     *
     * @return Space name
     **/
    public String getSpaceName() {
        return getProperty(SPACE);
    }

    /**
     * Sets space name in SpaceURL and into the Name Entry which is later used in LookupFinder.
     *
     * @param spaceName The name of the space.
     **/
    public void setSpaceName(String spaceName) {
        if (!JSpaceUtilities.isEmpty(spaceName))
            super.setProperty(SPACE, spaceName);

        if (!isRmiProtocol()) {
            // setting the name to use the container name if we look for the
            // container proxy
            if (JSpaceUtilities.isEmpty(spaceName))
                setServiceName(getContainerName());
                // setting the name to use the space/service name IF not *
            else if (!spaceName.equalsIgnoreCase(ANY))
                setServiceName(spaceName);
        }
    }

    /**
     * Sets the space url "left"/prefix part of the url. In other words, it enables you to set all
     * besides the url attributes, which are right side of the ? separator (is such exists).
     *
     * The method is used with an existing SpaceURL instance and with conjunction with the {@link
     * SpaceURL#setProperty(String, String)} method.
     *
     * Possible usage:
     *
     * If the spaceURL begins with jini://localhist/mySpace_container/mySpace?schema=cache&timeout=3000
     * and we want just to replace the properties prefix and keep the rest i.e.
     * jini://<code>*</code>/<code>*</code>/<code>*</code>?schema=cache&timeout=3000
     *
     * Then you need to call: spaceURL.setPropertiesPrefix(SpaceURL.JINI_PROTOCOL, SpaceURL.ANY,
     * SpaceURL.ANY, SpaceURL.ANY);
     *
     * @param protocolName  - if unknown protocol then will use Jini as a fallback
     * @param hostAndPort   - host:port, might be <code>*</code> if Jini://
     * @param containerName - might be <code>*</code> if Jini://
     * @param spaceName     - might be <code>*</code> if Jini://
     */
    public void setPropertiesPrefix(String protocolName, String hostAndPort,
                                    String containerName, String spaceName) {
        // if unknown protocol then will use Jini as a fallback
        if (JSpaceUtilities.isEmpty(protocolName)
                || !AVAILABLE_PROTOCOLS.contains(protocolName))
            super.setProperty(PROTOCOL_NAME, JINI_PROTOCOL);
        /*throw new MalformedURLException("Unknown protocol <" + protocolName
                    + "> was found.");*/
        else
            super.setProperty(PROTOCOL_NAME, protocolName);

        setProperty(HOST_NAME, hostAndPort);

        if (protocolName.equalsIgnoreCase(JINI_PROTOCOL))
            getGenericLookupAttributes().clear();

        setContainerName(containerName);
        setSpaceName(spaceName);

        refreshUrlString();
    }

    public void refreshUrlString() {
        final String oldUrl = getProperty(URL_NAME);
        final int suffixPos = oldUrl.indexOf('?');
        final String suffix = suffixPos < 0 ? "" : oldUrl.substring(suffixPos);
        String newUrl = getProperty(PROTOCOL_NAME) + "//" + getProperty(HOST_NAME) + "/" + getContainerName() + "/" + getProperty(SPACE) + suffix;
        super.setProperty(URL_NAME, newUrl);
    }

    /**
     * Sets the service name into the Name Entry which is later used in LookupFinder
     *
     * @param serviceName The name of the service.
     * @return serviceName same returns the same service name.
     */
    private void setServiceName(String serviceName) {
        if (!JSpaceUtilities.isEmpty(serviceName))
            setLookupAttribute(LOOKUP_SERVICE_NAME, serviceName);
    }

    /**
     * Sets the active election state lookup attribute.
     *
     * @param electionState String representation of an election state.
     * @see com.gigaspaces.cluster.activeelection.core.ActiveElectionState
     */
    public void setElectionState(String electionState) {
        if (!JSpaceUtilities.isEmpty(electionState))
            setLookupAttribute(LOOKUP_ACTIVE_ELECTION_STATE, electionState);
    }

    /**
     * Gets defined LookupService's attributes. Example: jini://<code>*</code>/<code>*</code>/JavaSpaces?clustername=grid&clustergroup=fenix
     * Entry[] attr = spaceURL.getLookupAttributes(); the values will be attr[]{
     * "com.j_spaces.lookup.entry.ClusterName", "com.j_spaces.lookup.entry.ClusterGroup" };
     *
     * @return LookupService's attributes
     **/
    Entry[] getLookupAttributes() {
        Map<String, Entry> lookupAttributes = getGenericLookupAttributes();
        return lookupAttributes.values().toArray(new Entry[lookupAttributes.size()]);
    }

    Entry getLookupAttribute(String key) {
        return getGenericLookupAttributes().get(key);
    }

    private void setLookupAttribute(String name, String attr) {
        Entry entry = LOOKUP_ATTRIBUTES.get(name);
        if (entry != null) {
            if (entry instanceof GenericEntry)
                setLookupAttribute(name, ((GenericEntry) entry).fromString(attr));
            else if (entry instanceof Name)
                setLookupAttribute(name, new Name(attr));
        }
    }

    public void setLookupAttribute(String name, Entry attr) {
        getGenericLookupAttributes().put(name, attr);
    }

    Map<String, Entry> getGenericLookupAttributes() {
        return _lookupAttributes;
    }

    /**
     * Gets the cluster schema name.
     *
     * @return Cluster Schema name
     */
    public String getClusterSchema() {
        return getProperty(CLUSTER_SCHEMA);
    }

    /**
     * Gets the space and container schema name.
     *
     * @return Space and container schema name
     */
    public String getSchema() {
        return getProperty(SCHEMA_NAME);
    }

    /**
     * Set SpaceURL properties for desired space proxy.
     *
     * @param spaceProxy Space proxy.
     **/
    public void setPropertiesForSpaceProxy(IJSpace spaceProxy) {
        /** set FIFO mode i.e: fifo */
        String fifoMode = getProperty(FIFO_MODE, "false");
        if (fifoMode.equalsIgnoreCase("true"))
            spaceProxy.setFifo(true);

        /** is optimistic locking enabled/disabled i.e: versioned*/
        String versioned = getProperty(VERSIONED);
        if (versioned != null) {
            boolean isVersioned = Boolean.valueOf(versioned);
            spaceProxy.setOptimisticLocking(isVersioned);
        }
    }

    /**
     * <pre>
     * Concatenates if not exist the specified attrName and attrValue to the end of the
     * <code>spaceURL</code>.
     * For i.e: SpaceURL.concatAttrIfNotExist( "jini://localhost/myCont/mySpace", timeout, 10000)
     * The return value is: jini://localhost/myCont/mySpace?timeout=10000
     * </pre>
     *
     * @param url   The space URL.
     * @param name  The attribute name.
     * @param value The attribute value (can be null, in case no value is required).
     * @return The new concatenated SpaceFinder URL.
     ***/
    public static String concatAttrIfNotExist(String url, String name, String value) {
        return SpaceUrlUtils.setPropertyInUrl(url, name, value, false);
    }

    public static boolean isValidProtocol(String protocol) {
        return AVAILABLE_PROTOCOLS.contains(protocol);
    }

    public long getLookupIntervalTimeout() {
        return Long.parseLong(getProperty(INTERVAL_TIMEOUT, Long.toString(DEFAULT_INTERVAL_TIMEOUT)));
    }

    public long getLookupTimeout() {
        String lookupTimeoutProp = (String) get(TIMEOUT);
        if (lookupTimeoutProp == null) {
            return LookupFinder.DEFAULT_TIMEOUT;
        }
        return Long.parseLong(lookupTimeoutProp);
    }

    public String[] getLookupGroups() {
        String groupProp = (String) get(GROUPS);
        if (groupProp == null) {
            return null;
        }
        return JSpaceUtilities.parseLookupGroups(groupProp);
    }

    public LookupLocator[] getLookupLocators() {
        String locatorsProp = (String) get(LOCATORS);
        if (locatorsProp == null) {
            return null;
        }
        return LookupManager.buildLookupLocators(locatorsProp);
    }

    private boolean isJiniGroupsEqual(SpaceURL spaceURL) {
        String groupsNameValue1 = (String) get(GROUPS);
        String groupsNameValue2 = (String) spaceURL.get(GROUPS);

        if (groupsNameValue1 == null && groupsNameValue2 == null) {
            return true;
        }

        if (groupsNameValue1 == null || groupsNameValue2 == null) {
            return false;
        }

        String[] groupsArray1 = JSpaceUtilities.parseLookupGroups(groupsNameValue1);
        String[] groupsArray2 = JSpaceUtilities.parseLookupGroups(groupsNameValue2);

        //put all elements of first string array to set
        Set<String> groupsSet1 = new HashSet<String>(groupsArray1.length);
        Collections.addAll(groupsSet1, groupsArray1);

        for (String groupName2 : groupsArray2) {
            if (groupsSet1.contains(groupName2)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns custom properties
     *
     * @return custom properties
     */
    public Properties getCustomProperties() {
        return _customProperties;
    }

    /**
     * Set custom properties
     *
     * @param customProperties customProperties
     */
    public void setCustomProperties(Properties customProperties) {
        put(PROPERTIES_FILE_NAME, customProperties);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        // Within the custom properties we remove the ones that can be an actual instnace
        // of the property (such as an actual instance of a filter provider) since they
        // won't be Serializable (they shouldn't!) and we only need them on this embedded Space.
        Properties serializedCustomProps = null;
        if (_customProperties != null) {
            serializedCustomProps = new Properties();
            serializedCustomProps.putAll(_customProperties);
            for (String property : NON_SERIALIZED_CUSTOM_PROPS)
                serializedCustomProps.remove(property);
        }

        // Write regular properties:
        out.writeInt(this.size());
        for (Map.Entry entry : this.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }

        // Write serializable custom properties:
        if (serializedCustomProps == null)
            out.writeInt(0);
        else {
            out.writeInt(serializedCustomProps.size());
            for (Map.Entry entry : serializedCustomProps.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }

        // Write lookup attributes:
        if (_lookupAttributes == null)
            out.writeInt(0);
        else {
            out.writeInt(_lookupAttributes.size());
            for (Map.Entry entry : _lookupAttributes.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Read regular properties:
        int size = in.readInt();
        for (int i = 0; i < size; i++)
            super.put(in.readObject(), in.readObject());

        // Read custom properties:
        _customProperties = new Properties();
        size = in.readInt();
        for (int i = 0; i < size; i++)
            _customProperties.put(in.readObject(), in.readObject());

        // Read lookup attributes:
        _lookupAttributes = Collections.synchronizedMap(new HashMap<String, Entry>());
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            Object key = in.readObject();
            Object value = in.readObject();
            _lookupAttributes.put((String) key, (Entry) value);
        }
    }

    void initialize(String url) {
        super.setProperty(URL_NAME, url);
        _lookupAttributes = Collections.synchronizedMap(new HashMap<String, Entry>());
        _customProperties = new Properties();
        final String[] attributes = url.split("[\\?&]");
        // Skip first, which is actually the prefix:
        for (int i = 1; i < attributes.length; i++) {
            // Backwards compatibility: support ?& and && combinations
            if (attributes[i].length() == 0)
                continue;
            String[] tokens = attributes[i].split("=", 2);
            String name = tokens[0].toLowerCase();
            String value;
            if (tokens.length < 2) {
                // handle boolean attrbutes - their existence indicates their
                // value should be set to true
                if (SpaceURL.isBooleanUrlAttribute(name))
                    value = "true";
                else
                    value = "";
            } else {
                value = tokens[1];
            }
            if (LOOKUP_ATTRIBUTES.containsKey(name))
                setLookupAttribute(name, value);
            else
                super.setProperty(name, value);
        }
    }
}
