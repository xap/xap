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

package com.gigaspaces.client;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.lookup.SpaceUrlUtils;
import com.gigaspaces.internal.server.space.SpaceInstanceConfig;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageConfig;
import com.gigaspaces.internal.sync.mirror.MirrorDistributedTxnConfig;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.CredentialsProviderHelper;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.gigaspaces.server.SpaceCustomComponent;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.Constants;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.client.SpaceURLParser;
import com.j_spaces.core.cluster.ReplicationFilterProvider;
import com.j_spaces.core.filters.FilterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.*;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyFactory {

    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE_URL);

    private Map<String, Object> parameters;
    private Properties properties;
    private Properties urlProperties;
    private Properties beanLevelProperties;
    private String schema;
    private String lookupGroups;
    private String lookupLocators;
    private Integer lookupTimeout;
    private Boolean fifo;
    private Boolean versioned;
    private Boolean mirror;
    private FilterProvider[] filterProviders;
    private Map<String, SqlFunction> userDefineSqlFunctions;
    private ReplicationFilterProvider replicationFilterProvider;
    private ManagedDataSource externalDataSource;
    private SpaceDataSource spaceDataSource;
    private SpaceSynchronizationEndpoint spaceSynchronizationEndpoint;
    private SpaceTypeDescriptor[] typeDescriptors;
    private QueryExtensionProvider[] queryExtensionProviders;
    private Properties cachePolicyProperties;
    private ClusterConfig clusterConfig;
    private String instanceId;
    private MirrorDistributedTxnConfig mirrorDistributedTxnConfig;
    private Boolean secured;
    private CredentialsProvider credentialsProvider;
    private AttributeStore attributeStore;
    private List<SpaceCustomComponent> customComponents = new ArrayList<SpaceCustomComponent>();

    public ISpaceProxy createSpaceProxy(String url) throws MalformedURLException, FinderException {
        String[] urls = StringUtils.tokenizeToStringArray(url, ";");
        SpaceURL[] spacesUrls = new SpaceURL[urls.length];
        for (int i = 0; i < urls.length; i++)
            spacesUrls[i] = createSpaceURL(urls[i]);
        return (ISpaceProxy) SpaceFinder.find(spacesUrls, credentialsProvider);
    }

    public ISpaceProxy createSpaceProxy(String name, boolean isRemote) throws MalformedURLException, FinderException {
        String url = (isRemote ? "jini://*/" + getContainerName(name) + "/" : "/./") + name;
        SpaceURL spaceURL = createSpaceURL(url);
        return (ISpaceProxy) SpaceFinder.find(spaceURL, credentialsProvider);
    }

    private String getContainerName(String spaceName) {
        if (!StringUtils.hasLength(instanceId) || instanceId.equals("*"))
            return "*";
        return spaceName + "_container" + instanceId;
    }

    private SpaceURL createSpaceURL(String url) throws MalformedURLException {
        return SpaceURLParser.parseURL(url, createProperties(SpaceUrlUtils.isRemoteProtocol(url)));
    }

    private Properties createProperties(boolean isRemote) {
        Properties props = new Properties();

        if (parameters != null)
            props.putAll(parameters);

        if (properties != null) {
            props.putAll(properties);
        }
        // copy over the space properties
        if (urlProperties != null) {
            for (Map.Entry<Object, Object> entry : urlProperties.entrySet()) {
                props.put(SpaceUrlUtils.toCustomUrlProperty((String) entry.getKey()), entry.getValue());
            }
        }

        if (lookupGroups != null)
            props.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.GROUPS), lookupGroups);
        if (lookupLocators != null)
            props.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.LOCATORS), lookupLocators);
        if (lookupTimeout != null)
            props.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.TIMEOUT), lookupTimeout.toString());
        if (schema != null)
            props.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.SCHEMA_NAME), schema);
        if (fifo != null)
            props.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.FIFO_MODE), Boolean.toString(fifo));
        if (versioned != null)
            props.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.VERSIONED), Boolean.toString(versioned));
        if (mirror != null)
            props.put(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.MIRROR), Boolean.toString(mirror));

        if (filterProviders != null && filterProviders.length > 0) {
            assertEmbedded(isRemote, "Filters");
            props.put(Constants.Filter.FILTER_PROVIDERS, filterProviders);
        }

        if (userDefineSqlFunctions != null && userDefineSqlFunctions.size() > 0) {
            props.put(Constants.SqlFunction.USER_SQL_FUNCTION, userDefineSqlFunctions);
        }
        if (replicationFilterProvider != null) {
            assertEmbedded(isRemote, "Replication filter provider");
            props.put(Constants.ReplicationFilter.REPLICATION_FILTER_PROVIDER, replicationFilterProvider);
        }

        if (externalDataSource != null) {
            assertEmbedded(isRemote, "External data source");
            if (spaceDataSource != null || spaceSynchronizationEndpoint != null)
                throw new IllegalArgumentException("Cannot set both externalDataSource and spaceDataSource/spaceSynchronizationEndpoint - it is recommended to use spaceDataSource/spaceSynchronizationEndpoint since externalDataSource is deprecated");
            if (_logger.isWarnEnabled())
                _logger.warn("externalDataSource is deprecated - instead use spaceDataSource and/or spaceSynchronizationEndpoint");
            props.put(Constants.DataAdapter.DATA_SOURCE, externalDataSource);
            props.put(Constants.StorageAdapter.FULL_STORAGE_PERSISTENT_ENABLED_PROP, "true");
        }

        if (spaceDataSource != null) {
            assertEmbedded(isRemote, "Space data source");
            props.put(Constants.DataAdapter.SPACE_DATA_SOURCE, spaceDataSource);
            props.put(Constants.StorageAdapter.FULL_STORAGE_PERSISTENT_ENABLED_PROP, "true");
        }

        if (spaceSynchronizationEndpoint != null) {
            assertEmbedded(isRemote, "Synchronization endpoint interceptor");
            props.put(Constants.DataAdapter.SPACE_SYNC_ENDPOINT, spaceSynchronizationEndpoint);
            props.put(Constants.StorageAdapter.FULL_STORAGE_PERSISTENT_ENABLED_PROP, "true");
        }

        SpaceInstanceConfig spaceInstanceConfig = (SpaceInstanceConfig) props.get(Constants.Space.SPACE_CONFIG);
        if (isRemote) {
            if (spaceInstanceConfig != null)
                throw new IllegalArgumentException(Constants.Space.SPACE_CONFIG + " can only be used with an embedded Space");
        } else {
            if (spaceInstanceConfig == null) {
                spaceInstanceConfig = new SpaceInstanceConfig();
                props.put(Constants.Space.SPACE_CONFIG, spaceInstanceConfig);
            }
        }
        if (typeDescriptors != null && typeDescriptors.length > 0) {
            assertEmbedded(isRemote, "Space Types");
            spaceInstanceConfig.setTypeDescriptors(typeDescriptors);
        }

        if (queryExtensionProviders != null && queryExtensionProviders.length > 0) {
            assertEmbedded(isRemote, "Query Extension Providers");
            spaceInstanceConfig.setQueryExtensionProviders(queryExtensionProviders);
        }

        if (cachePolicyProperties != null) {
            assertEmbedded(isRemote, "Cache policy");
            props.putAll(cachePolicyProperties);
        }

        if (!customComponents.isEmpty()) {
            assertEmbedded(isRemote, "Custom Components");
            for (SpaceCustomComponent component : customComponents) {
                spaceInstanceConfig.addCustomComponent(component);
            }
        }

        if (mirrorDistributedTxnConfig != null) {
            assertEmbedded(isRemote, "Distributed transaction processing configuration");
            if (schema == null || !schema.equalsIgnoreCase(Constants.Schemas.MIRROR_SCHEMA))
                throw new IllegalStateException("Distributed transaction processing configuration can only be set for a Mirror component");
            if (mirrorDistributedTxnConfig.getDistributedTransactionWaitTimeout() != null)
                props.put(Constants.Mirror.FULL_MIRROR_DISTRIBUTED_TRANSACTION_TIMEOUT,
                        mirrorDistributedTxnConfig.getDistributedTransactionWaitTimeout().toString());
            if (mirrorDistributedTxnConfig.getDistributedTransactionWaitForOperations() != null)
                props.put(Constants.Mirror.FULL_MIRROR_DISTRIBUTED_TRANSACTION_WAIT_FOR_OPERATIONS,
                        mirrorDistributedTxnConfig.getDistributedTransactionWaitForOperations().toString());
        }

        // copy over the external config overrides
        if (beanLevelProperties != null) {
            props.putAll(beanLevelProperties);
        }

        // no need for a shutdown hook in the space as well
        props.setProperty(Constants.Container.CONTAINER_SHUTDOWN_HOOK_PROP, "false");

        if (clusterConfig != null && clusterConfig.getSchema() != null) {
            props.setProperty(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.CLUSTER_SCHEMA), clusterConfig.getSchema());
            if (clusterConfig.getInstanceId() != null)
                props.setProperty(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.CLUSTER_MEMBER_ID), clusterConfig.getInstanceId().toString());
            if (clusterConfig.getBackupId() != null && clusterConfig.getBackupId() != 0)
                props.setProperty(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.CLUSTER_BACKUP_ID), clusterConfig.getBackupId().toString());
            if (clusterConfig.getNumberOfInstances() != null) {
                String totalMembers = clusterConfig.getNumberOfInstances().toString();
                if (clusterConfig.getNumberOfBackups() != null && clusterConfig.getNumberOfBackups() > -1)
                    totalMembers += "," + clusterConfig.getNumberOfBackups();
                props.setProperty(SpaceUrlUtils.toCustomUrlProperty(SpaceURL.CLUSTER_TOTAL_MEMBERS), totalMembers);
            }
        }

        // handle security
        if (beanLevelProperties != null) {
            CredentialsProvider credentialsProvider = CredentialsProviderHelper.extractMarshalledCredentials(beanLevelProperties, false);
            if (credentialsProvider != null)
                this.credentialsProvider = credentialsProvider;
        }

        if (credentialsProvider == null && props.contains(Constants.Security.USERNAME)) {
            String username = (String) props.remove(Constants.Security.USERNAME);
            String password = (String) props.remove(Constants.Security.PASSWORD);
            credentialsProvider = new DefaultCredentialsProvider(username, password);
        }

        if (credentialsProvider != null) {
            props.put(SpaceURL.SECURED, "true");
            CredentialsProviderHelper.appendCredentials(props, credentialsProvider);
        } else if (secured != null && secured) {
            props.put(SpaceURL.SECURED, "true");
        }

        if (attributeStore != null) {
            props.put(Constants.DirectPersistency.DIRECT_PERSISTENCY_ATTRIBURE_STORE_PROP, attributeStore);
        }

        return props;
    }

    private static void assertEmbedded(boolean isRemote, String componentName) {
        if (isRemote)
            throw new IllegalArgumentException(componentName + " can only be used with an embedded Space");
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public void setUrlProperties(Properties urlProperties) {
        this.urlProperties = urlProperties;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setLookupGroups(String lookupGroups) {
        this.lookupGroups = lookupGroups;
    }

    public void setLookupLocators(String lookupLocators) {
        this.lookupLocators = lookupLocators;
    }

    public void setLookupTimeout(Integer lookupTimeout) {
        this.lookupTimeout = lookupTimeout;
    }

    public void setFifo(boolean fifo) {
        this.fifo = fifo;
    }

    public void setVersioned(Boolean versioned) {
        this.versioned = versioned;
    }

    public void setMirror(Boolean mirror) {
        this.mirror = mirror;
    }

    public void setExternalDataSource(ManagedDataSource externalDataSource) {
        this.externalDataSource = externalDataSource;
    }

    public void setSpaceDataSource(SpaceDataSource spaceDataSource) {
        this.spaceDataSource = spaceDataSource;
    }

    public void setSpaceSynchronizationEndpoint(SpaceSynchronizationEndpoint spaceSynchronizationEndpoint) {
        this.spaceSynchronizationEndpoint = spaceSynchronizationEndpoint;
    }

    public void setTypeDescriptors(SpaceTypeDescriptor[] typeDescriptors) {
        this.typeDescriptors = typeDescriptors;
    }

    public void setQueryExtensionProviders(QueryExtensionProvider[] queryExtensionProviders) {
        this.queryExtensionProviders = queryExtensionProviders;
    }

    public void setCachePolicyProperties(Properties cachePolicyProperties) {
        this.cachePolicyProperties = cachePolicyProperties;
    }

    public void setBeanLevelProperties(Properties beanLevelProperties) {
        this.beanLevelProperties = beanLevelProperties;
    }

    public void setReplicationFilterProvider(ReplicationFilterProvider replicationFilterProvider) {
        this.replicationFilterProvider = replicationFilterProvider;
    }

    public void setFilterProviders(FilterProvider[] filterProviders) {
        this.filterProviders = filterProviders;
    }

    public void setUserDefineSqlFunctions(Map<String, SqlFunction> userDefineSqlFunctions) {
        this.userDefineSqlFunctions = userDefineSqlFunctions;
    }

    public void addFilterProvider(FilterProvider newFilterProvider) {
        if (filterProviders == null || filterProviders.length == 0) {
            filterProviders = new FilterProvider[]{newFilterProvider};
        } else {
            filterProviders = Arrays.copyOf(filterProviders, filterProviders.length + 1);
            filterProviders[filterProviders.length - 1] = newFilterProvider;
        }
    }

    public void addCustomComponent(SpaceCustomComponent component) {
        if (component != null)
            this.customComponents.add(component);
    }

    public void setClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setMirrorDistributedTxnConfig(MirrorDistributedTxnConfig mirrorDistributedTxnConfig) {
        this.mirrorDistributedTxnConfig = mirrorDistributedTxnConfig;
    }

    public void setSecured(Boolean secured) {
        this.secured = secured;
    }

    public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
    }

    public void setAttributeStore(AttributeStore attributeStore) {
        this.attributeStore = attributeStore;
    }

    public void setTieredStorageConfig(TieredStorageConfig tieredStorageConfig) {
        customComponents.add(tieredStorageConfig);
    }
}
