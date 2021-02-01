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


package org.openspaces.core.space;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.IJSpace;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.config.BlobStoreDataPolicyConfigurer;
import org.openspaces.core.config.BlobStoreDataPolicyFactoryBean;
import org.openspaces.core.config.CustomCachePolicyFactoryBean;
import org.openspaces.core.config.TieredStorageConfigurer;
import org.openspaces.core.extension.SpaceCustomComponentFactoryBean;
import org.openspaces.core.space.filter.FilterProviderFactory;
import org.openspaces.core.space.filter.replication.ReplicationFilterProviderFactory;
import org.openspaces.core.transaction.DistributedTransactionProcessingConfigurationFactoryBean;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author yuvalm
 */
public class EmbeddedSpaceConfigurer extends AbstractSpaceConfigurer {

    private final EmbeddedSpaceFactoryBean factoryBean;
    private final Properties properties = new Properties();
    private final List<FilterProviderFactory> filterProviderFactories = new ArrayList<FilterProviderFactory>();
    private final List<SpaceTypeDescriptor> typeDescriptors = new ArrayList<SpaceTypeDescriptor>();
    private final List<QueryExtensionProvider> queryExtensionProviders = new ArrayList<QueryExtensionProvider>();

    public EmbeddedSpaceConfigurer(String name) {
        this.factoryBean = new EmbeddedSpaceFactoryBean(name);
    }

    @Override
    public void close() {
        factoryBean.close();
    }

    @Override
    protected IJSpace createSpace() {
        factoryBean.setProperties(properties);
        factoryBean.setFilterProviders(filterProviderFactories.toArray(new FilterProviderFactory[filterProviderFactories.size()]));
        factoryBean.setSpaceTypes(typeDescriptors.toArray(new SpaceTypeDescriptor[typeDescriptors.size()]));
        factoryBean.setQueryExtensionProviders(queryExtensionProviders.toArray(new QueryExtensionProvider[queryExtensionProviders.size()]));
        factoryBean.afterPropertiesSet();
        return (IJSpace) factoryBean.getObject();
    }

    public EmbeddedSpaceConfigurer addProperty(String name, String value) {
        validate();
        properties.setProperty(name, value);
        return this;
    }

    public EmbeddedSpaceConfigurer addProperties(Properties properties) {
        validate();
        this.properties.putAll(properties);
        return this;
    }

    public EmbeddedSpaceConfigurer lookupGroups(String lookupGroups) {
        validate();
        factoryBean.setLookupGroups(lookupGroups);
        return this;
    }

    public EmbeddedSpaceConfigurer lookupGroups(String... lookupGroups) {
        validate();
        factoryBean.setLookupGroups(StringUtils.arrayToCommaDelimitedString(lookupGroups));
        return this;
    }

    public EmbeddedSpaceConfigurer lookupLocators(String lookupLocators) {
        validate();
        factoryBean.setLookupLocators(lookupLocators);
        return this;
    }

    public EmbeddedSpaceConfigurer lookupLocators(String... lookupLocators) {
        validate();
        factoryBean.setLookupLocators(StringUtils.arrayToCommaDelimitedString(lookupLocators));
        return this;
    }

    public EmbeddedSpaceConfigurer lookupTimeout(int lookupTimeout) {
        validate();
        factoryBean.setLookupTimeout(lookupTimeout);
        return this;
    }

    public EmbeddedSpaceConfigurer versioned(boolean versioned) {
        validate();
        factoryBean.setVersioned(versioned);
        return this;
    }

    public EmbeddedSpaceConfigurer credentials(String userName, String password) {
        return credentialsProvider(new DefaultCredentialsProvider(userName, password));
    }

    public EmbeddedSpaceConfigurer credentialsProvider(CredentialsProvider credentialsProvider) {
        validate();
        factoryBean.setCredentialsProvider(credentialsProvider);
        return this;
    }

    public EmbeddedSpaceConfigurer secured(boolean secured) {
        validate();
        factoryBean.setSecured(secured);
        return this;
    }

    public EmbeddedSpaceConfigurer schema(String schema) {
        validate();
        factoryBean.setSchema(schema);
        return this;
    }

    public EmbeddedSpaceConfigurer mirrored(boolean mirrored) {
        validate();
        factoryBean.setMirrored(mirrored);
        return this;
    }

    public EmbeddedSpaceConfigurer addFilterProvider(FilterProviderFactory filterProviderFactory) {
        validate();
        filterProviderFactories.add(filterProviderFactory);
        return this;
    }

    public EmbeddedSpaceConfigurer addSpaceType(SpaceTypeDescriptor spaceType) {
        validate();
        typeDescriptors.add(spaceType);
        return this;
    }

    public EmbeddedSpaceConfigurer addQueryExtensionProvider(QueryExtensionProvider queryExtensionProvider) {
        validate();
        queryExtensionProviders.add(queryExtensionProvider);
        return this;
    }

    public EmbeddedSpaceConfigurer replicationFilterProvider(ReplicationFilterProviderFactory replicationFilterProvider) {
        validate();
        factoryBean.setReplicationFilterProvider(replicationFilterProvider);
        return this;
    }

    public EmbeddedSpaceConfigurer externalDataSource(ManagedDataSource externalDataSource) {
        validate();
        factoryBean.setExternalDataSource(externalDataSource);
        return this;
    }

    public EmbeddedSpaceConfigurer spaceDataSource(SpaceDataSource spaceDataSource) {
        validate();
        factoryBean.setSpaceDataSource(spaceDataSource);
        return this;
    }

    public EmbeddedSpaceConfigurer spaceSynchronizationEndpoint(SpaceSynchronizationEndpoint synchronizationEndpoint) {
        validate();
        factoryBean.setSpaceSynchronizationEndpoint(synchronizationEndpoint);
        return this;
    }

    public EmbeddedSpaceConfigurer cachePolicy(CachePolicy cachePolicy) {
        validate();
        factoryBean.setCachePolicy(cachePolicy);
        return this;
    }

    public EmbeddedSpaceConfigurer clusterInfo(ClusterInfo clusterInfo) {
        validate();
        factoryBean.setClusterInfo(clusterInfo);
        return this;
    }

    public EmbeddedSpaceConfigurer registerForSpaceModeNotifications(boolean registerForSpaceMode) {
        validate();
        factoryBean.setRegisterForSpaceModeNotifications(registerForSpaceMode);
        return this;
    }

    public EmbeddedSpaceConfigurer primaryBackupListener(ISpaceModeListener primaryBackupListener) {
        validate();
        factoryBean.setPrimaryBackupListener(primaryBackupListener);
        return this;
    }

    public EmbeddedSpaceConfigurer customComponent(SpaceCustomComponentFactoryBean customComponentFactoryBean) {
        validate();
        factoryBean.setCustomComponent(customComponentFactoryBean);
        return this;
    }

    public EmbeddedSpaceConfigurer distributedTransactionProcessingConfiguration(
            DistributedTransactionProcessingConfigurationFactoryBean distributedTransactionProcessingConfiguration) {
        validate();
        factoryBean.setDistributedTransactionProcessingConfiguration(distributedTransactionProcessingConfiguration);
        return this;
    }

    public EmbeddedSpaceConfigurer customCachePolicy(CustomCachePolicyFactoryBean customCachePolicy) {
        validate();
        factoryBean.setCustomCachePolicy(customCachePolicy);
        return this;
    }

    public EmbeddedSpaceConfigurer blobStoreDataPolicy(BlobStoreDataPolicyFactoryBean blobStoreDataPolicy) {
        validate();
        factoryBean.setBlobStoreDataPolicy(blobStoreDataPolicy);
        return this;
    }

    public EmbeddedSpaceConfigurer blobStoreDataPolicy(BlobStoreDataPolicyConfigurer blobStoreDataPolicy) {
        validate();
        if (blobStoreDataPolicy != null)
            factoryBean.setCachePolicy(blobStoreDataPolicy.asCachePolicy());
        return this;
    }

    /**
     * Sets an attribute store
     */
    public EmbeddedSpaceConfigurer attributeStore(AttributeStore attributeStore) {
        validate();
        factoryBean.attributeStore(attributeStore);
        return this;
    }

    public EmbeddedSpaceConfigurer tieredStorage(TieredStorageConfigurer tieredStorageConfigurer) {
        validate();
        factoryBean.setCustomComponent(tieredStorageConfigurer);
        return this;
    }
}
