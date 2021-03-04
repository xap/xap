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

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.datasource.ManagedDataSource;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.gigaspaces.security.directory.UserDetails;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.j_spaces.core.IJSpace;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.config.TieredStorageConfigurer;
import org.openspaces.core.space.filter.FilterProviderFactory;
import org.openspaces.core.space.filter.replication.ReplicationFilterProviderFactory;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A simple configurer helper to create {@link IJSpace} instances. The configurer wraps {@link
 * org.openspaces.core.space.UrlSpaceFactoryBean} and providing a simpler means to configure it
 * using code.
 *
 * <p>An example of using it:
 * <pre>
 * SpaceConfigurer spaceConfigurer = new UrlSpaceConfigurer("/./mySpace")
 *          .lookupGroups("kimchy");
 * ...
 * spaceConfigurer.close();
 * </pre>
 *
 * @author kimchy
 */
public class UrlSpaceConfigurer implements SpaceConfigurer {

    private UrlSpaceFactoryBean urlSpaceFactoryBean;

    private IJSpace space;

    private final Properties properties = new Properties();

    private final Properties urlProperties = new Properties();

    private final Map<String, Object> parameters = new HashMap<String, Object>();

    private final List<FilterProviderFactory> filterProviderFactories = new ArrayList<FilterProviderFactory>();

    private final List<SpaceTypeDescriptor> typeDescriptors = new ArrayList<SpaceTypeDescriptor>();

    public UrlSpaceConfigurer(String url) {
        this.urlSpaceFactoryBean = new UrlSpaceFactoryBean(url);
    }

    public void setUrlSpaceFactoryBean(UrlSpaceFactoryBean urlSpaceFactoryBean) {
        this.urlSpaceFactoryBean = urlSpaceFactoryBean;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setParameters(java.util.Map)
     */
    public UrlSpaceConfigurer addParameter(String name, String value) {
        validate();
        parameters.put(name, value);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setProperties(java.util.Properties)
     */
    public UrlSpaceConfigurer addProperty(String name, String value) {
        validate();
        properties.setProperty(name, value);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setProperties(java.util.Properties)
     */
    public UrlSpaceConfigurer addProperties(Properties properties) {
        validate();
        this.properties.putAll(properties);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setUrlProperties(java.util.Properties)
     */
    public UrlSpaceConfigurer addUrlProperty(String name, String value) {
        validate();
        urlProperties.setProperty(name, value);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setSchema(String)
     */
    public UrlSpaceConfigurer schema(String schema) {
        validate();
        urlSpaceFactoryBean.setSchema(schema);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setFifo(boolean)
     * @deprecated Use {@link FifoSupport} instead.
     */
    @Deprecated
    public UrlSpaceConfigurer fifo(boolean fifo) {
        validate();
        urlSpaceFactoryBean.setFifo(fifo);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setLookupGroups(String)
     */
    public UrlSpaceConfigurer lookupGroups(String lookupGroups) {
        validate();
        urlSpaceFactoryBean.setLookupGroups(lookupGroups);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setLookupGroups(String)
     */
    public UrlSpaceConfigurer lookupGroups(String... lookupGroups) {
        validate();
        urlSpaceFactoryBean.setLookupGroups(StringUtils.arrayToCommaDelimitedString(lookupGroups));
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setLookupLocators(String)
     */
    public UrlSpaceConfigurer lookupLocators(String lookupLocators) {
        validate();
        urlSpaceFactoryBean.setLookupLocators(lookupLocators);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setLookupLocators(String)
     */
    public UrlSpaceConfigurer lookupLocators(String... lookupLocators) {
        validate();
        urlSpaceFactoryBean.setLookupLocators(StringUtils.arrayToCommaDelimitedString(lookupLocators));
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setLookupTimeout(Integer)
     */
    public UrlSpaceConfigurer lookupTimeout(int lookupTimeout) {
        validate();
        urlSpaceFactoryBean.setLookupTimeout(lookupTimeout);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setVersioned(boolean)
     */
    public UrlSpaceConfigurer versioned(boolean versioned) {
        validate();
        urlSpaceFactoryBean.setVersioned(versioned);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setNoWriteLease(boolean)
     */
    public UrlSpaceConfigurer noWriteLease(boolean noWriteLease) {
        validate();
        urlSpaceFactoryBean.setNoWriteLease(noWriteLease);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setMirror(boolean)
     */
    public UrlSpaceConfigurer mirror(boolean mirror) {
        validate();
        urlSpaceFactoryBean.setMirror(mirror);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setFilterProviders(org.openspaces.core.space.filter.FilterProviderFactory[])
     */
    public UrlSpaceConfigurer addFilterProvider(FilterProviderFactory filterProviderFactory) {
        validate();
        filterProviderFactories.add(filterProviderFactory);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setSpaceTypes(SpaceTypeDescriptor[])
     */
    public UrlSpaceConfigurer addSpaceType(SpaceTypeDescriptor spaceType) {
        validate();
        typeDescriptors.add(spaceType);
        return this;
    }


    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setReplicationFilterProvider(org.openspaces.core.space.filter.replication.ReplicationFilterProviderFactory)
     */
    public UrlSpaceConfigurer replicationFilterProvider(ReplicationFilterProviderFactory replicationFilterProvider) {
        validate();
        urlSpaceFactoryBean.setReplicationFilterProvider(replicationFilterProvider);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setExternalDataSource(com.gigaspaces.datasource.ManagedDataSource)
     */
    public UrlSpaceConfigurer externalDataSource(ManagedDataSource externalDataSource) {
        validate();
        urlSpaceFactoryBean.setExternalDataSource(externalDataSource);
        return this;
    }

    /**
     * @see UrlSpaceFactoryBean#setSpaceDataSource(SpaceDataSource)
     */
    public UrlSpaceConfigurer spaceDataSource(SpaceDataSource spaceDataSource) {
        validate();
        urlSpaceFactoryBean.setSpaceDataSource(spaceDataSource);
        return this;
    }

    public UrlSpaceConfigurer spaceSynchronizationEndpoint(SpaceSynchronizationEndpoint synchronizationEndpoint) {
        validate();
        urlSpaceFactoryBean.setSpaceSynchronizationEndpoint(synchronizationEndpoint);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setCachePolicy(CachePolicy)
     */
    public UrlSpaceConfigurer cachePolicy(CachePolicy cachePolicy) {
        validate();
        urlSpaceFactoryBean.setCachePolicy(cachePolicy);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setClusterInfo(org.openspaces.core.cluster.ClusterInfo)
     */
    public UrlSpaceConfigurer clusterInfo(ClusterInfo clusterInfo) {
        validate();
        urlSpaceFactoryBean.setClusterInfo(clusterInfo);
        return this;
    }

    /**
     * @see org.openspaces.core.space.AbstractSpaceFactoryBean#setRegisterForSpaceModeNotifications(boolean)
     */
    public UrlSpaceConfigurer registerForSpaceModeNotifications(boolean registerForSpaceMode) {
        validate();
        urlSpaceFactoryBean.setRegisterForSpaceModeNotifications(registerForSpaceMode);
        return this;
    }

    /**
     * @see org.openspaces.core.space.UrlSpaceFactoryBean#setSecured(boolean)
     */
    public UrlSpaceConfigurer secured(boolean secured) {
        urlSpaceFactoryBean.setSecured(secured);
        return this;
    }

    /**
     * Creates a secured space with the provided user name and password.
     *
     * @deprecated Use {@link #credentials(String, String)} instead.
     */
    @Deprecated
    public UrlSpaceConfigurer userDetails(String userName, String password) {
        return securityConfig(new SecurityConfig(userName, password));
    }

    /**
     * Creates a secured space with the provided user details.
     *
     * @deprecated Use {@link #credentialsProvider(CredentialsProvider)} instead.
     */
    @Deprecated
    public UrlSpaceConfigurer userDetails(UserDetails userDetails) {
        return credentialsProvider(new DefaultCredentialsProvider(userDetails));
    }

    /**
     * Creates a secured space with the provided user name and password.
     */
    public UrlSpaceConfigurer credentials(String userName, String password) {
        return securityConfig(new SecurityConfig(userName, password));
    }

    /**
     * Creates a secured space with the provided credentials provider.
     */
    public UrlSpaceConfigurer credentialsProvider(CredentialsProvider credentialsProvider) {
        return securityConfig(new SecurityConfig(credentialsProvider));
    }

    /**
     * @see org.openspaces.core.space.AbstractSpaceFactoryBean#setSecurityConfig(SecurityConfig)
     */
    public UrlSpaceConfigurer securityConfig(SecurityConfig securityConfig) {
        validate();
        urlSpaceFactoryBean.setSecurityConfig(securityConfig);
        return this;
    }

    /**
     * Sets a custom primary backup listener
     */
    public UrlSpaceConfigurer primaryBackupListener(ISpaceModeListener primaryBackupListener) {
        validate();
        urlSpaceFactoryBean.setPrimaryBackupListener(primaryBackupListener);
        return this;
    }

    /**
     * Sets an attribute store
     */
    public UrlSpaceConfigurer attributeStore(AttributeStore attributeStore) {
        validate();
        urlSpaceFactoryBean.attributeStore(attributeStore);
        return this;
    }

    /**
     * Creates or finds (if not already created) a new Space by calling {@link
     * UrlSpaceFactoryBean#afterPropertiesSet()}.
     */
    public IJSpace create() {
        if (space == null) {
            urlSpaceFactoryBean.setParameters(parameters);
            urlSpaceFactoryBean.setProperties(properties);
            urlSpaceFactoryBean.setUrlProperties(urlProperties);
            urlSpaceFactoryBean.setFilterProviders(filterProviderFactories.toArray(new FilterProviderFactory[filterProviderFactories.size()]));
            urlSpaceFactoryBean.setSpaceTypes(typeDescriptors.toArray(new SpaceTypeDescriptor[typeDescriptors.size()]));
            urlSpaceFactoryBean.afterPropertiesSet();
            space = (IJSpace) urlSpaceFactoryBean.getObject();
        }
        return space;
    }

    public void tieredStorage(TieredStorageConfigurer tieredStorageConfigurer) {
        urlSpaceFactoryBean.setCustomComponent(tieredStorageConfigurer);
    }

    /**
     * Creates or finds (if not already created) a new Space by calling {@link
     * UrlSpaceFactoryBean#afterPropertiesSet()}.
     *
     * @see #create()
     */
    public IJSpace space() {
        return create();
    }

    @Override
    public void close() {
        urlSpaceFactoryBean.close();
    }

    private void validate() {
        if (space != null) {
            throw new IllegalArgumentException("Can't invoke method, space() has already been called");
        }
    }
}
