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

package org.openspaces.persistency.hibernate;

import org.hibernate.SessionFactory;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * A factory bean which creates {@link DefaultHibernateSpaceDataSource}
 *
 * @author eitany
 * @since 9.5
 */
public class DefaultHibernateSpaceDataSourceFactoryBean implements FactoryBean<DefaultHibernateSpaceDataSource>, InitializingBean, ClusterInfoAware {


    protected final DefaultHibernateSpaceDataSourceConfigurer dataSourceConfigurer;

    public DefaultHibernateSpaceDataSourceFactoryBean() {
        this.dataSourceConfigurer = getConfigurer();
    }

    protected DefaultHibernateSpaceDataSourceFactoryBean(DefaultHibernateSpaceDataSourceConfigurer configurer) {
        this.dataSourceConfigurer = configurer;
    }

    /**
     * @deprecated Since 12.3 - Use {@link #DefaultHibernateSpaceDataSourceFactoryBean(DefaultHibernateSpaceDataSourceConfigurer)} instead.
     */
    @Deprecated
    protected DefaultHibernateSpaceDataSourceConfigurer getConfigurer() {
        return new DefaultHibernateSpaceDataSourceConfigurer();
    }

    private DefaultHibernateSpaceDataSource hibernateSpaceDataSource;

    /**
     * Injects the Hibernate SessionFactory to be used with this data source.
     */
    public void setSessionFactory(SessionFactory sessionFactory) {
        dataSourceConfigurer.sessionFactory(sessionFactory);
    }

    /**
     * Sets all the entries this Hibernate data source will work with. By default, will use
     * Hibernate meta data API in order to get the list of all the given entities it handles.
     *
     * <p>This list is used to filter out entities when performing all data source operations
     * exception for the {@link #initialLoad()} operation.
     *
     * <p>Usually, there is no need to explicitly set this.
     */
    public void setManagedEntries(String... entries) {
        dataSourceConfigurer.managedEntries(entries);
    }

    /**
     * Sets the fetch size that will be used when working with scrollable results. Defaults to
     * <code>100</code>.
     *
     * @see org.hibernate.query.Query#setFetchSize(int)
     */
    public void setFetchSize(int fetchSize) {
        dataSourceConfigurer.fetchSize(fetchSize);
    }

    /**
     * When performing initial load, this flag indicates if the generated query will order to
     * results by the id. By default set to <code>true</code> as it most times results in better
     * initial load performance.
     */
    public void setPerformOrderById(boolean performOrderById) {
        dataSourceConfigurer.performOrderById(performOrderById);
    }

    /**
     * Sets a list of entries that will be used to perform the {@link #initialLoad()} operation. By
     * default, will try and build a sensible list based on Hibernate meta data.
     *
     * <p>Note, sometimes an explicit list should be provided. For example, if we have a class A and
     * class B, and A has a relationship to B which is not component. If in the space, we only wish
     * to have A, and have B just as a field in A (and not as an Entry), then we need to explicitly
     * set the list just to A. By default, if we won't set it, it will result in two entries
     * existing in the Space, A and B, with A having a field of B as well.
     */
    public void setInitialLoadEntries(String... initialLoadEntries) {
        dataSourceConfigurer.initialLoadEntries(initialLoadEntries);
    }

    /**
     * The initial load operation uses the {@link org.openspaces.persistency.support.ConcurrentMultiDataIterator}.
     * This property allows to control the thread pool size of the concurrent multi data iterator.
     * Defaults to <code>10</code>.
     *
     * <p>Note, this usually will map one to one to the number of open connections / cursors against
     * the database.
     */
    public void setInitialLoadThreadPoolSize(int initialLoadThreadPoolSize) {
        dataSourceConfigurer.initialLoadThreadPoolSize(initialLoadThreadPoolSize);
    }

    /**
     * By default, the initial load process will chunk large tables and will iterate over the table
     * (entity) per chunk (concurrently). This setting allows to control the chunk size to split the
     * table by. By default, set to <code>100,000</code>. Batching can be disabled by setting
     * <code>-1</code>.
     */
    public void setInitialLoadChunkSize(int initalLoadChunkSize) {
        dataSourceConfigurer.initialLoadChunkSize(initalLoadChunkSize);
    }

    /**
     * Controls if scrollable result sets will be used with initial load operation. Defaults to
     * <code>true</code>.
     */
    public void setUseScrollableResultSet(boolean useScrollableResultSet) {
        dataSourceConfigurer.useScrollableResultSet(useScrollableResultSet);
    }

    public void setLimitResults(int limitResults) {
        dataSourceConfigurer.limitResults(limitResults);
    }

    /**
     * @see DefaultHibernateSpaceDataSourceConfigurer#initialLoadQueryScanningBasePackages(String[])
     */
    public void setInitialLoadQueryScanningBasePackages(String... initialLoadQueryScanningBasePackages) {
        dataSourceConfigurer.initialLoadQueryScanningBasePackages(initialLoadQueryScanningBasePackages);
    }

    /**
     * @see DefaultHibernateSpaceDataSourceConfigurer#clusterInfo(org.openspaces.core.cluster.ClusterInfo)
     */
    public void setClusterInfo(ClusterInfo clusterInfo) {
        dataSourceConfigurer.clusterInfo(clusterInfo);
    }

    /**
     * Feature switch for initial load entries augmentation (creation of partition-specific query
     * for entries). Defaults to <code>true</code>.
     */
    public void setAugmentInitialLoadEntries(boolean augmentInitialLoadEntries) {
        dataSourceConfigurer.augmentInitialLoadEntries(augmentInitialLoadEntries);
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.FactoryBean#getObject()
     */
    @Override
    public DefaultHibernateSpaceDataSource getObject() throws Exception {
        return hibernateSpaceDataSource;
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.FactoryBean#getObjectType()
     */
    @Override
    public Class<?> getObjectType() {
        return DefaultHibernateSpaceDataSource.class;
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.FactoryBean#isSingleton()
     */
    @Override
    public boolean isSingleton() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        hibernateSpaceDataSource = dataSourceConfigurer.create();
    }

}
