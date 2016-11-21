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

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.ManagedDataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.openspaces.persistency.hibernate.iterator.HibernateProxyRemoverIterator;
import org.openspaces.persistency.patterns.ManagedDataSourceEntriesProvider;
import org.openspaces.persistency.support.ConcurrentMultiDataIterator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A base class for Hibernate based external data source implementations.
 *
 * <p>See the different setters for more information and the {@link #init(java.util.Properties)}.
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link AbstractHibernateSpaceDataSource} or {@link
 * AbstractHibernateSpaceSynchronizationEndpoint} instead.
 */
@Deprecated
public abstract class AbstractHibernateExternalDataSource implements ManagedDataSource, ManagedDataSourceEntriesProvider {

    protected final Log logger = LogFactory.getLog(getClass());

    public static final String HIBERNATE_CFG_PROPERTY = "hibernate-config-file";

    private SessionFactory sessionFactory;

    private Set<String> managedEntries;

    private String[] initialLoadEntries;

    private int fetchSize = 100;

    private int initialLoadThreadPoolSize = 10;

    private int initialLoadChunkSize = 100000;

    private boolean performOrderById = true;

    private boolean createdSessionFactory = true;

    private boolean useScrollableResultSet = true;

    /**
     * Injects the Hibernate SessionFactory to be used with this external data source.
     */
    public void setSessionFactory(SessionFactory sessionFactory) {
        createdSessionFactory = false;
        this.sessionFactory = sessionFactory;
    }

    /**
     * Returns the Hibernate session factory.
     */
    public SessionFactory getSessionFactory() {
        return sessionFactory;
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
        this.managedEntries = new HashSet<String>();
        this.managedEntries.addAll(Arrays.asList(entries));
    }

    /**
     * Returns all the entries this Hibernate data source will work with. By default, will use
     * Hibernate meta data API in order to get the list of all the given entities it handles.
     *
     * <p>This list is used to filter out entities when performing all data source operations
     * exception for the {@link #initialLoad()} operation.
     */
    public String[] getManagedEntries() {
        return managedEntries.toArray(new String[managedEntries.size()]);
    }

    /**
     * Returns if the given entity name is part of the {@link #getManagedEntries()} list.
     */
    protected boolean isManagedEntry(String entityName) {
        return managedEntries.contains(entityName);
    }

    /**
     * Filter from the input map the unmapped field of this entity
     *
     * @param itemValues map of properties to filter
     */
    protected Map<String, Object> filterItemValue(String entityName, Map<String, Object> itemValues) {
        ClassMetadata classMetadata = sessionFactory.getClassMetadata(entityName);
        String[] propertyNames = classMetadata.getPropertyNames();
        List<String> names = Arrays.asList(propertyNames);
        Iterator<String> iterator = itemValues.keySet().iterator();
        while (iterator.hasNext()) {
            if (!names.contains(iterator.next()))
                iterator.remove();
        }
        return itemValues;
    }

    /**
     * Sets the fetch size that will be used when working with scrollable results. Defaults to
     * <code>100</code>.
     *
     * @see org.hibernate.Criteria#setFetchSize(int)
     */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * Returns the fetch size that will be used when working with scrollable results. Defaults to
     * <code>100</code>.
     *
     * @see org.hibernate.Criteria#setFetchSize(int)
     */
    protected int getFetchSize() {
        return fetchSize;
    }

    /**
     * When performing initial load, this flag indicates if the generated query will order to
     * results by the id. By default set to <code>true</code> as it most times results in better
     * initial load performance.
     */
    public void setPerformOrderById(boolean performOrderById) {
        this.performOrderById = performOrderById;
    }

    /**
     * When performing initial load, this flag indicates if the generated query will order to
     * results by the id. By default set to <code>true</code> as it most times results in better
     * initial load performance.
     */
    protected boolean isPerformOrderById() {
        return performOrderById;
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
        this.initialLoadEntries = initialLoadEntries;
    }

    /**
     * Returns a list of entries that will be used to perform the {@link #initialLoad()} operation.
     * By default, will try and build a sensible list based on Hibernate meta data.
     *
     * <p>Note, sometimes an explicit list should be provided. For example, if we have a class A and
     * class B, and A has a relationship to B which is not component. If in the space, we only wish
     * to have A, and have B just as a field in A (and not as an Entry), then we need to explicitly
     * set the list just to A. By default, if we won't set it, it will result in two entries
     * existing in the Space, A and B, with A having a field of B as well.
     */
    public String[] getInitialLoadEntries() {
        return initialLoadEntries;
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
        this.initialLoadThreadPoolSize = initialLoadThreadPoolSize;
    }

    /**
     * By default, the initial load process will chunk large tables and will iterate over the table
     * (entity) per chunk (concurrently). This setting allows to control the chunk size to split the
     * table by. By default, set to <code>100,000</code>. Batching can be disabled by setting
     * <code>-1</code>.
     */
    public void setInitialLoadChunkSize(int initalLoadChunkSize) {
        this.initialLoadChunkSize = initalLoadChunkSize;
    }

    /**
     * By default, the initial load process will chunk large tables and will iterate over the table
     * (entity) per chunk (concurrently). This setting allows to control the chunk size to split the
     * table by. By default, set to <code>100,000</code>. Batching can be disabled by setting
     * <code>-1</code>.
     */
    protected int getInitialLoadChunkSize() {
        return initialLoadChunkSize;
    }

    /**
     * Controls if scrollable resultsets will be used with initial load operation. Defaults to
     * <code>true</code>.
     */
    protected boolean isUseScrollableResultSet() {
        return useScrollableResultSet;
    }

    /**
     * Controls if scrollable resultsets will be used with initial load operation. Defaults to
     * <code>true</code>.
     */
    public void setUseScrollableResultSet(boolean useScrollableResultSet) {
        this.useScrollableResultSet = useScrollableResultSet;
    }

    /**
     * Initializes the hibernate data source. Called by the Space.
     *
     * <p>If the session factory was not injected using {@link #setSessionFactory(org.hibernate.SessionFactory)},
     * will try can create it from the Properties file expecting to find a property called
     * <code>hibernate-config-file</code> with the location of the Hibernate config file.
     *
     * <p>Initializes the {@link #setManagedEntries(String...)} if they were not set explicitly by
     * iterating over all the mapped classes in Hibernate and adding them.
     *
     * <p>Also initializes the {@link #setInitialLoadEntries(String...)} if not set explicitly.
     */
    public void init(Properties properties) throws DataSourceException {
        if (logger.isDebugEnabled()) {
            logger.debug("Using initialLoadChunkSize [" + initialLoadChunkSize + "], fetchSize [" + fetchSize + "], initialLoadThreadPoolSize [" + initialLoadThreadPoolSize + "], performOrderById [" + performOrderById + "]");
        }
        if (sessionFactory == null) {
            createdSessionFactory = true;
            String hibernateFile = properties.getProperty(HIBERNATE_CFG_PROPERTY);
            if (hibernateFile == null) {
                logger.error("No session factory injected, and [" + HIBERNATE_CFG_PROPERTY + "] is not provided in the properties file, can't create session factory");
            }else{
                logger.debug("[" + HIBERNATE_CFG_PROPERTY + "] " + hibernateFile +" is provided in the properties file " );
            }
            try {
                sessionFactory = SessionFactoryBuilder.getFactory(hibernateFile);
            } catch (Exception e) {
                throw new DataSourceException("Failed to create session factory from properties file [" + hibernateFile + "]", e);
            }
        }
        if (managedEntries == null) {
            managedEntries = new HashSet<String>();
            // try and derive the managedEntries
            Map<String, ClassMetadata> allClassMetaData = sessionFactory.getAllClassMetadata();
            for (String entityname : allClassMetaData.keySet()) {
                managedEntries.add(entityname);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Using Hibernate managedEntries [" + managedEntries + "]");
        }
        if (initialLoadEntries == null) {
            Set<String> initialLoadEntries = new HashSet<String>();
            // try and derive the managedEntries
            Map<String, ClassMetadata> allClassMetaData = sessionFactory.getAllClassMetadata();
            for (Map.Entry<String, ClassMetadata> entry : allClassMetaData.entrySet()) {
                String entityname = entry.getKey();
                ClassMetadata classMetadata = entry.getValue();
                if (classMetadata.isInherited()) {
                    String superClassEntityName = ((AbstractEntityPersister) classMetadata).getMappedSuperclass();
                    ClassMetadata superClassMetadata = allClassMetaData.get(superClassEntityName);
                    Class superClass = superClassMetadata.getMappedClass();
                    // only filter out classes that their super class has mappings
                    if (superClass != null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Entity [" + entityname + "] is inherited and has a super class ["
                                    + superClass + "] filtering it out for initial load managedEntries");
                        }
                        continue;
                    }
                }
                initialLoadEntries.add(entityname);
            }
            this.initialLoadEntries = initialLoadEntries.toArray(new String[initialLoadEntries.size()]);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Using Hibernate initial load managedEntries [" + Arrays.toString(initialLoadEntries) + "]");
        }
    }

    /**
     * Shuts down the data source. If the session factory was created by this data source, will
     * close it.
     */
    public void shutdown() throws DataSourceException {
        if (createdSessionFactory) {
            if (sessionFactory != null && !sessionFactory.isClosed()) {
                try {
                    sessionFactory.close();
                } finally {
                    sessionFactory = null;
                }
            }
        }
    }

    /**
     * A helper method that creates the initial load iterator using the {@link
     * org.openspaces.persistency.support.ConcurrentMultiDataIterator} with the provided {@link
     * #setInitialLoadThreadPoolSize(int)} thread pool size.
     */
    protected DataIterator createInitialLoadIterator(DataIterator[] iterators) {
        return new HibernateProxyRemoverIterator(new ConcurrentMultiDataIterator(iterators, initialLoadThreadPoolSize));
    }

    protected boolean isManaged(BulkItem bulkItem) {

        String typeName = bulkItem.getTypeName();
        if (!isManagedEntry(typeName)) {
            if (logger.isTraceEnabled()) {
                logger.trace("Entry [" + typeName + ":" + bulkItem + "] is not managed, filtering it out");
            }
            return false;
        }

        return true;
    }

    protected String getPartialUpdateHQL(BulkItem updateBulkItem, Map<String, Object> updatedValues) {

        final StringBuilder updateQueryBuilder = new StringBuilder("update ");
        updateQueryBuilder.append(updateBulkItem.getTypeName()).append(" set ");

        int i = 0;
        for (Map.Entry<String, Object> updateEntry : updatedValues.entrySet()) {
            updateQueryBuilder.append(updateEntry.getKey()).append("=:").append(updateEntry.getKey());
            if (i < updatedValues.size() - 1)
                updateQueryBuilder.append(',');
            i++;
        }

        updateQueryBuilder.append(" where ").append(updateBulkItem.getIdPropertyName())
                .append("=:id_").append(updateBulkItem.getIdPropertyName());
        String hql = updateQueryBuilder.toString();
        if (logger.isTraceEnabled()) {
            logger.trace("Partial Update HQL [" + hql + ']');
        }
        return hql;
    }

}
