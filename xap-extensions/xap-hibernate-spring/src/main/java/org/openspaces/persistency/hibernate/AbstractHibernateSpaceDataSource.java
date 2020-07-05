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

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.internal.client.spaceproxy.metadata.TypeDescFactory;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.kernel.ClassLoaderHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
 import org.hibernate.Metamodel;
import org.hibernate.SessionFactory;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.persistency.hibernate.iterator.HibernateProxyRemoverIterator;
import org.openspaces.persistency.patterns.ManagedEntriesSpaceDataSource;
import org.openspaces.persistency.support.ConcurrentMultiDataIterator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.persistence.metamodel.EntityType;

/**
 * A base class for Hibernate based {@link SpaceDataSource} implementations.
 *
 * @author eitany
 * @since 9.5
 */
public abstract class AbstractHibernateSpaceDataSource extends ManagedEntriesSpaceDataSource {

    protected static final Log logger = LogFactory.getLog(AbstractHibernateSpaceDataSource.class);

    private final int fetchSize;
    private final boolean performOrderById;
    private final Set<String> initialLoadEntries = new HashSet<String>();
    private final int initialLoadThreadPoolSize;
    private final int initialLoadChunkSize;
    private final boolean useScrollableResultSet;
    private final ManagedEntitiesContainer sessionManager;
    private final SessionFactory sessionFactory;
    private final int limitResults;
    private final Map<String, SpaceTypeDescriptor> initialLoadEntriesTypeDescs = new HashMap<String, SpaceTypeDescriptor>();
    private Set<String> managedTypes;

    public AbstractHibernateSpaceDataSource(SessionFactory sessionFactory, Set<String> managedEntries, int fetchSize,
                                            boolean performOrderById, String[] initialLoadEntries,
                                            int initialLoadThreadPoolSize, int initialLoadChunkSize,
                                            boolean useScrollableResultSet,
                                            String[] initialLoadQueryScanningBasePackages,
                                            boolean augmentInitialLoadEntries,
                                            ClusterInfo clusterInfo, int limitResults) {
        this.sessionFactory = sessionFactory;
        this.fetchSize = fetchSize;
        this.performOrderById = performOrderById;
        this.limitResults = limitResults;
        this.initialLoadEntries.addAll(createInitialLoadEntries(initialLoadEntries, sessionFactory));
        this.initialLoadThreadPoolSize = initialLoadThreadPoolSize;
        this.initialLoadChunkSize = initialLoadChunkSize;
        this.useScrollableResultSet = useScrollableResultSet;
        this.sessionManager = new ManagedEntitiesContainer(sessionFactory, managedEntries);
        this.initialLoadQueryScanningBasePackages = initialLoadQueryScanningBasePackages;
        this.augmentInitialLoadEntries = augmentInitialLoadEntries;
        this.clusterInfo = clusterInfo;
    }

    private Set<String> createInitialLoadEntries(String[] initialLoadEntries, SessionFactory sessionFactory) {
        Set<String> result = new HashSet<String>();
        if (initialLoadEntries != null) {
            for (String entry : initialLoadEntries) {
                result.add(entry);
            }
        } else {

            Metamodel metamodel = sessionFactory.getMetamodel();
            MetamodelImplementor metamodelImplementor = (MetamodelImplementor)metamodel;
            Set<EntityType<?>> entities = metamodel.getEntities();

            managedTypes = new HashSet<>();

            for ( EntityType entityType : entities ) {
                if (entityType.getJavaType() == null) continue;
                String entityName = entityType.getJavaType().getName();
                EntityPersister entityPersister = metamodelImplementor.entityPersister(entityName);
                managedTypes.add(entityName);
                if (entityPersister.isInherited()) {
                    AbstractEntityPersister abstractEntityPersister = (AbstractEntityPersister)entityPersister;
                    String superClassEntityName = abstractEntityPersister.getMappedSuperclass();
                    EntityPersister superClassEntityPersister = metamodelImplementor.entityPersister(superClassEntityName);
                    Class superClass = superClassEntityPersister.getMappedClass();

                    // only filter out classes that their super class has mappings
                    if (superClass != null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Entity [" + entityName + "] is inherited and has a super class ["
                                    + superClass + "] filtering it out for initial load managedEntries");
                        }
                        continue;
                    }
                }
                result.add(entityName);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Using Hibernate initial load managedEntries [" + Arrays.toString(initialLoadEntries) + "]");
        }
        return result;
    }

    protected Set<String> getInitialLoadEntries() {
        return initialLoadEntries;
    }

    protected int getInitialLoadChunkSize() {
        return initialLoadChunkSize;
    }

    protected boolean isUseScrollableResultSet() {
        return useScrollableResultSet;
    }

    protected SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    protected int getFetchSize() {
        return fetchSize;
    }

    protected boolean isPerformOrderById() {
        return performOrderById;
    }

    public int getLimitResults() {
        return limitResults;
    }

    /**
     * A helper method that creates the initial load iterator using the {@link
     * org.openspaces.persistency.support.ConcurrentMultiDataIterator} with the provided {@link
     * #setInitialLoadThreadPoolSize(int)} thread pool size.
     */
    protected DataIterator createInitialLoadIterator(DataIterator[] iterators) {
        return new HibernateProxyRemoverIterator(new ConcurrentMultiDataIterator(iterators, initialLoadThreadPoolSize));
    }

    /**
     * Returns if the given entity name is part of the {@link #getManagedEntries()} list.
     */
    protected boolean isManagedEntry(String entityName) {
        return sessionManager.isManagedEntry(entityName);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.SpaceDataSource#supportsInheritance()
     */
    @Override
    public boolean supportsInheritance() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.openspaces.persistency.patterns.ManagedEntriesSpaceDataSource#getManagedEntries()
     */
    @Override
    public Iterable<String> getManagedEntries() {
        return sessionManager.getManagedEntries();
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        super.initialMetadataLoad();
        TypeDescFactory typeDescFactory = new TypeDescFactory();
        for (String initialLoadEntryTypeName : initialLoadEntries) {
            try {
                initialLoadEntriesTypeDescs.put(initialLoadEntryTypeName, typeDescFactory.createPojoTypeDesc(ClassLoaderHelper.loadClass(initialLoadEntryTypeName), null, null));
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Initial load entity " + initialLoadEntryTypeName + " cannot be resolved!", e);
            }
        }
        putUnmappedAncestors(typeDescFactory);

        return null;
        // TODO: some tests are failing when this method returns type descriptors - verify why.
        //List<SpaceTypeDescriptor> sortedTypeDescriptors = TypeDescriptorUtils.sort(initialLoadEntriesTypeDescs.values());
        //return new DataIteratorAdapter<SpaceTypeDescriptor>(sortedTypeDescriptors.iterator());
    }

    private void putUnmappedAncestors(TypeDescFactory typeDescFactory) {
        Set<SpaceTypeDescriptor> mapped = new HashSet<SpaceTypeDescriptor>(initialLoadEntriesTypeDescs.values());
        for (SpaceTypeDescriptor entry : mapped) {
            String superTypeName = entry.getSuperTypeName();
            while (!Object.class.getName().equals(superTypeName) && !initialLoadEntriesTypeDescs.containsKey(superTypeName)) {
                try {
                    SpaceTypeDescriptor typeDescriptor = typeDescFactory.createPojoTypeDesc(ClassLoaderHelper.loadClass(superTypeName), null, null);
                    initialLoadEntriesTypeDescs.put(superTypeName, typeDescriptor);
                    superTypeName = typeDescriptor.getSuperTypeName();
                } catch (ClassNotFoundException e) {
                    // shouldn't happen...
                    throw new IllegalArgumentException("Initial load entity " + superTypeName + " cannot be resolved!", e);
                }
            }
        }
    }

    @Override
    protected void obtainInitialLoadQueries() {
        super.obtainInitialLoadQueries();
        if (!augmentInitialLoadEntries) {
            // feature switch
            return;
        }
        if (clusterInfo == null) {
            // can't augment without partition data...
            return;
        }
        Integer num = clusterInfo.getNumberOfInstances(), instanceId = clusterInfo.getInstanceId();
        if (num == null || instanceId == null) {
            return;
        }
        if(clusterInfo.getChunks() != null){
            String prefix = "MOD(?,"+PartitionToChunksMap.CHUNKS_COUNT+") IN (";
            List<String> allQueries = clusterInfo.getChunks().stream().map(Object::toString).collect(Collectors.toList());
            int pageSize = 100;
            Stream<String> queriesStream = IntStream.range(0, (allQueries.size() + pageSize - 1) / pageSize)
                    .mapToObj(i -> allQueries.subList(i * pageSize, Math.min(pageSize * (i + 1), allQueries.size())))
                    .map(list -> prefix + String.join(",", list) + ")");

            for (String type : initialLoadEntries) {
                if ((managedTypes == null || managedTypes.contains(type)) && !initialLoadChunksRoutingQueries.containsKey(type)) {
                    // there's no query for this type yet
                    SpaceTypeDescriptor typeDesc = initialLoadEntriesTypeDescs.get(type);
                    List<String> queries = queriesStream.map(query -> createInitialLoadQuery(typeDesc, query)).collect(Collectors.toList());
                    // add new initial load query for this type.
                    if (!queries.isEmpty()) {
                        initialLoadChunksRoutingQueries.put(type, queries);
                    }
                }
            }
        } else {
            // MOD(id,2) = 1
            //MOD(<routingProperty>,numberOfPartitions) = partitionId
            String query = "MOD(?," + num + ") = " + (instanceId - 1);
            // go through the initial load entries, check for matching queries, make queries for managed entries with a
            // numeric routing field (unless a query already exists)
            for (String type : initialLoadEntries) {
                if ((managedTypes == null || managedTypes.contains(type)) && !initialLoadQueries.containsKey(type)) {
                    processInitialLoadEntry(type, query);
                }
            }
        }
    }


    private void processInitialLoadEntry(String type, String query) {
        // there's no query for this type yet
        SpaceTypeDescriptor typeDesc = initialLoadEntriesTypeDescs.get(type);
        String typeQuery = createInitialLoadQuery(typeDesc, query);
        // add new initial load query for this type.
        if (null != typeQuery) {
            initialLoadQueries.put(type, typeQuery);
        }
    }

}