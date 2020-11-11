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

package org.openspaces.persistency.cassandra;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataIteratorAdapter;
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceIdsQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.j_spaces.kernel.pool.IResourceFactory;
import com.j_spaces.kernel.pool.IResourcePool;
import com.j_spaces.kernel.pool.IResourceProcedure;
import com.j_spaces.kernel.pool.ResourcePool;

import org.apache.cassandra.cql.jdbc.CassandraDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.persistency.ClusterInfoAwareSpaceDataSource;
import org.openspaces.persistency.cassandra.datasource.CQLQueryContext;
import org.openspaces.persistency.cassandra.datasource.CassandraTokenRangeAwareDataIterator;
import org.openspaces.persistency.cassandra.datasource.CassandraTokenRangeAwareInitialLoadDataIterator;
import org.openspaces.persistency.cassandra.datasource.SingleEntryDataIterator;
import org.openspaces.persistency.cassandra.error.SpaceCassandraDataSourceException;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.mapping.DefaultSpaceDocumentColumnFamilyMapper;
import org.openspaces.persistency.cassandra.meta.mapping.SpaceDocumentColumnFamilyMapper;
import org.openspaces.persistency.cassandra.meta.types.dynamic.PropertyValueSerializer;
import org.openspaces.persistency.cassandra.pool.ConnectionResource;
import org.openspaces.persistency.cassandra.pool.ConnectionResourceFactory;
import org.openspaces.persistency.support.SpaceTypeDescriptorContainer;
import org.openspaces.persistency.support.TypeDescriptorUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A Cassandra implementation of {@link com.gigaspaces.datasource.SpaceDataSource}.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CassandraSpaceDataSource extends ClusterInfoAwareSpaceDataSource {

    public static final String CQL_VERSION = "2.0.0";

    private static final Log logger = LogFactory.getLog(CassandraSpaceDataSource.class);

    private final SpaceDocumentColumnFamilyMapper mapper;

    private final IResourcePool<ConnectionResource> connectionPool;
    private final HectorCassandraClient hectorClient;

    private final int batchLimit;
    private final CassandraConsistencyLevel readConsistencyLevel;

    private final Object lock = new Object();
    private boolean closed = false;
    private Map<String, SpaceTypeDescriptor> initialLoadEntriesMap = new HashMap<String, SpaceTypeDescriptor>();

    public CassandraSpaceDataSource(
            PropertyValueSerializer fixedPropertyValueSerializer,
            PropertyValueSerializer dynamicPropertyValueSerializer,
            CassandraDataSource cassandraDataSource,
            HectorCassandraClient hectorClient,
            int minimumNumberOfConnections,
            int maximumNumberOfConnections,
            int batchLimit,
            String[] initialLoadQueryScanningBasePackages,
            boolean augmentInitialLoadEntries,
            ClusterInfo clusterInfo) {

        if (hectorClient == null) {
            throw new IllegalArgumentException("hectorClient must be set and initiated");
        }

        if (cassandraDataSource == null) {
            throw new IllegalArgumentException("dataSource must be set");
        }

        if (!CQL_VERSION.equals(cassandraDataSource.getVersion())) {
            throw new IllegalArgumentException("dataSource version must be set to " + CQL_VERSION);
        }

        if (minimumNumberOfConnections <= 0) {
            throw new IllegalArgumentException("minimumNumberOfConnections must be positive number");
        }

        if (maximumNumberOfConnections < minimumNumberOfConnections) {
            throw new IllegalArgumentException("maximumNumberOfConnections must not be smaller than" +
                    "minimumNumberOfConnections");
        }

        if (batchLimit <= 0) {
            throw new IllegalArgumentException("batchSize must be a positive number");
        }

        this.readConsistencyLevel = hectorClient.getReadConsistencyLevel();
        this.batchLimit = batchLimit;
        this.hectorClient = hectorClient;
        this.hectorClient.createMetadataColumnFamilyColumnFamilyIfNecessary();

        IResourceFactory<ConnectionResource> resourceFactory = new ConnectionResourceFactory(cassandraDataSource);
        connectionPool = new ResourcePool<ConnectionResource>(resourceFactory,
                minimumNumberOfConnections,
                maximumNumberOfConnections);

        mapper = new DefaultSpaceDocumentColumnFamilyMapper(fixedPropertyValueSerializer,
                dynamicPropertyValueSerializer);
        this.initialLoadQueryScanningBasePackages = initialLoadQueryScanningBasePackages;
        this.augmentInitialLoadEntries = augmentInitialLoadEntries;
        this.clusterInfo = clusterInfo;
    }

    /**
     * Closes open jdbc connections and the hector client connection pool.
     */
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            connectionPool.forAllResources(new IResourceProcedure<ConnectionResource>() {
                public void invoke(ConnectionResource resource) {
                    resource.close();
                }
            });
            closed = true;
        }
    }

    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {

        String typeName = query.getTypeDescriptor().getTypeName();
        ColumnFamilyMetadata metadata = hectorClient.getColumnFamilyMetadata(typeName);
        if (metadata == null) {
            metadata = hectorClient.fetchColumnFamilyMetadata(typeName, mapper);
            if (metadata == null) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Column family for type: " + typeName + " not found.");
                }
                return null;
            }
        }

        CQLQueryContext queryContext = null;
        if (query.supportsTemplateAsDocument()) {
            SpaceDocument templateDocument = query.getTemplateAsDocument();
            Map<String, Object> properties = templateDocument.getProperties();
            queryContext = new CQLQueryContext(properties, null, null);
        } else if (query.supportsAsSQLQuery()) {
            DataSourceSQLQuery sqlQuery = query.getAsSQLQuery();
            Object[] params = sqlQuery.getQueryParameters();
            queryContext = new CQLQueryContext(null, sqlQuery.getQuery(), params);
        } else {
            throw new SpaceCassandraDataSourceException("Unsupported data source query", null);
        }

        Object keyValue = getKeyValue(queryContext, metadata);
        boolean performIdQuery = keyValue != null && !templateHasPropertyOtherThanKey(queryContext, metadata);

        if (performIdQuery) {
            if (logger.isTraceEnabled()) {
                logger.trace("Performing single entry query for key: " + keyValue);
            }
            return new SingleEntryDataIterator(getByIdImpl(metadata.getTypeName(), keyValue));
        } else {
            int queryMaxResults = keyValue != null ? 1 : query.getBatchSize();
            int iteratorMaxResults = Integer.MAX_VALUE;
            int actualBatchLimit = (queryMaxResults < batchLimit) ? queryMaxResults : batchLimit;
            return new CassandraTokenRangeAwareDataIterator(mapper,
                    metadata,
                    connectionPool.getResource(),
                    queryContext,
                    iteratorMaxResults,
                    actualBatchLimit,
                    readConsistencyLevel);
        }
    }

    private Object getKeyValue(CQLQueryContext queryContext, ColumnFamilyMetadata metadata) {
        if (!queryContext.hasProperties()) {
            return null;
        }

        return queryContext.getProperties().get(metadata.getKeyName());
    }

    private boolean templateHasPropertyOtherThanKey(
            CQLQueryContext queryContext,
            ColumnFamilyMetadata metadata) {
        // This test is not really needed as it is only called after getKeyValue returned a
        // value differet than null, and this same test is performed there
        if (!queryContext.hasProperties()) {
            return true;
        }

        for (Entry<String, Object> entry : queryContext.getProperties().entrySet()) {
            if (!metadata.getKeyName().equals(entry.getKey()) &&
                    entry.getValue() != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Object getById(DataSourceIdQuery idQuery) {
        String typeName = idQuery.getTypeDescriptor().getTypeName();
        Object id = idQuery.getId();
        return getByIdImpl(typeName, id);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public DataIterator<Object> getDataIteratorByIds(DataSourceIdsQuery idsQuery) {
        String typeName = idsQuery.getTypeDescriptor().getTypeName();
        Object[] ids = idsQuery.getIds();
        Map<Object, SpaceDocument> documentsByKeys = hectorClient.readDocumentsByKeys(mapper, typeName, ids);
        return new DataIteratorAdapter<Object>((Iterator) documentsByKeys.values().iterator());
    }

    private Object getByIdImpl(String typeName, Object id) {
        return hectorClient.readDocmentByKey(mapper, typeName, id);
    }

    @Override
    public DataIterator<SpaceTypeDescriptor> initialMetadataLoad() {
        super.initialMetadataLoad();
        Map<String, ColumnFamilyMetadata> columnFamilies = hectorClient.populateColumnFamiliesMetadata(mapper);

        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Loaded the following types from Cassandra for initial metadata load:")
                    .append(StringUtils.NEW_LINE);

            for (ColumnFamilyMetadata metadata : columnFamilies.values()) {
                sb.append("\t").append(metadata).append(StringUtils.NEW_LINE);
            }

            logger.debug(sb.toString());
        }

        Map<String, SpaceTypeDescriptorContainer> typeDescriptors = new HashMap<String, SpaceTypeDescriptorContainer>();

        for (ColumnFamilyMetadata metadata : columnFamilies.values()) {
            String typeName = metadata.getTypeName();
            SpaceTypeDescriptorContainer spaceTypeDescriptorContainer = metadata.getTypeDescriptorData();
            typeDescriptors.put(typeName, spaceTypeDescriptorContainer);
            if (augmentInitialLoadEntries && spaceTypeDescriptorContainer.getTypeDescriptor().isPartitioned()) {
                initialLoadEntriesMap.put(typeName, spaceTypeDescriptorContainer.getTypeDescriptor());
            }
        }

        List<SpaceTypeDescriptor> result = TypeDescriptorUtils.sort(typeDescriptors);

        return new DataIteratorAdapter<SpaceTypeDescriptor>(result.iterator());
    }

    @Override
    public DataIterator<Object> initialDataLoad() {
        obtainInitialLoadQueries();

        Map<String, ColumnFamilyMetadata> columnFamilies = hectorClient.getColumnFamiliesMetadata();

        return new CassandraTokenRangeAwareInitialLoadDataIterator(mapper,
                columnFamilies,
                connectionPool.getResource(),
                initialLoadQueries,
                batchLimit,
                readConsistencyLevel);
    }

    /**
     * Returns <code>false</code>, inheritance is not supported.
     *
     * @return <code>false</code>.
     */
    @Override
    public boolean supportsInheritance() {
        return false;
    }

    protected Map<String, SpaceTypeDescriptor> getInitialLoadEntriesMap() {
        return initialLoadEntriesMap;
    }
}
