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
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.j_spaces.core.client.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.persistency.hibernate.iterator.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The default Hibernate space data source implementation. Based on Hibernate {@link Session}.
 *
 * @author eitany
 * @since 9.5
 */
public class DefaultHibernateSpaceDataSource extends AbstractHibernateSpaceDataSource {

    public DefaultHibernateSpaceDataSource(SessionFactory sessionFactory, Set<String> managedEntries, int fetchSize,
                                           boolean performOrderById, String[] initialLoadEntries, int initialLoadThreadPoolSize,
                                           int initialLoadChunkSize, boolean useScrollableResultSet, String[] initialLoadQueryScanningBasePackages,
                                           boolean augmentInitialLoadEntries, ClusterInfo clusterInfo, int limitResults) {
        super(sessionFactory, managedEntries, fetchSize, performOrderById, initialLoadEntries, initialLoadThreadPoolSize, initialLoadChunkSize,
                useScrollableResultSet, initialLoadQueryScanningBasePackages, augmentInitialLoadEntries, clusterInfo, limitResults);

    }

    /**
     * Performs the initial load operation. Iterates over the {@link #setInitialLoadEntries(String[])}
     * initial load entries. If {@link #getInitialLoadChunkSize()} is set to <code>-1</code>, will
     * use {@link org.openspaces.persistency.hibernate.iterator.DefaultScrollableDataIterator} for
     * each entity. If {@link #getInitialLoadChunkSize()} is set to a non <code>-1</code> value,
     * will use the {@link org.openspaces.persistency.hibernate.iterator.DefaultChunkScrollableDataIterator}.
     */
    @Override
    public DataIterator<Object> initialDataLoad() {
        List<DataIterator> iterators = new LinkedList<>();
        obtainInitialLoadQueries();
        Set<String> initialLoadTypes = new HashSet<>();
        if(!initialLoadChunksRoutingQueries.isEmpty()) {
            createChunksRoutingIterators(iterators, initialLoadTypes);
        }else {
            initialLoadTypes.addAll(initialLoadQueries.keySet());
            initialLoadTypes.addAll(getInitialLoadEntries());
            for (String type : initialLoadTypes) {
                String query = initialLoadQueries.getOrDefault(type, "");
                SQLQuery sqlQuery = new SQLQuery(type, query);
                if (getLimitResults() > 0) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load list iterator for entry [" + type + (query == null ? "]" : "], query: " + query));
                    }
                    iterators.add(new DefaultListQueryDataIterator(sqlQuery, getSessionFactory(), getLimitResults()));
                } else {
                    if (getInitialLoadChunkSize() == -1) {
                        if (isUseScrollableResultSet()) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Creating initial load scrollable iterator for entry [" + type + (query == null ? "]" : "], query: " + query));
                            }
                            iterators.add(new DefaultScrollableDataIterator(sqlQuery, getSessionFactory(), getFetchSize(), isPerformOrderById()));
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Creating initial load list iterator for entry [" + type + (query == null ? "]" : "], query: " + query));
                            }
                            iterators.add(new DefaultListQueryDataIterator(sqlQuery, getSessionFactory()));
                        }
                    } else {
                        if (isUseScrollableResultSet()) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Creating initial load chunk scrollable iterator for entry [" + type + (query == null ? "]" : "], query: " + query));
                            }
                            iterators.add(new DefaultChunkScrollableDataIterator(sqlQuery, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize()));
                        } else {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Creating initial load chunk list iterator for entry [" + type + (query == null ? "]" : "], query: " + query));
                            }
                            iterators.add(new DefaultChunkListDataIterator(sqlQuery, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize()));
                        }
                    }
                }
            }
        }
        DataIterator[] dataIterators = new DataIterator[iterators.size()];
        return createInitialLoadIterator(iterators.toArray(dataIterators));
    }

    private void createChunksRoutingIterators(List<DataIterator> iterators, Set<String> initialLoadTypes) {
        initialLoadTypes.addAll(initialLoadChunksRoutingQueries.keySet());
        initialLoadTypes.addAll(getInitialLoadEntries());
        for (String type : initialLoadTypes) {
            List<String> queries = initialLoadChunksRoutingQueries.get(type);
            List<SQLQuery> sqlQueries;
            if(queries.isEmpty()){
                sqlQueries = Collections.singletonList(new SQLQuery(type, ""));
            }else {
                sqlQueries = queries.stream().map(query -> new SQLQuery(type, query)).collect(Collectors.toList());
            }
            if (getLimitResults() > 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Creating initial load list iterator for entry [" + type + (queries.isEmpty() ? "]" : "], queries: " + queries));
                }
                for (SQLQuery sqlQuery : sqlQueries) {
                    iterators.add(new DefaultListQueryDataIterator(sqlQuery, getSessionFactory(), getLimitResults()));
                }
            } else {
                if (getInitialLoadChunkSize() == -1) {
                    if (isUseScrollableResultSet()) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Creating initial load scrollable iterator for entry [" + type + (queries.isEmpty() ? "]" : "], queries: " + queries));
                        }
                        for (SQLQuery sqlQuery : sqlQueries) {
                            iterators.add(new DefaultScrollableDataIterator(sqlQuery, getSessionFactory(), getFetchSize(), isPerformOrderById()));
                        }
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Creating initial load list iterator for entry [" + type + (queries.isEmpty() ? "]" : "], queries: " + queries));
                        }
                        for (SQLQuery sqlQuery : sqlQueries) {
                            iterators.add(new DefaultListQueryDataIterator(sqlQuery, getSessionFactory()));
                        }
                    }
                } else {
                    if (isUseScrollableResultSet()) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Creating initial load chunk scrollable iterator for entry [" + type + (queries.isEmpty() ? "]" : "], queries: " + queries));
                        }

                        for (SQLQuery sqlQuery : sqlQueries) {
                            iterators.add(new DefaultChunkScrollableDataIterator(sqlQuery, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize()));
                        }
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Creating initial load chunk list iterator for entry [" + type + (queries.isEmpty() ? "]" : "], queries: " + queries));
                        }
                        for (SQLQuery sqlQuery : sqlQueries) {
                            iterators.add(new DefaultChunkListDataIterator(sqlQuery, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize()));
                        }
                    }
                }
            }
        }
    }

    /**
     * Returns a {@link org.openspaces.persistency.hibernate.iterator.DefaultListQueryDataIterator}
     * for the given sql query.
     */
    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        if (!query.supportsAsSQLQuery())
            return null;

        DataSourceSQLQuery sqlQuery = query.getAsSQLQuery();

        if (!isManagedEntry(query.getTypeDescriptor().getTypeName())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Ignoring query (no mapping in hibernate) [" + sqlQuery + ']');
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Iterator over query [" + sqlQuery + ']');
        }
        return new HibernateProxyRemoverIterator(new DefaultListQueryDataIterator(sqlQuery, getSessionFactory()));
    }

}
