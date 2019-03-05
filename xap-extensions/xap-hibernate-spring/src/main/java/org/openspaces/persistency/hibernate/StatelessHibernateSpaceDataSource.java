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
import com.gigaspaces.datasource.SpaceDataSource;

import org.hibernate.SessionFactory;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.persistency.hibernate.iterator.HibernateProxyRemoverIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessChunkListDataIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessChunkScrollableDataIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessListQueryDataIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessScrollableDataIterator;

import java.util.Set;

/**
 * A {@link SpaceDataSource} implementation based on Hibernate {@link
 * org.hibernate.StatelessSession}.
 *
 * <p>Note, stateless session is much faster than regular Hibernate session, but at the expense of
 * not having a first level cache, as well as not performing any cascading operations (both in read
 * operations as well as dirty operations).
 *
 * @author eitany
 * @since 9.5
 */
public class StatelessHibernateSpaceDataSource extends AbstractHibernateSpaceDataSource {

    public StatelessHibernateSpaceDataSource(SessionFactory sessionFactory, Set<String> managedEntries, int fetchSize,
                                             boolean performOrderById, String[] initialLoadEntries, int initialLoadThreadPoolSize,
                                             int initialLoadChunkSize, boolean useScrollableResultSet, String[] initialLoadQueryScanningBasePackages,
                                             boolean augmentInitialLoadEntries, ClusterInfo clusterInfo) {
        super(sessionFactory, managedEntries, fetchSize, performOrderById, initialLoadEntries, initialLoadThreadPoolSize,
                initialLoadChunkSize, useScrollableResultSet, initialLoadQueryScanningBasePackages, augmentInitialLoadEntries, clusterInfo);
    }

    /**
     * Performs the initial load operation. Iterates over the {@link #setInitialLoadEntries(String[])}
     * inital load entries. If {@link #getInitialLoadChunkSize()} is set to <code>-1</code>, will
     * use {@link org.openspaces.persistency.hibernate.iterator.StatelessScrollableDataIterator} for
     * each entity. If {@link #getInitialLoadChunkSize()} is set to a non <code>-1</code> value,
     * will use the {@link org.openspaces.persistency.hibernate.iterator.StatelessChunkScrollableDataIterator}.
     */
    @Override
    public DataIterator initialDataLoad() {
        DataIterator[] iterators = new DataIterator[getInitialLoadEntries().size()];
        int iteratorCounter = 0;
        for (String entityName : getInitialLoadEntries()) {
            if (getInitialLoadChunkSize() == -1) {
                if (isUseScrollableResultSet()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load scrollable iterator for entry [" + entityName + "]");
                    }
                    iterators[iteratorCounter++] = new StatelessScrollableDataIterator(entityName, getSessionFactory(), getFetchSize(), isPerformOrderById());
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load list iterator for entry [" + entityName + "]");
                    }
                    iterators[iteratorCounter++] = new StatelessListQueryDataIterator(entityName, getSessionFactory());
                }
            } else {
                if (isUseScrollableResultSet()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load chunk scrollable iterator for entry [" + entityName + "]");
                    }
                    iterators[iteratorCounter++] = new StatelessChunkScrollableDataIterator(entityName, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize());
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load chunk list iterator for entry [" + entityName + "]");
                    }
                    iterators[iteratorCounter++] = new StatelessChunkListDataIterator(entityName, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize());
                }
            }
        }
        return createInitialLoadIterator(iterators);
    }

    /**
     * Returns a {@link org.openspaces.persistency.hibernate.iterator.StatelessListQueryDataIterator}
     * for the given query.
     */
    @Override
    public DataIterator getDataIterator(DataSourceQuery query) {
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
        return new HibernateProxyRemoverIterator(new StatelessListQueryDataIterator(sqlQuery, getSessionFactory()));
    }

}
