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
import com.gigaspaces.datasource.DataSourceIdQuery;
import com.gigaspaces.datasource.DataSourceQuery;
import com.gigaspaces.datasource.SpaceDataSourceException;

import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.persistency.hibernate.iterator.DefaultCriteriaByExampleDataIterator;
import org.openspaces.persistency.hibernate.iterator.HibernateIteratorUtils;
import org.openspaces.persistency.hibernate.iterator.HibernateProxyRemoverIterator;

import java.io.Serializable;
import java.util.Set;

/**
 * An extension over the default implementation that its main use is to override Hibernate bugs when
 * executing queries with composite objects.
 *
 * @author eitany
 * @since 9.5
 */
public class CriteriaHibernateSpaceDataSource extends DefaultHibernateSpaceDataSource {

    public CriteriaHibernateSpaceDataSource(SessionFactory sessionFactory, Set<String> managedEntries, int fetchSize,
                                            boolean performOrderById, String[] initialLoadEntries, int initialLoadThreadPoolSize,
                                            int initialLoadChunkSize, boolean useScrollableResultSet, String[] initialLoadQueryScanningBasePackages,
                                            boolean augmentInitialLoadEntries, ClusterInfo clusterInfo) {
        super(sessionFactory, managedEntries, fetchSize, performOrderById, initialLoadEntries,
                initialLoadThreadPoolSize, initialLoadChunkSize, useScrollableResultSet, initialLoadQueryScanningBasePackages,
                augmentInitialLoadEntries, clusterInfo);
    }

    /* (non-Javadoc)
     * @see org.openspaces.persistency.hibernate.DefaultHibernateSpaceDataSource#getDataIterator(com.gigaspaces.datasource.DataSourceQuery)
     */
    @Override
    public DataIterator<Object> getDataIterator(DataSourceQuery query) {
        if (!query.supportsTemplateAsObject())
            return super.getDataIterator(query);

        Object template = query.getTemplateAsObject();
        if (!isManagedEntry(query.getTypeDescriptor().getTypeName())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Ignoring template (no mapping in hibernate) [" + template + "]");
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Iterator over template [" + template + "]");
        }
        return new HibernateProxyRemoverIterator(new DefaultCriteriaByExampleDataIterator(template, getSessionFactory()));
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.datasource.SpaceDataSource#getById(com.gigaspaces.datasource.DataSourceIdQuery)
     */
    @Override
    public Object getById(DataSourceIdQuery idQuery) {

        if (!isManagedEntry(idQuery.getTypeDescriptor().getTypeName())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Ignoring template (no mapping in hibernate) [" + idQuery + "]");
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Read over template [" + idQuery + "]");
        }
        Object object = null;
        Session session = getSessionFactory().openSession();
        session.setFlushMode(FlushMode.NEVER);
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            object = session.get(idQuery.getTypeDescriptor().getTypeName(), (Serializable) idQuery.getId());
            tx.rollback();
        } catch (Exception e) {
            if (tx != null) {
                tx.rollback();
            }
            throw new SpaceDataSourceException("Exception caught while read with template [" + idQuery + "]", e);
        } finally {
            session.close();
        }
        return HibernateIteratorUtils.unproxy(object);
    }

}
