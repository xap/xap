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

import com.gigaspaces.datasource.BulkDataPersister;
import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.datasource.SQLDataProvider;
import com.j_spaces.core.client.SQLQuery;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.metadata.ClassMetadata;
import org.openspaces.persistency.hibernate.iterator.HibernateProxyRemoverIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessChunkListDataIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessChunkScrollableDataIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessListQueryDataIterator;
import org.openspaces.persistency.hibernate.iterator.StatelessScrollableDataIterator;

import java.util.List;
import java.util.Map;

/**
 * An external data source implementation based on Hiberante {@link org.hibernate.StatelessSession}.
 *
 * <p>Note, stateless session is much faster than regular Hibernate session, but at the expense of
 * not having a first level cache, as well as not performing any cascading operations (both in read
 * operations as well as dirty operations).
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link StatelessHibernateSpaceDataSource} or {@link
 * StatelessHibernateSpaceSynchronizationEndpoint} instead.
 */
public class StatelessHibernateExternalDataSource extends AbstractHibernateExternalDataSource implements BulkDataPersister, SQLDataProvider {

    private Log batchingLogger = LogFactory.getLog("org.hibernate.jdbc.BatchingBatcher");

    /**
     * Perform the given bulk changes using Hibernate {@link org.hibernate.StatelessSession}. First,
     * tries to perform "optimistic" operations without checking in advance for existence of certain
     * entity. If this fails, will try and perform the same operations again, simply with checking
     * if the entry exists or not.
     */
    public void executeBulk(List<BulkItem> bulkItems) throws DataSourceException {
        StatelessSession session = getSessionFactory().openStatelessSession();
        Transaction tr = session.beginTransaction();
        Exception batchModeException = null;
        try {
            for (BulkItem bulkItem : bulkItems) {
                if (!isManaged(bulkItem))
                    continue;

                switch (bulkItem.getOperation()) {
                    case BulkItem.REMOVE:
                        executeRemove(session, bulkItem);
                        break;
                    case BulkItem.WRITE:
                        executeWrite(session, bulkItem);
                        break;
                    case BulkItem.UPDATE:
                        executeUpdate(session, bulkItem);
                        break;
                    case BulkItem.PARTIAL_UPDATE:
                        executePartialUpdate(session, bulkItem);
                        break;
                    default:
                        break;
                }
            }
            tr.commit();
        } catch (Exception e) {
            rollbackTx(tr);
            batchModeException = new DataSourceException("Failed to execute bulk operation in batch mode", e);
        } finally {
            closeSession(session);
        }
        if (batchModeException == null) {
            // all is well, return
            return;
        } else {
            batchingLogger.error("Ignoring Hibernate StaleStateException, trying with exists batching");
        }

        // if something went wrong, do it with exists checks

        Object latest = null;
        session = getSessionFactory().openStatelessSession();
        tr = session.beginTransaction();
        try {
            for (BulkItem bulkItem : bulkItems) {
                if (!isManaged(bulkItem))
                    continue;

                latest = bulkItem;
                switch (bulkItem.getOperation()) {
                    case BulkItem.REMOVE:
                        executeRemoveIfExists(session, bulkItem);
                        break;
                    case BulkItem.WRITE:
                        executeWriteIfExists(session, bulkItem);
                        break;
                    case BulkItem.UPDATE:
                        executeUpdateIfExists(session, bulkItem);
                        break;
                    case BulkItem.PARTIAL_UPDATE:
                        executePartialUpdateIfExists(session, bulkItem);
                    default:
                        break;
                }
            }
            tr.commit();
        } catch (Exception e) {
            rollbackTx(tr);
            throw new DataSourceException("Failed to execute bulk operation, latest object [" + latest + "]", e);
        } finally {
            closeSession(session);
        }
    }

    private void executePartialUpdateIfExists(StatelessSession session, BulkItem bulkItem) {
        if (exists(bulkItem, session))
            executePartialUpdate(session, bulkItem);
    }

    private void executeWriteIfExists(StatelessSession session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();
        if (exists(bulkItem, session)) {
            if (logger.isTraceEnabled()) {
                logger.trace("[Exists WRITE] Update Entry [" + entry + "]");
            }
            session.update(entry);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("[Exists WRITE] Insert Entry [" + entry + "]");
            }
            session.insert(entry);
        }
    }


    private void executeUpdateIfExists(StatelessSession session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();
        if (exists(bulkItem, session)) {
            if (logger.isTraceEnabled()) {
                logger.trace("[Exists UPDATE] Update Entry [" + entry + "]");
            }
            session.update(entry);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("[Exists UPDATE] Insert Entry [" + entry + "]");
            }
            session.insert(entry);
        }
    }


    private void executeRemoveIfExists(StatelessSession session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();
        if (exists(bulkItem, session)) {
            if (logger.isTraceEnabled()) {
                logger.trace("[Exists REMOVE] Deleting Entry [" + entry + "]");
            }
            session.delete(entry);
        }
    }


    private void executeUpdate(StatelessSession session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();

        if (logger.isTraceEnabled()) {
            logger.trace("[Optimistic UPDATE] Update Entry [" + entry + "]");
        }
        session.update(entry);
    }


    private void executeWrite(StatelessSession session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();

        if (logger.isTraceEnabled()) {
            logger.trace("[Optimistic WRITE] Write Entry [" + entry + "]");
        }
        session.insert(entry);
    }


    private void executeRemove(StatelessSession session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();
        if (logger.isTraceEnabled()) {
            logger.trace("[Optimistic REMOVE] Deleting Entry [" + entry + "]");
        }
        session.delete(entry);
    }


    private void executePartialUpdate(StatelessSession session, BulkItem bulkItem) {
        if (logger.isTraceEnabled()) {
            logger.trace("Partial Update Entry [" + bulkItem.toString() + "]");
        }

        // filter non mapped properties 
        final Map<String, Object> itemValues = filterItemValue(bulkItem.getTypeName(), bulkItem.getItemValues());

        String hql = getPartialUpdateHQL(bulkItem, itemValues);

        Query query = session.createQuery(hql);

        for (Map.Entry<String, Object> updateEntry : itemValues.entrySet()) {
            query.setParameter(updateEntry.getKey(), updateEntry.getValue());
        }
        query.setParameter("id_" + bulkItem.getIdPropertyName(), bulkItem.getIdPropertyValue());
        query.executeUpdate();
    }

    /**
     * Returns a {@link org.openspaces.persistency.hibernate.iterator.StatelessListQueryDataIterator}
     * for the given query.
     */
    public DataIterator iterator(SQLQuery sqlQuery) throws DataSourceException {
        if (!isManagedEntry(sqlQuery.getTypeName())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Ignoring query (no mapping in hibernate) [" + sqlQuery + "]");
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Iterator over query [" + sqlQuery + "]");
        }
        return new HibernateProxyRemoverIterator(new StatelessListQueryDataIterator(sqlQuery, getSessionFactory()));
    }

    /**
     * Performs the initial load operation. Iterates over the {@link #setInitialLoadEntries(String[])}
     * inital load entries. If {@link #getInitialLoadChunkSize()} is set to <code>-1</code>, will
     * use {@link org.openspaces.persistency.hibernate.iterator.StatelessScrollableDataIterator} for
     * each entity. If {@link #getInitialLoadChunkSize()} is set to a non <code>-1</code> value,
     * will use the {@link org.openspaces.persistency.hibernate.iterator.StatelessChunkScrollableDataIterator}.
     */
    public DataIterator initialLoad() throws DataSourceException {
        DataIterator[] iterators = new DataIterator[getInitialLoadEntries().length];
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

    protected boolean exists(BulkItem bulkItem, StatelessSession session) {

        Criteria criteria = null;
        switch (bulkItem.getOperation()) {
            case BulkItem.REMOVE:
            case BulkItem.WRITE:
            case BulkItem.UPDATE:
                Object entry = bulkItem.getItem();
                criteria = session.createCriteria(entry.getClass().getName());
                ClassMetadata classMetaData = getSessionFactory().getClassMetadata(entry.getClass());
                criteria.add(Restrictions.idEq(classMetaData.getIdentifier(entry)));
                criteria.setProjection(Projections.rowCount());
                return ((Number) criteria.uniqueResult()).intValue() > 0;
            case BulkItem.PARTIAL_UPDATE:
                criteria = session.createCriteria(bulkItem.getTypeName());
                criteria.add(Restrictions.idEq(bulkItem.getIdPropertyValue()));
                criteria.setProjection(Projections.rowCount());
                return ((Number) criteria.uniqueResult()).intValue() > 0;
            default:
                return false;
        }
    }

    private void rollbackTx(Transaction tr) {
        try {
            tr.rollback();
        } catch (Exception e) {
            // ignore this exception
        }
    }

    private void closeSession(StatelessSession session) {
        session.close();
    }
}