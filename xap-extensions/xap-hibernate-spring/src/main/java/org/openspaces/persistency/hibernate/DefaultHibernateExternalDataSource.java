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

import org.hibernate.HibernateException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.openspaces.persistency.hibernate.iterator.DefaultChunkListDataIterator;
import org.openspaces.persistency.hibernate.iterator.DefaultChunkScrollableDataIterator;
import org.openspaces.persistency.hibernate.iterator.DefaultListQueryDataIterator;
import org.openspaces.persistency.hibernate.iterator.DefaultScrollableDataIterator;
import org.openspaces.persistency.hibernate.iterator.HibernateProxyRemoverIterator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * The default Hibernate external data source implementation. Based on Hibernate {@link Session}.
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link DefaultHibernateSpaceDataSource} or {@link
 * DefaultHibernateSpaceSynchronizationEndpoint} instead.
 */
@Deprecated
public class DefaultHibernateExternalDataSource extends AbstractHibernateExternalDataSource implements BulkDataPersister, SQLDataProvider {

    private boolean useMerge = false;
    private boolean deleteById = true;


    /**
     * If set to <code>true</code>, will use Hibernate <code>merge</code> to perform the
     * create/update, and will merge before calling delete. This might be required for complex
     * mappings (depends on Hibernate) at the expense of slower performance. Defaults to
     * <code>false</code>.
     */
    public void setUseMerge(boolean useMerge) {
        this.useMerge = useMerge;
    }

    /**
     * If set to <code>true</code> the object will be deleted using only its id. If set to
     * <code>false</code> the object will be deleted using the whole object. Defaults to
     * <code>true</code>.
     */
    public void setDeleteById(boolean deleteById) {
        this.deleteById = deleteById;
    }

    /**
     * Perform the given bulk changes using Hibernate {@link org.hibernate.Session}. <p/> <p>Note,
     * this implementation relies on Hibernate {@link org.hibernate.NonUniqueObjectException} in
     * case the entity is already associated with the given session, and in such a case, will result
     * in performing merge operation (which is more expensive).
     */
    public void executeBulk(List<BulkItem> bulkItems) throws DataSourceException {
        Session session = getSessionFactory().openSession();
        Transaction tr = session.beginTransaction();
        Object latest = null;
        try {
            for (BulkItem bulkItem : bulkItems) {

                if (!isManaged(bulkItem))
                    continue;

                latest = bulkItem;
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
            throw new DataSourceException("Failed to execute bulk operation, latest object [" + latest + "]", e);
        } finally {
            closeSession(session);
        }
    }

    private void executePartialUpdate(Session session, BulkItem bulkItem) {
        if (logger.isTraceEnabled()) {
            logger.trace("Partial Update Entry [" + bulkItem.toString() + ']');
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

    private void executeWrite(Session session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();

        if (logger.isTraceEnabled()) {
            logger.trace("Write Entry [" + entry + ']');
        }
        if (useMerge) {
            session.merge(entry);
        } else {
            try {
                session.saveOrUpdate(entry);
            } catch (HibernateException e) {
                session.merge(entry);
            }
        }

    }

    private void executeUpdate(Session session, BulkItem bulkItem) {
        Object entry = bulkItem.getItem();

        if (logger.isTraceEnabled()) {
            logger.trace("Update Entry [" + entry + ']');
        }
        if (useMerge) {
            session.merge(entry);
        } else {
            try {
                session.saveOrUpdate(entry);
            } catch (HibernateException e) {
                session.merge(entry);
            }
        }

    }

    private void executeRemove(Session session, BulkItem bulkItem) throws DataSourceException {
        Object entry = bulkItem.getItem();

        if (logger.isTraceEnabled()) {
            logger.trace("Deleting Entry [" + entry + ']');
        }


        if (deleteById) {

            Serializable id = (Serializable) (bulkItem.supportsGetSpaceId() ? bulkItem.getSpaceId() : getIdentifier(entry));
            if (id == null)
                throw new DataSourceException(
                        "Object id is null. Make sure object space id and hibernate id are the same property.");

            // ignore non existing objects - avoid unnecessary failures                            
            try {
                Object toDelete = session.load(entry.getClass(), id);

                if (toDelete != null)
                    session.delete(toDelete);
            } catch (ObjectNotFoundException e) {
                // ignore non existing objects - avoid unnecessary failures
                if (logger.isTraceEnabled()) {
                    logger.trace("Delete Entry failed [" + entry + ']', e);
                }
            }

        } else {
            if (useMerge) {
                session.delete(session.merge(entry));
            } else {
                try {
                    session.delete(entry);
                } catch (HibernateException e) {
                    session.delete(session.merge(entry));
                }
            }
        }

    }

    /**
     * Returns a {@link org.openspaces.persistency.hibernate.iterator.DefaultListQueryDataIterator}
     * for the given sql query.
     */
    public DataIterator iterator(SQLQuery sqlQuery) throws DataSourceException {
        if (!isManagedEntry(sqlQuery.getTypeName())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Ignoring query (no mapping in hibernate) [" + sqlQuery + ']');
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Iterator over query [" + sqlQuery + ']');
        }
        return new HibernateProxyRemoverIterator(new DefaultListQueryDataIterator(sqlQuery, getSessionFactory()));
    }

    /**
     * Performs the initial load operation. Iterates over the {@link #setInitialLoadEntries(String[])}
     * initial load entries. If {@link #getInitialLoadChunkSize()} is set to <code>-1</code>, will
     * use {@link org.openspaces.persistency.hibernate.iterator.DefaultScrollableDataIterator} for
     * each entity. If {@link #getInitialLoadChunkSize()} is set to a non <code>-1</code> value,
     * will use the {@link org.openspaces.persistency.hibernate.iterator.DefaultChunkScrollableDataIterator}.
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
                    iterators[iteratorCounter++] = new DefaultScrollableDataIterator(entityName, getSessionFactory(), getFetchSize(), isPerformOrderById());
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load list iterator for entry [" + entityName + "]");
                    }
                    iterators[iteratorCounter++] = new DefaultListQueryDataIterator(entityName, getSessionFactory());
                }
            } else {
                if (isUseScrollableResultSet()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load chunk scrollable iterator for entry [" + entityName + "]");
                    }
                    iterators[iteratorCounter++] = new DefaultChunkScrollableDataIterator(entityName, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize());
                } else {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Creating initial load chunk list iterator for entry [" + entityName + "]");
                    }
                    iterators[iteratorCounter++] = new DefaultChunkListDataIterator(entityName, getSessionFactory(), getFetchSize(), isPerformOrderById(), getInitialLoadChunkSize());
                }
            }
        }
        return createInitialLoadIterator(iterators);
    }

    private void rollbackTx(Transaction tr) {
        try {
            tr.rollback();
        } catch (Exception e) {
            // ignore this exception
        }
    }

    private void closeSession(Session session) {
        if (session.isOpen()) {
            session.close();
        }
    }

    /**
     * Extracts and returns the hibernate object identifier
     *
     * @return serializable
     */
    protected Serializable getIdentifier(Object o) {

        return getSessionFactory().getClassMetadata(o.getClass()).getIdentifier(o);

    }
}
