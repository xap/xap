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

import com.gigaspaces.datasource.DataSourceException;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.OperationsBatchData;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.sync.SpaceSynchronizationEndpointException;
import com.gigaspaces.sync.TransactionData;

import org.hibernate.HibernateException;
import org.hibernate.ObjectNotFoundException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * The default Hibernate {@link SpaceSynchronizationEndpoint} implementation. Based on Hibernate
 * {@link Session}.
 *
 * @author eitany
 * @since 9.5
 */
public class DefaultHibernateSpaceSynchronizationEndpoint extends AbstractHibernateSpaceSynchronizationEndpoint {

    private final boolean useMerge;
    private final boolean deleteById;

    public DefaultHibernateSpaceSynchronizationEndpoint(SessionFactory sessionFactory, Set<String> managedEntries, boolean useMerge, boolean deleteById) {
        super(sessionFactory, managedEntries);
        this.useMerge = useMerge;
        this.deleteById = deleteById;
    }

    /**
     * Perform the given transaction changes using Hibernate {@link org.hibernate.Session}. <p/>
     * <p>Note, this implementation relies on Hibernate {@link org.hibernate.NonUniqueObjectException}
     * in case the entity is already associated with the given session, and in such a case, will
     * result in performing merge operation (which is more expensive).
     */
    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {
        executeDataSyncOperations(transactionData.getTransactionParticipantDataItems());
    }

    /**
     * Perform the given batch changes using Hibernate {@link org.hibernate.Session}. <p/> <p>Note,
     * this implementation relies on Hibernate {@link org.hibernate.NonUniqueObjectException} in
     * case the entity is already associated with the given session, and in such a case, will result
     * in performing merge operation (which is more expensive).
     */
    @Override
    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
        executeDataSyncOperations(batchData.getBatchDataItems());
    }

    private void executeDataSyncOperations(DataSyncOperation[] dataSyncOperations) {
        Session session = getSessionFactory().openSession();
        Transaction tr = session.beginTransaction();
        Object latest = null;
        try {
            for (DataSyncOperation dataSyncOperation : dataSyncOperations) {

                if (!isManaged(dataSyncOperation))
                    continue;

                latest = dataSyncOperation;
                switch (dataSyncOperation.getDataSyncOperationType()) {
                    case REMOVE:
                        executeRemove(session, dataSyncOperation);
                        break;
                    case WRITE:
                        executeWrite(session, dataSyncOperation);
                        break;
                    case UPDATE:
                        executeUpdate(session, dataSyncOperation);
                        break;
                    case PARTIAL_UPDATE:
                        executePartialUpdate(session, dataSyncOperation);
                        break;
                    default:
                        break;
                }
            }
            tr.commit();
        } catch (Exception e) {
            rollbackTx(tr);
            throw new SpaceSynchronizationEndpointException("Failed to execute bulk operation, latest object [" + latest + "]", e);
        } finally {
            closeSession(session);
        }
    }

    private void executePartialUpdate(Session session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsDocument() || !dataSyncOperation.supportsGetTypeDescriptor())
            return;

        if (logger.isTraceEnabled()) {
            logger.trace("Partial Update Entry [" + dataSyncOperation.toString() + ']');
        }

        final SpaceTypeDescriptor typeDescriptor = dataSyncOperation.getTypeDescriptor();
        final String typeName = typeDescriptor.getTypeName();
        // filter non mapped properties 
        SpaceDocument spaceDocument = dataSyncOperation.getDataAsDocument();
        final Map<String, Object> itemValues = filterItemValue(typeName,
                spaceDocument.getProperties());


        final String hql = getPartialUpdateHQL(dataSyncOperation, itemValues);

        final Query query = session.createQuery(hql);

        for (Map.Entry<String, Object> updateEntry : itemValues.entrySet()) {
            query.setParameter(updateEntry.getKey(), updateEntry.getValue());
        }
        query.setParameter("id_" + typeDescriptor.getIdPropertyName(), spaceDocument.getProperty(typeDescriptor.getIdPropertyName()));
        query.executeUpdate();
    }

    private void executeWrite(Session session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();

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

    private void executeUpdate(Session session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();

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

    private void executeRemove(Session session, DataSyncOperation dataSyncOperation) throws DataSourceException {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();

        if (logger.isTraceEnabled()) {
            logger.trace("Deleting Entry [" + entry + ']');
        }


        if (deleteById) {
            Serializable id = (Serializable) (dataSyncOperation.supportsGetSpaceId() ? dataSyncOperation.getSpaceId() : getIdentifier(entry));
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
