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
import com.gigaspaces.sync.SpaceSynchronizationEndpointException;
import com.gigaspaces.sync.SynchronizationEndpointInterceptor;
import com.gigaspaces.sync.TransactionData;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.hibernate.StatelessSession;
import org.hibernate.Transaction;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.metadata.ClassMetadata;

import java.util.Map;
import java.util.Set;

/**
 * A {@link SynchronizationEndpointInterceptor} implementation based on Hibernate {@link
 * org.hibernate.StatelessSession}.
 *
 * <p>Note, stateless session is much faster than regular Hibernate session, but at the expense of
 * not having a first level cache, as well as not performing any cascading operations (both in read
 * operations as well as dirty operations).
 *
 * @author eitany
 * @since 9.5
 */
public class StatelessHibernateSpaceSynchronizationEndpoint extends AbstractHibernateSpaceSynchronizationEndpoint {

    private static final Log batchingLogger = LogFactory.getLog("org.hibernate.jdbc.BatchingBatcher");

    public StatelessHibernateSpaceSynchronizationEndpoint(SessionFactory sessionFactory, Set<String> managedEntries) {
        super(sessionFactory, managedEntries);
    }

    /**
     * Perform the given transation changes using Hibernate {@link org.hibernate.StatelessSession}.
     * First, tries to perform "optimistic" operations without checking in advance for existence of
     * certain entity. If this fails, will try and perform the same operations again, simply with
     * checking if the entry exists or not.
     */
    @Override
    public void onTransactionSynchronization(TransactionData transactionData) {
        executeDataSyncOperations(transactionData.getTransactionParticipantDataItems());
    }

    /**
     * Perform the given batch changes using Hibernate {@link org.hibernate.StatelessSession}.
     * First, tries to perform "optimistic" operations without checking in advance for existence of
     * certain entity. If this fails, will try and perform the same operations again, simply with
     * checking if the entry exists or not.
     */
    @Override
    public void onOperationsBatchSynchronization(OperationsBatchData batchData) {
        executeDataSyncOperations(batchData.getBatchDataItems());
    }

    private void executeDataSyncOperations(DataSyncOperation[] dataSyncOperations) {
        StatelessSession session = getSessionFactory().openStatelessSession();
        Transaction tr = session.beginTransaction();
        Exception batchModeException = null;
        try {
            for (DataSyncOperation dataSyncOperation : dataSyncOperations) {
                if (!isManaged(dataSyncOperation))
                    continue;

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
            for (DataSyncOperation dataSyncOperation : dataSyncOperations) {
                if (!isManaged(dataSyncOperation))
                    continue;

                latest = dataSyncOperation;
                switch (dataSyncOperation.getDataSyncOperationType()) {
                    case REMOVE:
                        executeRemoveIfExists(session, dataSyncOperation);
                        break;
                    case WRITE:
                        executeWriteIfExists(session, dataSyncOperation);
                        break;
                    case UPDATE:
                        executeUpdateIfExists(session, dataSyncOperation);
                        break;
                    case PARTIAL_UPDATE:
                        executePartialUpdateIfExists(session, dataSyncOperation);
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

    private void executePartialUpdateIfExists(StatelessSession session, DataSyncOperation dataSyncOperation) {
        if (exists(dataSyncOperation, session))
            executePartialUpdate(session, dataSyncOperation);
    }

    private void executeWriteIfExists(StatelessSession session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();
        if (exists(dataSyncOperation, session)) {
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


    private void executeUpdateIfExists(StatelessSession session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();
        if (exists(dataSyncOperation, session)) {
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


    private void executeRemoveIfExists(StatelessSession session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();
        if (exists(dataSyncOperation, session)) {
            if (logger.isTraceEnabled()) {
                logger.trace("[Exists REMOVE] Deleting Entry [" + entry + "]");
            }
            session.delete(entry);
        }
    }


    private void executeUpdate(StatelessSession session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();

        if (logger.isTraceEnabled()) {
            logger.trace("[Optimistic UPDATE] Update Entry [" + entry + "]");
        }
        session.update(entry);
    }


    private void executeWrite(StatelessSession session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();

        if (logger.isTraceEnabled()) {
            logger.trace("[Optimistic WRITE] Write Entry [" + entry + "]");
        }
        session.insert(entry);
    }


    private void executeRemove(StatelessSession session, DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsDataAsObject())
            return;

        Object entry = dataSyncOperation.getDataAsObject();
        if (logger.isTraceEnabled()) {
            logger.trace("[Optimistic REMOVE] Deleting Entry [" + entry + "]");
        }
        session.delete(entry);
    }


    private void executePartialUpdate(StatelessSession session, DataSyncOperation dataSyncOperation) {

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

    private void closeSession(StatelessSession session) {
        session.close();
    }

    private boolean exists(DataSyncOperation dataSyncOperation, StatelessSession session) {

        Criteria criteria = null;
        switch (dataSyncOperation.getDataSyncOperationType()) {
            case REMOVE:
            case WRITE:
            case UPDATE:
                if (!dataSyncOperation.supportsDataAsObject())
                    return false;
                Object entry = dataSyncOperation.getDataAsObject();
                criteria = session.createCriteria(entry.getClass().getName());
                ClassMetadata classMetaData = getSessionFactory().getClassMetadata(entry.getClass());
                criteria.add(Restrictions.idEq(classMetaData.getIdentifier(entry)));
                criteria.setProjection(Projections.rowCount());
                return ((Number) criteria.uniqueResult()).intValue() > 0;
            case PARTIAL_UPDATE:
                if (!dataSyncOperation.supportsGetTypeDescriptor())
                    return false;
                SpaceTypeDescriptor typeDescriptor = dataSyncOperation.getTypeDescriptor();
                criteria = session.createCriteria(typeDescriptor.getTypeName());
                criteria.add(Restrictions.idEq(typeDescriptor.getIdPropertyName()));
                criteria.setProjection(Projections.rowCount());
                return ((Number) criteria.uniqueResult()).intValue() > 0;
            default:
                return false;
        }
    }

}
