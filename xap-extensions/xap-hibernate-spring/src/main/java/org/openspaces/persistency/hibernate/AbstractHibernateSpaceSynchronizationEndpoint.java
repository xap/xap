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

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.DataSyncOperation;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.metadata.ClassMetadata;
import org.openspaces.persistency.patterns.ManagedEntriesSpaceSynchronizationEndpoint;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A base class for Hibernate based {@link SpaceSynchronizationEndpoint} implementations.
 *
 * @author eitany
 * @since 9.5
 */
public abstract class AbstractHibernateSpaceSynchronizationEndpoint extends ManagedEntriesSpaceSynchronizationEndpoint {

    protected static final Log logger = LogFactory.getLog(AbstractHibernateSpaceSynchronizationEndpoint.class);
    private final ManagedEntitiesContainer sessionManager;
    private final SessionFactory sessionFactory;

    public AbstractHibernateSpaceSynchronizationEndpoint(SessionFactory sessionFactory, Set<String> managedEntries) {
        this.sessionFactory = sessionFactory;
        this.sessionManager = new ManagedEntitiesContainer(sessionFactory, managedEntries);
    }

    /**
     * @return if dataSyncOperation managed return true , else return false
     */
    protected boolean isManaged(DataSyncOperation dataSyncOperation) {
        if (!dataSyncOperation.supportsGetTypeDescriptor())
            return false;

        String typeName = dataSyncOperation.getTypeDescriptor().getTypeName();
        if (!sessionManager.isManagedEntry(typeName)) {
            if (logger.isTraceEnabled()) {
                logger.trace("Entry [" + typeName + ":" + dataSyncOperation + "] is not managed, filtering it out");
            }
            return false;
        }
        return true;

    }

    /**
     * Filter from the input map the unmapped field of this entity
     *
     * @param itemValues map of properties to filter
     */
    protected Map<String, Object> filterItemValue(String entityName, Map<String, Object> itemValues) {
        ClassMetadata classMetadata = getSessionFactory().getClassMetadata(entityName);
        String[] propertyNames = classMetadata.getPropertyNames();
        List<String> names = Arrays.asList(propertyNames);
        HashMap<String, Object> filteredItems = new HashMap<String, Object>();
        Iterator<Entry<String, Object>> iterator = itemValues.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Object> next = iterator.next();
            if (names.contains(next.getKey())) {
                filteredItems.put(next.getKey(), next.getValue());
            }
        }
        return filteredItems;
    }

    protected String getPartialUpdateHQL(DataSyncOperation dataSyncOperation, Map<String, Object> updatedValues) {

        final StringBuilder updateQueryBuilder = new StringBuilder("update ");
        SpaceTypeDescriptor typeDescriptor = dataSyncOperation.getTypeDescriptor();
        updateQueryBuilder.append(typeDescriptor.getTypeName()).append(" set ");

        int i = 0;
        for (Map.Entry<String, Object> updateEntry : updatedValues.entrySet()) {
            updateQueryBuilder.append(updateEntry.getKey()).append("=:").append(updateEntry.getKey());
            if (i < updatedValues.size() - 1)
                updateQueryBuilder.append(',');
            i++;
        }

        updateQueryBuilder.append(" where ").append(typeDescriptor.getIdPropertyName())
                .append("=:id_").append(typeDescriptor.getIdPropertyName());
        String hql = updateQueryBuilder.toString();
        if (logger.isTraceEnabled()) {
            logger.trace("Partial Update HQL [" + hql + ']');
        }
        return hql;
    }

    protected void rollbackTx(Transaction tr) {
        try {
            tr.rollback();
        } catch (Exception e) {
            // ignore this exception
        }
    }

    /**
     * @return the sessionFactory
     */
    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

    /* (non-Javadoc)
     * @see org.openspaces.persistency.patterns.ManagedEntriesSynchronizationEndpointInterceptor#getManagedEntries()
     */
    @Override
    public Iterable<String> getManagedEntries() {
        return sessionManager.getManagedEntries();
    }


}