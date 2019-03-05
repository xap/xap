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
import com.gigaspaces.datasource.DataProvider;
import com.gigaspaces.datasource.DataSourceException;
import com.j_spaces.core.IGSEntry;

import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.metadata.ClassMetadata;
import org.openspaces.persistency.hibernate.iterator.DefaultCriteriaByExampleDataIterator;
import org.openspaces.persistency.hibernate.iterator.HibernateIteratorUtils;
import org.openspaces.persistency.hibernate.iterator.HibernateProxyRemoverIterator;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An extension over the default implementation that also implements {@link
 * com.gigaspaces.datasource.DataProvider} (not needed by default). The class main use is to
 * override Hibernate bugs when executing queries with composite objects.
 *
 * @author kimchy
 * @deprecated since 9.5 - use {@link CriteriaHibernateSpaceDataSource} instead.
 */
@Deprecated
public class CriteriaHibernateExternalDataSource extends DefaultHibernateExternalDataSource implements DataProvider {

    private Map<String, ClassMetadata> metaDataTable = new ConcurrentHashMap<String, ClassMetadata>();

    public Object read(Object template) throws DataSourceException {
        if (!isManagedEntry(getMetadata(template).getEntityName())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Ignoring template (no mapping in hibernate) [" + template + "]");
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Read over template [" + template + "]");
        }
        Object object = null;
        Session session = getSessionFactory().openSession();
        session.setFlushMode(FlushMode.NEVER);
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            object = session.get(getMetadata(template).getEntityName(), (Serializable) getId(template));
            tx.rollback();
        } catch (Exception e) {
            if (tx != null) {
                tx.rollback();
            }
            throw new DataSourceException("Exception caught while read with template [" + template + "]", e);
        } finally {
            session.close();
        }
        return HibernateIteratorUtils.unproxy(object);
    }

    public DataIterator iterator(Object template) throws DataSourceException {
        if (!isManagedEntry(getMetadata(template).getEntityName())) {
            if (logger.isTraceEnabled()) {
                logger.trace("Ignoring template (no mapping in hibernate) [" + template + "]");
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Iterator over template [" + template + "]");
        }
        return new HibernateProxyRemoverIterator(new DefaultCriteriaByExampleDataIterator(template, getSessionFactory()));
    }

    /**
     * Return pojo identifier
     */
    protected Object getId(Object template) {
        Object id = null;

        ClassMetadata classMetaData = getMetadata(template);

        if (classMetaData == null) //Unexpected class entity
            return null;

        if (template instanceof IGSEntry) {
            id = ((IGSEntry) template).getFieldValue(classMetaData.getIdentifierPropertyName());
        } else {
            id = classMetaData.getIdentifier(template);
        }

        return id;
    }

    /**
     * Return pojo entry metadata
     */
    protected ClassMetadata getMetadata(Object entry) {
        String pojoName = null;
        if (entry instanceof IGSEntry)
            pojoName = ((IGSEntry) entry).getClassName();
        else
            pojoName = entry.getClass().getName();

        ClassMetadata entryClassMetadata = metaDataTable.get(pojoName);
        if (entryClassMetadata == null) {
            entryClassMetadata = getSessionFactory().getClassMetadata(pojoName);
            if (entryClassMetadata != null)
                metaDataTable.put(pojoName, entryClassMetadata);
        }
        return entryClassMetadata;
    }

}
