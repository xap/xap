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


package org.openspaces.persistency.hibernate.iterator;

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.j_spaces.core.client.SQLQuery;

import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.HibernateException;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import java.util.Iterator;

/**
 * A simple iterator that iterates over a {@link com.j_spaces.core.client.SQLQuery} by creating an
 * Hiberante query using Hibernate {@link Session} and listing it.
 *
 * @author kimchy
 */
public class DefaultListQueryDataIterator implements DataIterator {

    protected final SQLQuery<?> sqlQuery;

    protected final DataSourceSQLQuery dataSourceSQLQuery;

    protected final String entityName;

    protected final SessionFactory sessionFactory;

    protected final int from;

    protected final int size;

    protected Transaction transaction;

    protected Session session;

    private Iterator iterator;

    protected int limitResults = -1;

    public DefaultListQueryDataIterator(SQLQuery sqlQuery, SessionFactory sessionFactory) {
        this.sqlQuery = sqlQuery;
        this.entityName = null;
        this.dataSourceSQLQuery = null;
        this.sessionFactory = sessionFactory;
        this.from = -1;
        this.size = -1;
    }

    public DefaultListQueryDataIterator(SQLQuery sqlQuery, SessionFactory sessionFactory, int limitResults) {
        this.sqlQuery = sqlQuery;
        this.entityName = null;
        this.dataSourceSQLQuery = null;
        this.sessionFactory = sessionFactory;
        this.from = -1;
        this.size = -1;
        this.limitResults = limitResults;
    }

    public DefaultListQueryDataIterator(String entityName, SessionFactory sessionFactory) {
        this.entityName = entityName;
        this.sqlQuery = null;
        this.dataSourceSQLQuery = null;
        this.sessionFactory = sessionFactory;
        this.from = -1;
        this.size = -1;
    }

    public DefaultListQueryDataIterator(String entityName, SessionFactory sessionFactory, int limitResults) {
        this.entityName = entityName;
        this.sqlQuery = null;
        this.dataSourceSQLQuery = null;
        this.sessionFactory = sessionFactory;
        this.from = -1;
        this.size = -1;
        this.limitResults = limitResults;
    }

    public DefaultListQueryDataIterator(DataSourceSQLQuery dataSourceSQLQuery, SessionFactory sessionFactory) {
        this.dataSourceSQLQuery = dataSourceSQLQuery;
        this.sqlQuery = null;
        this.entityName = null;
        this.sessionFactory = sessionFactory;
        this.from = -1;
        this.size = -1;
    }

    public DefaultListQueryDataIterator(String entityName, SessionFactory sessionFactory, int from, int size) {
        this.entityName = entityName;
        this.sqlQuery = null;
        this.dataSourceSQLQuery = null;
        this.sessionFactory = sessionFactory;
        this.from = from;
        this.size = size;
    }

    public DefaultListQueryDataIterator(SQLQuery sqlQuery, SessionFactory sessionFactory, int from, int size) {
        this.sqlQuery = sqlQuery;
        this.entityName = null;
        this.dataSourceSQLQuery = null;
        this.sessionFactory = sessionFactory;
        this.from = from;
        this.size = size;
    }

    public boolean hasNext() {
        if (iterator == null) {
            iterator = createIterator();
        }
        return iterator.hasNext();
    }

    public Object next() {
        return iterator.next();
    }

    public void remove() {
        throw new UnsupportedOperationException("remove not supported");
    }

    public void close() {
        try {
            if (transaction == null) {
                return;
            }
            transaction.commit();
        } finally {
            transaction = null;
            if (session != null && session.isOpen()) {
                session.close();
            }
        }
    }

    protected Iterator createIterator() {
        session = sessionFactory.openSession();
        transaction = session.beginTransaction();
        if (entityName != null) {
            return createIteratorFromCriteria();
        } else  if (sqlQuery != null) {
            return createIteratorFromSqlQuery();
        } else if (dataSourceSQLQuery != null) {
            return createIteratorFromDataSourceQuery();
        } else {
            throw new IllegalStateException("Either SQLQuery or entity must be provided");
        }
    }

    private Iterator createIteratorFromCriteria() {
        try {
            Criteria criteria = session.createCriteria(entityName);
            criteria.setCacheMode(CacheMode.IGNORE);
            criteria.setCacheable(false);
            criteria.setFlushMode(FlushMode.MANUAL);
            if (from >= 0) {
                criteria.setFirstResult(from);
                criteria.setMaxResults(size);
            } else if(limitResults > 0){
                criteria.setMaxResults(limitResults);
            }
            return criteria.list().iterator();
        } catch (HibernateException e) {
            throw new SpaceRuntimeException("Failed to create iterator for [" + entityName + "]", e);
        }
    }

    private Iterator createIteratorFromSqlQuery() {
        try {
            Query query = HibernateIteratorUtils.createQueryFromSQLQuery(sqlQuery, session);
            if (from >= 0) {
                query.setFirstResult(from);
                query.setMaxResults(size);
            } else if(limitResults > 0){
                query.setMaxResults(limitResults);
            }
            return query.list().iterator();
        } catch (HibernateException e) {
            throw new SpaceRuntimeException("Failed to create iterator for [" + sqlQuery + "]", e);
        }
    }

    private Iterator createIteratorFromDataSourceQuery() {
        try {
            Query query = HibernateIteratorUtils.createQueryFromDataSourceSQLQuery(dataSourceSQLQuery, session);
            if (from >= 0) {
                query.setFirstResult(from);
                query.setMaxResults(size);
            } else if(limitResults > 0){
                query.setMaxResults(limitResults);
            }
            return query.list().iterator();
        } catch (HibernateException e) {
            throw new SpaceRuntimeException("Failed to create iterator for [" + dataSourceSQLQuery + "]", e);
        }
    }

    protected Iterator createIterator(Query query) {
        return query.list().iterator();
    }
}
