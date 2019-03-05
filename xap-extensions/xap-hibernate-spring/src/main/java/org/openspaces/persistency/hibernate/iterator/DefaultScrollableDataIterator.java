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

import com.j_spaces.core.client.SQLQuery;

import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.Query;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Order;
import org.hibernate.metadata.ClassMetadata;

/**
 * A default scrollable result iterator based on Hibernate {@link Session}.
 *
 * @author kimchy
 */
public class DefaultScrollableDataIterator extends AbstractScrollableDataIterator {

    protected Session session;

    protected Transaction transaction;

    /**
     * Constructs a scrollable iterator over the given entity name.
     *
     * @param entityName       The entity name to scroll over
     * @param sessionFactory   The session factory to use to construct the session
     * @param fetchSize        The fetch size of the scrollabale result set
     * @param performOrderById Should the query perform order by id or not
     */
    public DefaultScrollableDataIterator(String entityName, SessionFactory sessionFactory, int fetchSize, boolean performOrderById) {
        super(entityName, sessionFactory, fetchSize, performOrderById);
    }

    /**
     * Constructs a scrollable iterator over the given entity name.
     *
     * @param entityName       The entity name to scroll over
     * @param sessionFactory   The session factory to use to constrcut the session
     * @param fetchSize        The fetch size of the scrollable result set
     * @param performOrderById Should the query perform order by id or not
     * @param from             The from index to scroll from
     * @param size             The size of data to scroll to
     */
    public DefaultScrollableDataIterator(String entityName, SessionFactory sessionFactory, int fetchSize, boolean performOrderById, int from, int size) {
        super(entityName, sessionFactory, fetchSize, performOrderById, from, size);
    }

    /**
     * Constructs a scrollable iterator over the given GigaSpaces <code>SQLQuery</code>.
     *
     * @param sqlQuery         The <code>SQLQuery</code> to scroll over
     * @param sessionFactory   The session factory to use to construct the session
     * @param fetchSize        The fetch size of the scrollabale result set
     * @param performOrderById Should the query perform order by id or not
     */
    public DefaultScrollableDataIterator(SQLQuery sqlQuery, SessionFactory sessionFactory, int fetchSize, boolean performOrderById) {
        super(sqlQuery, sessionFactory, fetchSize, performOrderById);
    }

    /**
     * Constructs a scrollable iterator over the given GigaSpaces <code>SQLQuery</code>.
     *
     * @param sqlQuery         The <code>SQLQuery</code> to scroll over
     * @param sessionFactory   The session factory to use to constrcut the session
     * @param fetchSize        The fetch size of the scrollable result set
     * @param performOrderById Should the query perform order by id or not
     * @param from             The from index to scroll from
     * @param size             The size of data to scroll to
     */
    public DefaultScrollableDataIterator(SQLQuery sqlQuery, SessionFactory sessionFactory, int fetchSize, boolean performOrderById, int from, int size) {
        super(sqlQuery, sessionFactory, fetchSize, performOrderById, from, size);
    }

    /**
     * Constructs a scrollable iterator over the given hibernate query string.
     *
     * @param hQuery         The hiberante query string to scroll over
     * @param sessionFactory The session factory to use to construct the session
     * @param fetchSize      The fetch size of the scrollabale result set
     */
    public DefaultScrollableDataIterator(String hQuery, SessionFactory sessionFactory, int fetchSize) {
        super(hQuery, sessionFactory, fetchSize);
    }

    /**
     * Constructs a scrollable iterator over the given hibernate query string.
     *
     * @param hQuery         The hiberante query string to scroll over
     * @param sessionFactory The session factory to use to constrcut the session
     * @param fetchSize      The fetch size of the scrollable result set
     * @param from           The from index to scroll from
     * @param size           The size of data to scroll to
     */
    public DefaultScrollableDataIterator(String hQuery, SessionFactory sessionFactory, int fetchSize, int from, int size) {
        super(hQuery, sessionFactory, fetchSize, from, size);
    }

    protected void doClose() {
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

    protected void clear() {
        session.clear();
    }

    protected ScrollableResults createCursor() {
        session = sessionFactory.openSession();
        session.setFlushMode(FlushMode.MANUAL);
        transaction = session.beginTransaction();
        if (entityName != null) {
            Criteria criteria = session.createCriteria(entityName);
            criteria.setCacheable(false);
            criteria.setFlushMode(FlushMode.MANUAL);
            criteria.setFetchSize(fetchSize);
            if (perfromOrderById) {
                ClassMetadata metadata = sessionFactory.getClassMetadata(entityName);
                String idPropName = metadata.getIdentifierPropertyName();
                if (idPropName != null) {
                    criteria.addOrder(Order.asc(idPropName));
                }
            }
            if (from >= 0) {
                if (from > 0)
                    criteria.setFirstResult(from);
                criteria.setMaxResults(size);
            }
            return criteria.scroll(ScrollMode.FORWARD_ONLY);
        } else if (sqlQuery != null) {
            Query query = HibernateIteratorUtils.createQueryFromSQLQuery(sqlQuery, session);
            query.setFetchSize(fetchSize);
            if (from >= 0) {
                if (from > 0)
                    query.setFirstResult(from);
                query.setMaxResults(size);
            }
            return query.scroll(ScrollMode.FORWARD_ONLY);
        } else if (hQuery != null) {
            Query query = session.createQuery(hQuery);
            query.setFetchSize(fetchSize);
            if (from >= 0) {
                if (from > 0)
                    query.setFirstResult(from);
                query.setMaxResults(size);
            }
            query.setCacheMode(CacheMode.IGNORE);
            query.setCacheable(false);
            query.setReadOnly(true);
            return query.scroll(ScrollMode.FORWARD_ONLY);
        } else {
            throw new IllegalStateException("No SQLQuery, entity, or Hibernate Query provided");
        }
    }
}
