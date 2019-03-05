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

import com.gigaspaces.datasource.DataIterator;

import org.hibernate.CacheMode;
import org.hibernate.Criteria;
import org.hibernate.FlushMode;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.criterion.Example;

import java.util.Iterator;

/**
 * A simple iterator that iterates over template object using Hibernate Criteria by example.
 *
 * @author kimchy
 */
public class DefaultCriteriaByExampleDataIterator implements DataIterator {

    protected final Object template;

    protected final SessionFactory sessionFactory;

    protected Transaction transaction;

    protected Session session;

    private Iterator iterator;

    public DefaultCriteriaByExampleDataIterator(Object template, SessionFactory sessionFactory) {
        this.template = template;
        this.sessionFactory = sessionFactory;
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
        Example example = Example.create(template);
        Criteria criteria = session.createCriteria(template.getClass()).add(example);
        criteria.setCacheMode(CacheMode.IGNORE);
        criteria.setCacheable(false);
        criteria.setFlushMode(FlushMode.NEVER);
        return criteria.list().iterator();
    }
}