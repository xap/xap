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

import com.gigaspaces.datasource.DataSourceSQLQuery;
import com.j_spaces.core.client.SQLQuery;

import org.hibernate.CacheMode;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.StatelessSession;
import org.hibernate.proxy.HibernateProxy;

/**
 * @author kimchy
 */
public class HibernateIteratorUtils {

    public static Object unproxy(Object entity) {
        if (entity instanceof HibernateProxy) {
            entity = ((HibernateProxy) entity).getHibernateLazyInitializer().getImplementation();
        }
        return entity;
    }

    public static Query createQueryFromSQLQuery(SQLQuery<?> sqlQuery, Session session) {
        String select = sqlQuery.getFromQuery();
        Query query = session.createQuery(select);
        Object[] preparedValues = sqlQuery.getParameters();
        if (preparedValues != null) {
            for (int i = 0; i < preparedValues.length; i++) {
                query.setParameter(i, preparedValues[i]);
            }
        }
        query.setCacheMode(CacheMode.IGNORE);
        query.setCacheable(false);
        query.setReadOnly(true);
        return query;
    }

    public static Query createQueryFromSQLQuery(SQLQuery<?> sqlQuery, StatelessSession session) {
        String select = sqlQuery.getFromQuery();
        Query query = session.createQuery(select);
        Object[] preparedValues = sqlQuery.getParameters();
        if (preparedValues != null) {
            for (int i = 0; i < preparedValues.length; i++) {
                query.setParameter(i, preparedValues[i]);
            }
        }
        query.setReadOnly(true);
        return query;
    }

    public static Query createQueryFromDataSourceSQLQuery(DataSourceSQLQuery dataSourceSQLQuery, Session session) {
        String select = dataSourceSQLQuery.getFromQuery();
        Query query = session.createQuery(select);
        Object[] preparedValues = dataSourceSQLQuery.getQueryParameters();
        if (preparedValues != null) {
            for (int i = 0; i < preparedValues.length; i++) {
                query.setParameter(i, preparedValues[i]);
            }
        }
        query.setReadOnly(true);
        return query;
    }

    public static Query createQueryFromDataSourceSQLQuery(DataSourceSQLQuery dataSourceSQLQuery, StatelessSession session) {
        String select = dataSourceSQLQuery.getFromQuery();
        Query query = session.createQuery(select);
        Object[] preparedValues = dataSourceSQLQuery.getQueryParameters();
        if (preparedValues != null) {
            for (int i = 0; i < preparedValues.length; i++) {
                query.setParameter(i, preparedValues[i]);
            }
        }
        query.setReadOnly(true);
        return query;
    }
}
