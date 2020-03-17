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

import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;
import org.hibernate.CacheMode;
import org.hibernate.query.Query;
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
        String select = toString(dataSourceSQLQuery);
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
        String select = toString(dataSourceSQLQuery);
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

    private static String toString(DataSourceSQLQuery dataSourceSQLQuery) {
        String query = dataSourceSQLQuery.getFromQuery();
        if (DefaultSQLQueryBuilder.ADAPT_POSITIONAL_PARAMETERS) {
            Object[] parameters = dataSourceSQLQuery.getQueryParameters();
            query = adaptPositionalParameters(query, parameters == null ? 0 : parameters.length);
        }
        return query;
    }

    private static String adaptPositionalParameters(String query, int numOfParameters) {
        if (numOfParameters == 0)
            return query;
        String[] tokens = query.split("\\?", -1);
        if (tokens.length != numOfParameters + 1)
            throw new IllegalArgumentException("Cannot convert query positional parameters to jpa format: [" + query + "] - params=" + numOfParameters + ", tokens=" + tokens.length);
        StringBuilder sb = new StringBuilder();
        for (int i=0 ; i < numOfParameters ; i++) {
            sb.append(tokens[i]).append(DefaultSQLQueryBuilder.BIND_PARAMETER).append(i);
        }
        sb.append(tokens[tokens.length-1]);
        return sb.toString();
    }
}
