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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;

import java.util.LinkedList;
import java.util.List;

/**
 * Provides a logical OR combination for a list of custom queries.
 *
 * @author Anna Pavtulov
 * @since 8.0.1
 */
@com.gigaspaces.api.InternalApi
public class CompoundOrCustomQuery extends AbstractCompundCustomQuery {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor for Externalizable.
     */
    public CompoundOrCustomQuery() {
    }

    public CompoundOrCustomQuery(List<ICustomQuery> subQueries) {
        super(subQueries);
    }


    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        final int length = _subQueries.size();
        for (int i = 0; i < length; i++)
            if (_subQueries.get(i).matches(cacheManager, entry, skipAlreadyMatchedIndexPath))
                return true;

        return false;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
     */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {

        List preparedValues = new LinkedList();
        StringBuilder b = new StringBuilder();
        for (ICustomQuery query : _subQueries) {
            SQLQuery sqlQuery = query.toSQLQuery(typeDesc);

            if (b.length() > 0)
                b.append(DefaultSQLQueryBuilder.OR);
            b.append(sqlQuery.getQuery());

            if (sqlQuery.getParameters() == null)
                continue;

            for (int i = 0; i < sqlQuery.getParameters().length; i++) {
                preparedValues.add(sqlQuery.getParameters()[i]);
            }
        }


        return new SQLQuery(typeDesc.getTypeName(), b.toString(), preparedValues.toArray());
    }
}
