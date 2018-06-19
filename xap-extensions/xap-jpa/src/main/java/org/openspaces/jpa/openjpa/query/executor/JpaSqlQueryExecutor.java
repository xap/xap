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

package org.openspaces.jpa.openjpa.query.executor;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.SQLQuery;

import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.openspaces.jpa.StoreManager;
import org.openspaces.jpa.openjpa.query.SpaceResultObjectProvider;

/**
 * Executes JPA's translated expression tree using GigaSpaces SQLQuery template.
 *
 * @author idan
 * @since 8.0
 */
class JpaSqlQueryExecutor extends AbstractJpaQueryExecutor {

    protected SQLQuery<Object> _sqlQuery;

    public JpaSqlQueryExecutor(QueryExpressions expression, ClassMetaData cm, Object[] parameters) {
        super(expression, cm, parameters);
    }

    @Override
    public ResultObjectProvider execute(StoreManager store) throws Exception {
        final ISpaceProxy proxy = (ISpaceProxy) store.getConfiguration().getSpace();
        final Object[] result = proxy.readMultiple(_sqlQuery, store.getCurrentTransaction(), Integer.MAX_VALUE);
        final IEntryPacket[] entries = new IEntryPacket[result.length];
        for (int i = 0; i < result.length; i++) {
            entries[i] = proxy.getDirectProxy().getTypeManager().getEntryPacketFromObject(result[i],
                    ObjectType.POJO);
        }
        return new SpaceResultObjectProvider(_classMetaData, entries, store);
    }

    @Override
    protected void build() {
        super.build();
        _sqlQuery = new SQLQuery<Object>(_classMetaData.getDescribedType().getName(),
                _sql.toString());
        // Set query parameters (if needed) - the parameters are ordered by index
        for (int i = 0; i < _parameters.length; i++) {
            _sqlQuery.setParameter(i + 1, _parameters[i]);
        }
    }

}
