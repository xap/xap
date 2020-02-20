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
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.serialization.IllegalSerializationVersionException;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

/**
 * Provides a logical AND combination for a list of custom queries.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class CompoundAndCustomQuery extends AbstractCompundCustomQuery {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    // If serialization changes, increment GigaspacesVersionID and modify read/writeExternal appropiately.
    private static final byte GigaspacesVersionID = 1;

    /**
     * Default constructor for Externalizable.
     */
    public CompoundAndCustomQuery() {
    }

    public CompoundAndCustomQuery(List<ICustomQuery> subQueries) {
        super(subQueries);
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        final int length = _subQueries.size();
        for (int i = 0; i < length; i++)
            if (!_subQueries.get(i).matches(cacheManager, entry, skipAlreadyMatchedIndexPath))
                return false;

        return true;
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
                b.append(DefaultSQLQueryBuilder.AND);
            b.append(sqlQuery.getQuery());

            if (sqlQuery.getParameters() == null)
                continue;

            for (int i = 0; i < sqlQuery.getParameters().length; i++) {
                preparedValues.add(sqlQuery.getParameters()[i]);
            }
        }


        return new SQLQuery(typeDesc.getTypeName(), b.toString(), preparedValues.toArray());
    }



    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        byte version = in.readByte();

        if (version == GigaspacesVersionID)
            readExternalV1(in);
        else {
            switch (version) {
                default:
                    throw new IllegalSerializationVersionException(TypeDesc.class, version);
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        out.writeByte(GigaspacesVersionID);
        writeExternalV1(out);
    }

    private void readExternalV1(ObjectInput in)
            throws IOException, ClassNotFoundException {
    }

    private void writeExternalV1(ObjectOutput out)
            throws IOException {
    }

}
