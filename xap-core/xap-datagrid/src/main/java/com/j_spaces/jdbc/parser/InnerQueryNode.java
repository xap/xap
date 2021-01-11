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

package com.j_spaces.jdbc.parser;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.jdbc.ResultEntry;
import com.j_spaces.jdbc.SelectQuery;
import com.j_spaces.jdbc.executor.IQueryExecutor;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;

/**
 * Represents an inner (sub) query in a WHERE clause condition.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class InnerQueryNode extends LiteralNode {
    private static final long serialVersionUID = 1L;

    protected SelectQuery _innerQuery;
    protected ResultEntry _results;

    public InnerQueryNode() {
    }

    public InnerQueryNode(SelectQuery innerQuery) {
        super(null);
        _innerQuery = innerQuery;
    }

    public SelectQuery getInnerQuery() {
        return _innerQuery;
    }

    public ResultEntry getResult() {
        return _results;
    }

    public void setResults(ResultEntry resultEntry) {
        _results = resultEntry;
    }

    /**
     * Gets whether the result is a single value result
     */
    public boolean isSingleResult() {
        return _results.getFieldNames().length == 1
                && _results.getFieldValues().length == 1;
    }

    /**
     * Gets whether the result is empty
     */
    public boolean isEmptyResult() {
        return _results.getRowNumber() == 0;
    }

    /**
     * Gets whether the result only contains one column.
     */
    public boolean isSingleColumnResult() {
        return _results.getFieldNames().length == 1;
    }

    /**
     * Gets the inner query's single result
     */
    public Object getSingleResult() {
        return isSingleResult() ? _results.getFieldValues()[0][0] : null;
    }

    @Override
    public Object getValue() {
        if (isSingleResult())
            return getSingleResult();
        return null;
    }

    @Override
    public void accept(IQueryExecutor executor, ISpaceProxy space,
                       Transaction txn, int readModifier, int max) throws SQLException {
        executor.execute(this, space, txn, readModifier, max);
    }

    @Override
    public Object getConvertedObject(ITypeDesc typeDesc, String propertyName)
            throws SQLException {
        return getValue();
    }

    @Override
    public Object clone() {
        InnerQueryNode clone = new InnerQueryNode((SelectQuery) _innerQuery.clone());
        return clone;
    }

    @Override
    public boolean isInnerQuery() {
        return true;
    }

    @Override
    public String toString() {
        if (_results != null)
            return _results.toString();
        return super.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _innerQuery);
        IOUtils.writeObject(out, _results);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _innerQuery = IOUtils.readObject(in);
        _results = IOUtils.readObject(in);
    }
}
