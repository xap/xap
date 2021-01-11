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
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.executor.IQueryExecutor;

import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

/**
 * This is a parent class for the ColumnNode and the LiteralNodes. it represents nodes in the
 * expression tree that have no children
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class ValueNode extends ExpNode {
    private static final long serialVersionUID = 1L;

    public ValueNode() {
        super();
    }

    @Override
    public boolean isValidCompare(Object ob1, Object ob2) {
        return false;
    }

    @Override
    public Object clone() {
        return this; // no children
    }

    @Override
    public boolean isJoined() {
        return false;
    }

    @Override
    public ExpNode newInstance() {
        return new ValueNode();
    }

    @Override
    public void accept(IQueryExecutor executor, ISpaceProxy space, Transaction txn, int readModifier, int max) throws SQLException {
    }

    @Override
    public void accept(QueryTemplateBuilder builder) throws SQLException {
    }
}
