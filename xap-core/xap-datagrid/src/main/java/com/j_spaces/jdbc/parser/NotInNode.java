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
 * The NotInNode. it is used for cases of nested queries as part of a WHERE clause.
 *
 * @author Alex
 */
@com.gigaspaces.api.InternalApi
public class NotInNode extends AbstractInNode {
    private static final long serialVersionUID = 1L;

    public NotInNode() {
        super();
    }

    @Override
    public boolean isValidCompare(Object ob1, Object ob2) {
        // never called here
        return false;
    }

    @Override
    public boolean isJoined() {
        return false;
    }

    @Override
    public ExpNode newInstance() {
        return new NotInNode();
    }

    @Override
    public void accept(QueryTemplateBuilder builder) throws SQLException {
        builder.build(this);
    }

    @Override
    public void accept(IQueryExecutor executor, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        executor.execute(this, space, txn, readModifier, max);
    }
}
