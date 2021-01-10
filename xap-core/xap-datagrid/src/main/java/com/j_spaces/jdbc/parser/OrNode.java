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
import com.j_spaces.jdbc.query.QueryTableData;

import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

/**
 * This is the OR Node that represents an 'OR' between conditions
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class OrNode extends ExpNode {
    private static final long serialVersionUID = 1L;

    public OrNode(ExpNode leftChild, ExpNode rightChild) {
        super(leftChild, rightChild);
    }

    public OrNode() {
        super(null, null);
    }

    @Override
    public boolean isValidCompare(Object ob1, Object ob2) {
        //this method is never called here really so it doesn't matter
        return false;
    }

    @Override
    public boolean isJoined() {
        return false;
    }

    @Override
    public ExpNode newInstance() {
        return new OrNode();
    }

    @Override
    public void accept(QueryTemplateBuilder builder) throws SQLException {
        builder.build(this);
    }

    public void accept(IQueryExecutor executor, ISpaceProxy space, Transaction txn, int readModifier, int max) throws SQLException {
        executor.execute(this, space, txn, readModifier, max);
    }

    @Override
    public boolean createJoinIndex(QueryTableData tableData) {
        return false;
    }

    @Override
    public String toString() {
        return "(" + toString(" or ") + ")";
    }
}
