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

package com.j_spaces.jdbc.executor;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderCallable;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.parser.AndNode;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.parser.InNode;
import com.j_spaces.jdbc.parser.NotInNode;
import com.j_spaces.jdbc.parser.OrNode;
import com.j_spaces.jdbc.query.IQueryResultSet;

import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

/**
 * Executes queries against the space. The executor traverses the expression tree, and executes
 * queries according to the attached space templates.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class QueryExecutor extends AbstractQueryExecutor {
    private static final long serialVersionUID = 1L;

    // Required for Externalizable
    public QueryExecutor() {
    }

    public QueryExecutor(AbstractDMLQuery query) {
        super(query);
    }

    public void execute(OrNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        // if template is simple - just execute it
        if (exp.getTemplate() != null) {
            IQueryResultSet<IEntryPacket> results = executeTemplate(exp.getTemplate(), space, txn, readModifier, max);
            this.setResults(exp, results);
            return;
        }

        // Handle null children - happens in case of using 'or' with rownum
        if (exp.getLeftChild() == null) {
            this.setResults(exp, extractResults(exp.getRightChild()));
            return;
        }

        if (exp.getRightChild() == null) {
            this.setResults(exp, extractResults(exp.getLeftChild()));
            return;
        }


        IQueryResultSet<IEntryPacket> leftResult = extractResults(exp.getLeftChild());

        IQueryResultSet<IEntryPacket> rightResult = extractResults(exp.getRightChild());


        // check if maximum entries was fetched - if so no need to execute the
        // right query
        if (leftResult.size() >= max) {
            this.setResults(exp, leftResult);
            return;
        }
        if (rightResult.size() >= max) {
            this.setResults(exp, rightResult);
            return;
        }
        // if left OR expression didn't return any result - return the right
        // result
        if (leftResult.isEmpty()) {
            this.setResults(exp, rightResult);
            return;
        }

        if (rightResult.isEmpty()) {
            this.setResults(exp, leftResult);
            return;
        }

        // if both results ae not empty - unify them
        IQueryResultSet<IEntryPacket> results = leftResult.union(rightResult);
        this.setResults(exp, results);

    }

    public void execute(AndNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        // if template is simple - just execute it
        if (exp.getTemplate() != null) {
            IQueryResultSet<IEntryPacket> results = executeTemplate(exp.getTemplate(), space, txn, readModifier, max);
            this.setResults(exp, results);
            return;
        }
        // Handle null children - happens in case of using 'and' with rownum
        if (exp.getLeftChild() == null) {
            setResults(exp, extractResults(exp.getRightChild()));
            return;
        }

        if (exp.getRightChild() == null) {
            setResults(exp, extractResults(exp.getLeftChild()));
            return;
        }


        IQueryResultSet<IEntryPacket> leftResult = extractResults(exp.getLeftChild());
        IQueryResultSet<IEntryPacket> rightResult = extractResults(exp.getRightChild());


        // if left AND expression didn't return any result - return,
        // no need to execute the right expression
        if (leftResult.isEmpty()) {
            setResults(exp, leftResult);
            return;
        }

        if (rightResult.isEmpty()) {
            setResults(exp, rightResult);
            return;
        }


        IQueryResultSet<IEntryPacket> andResult = leftResult.intersect(rightResult);
        setResults(exp, andResult);

    }

    public void execute(InNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        QueryTemplatePacket template = exp.getTemplate();
        // If template wasn't set during the building phase - inner query
        if (template == null) {
            // Validate inner query results
            exp.validateInnerQueryResult();

            // Build a template for current node
            template = QueryTemplateBuilder.buildTemplate(exp, query);
            template.prepareForSpace(query.getTypeInfo());
        }

        IQueryResultSet<IEntryPacket> results = executeTemplate(template, space, txn, readModifier, max);
        setResults(exp, results);
    }

    public void execute(NotInNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {

        QueryTemplatePacket template = exp.getTemplate();
        // If template wasn't set during the building phase - inner query
        if (template == null) {
            // Validate inner query results
            exp.validateInnerQueryResult();

            // Build a template for current node
            template = QueryTemplateBuilder.buildTemplate(exp, query);
            template.prepareForSpace(query.getTypeInfo());
        }

        IQueryResultSet<IEntryPacket> results = executeTemplate(template, space, txn, readModifier, max);
        setResults(exp, results);

    }

    public static class ExecutionTask extends ContextClassLoaderCallable<IQueryResultSet<IEntryPacket>> {
        // fields necessary for template execution
        private ISpaceProxy _space;
        private Transaction _transaction;
        private int _maxObjects;
        private int _readModifier;
        private QueryTemplatePacket _packet;
        private SpaceContext _spaceContext;

        public ExecutionTask(QueryTemplatePacket packet, ISpaceProxy space, Transaction transaction,
                             int maxObjects, int readModifier) {
            super();
            _packet = packet;
            _space = space;
            _transaction = transaction;
            _maxObjects = maxObjects;
            _readModifier = readModifier;

            // if the space is secured save the security context from the current thread
            // for later usage by other threads the task will be running from.
            if (space.isSecured())
                _spaceContext = _space.getDirectProxy().getSecurityManager().getThreadSpaceContext();
        }

        @Override
        protected IQueryResultSet<IEntryPacket> execute() throws Exception {
            // if the space is secured, attach context to current thread
            SpaceContext prevContext = null;
            if (_spaceContext != null)
                prevContext = _space.getDirectProxy().getSecurityManager().setThreadSpaceContext(_spaceContext);

            IQueryResultSet<IEntryPacket> result;
            try {
                result = _packet.readMultiple(_space, _transaction, _maxObjects, _readModifier);

            } finally {
                if (_spaceContext != null)
                    _space.getDirectProxy().getSecurityManager().setThreadSpaceContext(prevContext);
            }
            return result;
        }
    }

    public IQueryResultSet<IEntryPacket> execute(ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        return traverseExpressionTree(query.getExpTree(), space, txn, readModifier, max);
    }

    public void execute(ExpNode expNode, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        IQueryResultSet<IEntryPacket> results = executeTemplate(expNode.getTemplate(), space, txn, readModifier, max);
        setResults(expNode, results);
    }
}
