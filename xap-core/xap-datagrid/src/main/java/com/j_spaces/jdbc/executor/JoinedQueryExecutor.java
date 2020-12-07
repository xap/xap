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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.jdbc.*;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.parser.AbstractInNode;
import com.j_spaces.jdbc.parser.AndNode;
import com.j_spaces.jdbc.parser.ColumnNode;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.parser.InNode;
import com.j_spaces.jdbc.parser.InnerQueryNode;
import com.j_spaces.jdbc.parser.LiteralNode;
import com.j_spaces.jdbc.parser.NotInNode;
import com.j_spaces.jdbc.parser.OrNode;
import com.j_spaces.jdbc.parser.ValueNode;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.JoinedQueryResult;
import com.j_spaces.jdbc.query.QueryTableData;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Executes a join query. Create a cartesian product of all the tables , keep only the products that
 * satisfy the where condition
 *
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class JoinedQueryExecutor extends AbstractQueryExecutor {
    private static final long serialVersionUID = 1L;

    // the entry that is currently undergoing the matching
    private JoinedEntry _currentEntry;
    private HashMap<ExpNode, Boolean> _currentEntryResults = new HashMap<>();
    private HashMap<ExpNode, Set<LiteralNode>> _inNodeValues = new HashMap<>();
    // optimization of the tree traversal - built once an any additional traversal
    // doesn't require to use the stack
    private ExpNode[] _traversalOrder;

    // Required for Externalizable
    public JoinedQueryExecutor() {
    }

    public JoinedQueryExecutor(AbstractDMLQuery query) {
        super(query);
    }

    public IQueryResultSet<IEntryPacket> execute(ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {

        JoinedQueryResult result = new JoinedQueryResult();
        JoinedIterator iter = new JoinedIterator(query.getTablesData(), space, txn);

        QueryTableData firstTableInMatch = iter.getFirstTable();
        boolean skipMatching = SelectQuery.pushDownPredicatesToSpace && firstTableInMatch.getJoinCondition() == null;
        boolean isOuterJoin = firstTableInMatch.getJoinType() == Join.JoinType.LEFT;
        while (skipMatching && firstTableInMatch.getJoinTable() != null) {
            firstTableInMatch = firstTableInMatch.getJoinTable();
            skipMatching = firstTableInMatch.getJoinCondition() != null;
            isOuterJoin = isOuterJoin || firstTableInMatch.getJoinType() == Join.JoinType.LEFT;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug(">> isOuterJoin = " + isOuterJoin+", skipMatching = "+ skipMatching+", pushDownPredicatesToSpace = " + SelectQuery.pushDownPredicatesToSpace);
        }


        while (iter.next()) {
            _currentEntry = iter.get();

            // check if the joined entry satisfies the query condition.
            // run the whole query tree on the entry


            boolean matches = (skipMatching && isOuterJoin) || matchesExpressionTree(query.getExpTree(), space, txn, readModifier, max);
            // if the entry matched the whole expression tree - add it to the result set
            // otherwise it is omitted 
            if (matches) {
                result.add(_currentEntry);
            }

            // check if there are enough results 
            if (result.size() >= max)
                break;
        }

        iter.close();
        return result;
    }

    @Override
    public void execute(ExpNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        // handle join expression node
        if (exp.isJoined()) {
            executeJoin(exp);
            return;
        }

        boolean isInRange = exp.getTemplate().matches(_currentEntry);

        setResults(exp, isInRange);
    }

    public boolean getResults(ExpNode node) {
        return _currentEntryResults.get(node);
    }

    public void setResults(ExpNode node, boolean result) {
        _currentEntryResults.put(node, result);
    }

    /**
     * Execute JOIN expression that is a join between two tables
     */
    private void executeJoin(ExpNode exp) {
        ColumnNode left = (ColumnNode) exp.getLeftChild();
        ColumnNode right = (ColumnNode) exp.getRightChild();

        int leftEntryIndex = left.getColumnData().getColumnTableData().getTableIndex();
        int rightEntryIndex = right.getColumnData().getColumnTableData().getTableIndex();

        Object leftJoinValue = left.getFieldValue(_currentEntry.getEntry(leftEntryIndex));
        Object rightJoinValue = right.getFieldValue(_currentEntry.getEntry(rightEntryIndex));

        boolean isInRange = exp.isValidCompare(leftJoinValue, rightJoinValue);

        setResults(exp, isInRange);
    }

    @Override
    public void execute(AndNode exp, ISpaceProxy space, Transaction txn,
                        int readModifier, int max) throws SQLException {
        // if template is simple - just execute it
        if (exp.getTemplate() != null) {
            setResults(exp, exp.getTemplate().matches(_currentEntry));

            return;
        }
        // Handle null children - happens in case of using 'and' with rownum
        if (exp.getLeftChild() == null) {
            setResults(exp, getResults(exp.getRightChild()));
            return;
        }

        if (exp.getRightChild() == null) {
            setResults(exp, getResults(exp.getLeftChild()));
            return;
        }

        boolean leftResult = getResults(exp.getLeftChild());
        // if left AND expression didn't return any result - return,
        // no need to execute the right expression
        if (!leftResult) {
            setResults(exp, false);
            return;
        }

        boolean rightResult = getResults(exp.getRightChild());
        if (!rightResult) {
            setResults(exp, false);
            return;
        }

        setResults(exp, true);
    }

    private boolean executeIn(AbstractInNode exp, ISpaceProxy space, Transaction txn, int readModifier) throws SQLException {
        QueryTemplatePacket template = exp.getTemplate();
        if (template == null) {
            // if template wasn't set during the building phase - inner query
            // execute the query , get the values and build the template
            Set<LiteralNode> valueSet = _inNodeValues.get(exp);
            if (valueSet == null) {
                // Execute inner query
                execute((InnerQueryNode) exp.getRightChild(), space, txn, readModifier, Integer.MAX_VALUE);
                // Validate inner query result
                exp.validateInnerQueryResult();
                valueSet = exp.getValuesList();
                _inNodeValues.put(exp, valueSet);
            }

            ColumnNode left = (ColumnNode) exp.getLeftChild();
            int entryTableIndex = left.getColumnData().getColumnTableData().getTableIndex();
            Object joinValue = left.getFieldValue(_currentEntry.getEntry(entryTableIndex));
            boolean isInRange = false;

            for (LiteralNode value : valueSet) {
                if (value.getValue().equals(joinValue)) {
                    isInRange = true;
                    break;
                }
            }
            return isInRange;
        } else {
            boolean isInRange = exp.getTemplate().matches(_currentEntry);
            setResults(exp, isInRange);
            return isInRange;
        }
    }

    @Override
    public void execute(InNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        setResults(exp, executeIn(exp, space, txn, readModifier));
    }

    @Override
    public void execute(NotInNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        setResults(exp, executeIn(exp, space, txn, readModifier));
    }

    @Override
    public void execute(OrNode exp, ISpaceProxy space, Transaction txn, int readModifier, int max)
            throws SQLException {
        // if template is simple - just execute it
        if (exp.getTemplate() != null) {
            setResults(exp, exp.getTemplate().matches(_currentEntry));
            return;
        }

        // Handle null children - happens in case of using 'or' with rownum
        if (exp.getLeftChild() == null) {
            setResults(exp, getResults(exp.getRightChild()));
            return;
        }

        if (exp.getRightChild() == null) {
            setResults(exp, getResults(exp.getLeftChild()));
            return;
        }

        boolean leftResult = getResults(exp.getLeftChild());
        boolean rightResult = getResults(exp.getRightChild());

        setResults(exp, leftResult || rightResult);
    }

    public Comparator<IEntryPacket> getGroupByComparator(List<SelectColumn> groupColumns) {
        return new GroupByComparator(groupColumns);
    }

    /**
     * This private class implements the Comparator and is used to sort the arrays of values when
     * GROUP BY is used in the query.
     */
    private static class GroupByComparator implements Comparator<IEntryPacket> {
        private final List<SelectColumn> groupColumns;

        GroupByComparator(List<SelectColumn> groupCols) {
            groupColumns = groupCols;
        }

        /**
         * Compare two arrays of values - only the group by columns are compared
         */
        @Override
        public int compare(IEntryPacket e1, IEntryPacket e2) {

            if (e1 == null) {
                if (e2 == null)
                    return 0;
                return -1;
            }

            if (e2 == null)
                return 1;

            int rc = 0;

            JoinedEntry j1 = (JoinedEntry) e1;
            JoinedEntry j2 = (JoinedEntry) e2;

            for (SelectColumn groupCol : groupColumns) {
                e1 = j1.getEntry(groupCol.getColumnTableData().getTableIndex());
                e2 = j2.getEntry(groupCol.getColumnTableData().getTableIndex());

                Object obj1 = groupCol.getFieldValue(e1);
                Object obj2 = groupCol.getFieldValue(e2);

                if (obj1 == null && obj2 == null)
                    rc = 0;
                else if (obj1 == null && obj2 != null)
                    rc = -1;
                else if (obj1 != null && obj2 == null)
                    rc = 1;
                else
                    rc = ((Comparable) obj1).compareTo(obj2);

                if (rc != 0)
                    return rc;
            }

            return rc;
        }
    }

    /**
     * Traverse the binary expression tree non-recursively using a custom stack The tree has to be
     * traversed in postorder - the parent is traversed after its children.
     */
    protected boolean matchesExpressionTree(ExpNode root, ISpaceProxy space, Transaction txn, int readModifier, int max) throws SQLException {
        if (root == null)
            return true;

        if (_traversalOrder != null) {
            for (int i = 0; i < _traversalOrder.length; i++) {
                ExpNode node = _traversalOrder[i];

                // special handling for the root 
                // if the root is executed it can optimize the query by limiting the result set
                // this can't be done on intermediate results
                if (i < _traversalOrder.length - 1)
                    node.accept(this, space, txn, readModifier, Integer.MAX_VALUE);
                else
                    node.accept(this, space, txn, readModifier, max);
            }
            return getResults(root);
        }

        Stack<ExpNode> stack = new Stack<>();
        Stack<ExpNode> stack2 = new Stack<>();

        stack.push(root);
        while (!stack.isEmpty()) {
            ExpNode curr = stack.pop();

            if (!(curr instanceof ValueNode))
                stack2.push(curr);

            // don't traverse nodes that are already aggregated at the parent level
            if (curr.getTemplate() == null) {
                if (curr.getLeftChild() != null)
                    stack.push(curr.getLeftChild());
                if (curr.getRightChild() != null)
                    stack.push(curr.getRightChild());
            }
        }

        _traversalOrder = new ExpNode[stack2.size()];
        int index = 0;
        while (!stack2.isEmpty()) {
            ExpNode node = stack2.pop();
            _traversalOrder[index++] = node;
            // special handling for the root 
            // if the root is executed it can optimize the query by limiting the result set
            // this can't be done on intermediate results
            if (stack2.isEmpty())
                node.accept(this, space, txn, readModifier, max);
            else
                node.accept(this, space, txn, readModifier, Integer.MAX_VALUE);
        }

        return getResults(root);
    }

    /**
     * An iterator over joined tables
     *
     * @author anna
     * @since 7.1
     */
    private class JoinedIterator {
        private QueryTableData _tableData;
        private final List<QueryTableData> _tablesData;

        public JoinedIterator(List<QueryTableData> tablesData, ISpaceProxy space, Transaction txn)
                throws SQLException {
            _tablesData = tablesData;

            try {
                for (QueryTableData tableData : query.getTablesData()) {
                    // first all entries for each table in the query
                    tableData.init(space, txn, query);
                }
            } catch (Exception e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(e.getMessage(), e);
                }
                throw new SQLException("Failed to read objects: " + e.getMessage(), "GSP", -111, e);
            }

            for (QueryTableData tableData : _tablesData) {
                //check for sequence beginning
                if (!tableData.isJoined()) {
                    _tableData = tableData;
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Choose tabledata: " + _tableData.getTableName());
                    }
                    break;
                }
            }
        }

        boolean next() {
            return _tableData.next();
        }

        public JoinedEntry get() {
            IEntryPacket[] entries = new IEntryPacket[_tablesData.size()];
            for (int i = 0; i < entries.length; i++) {
                entries[i] = _tablesData.get(i).getCurrentEntry();
            }
            return new JoinedEntry(entries);
        }

        public void close() {
            for (QueryTableData t : _tablesData) {
                t.clear();
            }
        }

        public QueryTableData getFirstTable() {
            return _tableData;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _currentEntry);
        IOUtils.writeObject(out, _currentEntryResults);
        IOUtils.writeObject(out, _inNodeValues);
        IOUtils.writeObject(out, _traversalOrder);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _currentEntry = IOUtils.readObject(in);
        _currentEntryResults = IOUtils.readObject(in);
        _inNodeValues = IOUtils.readObject(in);
        _traversalOrder = IOUtils.readObject(in);
    }
}
