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

package com.j_spaces.jdbc.query;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.jdbc.*;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.executor.EntriesCursor;
import com.j_spaces.jdbc.executor.ScanCursor;
import com.j_spaces.jdbc.parser.ColumnNode;
import com.j_spaces.jdbc.parser.ExpNode;

import net.jini.core.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;


/**
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class QueryTableData implements Serializable {
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    private final String _tableName;
    private final String _tableAlias;
    // the sequential index of the table in the "from" clause
    private final int _tableIndex;

    // the space type descriptor for this table/class
    private ITypeDesc _typeDesc;

    private ExpNode _joinCondition;
    private ExpNode _tableCondition;

    private QueryTableData _joinTable;
    private Join.JoinType _joinType;

    //use thread local because this object is cached and used by several threads
    private transient ThreadLocal<EntriesCursor> _entriesCursor = new ThreadLocal<EntriesCursor>();

    private boolean _isJoined;
    private boolean _hasAsterixSelectColumns;
    private Query subQuery;
    private ExpNode _expTree;

    public QueryTableData(String name, String alias, int index) {
        _tableName = name;
        _tableAlias = alias;
        _tableIndex = index;
    }

    public boolean hasAsterixSelectColumns() {
        return _hasAsterixSelectColumns;
    }

    public void setAsterixSelectColumns(boolean hasAsterixSelectColumns) {
        this._hasAsterixSelectColumns = hasAsterixSelectColumns;
    }

    public String getTableName() {
        return _tableName;
    }

    public String getTableAlias() {
        return _tableAlias;
    }

    public int getTableIndex() {
        return _tableIndex;
    }

    public ITypeDesc getTypeDesc() {
        return _typeDesc;
    }

    public void setTypeDesc(ITypeDesc typeDesc) {
        _typeDesc = typeDesc;
    }

    public ExpNode getJoinCondition() {
        return _joinCondition;
    }

    public void setJoinCondition(ExpNode joinIndex) {
        _joinCondition = joinIndex;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("  QueryTableData\n    [\n        getTableAlias()=");
        builder.append(getTableAlias());

        builder.append(", \n        getTableName()=");
        builder.append(getTableName());

        builder.append(", \n        getTableCondition()=");
        builder.append(getTableCondition());
        builder.append(", \n        getJoinTable()=");

        if (getJoinTable() != null)
            builder.append(getJoinTable().getTableName());
        builder.append(", \n        getJoinExpression()=");
        builder.append(getJoinCondition());
        builder.append(", \n        getTableIndex()=");
        builder.append(getTableIndex());
        builder.append("\n    ]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((_tableAlias == null) ? 0 : _tableAlias.hashCode());
        result = prime * result + _tableIndex;
        result = prime * result
                + ((_tableName == null) ? 0 : _tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueryTableData other = (QueryTableData) obj;
        if (_tableAlias == null) {
            if (other._tableAlias != null)
                return false;
        } else if (!_tableAlias.equals(other._tableAlias))
            return false;
        if (_tableIndex != other._tableIndex)
            return false;
        if (_tableName == null) {
            if (other._tableName != null)
                return false;
        } else if (!_tableName.equals(other._tableName))
            return false;
        return true;
    }

    /**
     * @return IEntryPacket
     */
    public IEntryPacket getCurrentEntry() {
        return getEntriesCursor().getCurrentEntry();
    }


    /**
     * @return true _joinTable doesn't finished , false if finished
     */
    public boolean next() {
        if (_joinTable == null)
            return getEntriesCursor().next();

        while (hasNext()) {
            if (_joinTable.next())
                return true;
            //if joined table can't advance any further - increment this table cursor
            // and reset the joined
            if (getEntriesCursor().next()) {
                _joinTable.reset();
            } else {
                return false;
            }

        }
        return false;
    }

    private boolean hasNext() {
        if (getEntriesCursor().isBeforeFirst()) {
            return getEntriesCursor().next();
        }
        return true;

    }

    public void reset() {

        getEntriesCursor().reset();

        if (_joinTable != null)
            _joinTable.reset();
    }


    public QueryTableData getJoinTable() {
        return _joinTable;
    }

    public void setJoinTable(QueryTableData joinTable) {
        _joinTable = joinTable;
    }

    public EntriesCursor getEntriesCursor() {
        return _entriesCursor.get();
    }

    public void setEntriesCursor(EntriesCursor entriesCursor) {
        _entriesCursor.set(entriesCursor);
    }

    public ExpNode getTableCondition() {
        return _tableCondition;
    }

    public void setTableCondition(ExpNode tableCondition) {
        _tableCondition = tableCondition;
    }

    public boolean isJoined() {
        return _isJoined;
    }

    public void setJoined(boolean isJoined) {
        _isJoined = isJoined;
    }

    public void joinRight(ExpNode exp) {

        QueryTableData rightTable = ((ColumnNode) exp.getRightChild()).getColumnData()
                .getColumnTableData();

        // if this table is not joined yet and matches the join condition
        // try to join it with the right table
        if (getJoinTable() == null) {
            if ((!rightTable.isJoined()) && !rightTable.references(this)) {
                setJoinTable(rightTable);

                rightTable.setJoinCondition(exp);

                rightTable.setJoined(true);
            }
        }

    }

    public void joinLeft(ExpNode exp) {

        QueryTableData leftTable = ((ColumnNode) exp.getLeftChild()).getColumnData()
                .getColumnTableData();

        // if this table is not joined yet and matches the join condition
        // try to join it with the left table
        if (getJoinTable() == null) {
            if ((!leftTable.isJoined()) && !leftTable.references(this)) {
                setJoinTable(leftTable);

                leftTable.setJoinCondition(exp);

                leftTable.setJoined(true);

            } else if (this.getJoinType() == Join.JoinType.LEFT) {// if already joined and is LEFT JOIN then just set the condition
                if (exp.isJoined() && ((ColumnNode) leftTable.getJoinCondition().getRightChild()).getColumnPath().equals(((ColumnNode) exp.getLeftChild()).getColumnPath())) {
                    setJoinCondition(exp);
                }
            }
        }

    }

    /**
     * returns true is this table references given dest table
     */
    private boolean references(QueryTableData dest) {

        QueryTableData source = this;

        while (source != null) {
            if (source.equals(dest))
                return true;

            source = source.getJoinTable();
        }

        return false;
    }

    public QueryTemplatePacket getTemplate(QueryResultTypeInternal queryResultType) {
        if (getTableCondition() != null)
            return getTableCondition().getTemplate();
        else
            return new QueryTemplatePacket(this, queryResultType);
    }

    /**
     * Fetch the entries from space that match this table condition
     */
    public void init(ISpaceProxy space, Transaction txn, AbstractDMLQuery query)
            throws Exception {

        List<String> output = new LinkedList<>();
        IQueryResultSet<IEntryPacket> tableEntries;
        if (subQuery != null) {
            tableEntries = executeSubQuery(space, txn, false);
        } else {
            QueryTemplatePacket template = getTemplate(query.getQueryResultType());
            output.add("Table: "+this.getTableName()+", Template: " + template.getRanges());
            tableEntries = template.readMultiple(space, txn, Integer.MAX_VALUE, query.getReadModifier());
        }
        output.add("\tJoin condition: " + _joinCondition+", joinTable: " + (_joinTable == null ? "NONE!" : _joinTable.getTableName()));
        if (_joinCondition != null) {
            output.add("\t Creating HashCursor");
            setEntriesCursor(_joinCondition.createIndex(this, tableEntries));
        }else {
            output.add("\t Creating ScanCursor");
            setEntriesCursor(new ScanCursor(tableEntries));
        }
        output.add("------");

        _logger.info(String.join("\n", output));
        System.out.println(String.join("\n", output));

    }

    public IQueryResultSet<IEntryPacket> executeSubQuery(ISpaceProxy space, Transaction txn, boolean flatten) throws Exception{
        if (subQuery instanceof AbstractDMLQuery) {
            // sub query results should be returned as entry packets and not converted.
            ((AbstractDMLQuery) subQuery).setConvertResultToArray(false);
        }
        if (subQuery instanceof SelectQuery)
            ((SelectQuery) subQuery).setFlattenResults(flatten);
        ResponsePacket rp = subQuery.executeOnSpace(space, txn);
        return (IQueryResultSet<IEntryPacket>) rp.getResultSet();
    }

    /**
     * Traverse the expression root(preorder) and create a join index for given table if possible
     */
    public void createJoinIndex(ExpNode root) {
        if (root == null)
            return;

        Stack<ExpNode> stack = new Stack<ExpNode>();

        stack.push(root);
        while (!stack.isEmpty()) {

            ExpNode curr = stack.pop();

            boolean processChildren = curr.createJoinIndex(this);

            if (!processChildren)
                continue;

            if (curr.getLeftChild() != null)
                stack.push(curr.getLeftChild());
            if (curr.getRightChild() != null)
                stack.push(curr.getRightChild());

        }

    }

    public void clear() {
        setEntriesCursor(null);
    }

    public Query getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
    }

    public boolean supportsDynamicProperties() {
        return _typeDesc != null && _typeDesc.supportsDynamicProperties();
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        _entriesCursor = new ThreadLocal<EntriesCursor>();
    }

    public Join.JoinType getJoinType() {
        return _joinType;
    }

    public void setJoinType(Join.JoinType _joinType) {
        this._joinType = _joinType;
    }

    public ExpNode getExpTree() {
        return _expTree;
    }

    public void setExpTree(ExpNode _expTree) {
        this._expTree = _expTree;
    }

}
