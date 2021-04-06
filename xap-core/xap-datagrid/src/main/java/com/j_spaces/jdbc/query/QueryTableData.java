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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.Constants;
import com.j_spaces.jdbc.*;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.executor.EntriesCursor;
import com.j_spaces.jdbc.executor.ScanCursor;
import com.j_spaces.jdbc.parser.ColumnNode;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.kernel.JSpaceUtilities;
import net.jini.core.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class QueryTableData implements Externalizable, Cloneable {
    private static final long serialVersionUID = 1L;

    private String _tableName;
    private String _tableAlias;
    // the sequential index of the table in the "from" clause
    private int _tableIndex;

    // the space type descriptor for this table/class
    private ITypeDesc _typeDesc;

    private ExpNode _joinCondition;
    private ExpNode _tableCondition;

    private QueryTableData _joinTable;
    private Join.JoinType _joinType;

    //use thread local because this object is cached and used by several threads
    private transient ThreadLocal<EntriesCursor> _entriesCursor = new ThreadLocal<>();

    private boolean _isJoined;
    private boolean _hasAsterixSelectColumns;
    private Query subQuery;

    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    // Required by Externalizable
    public QueryTableData() {
    }

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
        builder.append("  QueryTableData , hashCode=" + hashCode() + "  [ getTableAlias()=");
        builder.append(getTableAlias());

        builder.append(", getTableName()=");
        builder.append(getTableName());

        builder.append(", \n        getTableCondition()=");
        builder.append(getTableCondition());
        builder.append(", HashCode TableCondition=" + ( ( getTableCondition() != null ) ? getTableCondition().hashCode() : "NULL" ));

/*        builder.append(", \n        getJoinTable()=");

        if (getJoinTable() != null)
            builder.append(getJoinTable().getTableName());*/
/*        builder.append(", \n        getJoinExpression()=");
        builder.append(getJoinCondition());*/
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
        //_logger.info( "#### getCurrentEntry, EntriesCursor classname=" + getEntriesCursor().getClass().getName() );
        return getEntriesCursor().getCurrentEntry();
    }


    /**
     * @return true _joinTable doesn't finished , false if finished
     */
    public boolean next() {

        _logger.info( "QueryTableData, next 1, _joinTable=" + _joinTable );

        if (_joinTable == null)
            return getEntriesCursor().next();

        while (hasNext()) {
            _logger.info( "QueryTableData, hasNext 1" );
            if (_joinTable.next()) {
                _logger.info( "QueryTableData, next 2, return true");
                return true;
            }
            _logger.info( "QueryTableData, hasNext 2" );
            //if joined table can't advance any further - increment this table cursor
            // and reset the joined
            if (getEntriesCursor().next()) {
                _logger.info( "QueryTableData, _joinTable.reset() !" );
                _joinTable.reset();
            } else {
                _logger.info( "QueryTableData, next 3, return false");
                return false;
            }

        }

        _logger.info( "QueryTableData, next 4, return false");
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
        //TODO
        EntriesCursor entriesCursor = _entriesCursor.get();
        _logger.info( "---getEntriesCursor, hashCode=" + hashCode() + ", threadId=" + Thread.currentThread().getId() +
                ", _entriesCursor hashcode=" + _entriesCursor.hashCode() +
                ( ", entriesCursor=" + ( entriesCursor != null ? entriesCursor.hashCode() : "NULL" ) ) );
        return entriesCursor;
    }

    public void setEntriesCursor(EntriesCursor entriesCursor) {
        _logger.info( "---setEntriesCursor, hashCode=" + hashCode() + ", threadId=" + Thread.currentThread().getId() +
                ", _entriesCursor hashcode=" + _entriesCursor.hashCode() +
                ( ", entriesCursorClassName=" + ( entriesCursor != null? entriesCursor.getClass().getName() : "NULL" ) ) +
                ( ", entriesCursor=" + ( entriesCursor != null? entriesCursor.hashCode() : "NULL" ) ) +
                ( ", CurrentEntry=" + ( entriesCursor != null? entriesCursor.getCurrentEntry() : "NULL" ) ) );

        _entriesCursor.set(entriesCursor);
    }

    public ExpNode getTableCondition() {
        return _tableCondition;
    }

    public void setTableCondition(ExpNode tableCondition) {
        _logger.info( "!!! setTableCondition, this.hashCode=" + hashCode() +" , setTableCondition=" + tableCondition +
                        ", hashCode=" + ( tableCondition != null ? tableCondition.hashCode() : "NULL" ) + ", stackTrace:" +
                                    JSpaceUtilities.getCallStackTraces(12) );

        _tableCondition = tableCondition;//tableCondition != null ? ( ExpNode )tableCondition.clone() : null
    }

    public boolean isJoined() {
        return _isJoined;
    }

    public void setJoined(boolean isJoined) {
        _isJoined = isJoined;
    }

    public void join(ExpNode exp) {

        QueryTableData rightTable = ((ColumnNode) exp.getRightChild()).getColumnData()
                .getColumnTableData();

        // if this table is not joined yet and matches the join condition
        // try to join it with the right table
        if (getJoinTable() == null) {
            if (!rightTable.isJoined() && !rightTable.references(this)) {
                setJoinTable(rightTable);

                rightTable.setJoinCondition(exp);

                rightTable.setJoined(true);

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
        _logger.info( "~~ In getTemplate, getTableCondition=" + getTableCondition() );
        if (getTableCondition() != null) {
            _logger.info( "~~ In getTemplate, expNode hashCode=" + getTableCondition().hashCode() );
            return getTableCondition().getTemplate();
        }
        else
            return new QueryTemplatePacket(this, queryResultType);
    }

    /**
     * Fetch the entries from space that match this table condition
     */
    public void init(ISpaceProxy space, Transaction txn, AbstractDMLQuery query)
            throws Exception {

        IQueryResultSet<IEntryPacket> tableEntries;
        if (subQuery != null) {
            tableEntries = executeSubQuery(space, txn);
        } else {
            QueryTemplatePacket template = getTemplate(query.getQueryResultType());
            _logger.info("> init, TEMPLATE=" + template /*+ ", query=" + query*/);
            tableEntries = template.readMultiple(space, txn, Integer.MAX_VALUE, query.getReadModifier());
        }

        if (_joinCondition != null) {
            EntriesCursor entriesCursor = _joinCondition.createIndex(this, tableEntries);
            _logger.info("> init, TEMPLATE, threadId=" + Thread.currentThread().getId() + ", entriesCursor==" + entriesCursor /*+ ", query=" + query*/);
            setEntriesCursor( entriesCursor );
        }
        else {
            ScanCursor scanCursor = new ScanCursor(tableEntries);
            _logger.info("> init, threadId=" + Thread.currentThread().getId() + ", ELSE, scanCursor=" + scanCursor );
            setEntriesCursor(scanCursor);
        }

    }

    public IQueryResultSet<IEntryPacket> executeSubQuery(ISpaceProxy space, Transaction txn) throws Exception{
        if (subQuery instanceof AbstractDMLQuery) {
            // sub query results should be returned as entry packets and not converted.
            ((AbstractDMLQuery) subQuery).setConvertResultToArray(false);
        }
        ResponsePacket rp = subQuery.executeOnSpace(space, txn);
        return (IQueryResultSet<IEntryPacket>) rp.getResultSet();
    }

    /**
     * Traverse the expression root(preorder) and create a join index for given table if possible
     */
    public void createJoinIndex(ExpNode root) {
        if (root == null)
            return;

        Stack<ExpNode> stack = new Stack<>();
        _logger.info( ">> createJoinIndex=" + hashCode() + ", toString=" + toString() );
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
        _entriesCursor = new ThreadLocal<>();
    }

    public Join.JoinType getJoinType() {
        return _joinType;
    }

    public void setJoinType(Join.JoinType _joinType) {
        this._joinType = _joinType;
    }

    public boolean isBroadcastTable(){
        return _typeDesc != null && _typeDesc.isBroadcast();
    }

    private static final short FLAG_TABLE_NAME = 1 << 0;
    private static final short FLAG_TABLE_ALIAS = 1 << 1;
    private static final short FLAG_TYPE_DESC = 1 << 2;
    private static final short FLAG_JOIN_CONDITION = 1 << 3;
    private static final short FLAG_TABLE_CONDITION = 1 << 4;
    private static final short FLAG_JOIN_TABLE = 1 << 5;
    private static final short FLAG_JOINED = 1 << 6;
    private static final short FLAG_ASTERISK_COLS = 1 << 7;
    private static final short FLAG_SUBQUERY = 1 << 8;

    private short buildFlags() {
        short flags = 0;

        if (_tableName != null)
            flags |= FLAG_TABLE_NAME;
        if (_tableAlias != null)
            flags |= FLAG_TABLE_ALIAS;
        if (_typeDesc != null)
            flags |= FLAG_TYPE_DESC;
        if (_joinCondition != null)
            flags |= FLAG_JOIN_CONDITION;
        if (_tableCondition != null)
            flags |= FLAG_TABLE_CONDITION;
        if (_joinTable != null)
            flags |= FLAG_JOIN_TABLE;
        if (_isJoined)
            flags |= FLAG_JOINED;
        if (_hasAsterixSelectColumns)
            flags |= FLAG_ASTERISK_COLS;
        if (subQuery != null)
            flags |= FLAG_SUBQUERY;

        return flags;
    }

    @Override
    public QueryTableData clone() throws CloneNotSupportedException {
        QueryTableData clonedQueryTableData = (QueryTableData)super.clone();

        EntriesCursor entriesCursor = this._entriesCursor.get();

        _logger.info("~~~ QueryTableData CLONE, hashCode=" + hashCode() + ", threadId=" + Thread.currentThread().getId() +
                ", entriesCursor.hashCode=" + this._entriesCursor.hashCode() +
                ", clonedQueryTableData.hashCode=" + clonedQueryTableData._entriesCursor.hashCode() +
                ", clonedQueryTableData.getTableCondition:" + clonedQueryTableData.getTableCondition() +
                ", entriesCursor=" + entriesCursor);

        //clonedQueryTableData._entriesCursor = new ThreadLocal<>();

        //clonedQueryTableData._entriesCursor.set();
/*
        if( entriesCursor != null ) {
            queryTableData._entriesCursor.set( entriesCursor );
            IEntryPacket currentEntry = entriesCursor.getCurrentEntry();
            IEntryPacket clonedEntryPacket =  currentEntry != null ? currentEntry.clone() : null;
            _logger.info("~~~ QueryTableData CLONE, ! hashCode=" + hashCode() + ", threadId=" + Thread.currentThread().getId() +
                    ", entriesCursor=" + entriesCursor +
                    ", entriesCursor.getCurrentEntry()=" + currentEntry +
                    ", entriesCursor.getCurrentEntry hashCode=" + ( currentEntry != null ? currentEntry.hashCode() : "NULL" ) +
                    ", clonedEntryPacket=" + clonedEntryPacket +
                    ", clonedEntryPacket hashCode=" + ( clonedEntryPacket != null ? clonedEntryPacket.hashCode() : "NULL" ) );
        }*/

/*
        //QueryTableData queryTableData = new QueryTableData(this.getTableName(), this.getTableAlias(), this.getTableIndex());
        queryTableData.setAsterixSelectColumns(_hasAsterixSelectColumns);
        //queryTableData.setEntriesCursor();
        queryTableData.setJoined(isJoined());
        */
        /*
        if( getJoinTable() != null ) {
            clonedQueryTableData.setJoinTable(getJoinTable().clone());
        }
        clonedQueryTableData.setJoinType(getJoinType());
        if( getJoinCondition() != null ) {
            clonedQueryTableData.setJoinCondition((ExpNode)getJoinCondition().clone());
        }
        clonedQueryTableData.setSubQuery(getSubQuery());
        if( getTableCondition() != null ) {
            clonedQueryTableData.setTableCondition( (ExpNode)getTableCondition().clone() );
        }
        clonedQueryTableData.setTypeDesc(getTypeDesc());
*/
        /*
        if( this.getEntriesCursor() != null ) {
            queryTableData.setEntriesCursor(this.getEntriesCursor());
        }*/
        return clonedQueryTableData;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(buildFlags());
        if (_tableName != null)
            IOUtils.writeRepetitiveString(out, _tableName);
        if (_tableAlias != null)
            IOUtils.writeRepetitiveString(out, _tableAlias);
        out.writeInt(_tableIndex);
        if (_typeDesc != null)
            IOUtils.writeObject(out, _typeDesc);
        if (_joinCondition != null)
            IOUtils.writeObject(out, _joinCondition);
        if (_tableCondition != null)
            IOUtils.writeObject(out, _tableCondition);
        if (_joinTable != null)
            IOUtils.writeObject(out, _joinTable);
        out.writeByte(Join.JoinType.toCode(_joinType));
        if (subQuery != null)
            IOUtils.writeObject(out, subQuery);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        short flags = in.readShort();
        if ((flags & FLAG_TABLE_NAME) != 0)
            _tableName = IOUtils.readRepetitiveString(in);
        if ((flags & FLAG_TABLE_ALIAS) != 0)
            _tableAlias = IOUtils.readRepetitiveString(in);
        _tableIndex = in.readInt();
        if ((flags & FLAG_TYPE_DESC) != 0)
            _typeDesc = IOUtils.readObject(in);
        if ((flags & FLAG_JOIN_CONDITION) != 0)
            _joinCondition = IOUtils.readObject(in);
        if ((flags & FLAG_TABLE_CONDITION) != 0)
            _tableCondition = IOUtils.readObject(in);
        if ((flags & FLAG_JOIN_TABLE) != 0)
            _joinTable = IOUtils.readObject(in);
        _joinType = Join.JoinType.fromCode(in.readByte());
        _isJoined = (flags & FLAG_JOINED) != 0;
        _hasAsterixSelectColumns = (flags & FLAG_ASTERISK_COLS) != 0;
        if ((flags & FLAG_SUBQUERY) != 0)
           subQuery = IOUtils.readObject(in);
    }
}
