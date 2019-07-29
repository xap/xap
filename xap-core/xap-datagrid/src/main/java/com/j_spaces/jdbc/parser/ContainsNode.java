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

import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.executor.EntriesCursor;
import com.j_spaces.jdbc.executor.HashedEntriesCursor;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.QueryTableData;
import com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder;

import java.sql.SQLException;

/**
 * An expression node for a contains argument. SQL Syntax:	collection[*] = ? array[*] = ?
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ContainsNode extends ExpNode {

    // Holds the contains fields indexes.
    private short _templateMatchCode;
    private String _path;
    private String relation;

    public ContainsNode() {
        super();
    }

    public ContainsNode(ColumnNode columnNode, ExpNode operatorNode, short templateMatchCode ) {
        super(columnNode, operatorNode);
        this._templateMatchCode = templateMatchCode;
        this._path = columnNode.getName();
    }

    public ContainsNode(ColumnNode columnNode, ExpNode operatorNode, short templateMatchCode, String relation ){
        this(columnNode,operatorNode,templateMatchCode);
        this.relation = relation;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.parser.ExpNode#newInstance()
     */
    @Override
    public ExpNode newInstance() {
        return new ContainsNode();
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.parser.ExpNode#accept(com.j_spaces.jdbc.builder.QueryTemplateBuilder)
     */
    @Override
    public void accept(QueryTemplateBuilder builder) throws SQLException {
        builder.buildContainsTemplate(this);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return toString(DefaultSQLQueryBuilder.mapCodeToSign(TemplateMatchCodes.CONTAINS_TOKEN));
    }

    @Override
    public boolean isValidCompare(Object ob1, Object ob2) {
        return false;
    }

    @Override
    public EntriesCursor createIndex(QueryTableData table,
                                     IQueryResultSet<IEntryPacket> entries) {

        return new HashedEntriesCursor(table, this, entries);
    }

    @Override
    public Object clone() {
        ContainsNode cloned = (ContainsNode) super.clone();
        cloned._templateMatchCode = _templateMatchCode;
        cloned._path = _path;
        cloned.relation = relation;
        return cloned;
    }

    /**
     * @return The node's full path including the "[*]" operator.
     */
    public String getPath() {
        return _path;
    }

    /**
     * @return The template match code representing the comparison operator to be used for matching.
     */
    public short getTemplateMatchCode() {
        return _templateMatchCode;
    }

    public boolean isRelationNode(){
        return _templateMatchCode == TemplateMatchCodes.RELATION;
    }

    public String getRelation() {
        return relation;
    }
}
