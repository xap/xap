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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.query.QueryColumnData;
import com.j_spaces.jdbc.query.QueryTableData;

import java.sql.SQLException;
import java.util.TreeMap;

/**
 * node holding a root inside contains - points at a subtree of containItemNodes
 *
 * @author Yechiel Fefer
 * @since 9.6
 */

@com.gigaspaces.api.InternalApi
public class ContainsItemsRootNode extends LiteralNode {
    //the subtree of containsIemConditions
    private ExpNode _containsSubs;
    private String _root;
    private ColumnNode _rootColumnNode;
    private static String _dummyValue = "";

    public ContainsItemsRootNode(Object containsSubs, String root, ColumnNode rootColumnNode) {
        super(_dummyValue);
        _containsSubs = (ExpNode) containsSubs;
        _root = root;
        _rootColumnNode = rootColumnNode;
    }

    /*
     * returns true if its a contains-item root node 
     */
    @Override
    public boolean isContainsItemsRootNode() {
        return true;
    }

    public ExpNode getContainsSubtree() {
        return _containsSubs;
    }

    public String getRoot() {
        return _root;
    }

    public ColumnNode getRootColumnNode() {
        return _rootColumnNode;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.parser.ExpNode#accept(com.j_spaces.jdbc.builder.QueryTemplateBuilder)
     */
    @Override
    public void accept(QueryTemplateBuilder builder) throws SQLException {
        builder.buildContainsItemsRootTemplate(this);
    }

    @Override
    public void prepareValues(Object[] values) throws SQLException {
        _containsSubs.prepareValues(values);
    }

    @Override
    public String prepareTemplateValues(TreeMap values, String colName)
            throws SQLException {
        return _containsSubs.prepareTemplateValues(values, colName);
    }

    @Override
    public Object clone() {
        Object subsCloned = _containsSubs.clone();
        ContainsItemsRootNode clone = new ContainsItemsRootNode(subsCloned, _root, _rootColumnNode);
        return clone;
    }

    public ITypeDesc createTypeDesc()  {
        QueryColumnData queryColumnData = _rootColumnNode.getColumnData();
        QueryTableData tableData = queryColumnData.getColumnTableData();
        return tableData.getTypeDesc();
    }
}
