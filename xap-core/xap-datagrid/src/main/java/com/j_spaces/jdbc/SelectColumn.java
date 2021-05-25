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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.jdbc.query.QueryColumnData;
import com.j_spaces.jdbc.query.QueryTableData;

import java.io.*;
import java.sql.SQLException;

/**
 * This class represents a column in a select query.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class SelectColumn implements Externalizable {
    private static final long serialVersionUID = 1L;

    private String name = null;
    private String alias = null;

    private boolean isSum = false;       //if this query is a sum function
    private boolean isCount = false;     //if this query is a count function
    private int projectedIndex = -1; //the index of the column in projection table

    private Object initValue;            //if this query is a have init Value
    private boolean isInitValue = false; //if this query is a have init Value
    private String funcName;
    private boolean isVisible = true;

    private QueryColumnData _columnData;
    private boolean _isDynamic;

    public SelectColumn() {
        _isDynamic = false;
    }

    public SelectColumn(String columnPath) {
        super();
        name = columnPath;
        _isDynamic = false;
    }

    public SelectColumn(String columnName, String columnAlias) {
        this(columnName);
        alias = columnAlias;
    }

    public SelectColumn(QueryTableData tableData, String columnPath) throws SQLException {
        this(columnPath);
        _columnData = new QueryColumnData(tableData, columnPath);
    }

    public SelectColumn(QueryTableData tableData, String columnPath, boolean isDynamic) throws SQLException {
        name = columnPath;
        _columnData = new QueryColumnData(tableData, columnPath);
        _isDynamic = isDynamic;
    }

    public void createColumnData(AbstractDMLQuery query) throws SQLException {
        _columnData = QueryColumnData.newColumnData(name, query);
        // Assign the column's name (including path for nested properties)
        name = _columnData.getColumnPath();
    }

    public boolean hasAlias() {
        return alias != null;
    }

    /**
     * Gets the column's alias. If no alias was set the column name is returned. If the column is a
     * function column.. the function name is returned. (COUNT(*) etc..)
     *
     * @return The column alias
     */
    public String getAlias() {
        if (hasAlias())
            return alias;
        return toString();
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public boolean isAllColumns() {
        return _columnData.isAsterixColumn();
    }


    /**
     * @return - is this a count function select. This is done this way because group by are not yet
     * supported so if there is a count, then there is nothing else to select.
     */
    public boolean isCount() {
        return isCount;
    }

    /**
     * @return - is this a sum function select. This is done this way because group by are not yet
     * supported so if there is a sum, then there is nothing else to select.
     */
    public boolean isSum() {
        return isSum;
    }

    /**
     * if this is a count function
     */
    public void setCount(boolean isCount) {
        this.isCount = isCount;
    }


    /**
     * @return - is this a UID function select.
     */
    public boolean isUid() {
        return _columnData.isUidColumn();
    }


    /**
     * if have default value function
     */
    public void setValue(Object initValues) {
        this.initValue = initValues;
        isInitValue = true;
    }

    public Object getValue() {
        return this.initValue;
    }

    /**
     * if have default value function
     */
    public boolean isInitValues() {
        return isInitValue;
    }

    /**
     * if this is a sum function
     */
    public void setSum(boolean isSum) {
        this.isSum = isSum;
    }

    public boolean isFunction() {
        return (isSum || isCount);
    }

    public boolean isAggregatedFunction() {
        return funcName != null;
    }

    public boolean isDynamic() {
        return _isDynamic;
    }

    @Override
    public boolean equals(Object ob) {
        if (ob instanceof SelectColumn) {
            SelectColumn another = (SelectColumn) ob;
            return (this.getName().equals(another.getName()));
        }
        return false;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        if (funcName != null) {
            buffer.append(funcName + "(");
            buffer.append(getName());
            buffer.append(")");
        } else {
            buffer.append(getName());
        }
        return buffer.toString();
    }

    public void setFunctionName(String funcName) {
        this.funcName = funcName;
    }

    public String getFunctionName() {
        return funcName;
    }

    public void setVisible(boolean flag) {
        isVisible = flag;
    }

    public boolean isVisible() {
        return isVisible;
    }

    /**
     * @return Returns the projectedIndex.
     */
    public int getProjectedIndex() {
        return projectedIndex;
    }

    /**
     * @param projectedIndex The projectedIndex to set.
     */
    public void setProjectedIndex(int projectedIndex) {
        this.projectedIndex = projectedIndex;
    }


    public QueryTableData getColumnTableData() {
        return _columnData.getColumnTableData();
    }


    public int getColumnIndexInTable() {
        return _columnData.getColumnIndexInTable();
    }

    public QueryColumnData getColumnData() {
        return _columnData;
    }

    public Object getFieldValue(IEntryPacket entry) {

        if (isUid())
            return entry.getUID();

        else if (isInitValues())
            return getValue();

        return SQLUtil.getFieldValue(entry, _columnData);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, name);
        IOUtils.writeString(out, alias);
        out.writeBoolean(isSum);
        out.writeBoolean(isCount);
        out.writeInt(projectedIndex);
        IOUtils.writeObject(out, initValue);
        out.writeBoolean(isInitValue);
        IOUtils.writeString(out, funcName);
        out.writeBoolean(isVisible);
        IOUtils.writeObject(out, _columnData);
        out.writeBoolean(_isDynamic);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = IOUtils.readString(in);
        alias = IOUtils.readString(in);
        isSum = in.readBoolean();
        isCount = in.readBoolean();
        projectedIndex = in.readInt();
        initValue = IOUtils.readObject(in);
        isInitValue = in.readBoolean();
        funcName = IOUtils.readString(in);
        isVisible = in.readBoolean();
        _columnData = IOUtils.readObject(in);
        _isDynamic = in.readBoolean();
    }
}
