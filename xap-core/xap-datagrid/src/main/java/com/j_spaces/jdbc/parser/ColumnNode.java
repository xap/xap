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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;
import com.j_spaces.jdbc.query.QueryColumnData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.List;


/**
 * This is a column Node. it has a name.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class ColumnNode extends ValueNode {
    private static final long serialVersionUID = 1L;

    private QueryColumnData _columnData;
    private String _name;
    private FunctionCallDescription functionCallDescription;
    private String tableName;

    public ColumnNode() {
        super();
    }

    public ColumnNode(String columnPath) {
        this();
        _name = columnPath;
    }

    public ColumnNode(String columnPath, String tableName) {
        this(columnPath);
        this.tableName = tableName;
    }

    public FunctionCallDescription getFunctionCallDescription() {
        return functionCallDescription;
    }

    public void setFunctionCallDescription(FunctionCallDescription functionCallDescription) {
        this.functionCallDescription = functionCallDescription;
    }

    public void createColumnData(AbstractDMLQuery query) throws SQLException {
        _columnData = QueryColumnData.newColumnData(tableName == null ? _name : tableName + "." + _name, query);
    }

    public QueryColumnData getColumnData() {
        return _columnData;
    }

    public String getColumnPath() {
        return _columnData.getColumnPath();
    }

    @Override
    public String toString() {
        if (functionCallDescription == null) {
            return _name;
        } else {
            return functionCallDescription.getName() + "/" + functionCallDescription.getNumberOfArguments() + " (" + _name + ")";
        }
    }

    @Override
    public void accept(QueryTemplateBuilder builder) throws SQLException {
        builder.build(this);
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        if (_columnData != null)
            throw new IllegalStateException("Can't set column name after column data has been set.");
        this._name = name;
    }

    public Object getFieldValue(IEntryPacket entry) {
        return SQLUtil.getFieldValue(entry, _columnData);
    }

    @Override
    public void prepareValues(Object[] values) throws SQLException {
        if (functionCallDescription != null) {
            List<Object> args = functionCallDescription.getArgs();
            for (int i = 0; i < args.size(); i++) {
                Object arg = args.get(i);
                if (arg instanceof PreparedNode) {
                    ((PreparedNode) arg).prepareValues(values);
                    args.set(i, ((PreparedNode) arg).getValue());
                }
            }
        }
        super.prepareValues(values);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _columnData);
        IOUtils.writeString(out, _name);
        IOUtils.writeObject(out, functionCallDescription);
        IOUtils.writeString(out, tableName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _columnData = IOUtils.readObject(in);
        _name = IOUtils.readString(in);
        functionCallDescription = IOUtils.readObject(in);
        tableName = IOUtils.readString(in);
    }

    public ColumnNode setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getTableName() {
        return tableName;
    }
}
