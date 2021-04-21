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
package com.gigaspaces.jdbc.handlers;

import com.gigaspaces.jdbc.model.table.TableContainer;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.List;

public class SingleConditionHandler extends UnsupportedExpressionVisitor {

    private Object[] preparedValues;
    private Column column;

    private Object value;
    private List<TableContainer> tables;

    SingleConditionHandler(List<TableContainer> tables, Object[] preparedValues) {
        this.tables = tables;
        this.preparedValues = preparedValues;
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        this.value = preparedValues[jdbcParameter.getIndex() - 1];
    }

    @Override
    public void visit(LongValue longValue) {
        if (column.getColumnName().equalsIgnoreCase("rowNum")) {
            this.value = (int)longValue.getValue();
        } else {
            try {
                this.value = getTable().getColumnValue(getColumn().getColumnName(), longValue.getValue());
            } catch (SQLException e) {
                this.value = longValue.getValue();
            }
        }
    }

    @Override
    public void visit(StringValue stringValue) {
        try {
            this.value = getTable().getColumnValue(getColumn().getColumnName(), stringValue.getValue());
            if (this.value.getClass().equals(java.util.Date.class)) {
                throw new UnsupportedOperationException("java.util.Date is not supported");
            }
        } catch (SQLException e) {
            this.value = stringValue.getValue();
        }
    }

    @Override
    public void visit(Column tableColumn) {
        this.column = tableColumn;
    }

    Column getColumn() {
        return column;
    }

    TableContainer getTable() {
        return QueryColumnHandler.getTableForColumn(column, tables);
    }

    Object getValue() {
        return value;
    }


    @Override
    public void visit(NullValue nullValue) {
        this.value = null;
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        this.value = doubleValue.getValue();
    }

    @Override
    public void visit(DateValue dateValue) {
        this.value = dateValue.getValue();
    }

    @Override
    public void visit(TimeValue timeValue) {
        this.value = timeValue.getValue();
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        this.value = timestampValue.getValue();
    }


}
