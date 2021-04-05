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
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;

import java.util.List;

public class SingleConditionHandler extends UnsupportedExpressionVisitor {

    private Object[] preparedValues;
    private Column column;
    private TableContainer table;

    private Object value;

    SingleConditionHandler(Object[] preparedValues) {
        this.preparedValues = preparedValues;
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        this.value = preparedValues[jdbcParameter.getIndex() - 1];
    }

    @Override
    public void visit(LongValue longValue) {
        this.value = (int) longValue.getValue();
    }

    @Override
    public void visit(StringValue stringValue) {
        this.value = stringValue.getValue();
    }

    @Override
    public void visit(Column tableColumn) {
        this.column = tableColumn;
    }

    Column getColumn() {
        return column;
    }

    TableContainer getTable(List<TableContainer> tables) {
        return QueryColumnHandler.getTableForColumn(column, tables);
    }

    Object getValue() {
        return value;
    }
}
