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
package com.gigaspaces.jdbc.jsql.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.table.*;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class QueryColumnHandler extends SelectItemVisitorAdapter {
    //TODO: consider not to pass queryExecutor but its relevant fields, when we need to serialize this object.
    private final QueryExecutor queryExecutor;
    private int columnCounter = 0;

    public QueryColumnHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    public static TableContainer getTableForColumn(Column column, List<TableContainer> tables) {
        TableContainer tableContainer = null;
        for (TableContainer table : tables) {
            if (column.getTable() != null && !column.getTable().getFullyQualifiedName().equals(table.getTableNameOrAlias()))
                continue;
            if (column.getColumnName().equalsIgnoreCase(IQueryColumn.UUID_COLUMN)) {
                if (tableContainer == null) {
                    tableContainer = table;
                } else {
                    throw new IllegalArgumentException("Ambiguous column name [" + column.getColumnName() + "]");
                }
            }
            if (table.hasColumn(column.getColumnName())) {
                if (tableContainer == null) {
                    tableContainer = table;
                } else {
                    throw new IllegalArgumentException("Ambiguous column name [" + column.getColumnName() + "]");
                }
            }
        }
        if (tableContainer == null) {
            throw new ColumnNotFoundException("Could not find column [" + column.getColumnName() + "]");
        }
        return tableContainer;
    }

    @Override
    public void visit(AllColumns columns) {
        this.queryExecutor.setAllColumnsSelected(true);
        this.queryExecutor.getTables().forEach(this::fillQueryColumns);
    }

    @Override
    public void visit(AllTableColumns tableNameContainer) {
        this.queryExecutor.setAllColumnsSelected(true);
        for (TableContainer table : this.queryExecutor.getTables()) {
            final Alias alias = tableNameContainer.getTable().getAlias();
            final String aliasName = alias == null ? null : alias.getName();
            final String tableNameOrAlias = table.getTableNameOrAlias();
            if (tableNameOrAlias.equals(tableNameContainer.getTable().getFullyQualifiedName())
                    || tableNameOrAlias.equals(aliasName)) {

                fillQueryColumns(table);
                break;
            }
        }
    }

    private void fillQueryColumns(TableContainer table) {
        table.getAllColumnNames().forEach(columnName -> {
            if (table instanceof TempTableContainer) {
                long existingNames = table.getVisibleColumns().stream().filter(queryColumn -> queryColumn.getName().equalsIgnoreCase(columnName)).count();
                if (existingNames != 0) {
                    throw new IllegalArgumentException("Ambiguous column name [" + columnName + "]");
                }
            }
            IQueryColumn qc = table.addQueryColumn(columnName, null, true, columnCounter++);
            this.queryExecutor.addColumn(qc);
        });
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        final List<TableContainer> tables = this.queryExecutor.getTables();
        selectExpressionItem.getExpression().accept(new ExpressionVisitorAdapter() {
            private String alias = getStringOrNull(selectExpressionItem.getAlias());
            private List<TableContainer> tableContainers = new ArrayList<>();
            private List<Column> columns = new ArrayList<>();
            private boolean isFromFunction = false;

            @Override
            public void visit(Column column) {
                TableContainer table = getTableForColumn(column, tables);
                if (isFromFunction) {
                    this.columns.add(column);
                    this.tableContainers.add(table);
                } else {
                    IQueryColumn qc = table.addQueryColumn(column.getColumnName(), this.alias, true, columnCounter++);
                    queryExecutor.addColumn(qc);
                }
            }

            @Override
            public void visit(Function function) {
                this.isFromFunction = true;
                super.visit(function);
                AggregationFunctionType aggregationFunctionType = AggregationFunctionType.valueOf(function.getName().toUpperCase());
                queryExecutor.setAllColumnsSelected(function.isAllColumns());
                if (this.columns.size() > 1) { //TODO @sagiv: later need to supports max(age, 10) for example.
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + function.getName() + "()], expected 1 column but was " + this.columns.size());
                }
                if (function.isAllColumns()
                        && aggregationFunctionType != AggregationFunctionType.COUNT) {
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + function.getName() + "()], expected 1 column but was '*'");
                }
                AggregationColumn aggregationColumn;
                if (getTableContainer() != null) {  // assume we have only one table because of the checks above.
                    IQueryColumn qc = getTableContainer().addQueryColumn(getColumnName(function.isAllColumns()),
                            getColumnAlias(), false, -1);
                    aggregationColumn = new AggregationColumn(aggregationFunctionType, getFunctionAlias(function), qc, true,
                            function.isAllColumns(), columnCounter++);
                    getTableContainer().addAggregationColumn(aggregationColumn);
                    queryExecutor.addColumn(qc);
                } else { // for example select COUNT(*) FROM table1 INNER JOIN table2...
                    aggregationColumn = new AggregationColumn(aggregationFunctionType, getFunctionAlias(function),
                            null, true, function.isAllColumns(), columnCounter++);
                    queryExecutor.getTables().forEach(tableContainer -> tableContainer.addAggregationColumn(aggregationColumn));
                    queryExecutor.getTables().forEach(this::addAllTableColumn);
                }
                queryExecutor.addAggregationColumn(aggregationColumn);
            }

            private String getFunctionAlias(Function function) {
                if (this.alias == null) {
                    String columnAlias;
                    if (function.isAllColumns()) {
                        columnAlias = "*";
                    } else {
                        columnAlias = getColumnAlias();
                    }
                    return String.format("%s(%s)", function.getName().toLowerCase(Locale.ROOT), columnAlias);
                }
                return this.alias;
            }

            private String getColumnName(boolean isAllColumn) {
                if (!this.columns.isEmpty()) {
                    return this.columns.get(0).getColumnName();
                }
                return isAllColumn ? "*" : null;
            }

            private String getColumnAlias() {
                if (!this.columns.isEmpty()) {
                    return this.columns.get(0).getName(true);
                }
                return null;
            }

            private TableContainer getTableContainer() {
                return this.tableContainers.isEmpty() ? null : this.tableContainers.get(0);
            }

            private void addAllTableColumn(TableContainer table) {
                table.getAllColumnNames().forEach(columnName -> {
                    IQueryColumn qc = table.addQueryColumn(columnName, null, false, -1);
                    queryExecutor.addColumn(qc);
                });
            }
        });
    }

    private String getStringOrNull(Alias alias) {
        return alias == null ? null : alias.getName();
    }
}
