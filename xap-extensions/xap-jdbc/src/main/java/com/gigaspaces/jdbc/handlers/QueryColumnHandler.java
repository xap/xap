package com.gigaspaces.jdbc.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.table.AggregationFunction;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
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
import java.util.Objects;

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
            if (column.getColumnName().equalsIgnoreCase(QueryColumn.UUID_COLUMN)) {
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
            QueryColumn qc = table.addQueryColumn(columnName, null, true, columnCounter++);
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
                if(isFromFunction) {
                    this.columns.add(column);
                    this.tableContainers.add(table);
                    //TODO: validate alias = null!
                    QueryColumn qc = table.addQueryColumn(column.getColumnName(),null , false, columnCounter++);
                    queryExecutor.addColumn(qc);
                } else {
                    QueryColumn qc = table.addQueryColumn(column.getColumnName(), this.alias, true, columnCounter++);
                    queryExecutor.addColumn(qc);
                }
            }

            @Override
            public void visit(Function function) {
                this.isFromFunction = true;
                super.visit(function);
                AggregationFunction.AggregationFunctionType aggregationFunctionType = AggregationFunction.AggregationFunctionType.valueOf(function.getName().toUpperCase());
                queryExecutor.setAllColumnsSelected(function.isAllColumns());
                if(this.columns.size() > 1) { //TODO: later need to supports max(age, 10) for example.
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + function.getName() + "()], expected 1 column but was " + this.columns.size());
                }
                if(function.isAllColumns()
                        && aggregationFunctionType != AggregationFunction.AggregationFunctionType.COUNT) {
                    throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                            + function.getName() + "()], expected 1 column but was '*'");
                }
                AggregationFunction aggregationFunction = new AggregationFunction(aggregationFunctionType, function.getName(),
                        this.alias, getColumnName(function.isAllColumns()), getColumnAlias(), getTableContainer(),
                        true, function.isAllColumns(), columnCounter++);
                if (getTableContainer() != null) {
                    getTableContainer().addAggregationFunctionColumn(aggregationFunction);
                } else {
                    //TODO: in case its * which table? all?
                    queryExecutor.getTables().forEach(tableContainer -> tableContainer.addAggregationFunctionColumn(aggregationFunction));
                    queryExecutor.getTables().forEach(this::addAllTableColumn);
                }
                queryExecutor.addAggregationFunction(aggregationFunction);
            }

            private String getColumnName(boolean isAllColumn) {
                if(!this.columns.isEmpty()) {
                    return this.columns.get(0).getColumnName();
                }
                return isAllColumn ? "*" : null;
            }

            private String getColumnAlias() {
                if(!this.columns.isEmpty()) {
                    String fullName = this.columns.get(0).getName(true);
                    //for example max(P1.name), P1 is not alias.
                    if(Objects.equals(fullName, this.columns.get(0).getName(false))) return null;
//                    if(Objects.equals(fullName, this.columns.get(0).getName(false))) return fullName;
                    int lastIndex = fullName.lastIndexOf(".");
                    if (lastIndex != -1) {
                        return fullName.substring(0, lastIndex);
                    }
                }
                return null;
            }

            private TableContainer getTableContainer() { //TODO: should be all tables? and not just the first one?
                return this.tableContainers.isEmpty() ? null : this.tableContainers.get(0);
            }

            private void addAllTableColumn(TableContainer table) {
                table.getAllColumnNames().forEach(columnName -> {
                    QueryColumn qc = table.addQueryColumn(columnName, null, false, columnCounter++);
                    queryExecutor.addColumn(qc);
                });
            }
        });
    }

    private String getStringOrNull(Alias alias) {
        return alias == null ? null : alias.getName();
    }
}
