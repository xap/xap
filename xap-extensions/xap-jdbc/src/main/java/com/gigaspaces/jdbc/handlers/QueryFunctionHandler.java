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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.ArrayList;
import java.util.List;

public class QueryFunctionHandler extends ExpressionVisitorAdapter {
    //TODO: consider not to pass queryExecutor but its relevant fields, when we need to serialize this object.
    private final QueryExecutor queryExecutor;
    private String alias;
    private List<TableContainer> tableContainers = new ArrayList<>();
    private List<Column> columns = new ArrayList<>();

    public QueryFunctionHandler(QueryExecutor queryExecutor) {
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
    public void visit(Column column) {
        this.columns.add(column);
        this.tableContainers.add(getTableForColumn(column, this.queryExecutor.getTables()));
    }

    @Override
    public void visit(Function function) {
//                function.isDistinct() //TODO: block?
//                function.isIgnoreNulls()
//                function.isEscaped()
        super.visit(function);
        AggregationFunction.AggregationFunctionType aggregationFunctionType = AggregationFunction.AggregationFunctionType.valueOf(function.getName().toUpperCase());
        queryExecutor.setAllColumnsSelected(function.isAllColumns());
        if(this.columns.size() > 1) {
            throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                    + function.getName() + "()], expected 1 but was " + this.columns.size());
        }
        if(function.isAllColumns()
                && aggregationFunctionType != AggregationFunction.AggregationFunctionType.COUNT) {
            throw new IllegalArgumentException("Wrong number of arguments to aggregation function ["
                    + function.getName() + "()], expected 1 but was '*'");
        }
        AggregationFunction aggregationFunction = new AggregationFunction(aggregationFunctionType, function.toString(),
                this.alias, getColumnName(function.isAllColumns()), getColumnAlias(), getTableContainer(), true,
                function.isAllColumns());
        if (getTableContainer() != null) {
            getTableContainer().addAggregationFunction(aggregationFunction);
        } else {
            //TODO: in case its * which table? all?
            this.queryExecutor.getTables().forEach(tableContainer -> tableContainer.addAggregationFunction(aggregationFunction));
        }
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        this.alias = getStringOrNull(selectExpressionItem.getAlias());
        selectExpressionItem.getExpression().accept(this);
    }

    private String getColumnName(boolean isAllColumn) {
        if(!this.columns.isEmpty()) {
            return isAllColumn ? "*" : this.columns.get(0).getColumnName();
        }
        return null;
    }

    private String getColumnAlias() {
        if(!this.columns.isEmpty()) {
            String fullName = this.columns.get(0).getName(true);
            int lastIndex = fullName.lastIndexOf(".");
            if (lastIndex != -1) {
                return fullName.substring(0, lastIndex);
            }
        }
        return null;
    }

    private TableContainer getTableContainer() {
        return this.tableContainers.isEmpty() ? null : this.tableContainers.get(0);
    }

    private String getStringOrNull(Alias alias) {
        return alias == null ? null : alias.getName();
    }
}
