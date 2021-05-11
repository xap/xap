package com.gigaspaces.jdbc.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.ColumnNotFoundException;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

import java.util.List;

public class QueryColumnHandler extends SelectItemVisitorAdapter {
    //TODO: queryExecutor as field?
    private final List<TableContainer> tables;
    private final List<QueryColumn> queryColumns;
    private boolean isAllColumnsSelected = false;

    public QueryColumnHandler(QueryExecutor queryExecutor) {
        this.tables = queryExecutor.getTables();
        this.queryColumns = queryExecutor.getQueryColumns();
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
        setAllColumnsSelected(true);
        tables.forEach(this::fillQueryColumns);
    }

    @Override
    public void visit(AllTableColumns tableNameContainer) {
        setAllColumnsSelected(true);
        for (TableContainer table : tables) {
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
            QueryColumn qc = table.addQueryColumn(columnName, null, true);
            queryColumns.add(qc);
        });
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        selectExpressionItem.getExpression().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Column column) {
                TableContainer table = getTableForColumn(column, tables);
                QueryColumn qc = table.addQueryColumn(column.getColumnName(), getStringOrNull(selectExpressionItem.getAlias()), true);
                queryColumns.add(qc);
            }
        });
    }


    private String getStringOrNull(Alias alias) {
        return alias == null ? null : alias.getName();
    }

    public boolean isAllColumnsSelected() {
        return isAllColumnsSelected;
    }

    public void setAllColumnsSelected(boolean isAllColumnsSelected) {
        this.isAllColumnsSelected = isAllColumnsSelected;
    }
}
