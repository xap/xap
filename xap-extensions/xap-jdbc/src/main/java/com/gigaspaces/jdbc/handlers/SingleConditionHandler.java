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
