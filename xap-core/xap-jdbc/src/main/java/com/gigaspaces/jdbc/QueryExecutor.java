package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.TempTableContainer;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryExecutor extends SelectVisitorAdapter implements FromItemVisitor {
    private final List<TableContainer> tables = new ArrayList<>();
    private final IJSpace space;

    public QueryExecutor(IJSpace space) {
        this.space = space;
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        plainSelect.getFromItem().accept(this);
        plainSelect.getSelectItems().forEach(x -> x.accept(new SelectItemVisitorAdapter() {
            @Override
            public void visit(SelectExpressionItem selectExpressionItem) {
                selectExpressionItem.getExpression().accept(new ExpressionVisitorAdapter() {
                    @Override
                    public void visit(Column column) {
                        tables.get(0).addColumn(column.getColumnName(), getStringOrNull(selectExpressionItem.getAlias()));
                    }
                });
            }
        }));
    }


    @Override
    public void visit(Table table) {
        tables.add(new ConcreteTableContainer(table.getFullyQualifiedName(), table.getAlias() == null ? null : table.getAlias().getName(), space));
    }

    @Override
    public void visit(SubSelect subSelect) {
        QueryExecutor subQueryExecutor = new QueryExecutor(space);
        try {
            tables.add(new TempTableContainer(subQueryExecutor.execute(subSelect.getSelectBody()), subSelect.getAlias() == null ? null : subSelect.getAlias().getName()));
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("TODO");
        }

    }

    @Override
    public void visit(SubJoin subjoin) {

    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {

    }

    @Override
    public void visit(ValuesList valuesList) {

    }

    @Override
    public void visit(TableFunction tableFunction) {

    }

    @Override
    public void visit(ParenthesisFromItem aThis) {

    }

    public QueryResult execute(SelectBody selectBody) throws SQLException {
        selectBody.accept(this);

        if (tables.size() == 1) { //Simple Query
            return tables.get(0).getResult();
        }
        throw new UnsupportedOperationException("Unsupported yet");
    }


    private String getStringOrNull(Alias alias) {
        return alias == null ? null : alias.getName();
    }


}
