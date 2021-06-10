package com.gigaspaces.jdbc.handlers;

import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class NotConditionHandler extends UnsupportedExpressionVisitor{
    private final Object[] preparedValues;
    private final List<TableContainer> tables;
    private final Map<TableContainer, Expression> expTree;
    private final Map<TableContainer, QueryTemplatePacket> qtpMap;


    public Map<TableContainer, QueryTemplatePacket> getQTPMap() {
        return qtpMap;
    }

    public Map<TableContainer, Expression> getExpTree() {
        return expTree;
    }

    public NotConditionHandler(List<TableContainer> tables, Object[] preparedValues) {
        this.preparedValues = preparedValues;
        this.tables = tables;
        this.qtpMap = new LinkedHashMap<>();
        this.expTree = new LinkedHashMap<>();
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        WhereHandler handler = new WhereHandler(tables, preparedValues);
        NotEqualsTo notEqualsTo = new NotEqualsTo(equalsTo.getLeftExpression(), equalsTo.getRightExpression());
        notEqualsTo.accept(handler);
        fillQtpMapAndExpTree(handler);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        WhereHandler handler = new WhereHandler(tables, preparedValues);
        EqualsTo equalsTo = new EqualsTo(notEqualsTo.getLeftExpression(), notEqualsTo.getRightExpression());
        equalsTo.accept(handler);
        fillQtpMapAndExpTree(handler);
    }

    private void fillQtpMapAndExpTree(WhereHandler handler) {
        for (Map.Entry<TableContainer, QueryTemplatePacket> table : handler.getQTPMap().entrySet()) {
            this.qtpMap.put(table.getKey(), table.getValue());
        }
        for (Map.Entry<TableContainer, Expression> table : handler.getExpTree().entrySet()) {
            this.expTree.put(table.getKey(), table.getValue());
        }
    }

}
