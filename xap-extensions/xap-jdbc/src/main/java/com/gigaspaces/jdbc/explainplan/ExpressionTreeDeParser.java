package com.gigaspaces.jdbc.explainplan;

import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class ExpressionTreeDeParser extends ExpressionDeParser {
    private Boolean wrap = true;

    private void wrap(Runnable r) {
        buffer.append("(");
        r.run();
        buffer.append(")");
    }

    private void wrapOnce(Runnable r) {
        if (wrap) {
            wrap = false;
            wrap(r);
        } else {
            r.run();
        }
    }



    @Override
    public void visit(AndExpression andExpression) {
        wrapOnce(() -> super.visit(andExpression));
        wrap = true;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        wrap(() -> super.visit(equalsTo));
    }

    @Override
    public void visit(Between between) {
        wrap(() -> super.visit(between));
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        wrap(() -> super.visit(greaterThan));
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        wrap(() -> super.visit(greaterThanEquals));
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        wrap(() -> super.visit(isNullExpression));
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        wrap(() -> super.visit(likeExpression));
    }

    @Override
    public void visit(MinorThan minorThan) {
        wrap(() -> super.visit(minorThan));
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        wrap(() -> super.visit(minorThanEquals));
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        wrap(() -> super.visit(notEqualsTo));
    }
}
