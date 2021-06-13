package com.gigaspaces.jdbc.jsql.handlers;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public abstract class UnsupportedExpressionVisitor implements ExpressionVisitor {


    @Override
    public void visit(BitwiseRightShift aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(BitwiseLeftShift aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(NullValue nullValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Function function) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(SignedExpression signedExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(JdbcNamedParameter jdbcNamedParameter) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(DoubleValue doubleValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(LongValue longValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(HexValue hexValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(DateValue dateValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(TimeValue timeValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(StringValue stringValue) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Addition addition) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Division division) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(IntegerDivision division) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Multiplication multiplication) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Subtraction subtraction) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(AndExpression andExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(OrExpression orExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Between between) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(InExpression inExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(FullTextSearch fullTextSearch) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(IsBooleanExpression isBooleanExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(MinorThan minorThan) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Column tableColumn) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(WhenClause whenClause) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Concat concat) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Matches matches) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(CastExpression cast) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(Modulo modulo) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(AnalyticExpression aexpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(ExtractExpression eexpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(IntervalExpression iexpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(OracleHierarchicalExpression oexpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(RegExpMatchOperator rexpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(JsonExpression jsonExpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(JsonOperator jsonExpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(RegExpMySQLOperator regExpMySQLOperator) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(UserVariable var) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(NumericBind bind) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(KeepExpression aexpr) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(MySQLGroupConcat groupConcat) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(ValueListExpression valueList) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(RowConstructor rowConstructor) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(OracleHint hint) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(TimeKeyExpression timeKeyExpression) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(DateTimeLiteralExpression literal) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(NotExpression aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(NextValExpression aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(CollateExpression aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(SimilarToExpression aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(ArrayExpression aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(VariableAssignment aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void visit(XMLSerializeExpr aThis) {
        throw new UnsupportedOperationException("Unsupported");
    }
}
