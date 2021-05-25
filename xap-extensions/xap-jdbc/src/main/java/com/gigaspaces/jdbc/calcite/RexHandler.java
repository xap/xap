package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.jsql.QueryColumnHandler;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.jdbc.SQLUtil;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;
import com.j_spaces.jdbc.builder.range.EqualValueRange;
import com.j_spaces.jdbc.builder.range.NotEqualValueRange;
import com.j_spaces.jdbc.builder.range.Range;
import com.j_spaces.jdbc.builder.range.SegmentRange;
import org.apache.calcite.rex.*;

import java.sql.SQLException;
import java.util.*;

public class RexHandler extends RexShuttle {
    private final RexProgram program;
    private final QueryExecutor queryExecutor;
    private final Map<TableContainer, QueryTemplatePacket> qtpMap;
    private final Deque<Integer> stack = new ArrayDeque<>();
    private final List<String> fields = new ArrayList<>();

    public RexHandler(RexProgram program, QueryExecutor queryExecutor) {
        this.program = program;
        this.queryExecutor = queryExecutor;
        this.qtpMap = new LinkedHashMap<>();
    }

    public Map<TableContainer, QueryTemplatePacket> getQTPMap() {
        return qtpMap;
    }

    @Override
    public RexNode visitCall(RexCall call) { // "="
        RexCall visitCall = (RexCall) super.visitCall(call);
        handleRexCall(visitCall);
        return visitCall;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) { // x, y, z
        fields.add(program.getInputRowType().getFieldNames().get(inputRef.getIndex()));
        return super.visitInputRef(inputRef);
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) { // index of x in metalist, index of literal in metalist
        stack.push(localRef.getIndex());
        return super.visitLocalRef(localRef);
    }

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        return super.visitDynamicParam(dynamicParam);
    }

    private void handleRexCall(RexCall call){
        RexNode leftOp = program.getExprList().get(stack.pop()); // 5 = x
        RexNode rightOp = program.getExprList().get(stack.pop());
        String column = null;
        Object value = null;
        Range range = null;
        switch (leftOp.getKind()){
            case LITERAL:
                value = ((RexLiteral) leftOp).getValue();
                break;
            case INPUT_REF:
                column = fields.get(((RexInputRef) leftOp).getIndex());
            default:
                break;
        }
        switch (rightOp.getKind()){
            case LITERAL:
                value = ((RexLiteral) rightOp).getValue();
                break;
            case INPUT_REF:
                column = fields.get(((RexInputRef) rightOp).getIndex());
            default:
                break;
        }
        TableContainer table = getTableForColumn(column);
        assert table != null;
        try {
            value = table.getColumnValue(column, value);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        switch (call.getKind()) {
            case AND:
                break;
            case OR:
                break;
            case EQUALS:
                range = new EqualValueRange(column, value);
                break;
            case NOT_EQUALS:
                range = new NotEqualValueRange(column, value);
                break;
            case LESS_THAN:
                range = new SegmentRange(column, null, false, (Comparable) value, false);
                break;
            case LESS_THAN_OR_EQUAL:
                range = new SegmentRange(column, null, false, (Comparable) value, true);
                break;
            case GREATER_THAN:
                range = new SegmentRange(column, (Comparable) value, false, null, false);
                break;
            case GREATER_THAN_OR_EQUAL:
                range = new SegmentRange(column, (Comparable) value, true, null, false);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",call.getKind()));
        }
        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
    }

    private TableContainer getTableForColumn(String column){
        for (TableContainer table : queryExecutor.getTables()) {
            if (table.hasColumn(column)) {
                return table;
            }
        }
        return null;
    }
    private void handleLiteral(RexLiteral literal){
        System.out.println(literal);
    }

    private void and(RexHandler leftHandler, RexHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<TableContainer, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().and(((UnionTemplatePacket) rightTable)));
            } else {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().and(rightTable));
            }
        }

        for (Map.Entry<TableContainer, QueryTemplatePacket> rightTable : rightHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket leftTable = leftHandler.getQTPMap().get(rightTable.getKey());
            if (leftTable == null) {
                this.qtpMap.put(rightTable.getKey(), rightTable.getValue());
            } else {
                // already handled
            }
        }

    }

    private void or(RexHandler leftHandler, RexHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<TableContainer, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().union(((UnionTemplatePacket) rightTable)));
            } else {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().union(rightTable));
            }
        }

        for (Map.Entry<TableContainer, QueryTemplatePacket> rightTable : rightHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket leftTable = leftHandler.getQTPMap().get(rightTable.getKey());
            if (leftTable == null) {
                this.qtpMap.put(rightTable.getKey(), rightTable.getValue());
            } else {
                // Already handled above
            }
        }
    }

//    @Override
//    public void visit(EqualsTo equalsTo) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        equalsTo.getLeftExpression().accept(handler);
//        equalsTo.getRightExpression().accept(handler);
//
//        TableContainer table = handler.getTable();
//        Range range = new EqualValueRange(handler.getColumn().getColumnName(), handler.getValue());
//        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//        expTree.put(table, equalsTo);
//    }

//    @Override
//    public void visit(NotEqualsTo notEqualsTo) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        notEqualsTo.getLeftExpression().accept(handler);
//        notEqualsTo.getRightExpression().accept(handler);
//
//        TableContainer table = handler.getTable();
//        Range range = new NotEqualValueRange(handler.getColumn().getColumnName(), handler.getValue());
//        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//        expTree.put(table, notEqualsTo);
//    }

//    @Override
//    public void visit(MinorThanEquals minorThanEquals) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        minorThanEquals.getLeftExpression().accept(handler);
//        minorThanEquals.getRightExpression().accept(handler);
//        if (handler.getColumn().getColumnName().equalsIgnoreCase("rowNum") && handler.getValue() instanceof Integer) {
//            Integer value = ((Integer) handler.getValue());
//            tables.forEach(t -> t.setLimit(value));
//        } else {
//            TableContainer table = handler.getTable();
//            Range range = new SegmentRange(handler.getColumn().getColumnName(), null, false, castToComparable(handler.getValue()), true);
//            qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//            expTree.put(table, minorThanEquals);
//        }
//
//    }
//
//    @Override
//    public void visit(MinorThan minorThan) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        minorThan.getLeftExpression().accept(handler);
//        minorThan.getRightExpression().accept(handler);
//        if (handler.getColumn().getColumnName().equalsIgnoreCase("rowNum") && handler.getValue() instanceof Integer) {
//            Integer value = ((Integer) handler.getValue());
//            tables.forEach(t -> t.setLimit(value - 1));
//        } else {
//            TableContainer table = handler.getTable();
//            Range range = new SegmentRange(handler.getColumn().getColumnName(), null, false, castToComparable(handler.getValue()), false);
//            qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//            expTree.put(table, minorThan);
//        }
//    }
//
//    @Override
//    public void visit(GreaterThan greaterThan) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        greaterThan.getLeftExpression().accept(handler);
//        greaterThan.getRightExpression().accept(handler);
//        TableContainer table = handler.getTable();
//        Range range = new SegmentRange(handler.getColumn().getColumnName(), castToComparable(handler.getValue()), false, null, false);
//        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//        expTree.put(table, greaterThan);
//    }
//
//    @Override
//    public void visit(GreaterThanEquals greaterThanEquals) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        greaterThanEquals.getLeftExpression().accept(handler);
//        greaterThanEquals.getRightExpression().accept(handler);
//        TableContainer table = handler.getTable();
//        Range range = new SegmentRange(handler.getColumn().getColumnName(), castToComparable(handler.getValue()), true, null, false);
//        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//        expTree.put(table, greaterThanEquals);
//    }

    /**
     * Cast the object to Comparable otherwise throws an IllegalArgumentException exception
     */
    private static Comparable castToComparable(Object obj) {
        try {
            //NOTE- a check for Comparable interface implementation is be done in the proxy
            return (Comparable) obj;
        } catch (ClassCastException cce) {
            throw new IllegalArgumentException("Type " + obj.getClass() +
                    " doesn't implement Comparable, Serialization mode might be different than " + StorageType.OBJECT + ".", cce);
        }
    }


//    @Override
//    public void visit(IsNullExpression isNullExpression) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        isNullExpression.getLeftExpression().accept(handler);
//        TableContainer table = handler.getTable();
//        Range range = isNullExpression.isNot() ? new NotNullRange(handler.getColumn().getColumnName()) : new IsNullRange(handler.getColumn().getColumnName());
//        if(table.getJoinInfo() != null && table.getJoinInfo().insertRangeToJoinInfo(range)){
//            return;
//        }
//        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//        expTree.put(table, isNullExpression);
//    }
//
//    @Override
//    public void visit(LikeExpression likeExpression) {
//        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
//        likeExpression.getLeftExpression().accept(handler);
//        likeExpression.getRightExpression().accept(handler);
//        TableContainer table = handler.getTable();
//        String regex = ((String) handler.getValue()).replaceAll("%", ".*").replaceAll("_", ".");
//
//        Range range = likeExpression.isNot() ? new NotRegexRange(handler.getColumn().getColumnName(), regex) : new RegexRange(handler.getColumn().getColumnName(), regex);
//        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//        expTree.put(table, likeExpression);
//    }
//
//    @Override
//    public void visit(Between between) {
//        SingleConditionHandler handlerStart = new SingleConditionHandler(tables, preparedValues);
//        between.getLeftExpression().accept(handlerStart);
//        between.getBetweenExpressionStart().accept(handlerStart);
//        TableContainer table = handlerStart.getTable();
//
//        SingleConditionHandler handlerEnd = new SingleConditionHandler(tables, preparedValues);
//        between.getLeftExpression().accept(handlerEnd);
//        between.getBetweenExpressionEnd().accept(handlerEnd);
//
//        if (!between.isNot()) {
//            Range range = new SegmentRange(handlerStart.getColumn().getColumnName(), castToComparable(handlerStart.getValue()), true, castToComparable(handlerEnd.getValue()), true);
//            qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
//            expTree.put(table, between);
//        } else {
//            throw new UnsupportedOperationException("NOT BETWEEN is not supported");
//        }
//    }
//
//    @Override
//    public void visit(Parenthesis parenthesis) {
//        parenthesis.getExpression().accept(this);
//    }
}
