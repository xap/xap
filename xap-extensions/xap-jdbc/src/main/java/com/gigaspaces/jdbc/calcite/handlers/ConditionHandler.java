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
package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.gigaspaces.metadata.StorageType;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.builder.UnionTemplatePacket;
import com.j_spaces.jdbc.builder.range.*;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ConditionHandler extends RexShuttle {
    private final RexProgram program;
    private final QueryExecutor queryExecutor;
    private final Map<TableContainer, QueryTemplatePacket> qtpMap;
    private final List<String> fields;
    private final TableContainer tableContainer;

    public ConditionHandler(RexProgram program, QueryExecutor queryExecutor, List<String> inputFields) {
        this.program = program;
        this.queryExecutor = queryExecutor;
        this.fields = inputFields;
        this.qtpMap = new LinkedHashMap<>();
        this.tableContainer = null;
    }

    public ConditionHandler(RexProgram program, QueryExecutor queryExecutor, List<String> inputFields,
                            TableContainer tableContainer) {
        this.program = program;
        this.queryExecutor = queryExecutor;
        this.fields = inputFields;
        this.qtpMap = new LinkedHashMap<>();
        this.tableContainer = tableContainer;
    }

    public Map<TableContainer, QueryTemplatePacket> getQTPMap() {
        return qtpMap;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        handleRexCall(call);
        return call;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
        final RexNode node = getNode(localRef);
        if (!(node instanceof  RexLocalRef)) {
            node.accept(this);
        }
        return localRef;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        if(inputRef.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
            String column = fields.get(inputRef.getIndex());
            TableContainer table = getTableForColumn(column);
            assert table != null;
            Range range = new EqualValueRange(column, true); // always equality to true, otherwise the path goes through the handleRexCall method.
            qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
        }
        return inputRef;
    }

    private void handleRexCall(RexCall call){
        switch (call.getKind()) {
            case AND: {
                ConditionHandler leftHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                leftOp.accept(leftHandler);
                for (int i = 1; i < call.getOperands().size(); i++) {
                    RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(i));
                    ConditionHandler rightHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                    rightOp.accept(rightHandler);
                    and(leftHandler, rightHandler);
                }
                break;
            }
            case OR: {
                ConditionHandler leftHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                leftOp.accept(leftHandler);
                for (int i = 1; i < call.getOperands().size(); i++) {
                    RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(i));
                    ConditionHandler rightHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                    rightOp.accept(rightHandler);
                    or(leftHandler, rightHandler);
                }
                break;
            }
            case EQUALS:
            case NOT_EQUALS:
            case LIKE:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL: {
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(1));
                handleTwoOperandsCall(leftOp, rightOp, call.getKind(), false);
                break;
            }
            case IS_NULL:
            case IS_NOT_NULL:
            case NOT: {
                handleSingleOperandsCall(getNode((RexLocalRef) call.getOperands().get(0)), call.getKind());
                break;
            }
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",call.getKind()));
        }
    }

    private void handleNotRexCall(RexCall call){
        switch (call.getKind()) {
            case AND: {
                ConditionHandler leftHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                leftHandler.handleSingleOperandsCall(leftOp, SqlKind.NOT);
                for (int i = 1; i < call.getOperands().size(); i++) {
                    RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(i));
                    ConditionHandler rightHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                    rightHandler.handleSingleOperandsCall(rightOp, SqlKind.NOT);
                    or(leftHandler, rightHandler);
                }
                break;
            }
            case OR: {
                ConditionHandler leftHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                RexNode leftOp = getNode((RexLocalRef) call.getOperands().get(0));
                leftHandler.handleSingleOperandsCall(leftOp, SqlKind.NOT);
                for (int i = 1; i < call.getOperands().size(); i++) {
                    RexNode rightOp = getNode((RexLocalRef) call.getOperands().get(i));
                    ConditionHandler rightHandler = new ConditionHandler(program, queryExecutor, fields, tableContainer);
                    rightHandler.handleSingleOperandsCall(rightOp, SqlKind.NOT);
                    and(leftHandler, rightHandler);
                }
                break;
            }
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",call.getKind()));
        }
    }

    private void handleSingleOperandsCall(RexNode operand, SqlKind sqlKind){
        String column = null;
        Range range = null;
        RexNode leftOp = null;
        RexNode rightOp = null;
        switch (operand.getKind()){
            case INPUT_REF:
                column = fields.get(((RexInputRef) operand).getIndex());
                break;
            case EQUALS:
            case NOT_EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LIKE:
                leftOp = getNode((RexLocalRef) ((RexCall) operand).getOperands().get(0));
                rightOp = getNode((RexLocalRef) ((RexCall) operand).getOperands().get(1));
                handleTwoOperandsCall(leftOp, rightOp, operand.getKind(), SqlKind.NOT.equals(sqlKind));
                return;
            case OR:
            case AND:
                if (SqlKind.NOT.equals(sqlKind)) {
                    handleNotRexCall((RexCall) operand);
                } else {
                    handleRexCall((RexCall) operand);
                }
                return;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",operand.getKind()));
        }
        TableContainer table = getTableForColumn(column);
        assert table != null;
        switch (sqlKind) {
            case IS_NULL:
                range = new IsNullRange(column);
                if(table.getJoinInfo() != null){
                    table.getJoinInfo().insertRangeToJoinInfo(range);
                    return;
                }
                break;
            case IS_NOT_NULL:
                range = new NotNullRange(column);
                if(table.getJoinInfo() != null){
                    table.getJoinInfo().insertRangeToJoinInfo(range);
                    return;
                }
                break;
            case NOT:
                if (!operand.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN)) {
                    throw new UnsupportedOperationException("Queries with NOT on non-boolean column are not supported yet");
                }
                range = new EqualValueRange(column, false);
                if(table.getJoinInfo() != null){
                    table.getJoinInfo().insertRangeToJoinInfo(range);
                    return;
                }
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
    }

    private void handleTwoOperandsCall(RexNode leftOp, RexNode rightOp, SqlKind sqlKind, boolean isNot){
        String column = null;
        boolean isRowNum = false;
        Object value = null;
        Range range = null;
        switch (leftOp.getKind()){
            case LITERAL:
                value = CalciteUtils.getValue((RexLiteral) leftOp);
            case INPUT_REF:
                column = fields.get(((RexInputRef) leftOp).getIndex());
                break;
            case CAST:
                handleTwoOperandsCall(getNode((RexLocalRef) ((RexCall) leftOp).getOperands().get(0)), rightOp, sqlKind, isNot);
                return; //return from recursion
            case DYNAMIC_PARAM:
                value = queryExecutor.getPreparedValues()[((RexDynamicParam) leftOp).getIndex()];
                break;
            case ROW_NUMBER:
                isRowNum = true;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        switch (rightOp.getKind()){
            case LITERAL:
                value = CalciteUtils.getValue((RexLiteral) rightOp);
                break;
            case INPUT_REF:
                column = fields.get(((RexInputRef) rightOp).getIndex());
                break;
            case CAST:
                handleTwoOperandsCall(leftOp, getNode((RexLocalRef) ((RexCall) rightOp).getOperands().get(0)), sqlKind, isNot);
                return; //return from recursion
            case DYNAMIC_PARAM:
                value = queryExecutor.getPreparedValues()[((RexDynamicParam) rightOp).getIndex()];
                break;
            case ROW_NUMBER:
                isRowNum = true;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        if(isRowNum) {
            handleRowNumber(sqlKind, value);
            return; //return and don't continue.
        }

        TableContainer table = getTableForColumn(column);
        assert table != null;
        try {
            value = table.getColumnValue(column, value);
        } catch (SQLException e) {
            throw new SQLExceptionWrapper(e);//throw as runtime.
        }
        sqlKind = isNot ? sqlKind.negateNullSafe() : sqlKind;
        switch (sqlKind) {
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
            case LIKE:
                String regex = ((String) value).replaceAll("%", ".*").replaceAll("_", ".");
                range = isNot ? new NotRegexRange(column, regex) : new RegexRange(column, regex);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        qtpMap.put(table, table.createQueryTemplatePacketWithRange(range));
    }

    private void handleRowNumber(SqlKind sqlKind, Object value) {
        if (!(value instanceof Number)) { //TODO: bigDecimal...
            throw new IllegalArgumentException("rowNum value must be of type Integer, but was [" + value.getClass() +"]");
        }
        Integer limit = ((Number) value).intValue();
        if(limit < 0) {
            throw new IllegalArgumentException("rowNum value must be greater than 0");
        }
        switch (sqlKind) {
            case LESS_THAN:
                queryExecutor.getTables().forEach(tableContainer -> tableContainer.setLimit(limit - 1));
                break;
            case LESS_THAN_OR_EQUAL:
                queryExecutor.getTables().forEach(tableContainer -> tableContainer.setLimit(limit));
                break;
            default:
                throw new UnsupportedOperationException("rowNum supports less than / less than or equal, but was " +
                        "[" + sqlKind + "]");

        }
    }


    private TableContainer getTableForColumn(String column){
        if(tableContainer != null) {
            return tableContainer;
        }
        for (TableContainer table : queryExecutor.getTables()) {
            if (table.hasColumn(column)) {
                return table;
            }
        }
        return null;
    }

    private void and(ConditionHandler leftHandler, ConditionHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<TableContainer, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().and(((UnionTemplatePacket) rightTable)));
            } else {
                QueryTemplatePacket existingQueryTemplatePacket = this.qtpMap.get(leftTable.getKey());
                if (existingQueryTemplatePacket == null) {
                    this.qtpMap.put(leftTable.getKey(), leftTable.getValue().and(rightTable));
                } else {
                    this.qtpMap.put(leftTable.getKey(), existingQueryTemplatePacket.and(rightTable));
                }
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

    private void or(ConditionHandler leftHandler, ConditionHandler rightHandler) {
        //fill qtpMap
        for (Map.Entry<TableContainer, QueryTemplatePacket> leftTable : leftHandler.getQTPMap().entrySet()) {
            QueryTemplatePacket rightTable = rightHandler.getQTPMap().get(leftTable.getKey());
            if (rightTable == null) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue());
            } else if (rightTable instanceof UnionTemplatePacket) {
                this.qtpMap.put(leftTable.getKey(), leftTable.getValue().union(((UnionTemplatePacket) rightTable)));
            } else {
                QueryTemplatePacket existingQueryTemplatePacket = this.qtpMap.get(leftTable.getKey());
                if (existingQueryTemplatePacket == null) {
                    this.qtpMap.put(leftTable.getKey(), leftTable.getValue().union(rightTable));
                } else {
                    this.qtpMap.put(leftTable.getKey(), existingQueryTemplatePacket.union(rightTable));
                }
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

    private RexNode getNode(RexLocalRef localRef){
        return program.getExprList().get(localRef.getIndex());
    }
}
