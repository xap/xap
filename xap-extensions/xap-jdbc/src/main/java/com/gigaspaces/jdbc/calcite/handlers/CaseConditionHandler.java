package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.sql.SQLException;
import java.util.List;

public class CaseConditionHandler extends RexShuttle {
    private final RexProgram program;
    private final QueryExecutor queryExecutor;
    private final List<String> inputFields;
    private final TableContainer tableContainer;
    private final CaseColumn caseColumn;


    public CaseConditionHandler(RexProgram program, QueryExecutor queryExecutor, List<String> inputFields,
                                TableContainer tableContainer, CaseColumn caseColumn) {
        this.program = program;
        this.queryExecutor = queryExecutor;
        this.inputFields = inputFields;
        this.tableContainer = tableContainer;
        this.caseColumn = caseColumn;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        handleRexCall(call, null);
        return call;
    }

    private void handleRexCall(RexCall call, ICaseCondition caseCondition){
        for (int i = 0; i < call.getOperands().size(); i++) {
            RexNode operand = call.getOperands().get(i);
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = getNode(((RexLocalRef) operand));
                switch (rexNode.getKind()) {
                    case INPUT_REF:
                        String fieldName = inputFields.get(((RexInputRef) rexNode).getIndex());
                        TableContainer tableForColumn = getTableForColumn(fieldName);
                        IQueryColumn queryColumn = tableForColumn.addQueryColumn(fieldName, null, false, -1);
                        caseCondition.setResult(queryColumn);
                        caseColumn.addCaseCondition(caseCondition);
                        caseCondition = null;
                        break;
                    case LITERAL:
                        if(caseCondition != null) {
                            caseCondition.setResult(CalciteUtils.getValue((RexLiteral) rexNode));
                        } else {
                            caseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.DEFAULT_TRUE, CalciteUtils.getValue((RexLiteral) rexNode));
                        }
                        caseColumn.addCaseCondition(caseCondition);
                        caseCondition = null;
                        break;
                    case EQUALS:
                    case NOT_EQUALS:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        List<RexNode> operands = ((RexCall) rexNode).getOperands();
                        RexNode leftOp = getNode(((RexLocalRef) operands.get(0)));
                        RexNode rightOp = getNode(((RexLocalRef) operands.get(1)));
                        if (caseCondition == null) {
                            caseCondition = handleTwoOperandsCall(leftOp, rightOp, rexNode.getKind(), false);
                        } else if (caseCondition instanceof CompoundCaseCondition) {
                            ((CompoundCaseCondition) caseCondition).addCaseCondition(handleTwoOperandsCall(leftOp, rightOp, rexNode.getKind(), false));
                        }
                        break;
                    case OR:
                        if (!(caseCondition instanceof CompoundCaseCondition)) {
                            caseCondition = new CompoundCaseCondition();
                        }
                        ((CompoundCaseCondition) caseCondition).addCompoundConditionCode(CompoundCaseCondition.CompoundConditionCode.OR);
                        handleRexCall((RexCall) rexNode, caseCondition);
                        break;
                    case AND:
                        if (!(caseCondition instanceof CompoundCaseCondition)) {
                            caseCondition = new CompoundCaseCondition();
                        }
                        ((CompoundCaseCondition) caseCondition).addCompoundConditionCode(CompoundCaseCondition.CompoundConditionCode.AND);
                        handleRexCall((RexCall) rexNode, caseCondition);
                        break;
                    case CASE:
                        CaseColumn nestedCaseColumn = new CaseColumn(caseColumn.getName(), caseColumn.getReturnType()
                                , caseColumn.getColumnOrdinal());
                        CaseConditionHandler caseHandler = new CaseConditionHandler(program, queryExecutor, inputFields,
                                tableContainer, nestedCaseColumn);
                        caseHandler.handleRexCall((RexCall) rexNode, null);
                        caseCondition.setResult(nestedCaseColumn);
                        caseColumn.addCaseCondition(caseCondition);
                        caseCondition = null;
                        break;
                    default:
                        throw new UnsupportedOperationException("Wrong CASE condition kind [" + operand.getKind() + "]");
                }
            } else {
                throw new IllegalStateException("CASE operand kind should be LOCAL_REF but was [" + operand.getKind() + "]");
            }
        }
    }

    private SingleCaseCondition handleTwoOperandsCall(RexNode leftOp, RexNode rightOp, SqlKind sqlKind, boolean isNot){
        String column = null;
        boolean isRowNum = false; //TODO: @sagiv needed?
        Object value = null;
        SingleCaseCondition singleCaseCondition = null;
        switch (leftOp.getKind()){
            case LITERAL:
                value = CalciteUtils.getValue((RexLiteral) leftOp);
            case INPUT_REF:
                column = inputFields.get(((RexInputRef) leftOp).getIndex());
                break;
            case CAST:
                return handleTwoOperandsCall(getNode((RexLocalRef) ((RexCall) leftOp).getOperands().get(0)), rightOp,
                        sqlKind, isNot);//return from recursion
            case DYNAMIC_PARAM://TODO: @sagiv needed? add test and check
                value = queryExecutor.getPreparedValues()[((RexDynamicParam) leftOp).getIndex()];
                break;
            case ROW_NUMBER://TODO: @sagiv needed? add test and check
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
                column = inputFields.get(((RexInputRef) rightOp).getIndex());
                break;
            case CAST:
                return handleTwoOperandsCall(leftOp, getNode((RexLocalRef) ((RexCall) rightOp).getOperands().get(0)),
                        sqlKind, isNot); //return from recursion
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
            return null; //return and don't continue.
        }

        TableContainer tableForColumn = getTableForColumn(column);
        try {
            value = SQLUtil.cast(((ConcreteTableContainer) tableForColumn).getTypeDesc(), column, value, false);
        } catch (SQLException e) {
            throw new SQLExceptionWrapper(e);//throw as runtime.
        }
        assert value != null;
        assert column != null;

        tableForColumn.addQueryColumn(column, null, false, -1);

        sqlKind = isNot ? sqlKind.negateNullSafe() : sqlKind;
        switch (sqlKind) {
            case EQUALS:
                singleCaseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.EQ, value, value.getClass(), column);
                break;
            case NOT_EQUALS:
                singleCaseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.NE, value, value.getClass(), column);
                break;
            case LESS_THAN:
                singleCaseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.LT, value, value.getClass(), column);
                break;
            case LESS_THAN_OR_EQUAL:
                singleCaseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.LE, value, value.getClass(), column);
                break;
            case GREATER_THAN:
                singleCaseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.GT, value, value.getClass(), column);
                break;
            case GREATER_THAN_OR_EQUAL:
                singleCaseCondition = new SingleCaseCondition(SingleCaseCondition.ConditionCode.GE, value, value.getClass(), column);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        return singleCaseCondition;
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
        throw new IllegalStateException("Could not find table for column [" + column + "]");
    }

    private RexNode getNode(RexLocalRef localRef){
        return program.getExprList().get(localRef.getIndex());
    }
}
