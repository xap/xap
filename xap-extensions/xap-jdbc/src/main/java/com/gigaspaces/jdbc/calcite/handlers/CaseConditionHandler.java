package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.table.CaseColumn;
import com.gigaspaces.jdbc.model.table.CaseCondition;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.TableContainer;
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
        handleRexCall(call);
        return call;
    }

    private void handleRexCall(RexCall call){
        CaseCondition caseCondition = null;
        for (int i = 0; i < call.getOperands().size(); i++) {
            RexNode operand = call.getOperands().get(i);
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = program.getExprList().get(((RexLocalRef) operand).getIndex());
                switch (rexNode.getKind()) {
                    case LITERAL:
                        if(caseCondition != null) {
                            caseCondition.setResult(CalciteUtils.getValue((RexLiteral) rexNode));
                        } else {
                            caseCondition = new CaseCondition(CaseCondition.ConditionCode.DEFAULT, CalciteUtils.getValue((RexLiteral) rexNode));
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
                        RexNode leftOp = program.getExprList().get(((RexLocalRef) operands.get(0)).getIndex());
                        RexNode rightOp = program.getExprList().get(((RexLocalRef) operands.get(1)).getIndex());
                        caseCondition = handleTwoOperandsCall(leftOp, rightOp, rexNode.getKind(), false);
                        break;
                    default:
                        throw new UnsupportedOperationException("Wrong CASE condition kind [" + operand.getKind() + "]");
                }
            } else {
                throw new IllegalStateException("CASE operand kind should be LOCAL_REF but was [" + operand.getKind() + "]");
            }
        }
    }

    private CaseCondition handleTwoOperandsCall(RexNode leftOp, RexNode rightOp, SqlKind sqlKind, boolean isNot){
        String column = null;
        boolean isRowNum = false;
        Object value = null;
        CaseCondition caseCondition = null;
        switch (leftOp.getKind()){
            case LITERAL:
                value = CalciteUtils.getValue((RexLiteral) leftOp);
            case INPUT_REF:
                column = inputFields.get(((RexInputRef) leftOp).getIndex());
                break;
            case CAST:
                return handleTwoOperandsCall(getNode((RexLocalRef) ((RexCall) leftOp).getOperands().get(0)), rightOp,
                        sqlKind, isNot);//return from recursion
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

        try {
            value = SQLUtil.cast(((ConcreteTableContainer) tableContainer).getTypeDesc(), column, value, false);
        } catch (SQLException e) {
            throw new SQLExceptionWrapper(e);//throw as runtime.
        }
        assert value != null;

        sqlKind = isNot ? sqlKind.negateNullSafe() : sqlKind;
        switch (sqlKind) {
            case EQUALS:
                caseCondition = new CaseCondition(CaseCondition.ConditionCode.EQ, value, value.getClass(), column);
                break;
            case NOT_EQUALS:
                caseCondition = new CaseCondition(CaseCondition.ConditionCode.NE, value, value.getClass(), column);
                break;
            case LESS_THAN:
                caseCondition = new CaseCondition(CaseCondition.ConditionCode.LT, value, value.getClass(), column);
                break;
            case LESS_THAN_OR_EQUAL:
                caseCondition = new CaseCondition(CaseCondition.ConditionCode.LE, value, value.getClass(), column);
                break;
            case GREATER_THAN:
                caseCondition = new CaseCondition(CaseCondition.ConditionCode.GT, value, value.getClass(), column);
                break;
            case GREATER_THAN_OR_EQUAL:
                caseCondition = new CaseCondition(CaseCondition.ConditionCode.GE, value, value.getClass(), column);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Queries with %s are not supported",sqlKind));
        }
        return caseCondition;
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

    private RexNode getNode(RexLocalRef localRef){
        return program.getExprList().get(localRef.getIndex());
    }
}
