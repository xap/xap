package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCaseOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SingleTableProjectionHandler extends RexShuttle {
    private final RexProgram program;
    private final TableContainer tableContainer;
    private final List<String> inputFields;
    private final List<String> outputFields;
    private final boolean isRoot;
    private final QueryExecutor queryExecutor;

    public SingleTableProjectionHandler(RexProgram program, TableContainer tableContainer, boolean isRoot, QueryExecutor queryExecutor) {
        this.program = program;
        this.tableContainer = tableContainer;
        this.inputFields = program.getInputRowType().getFieldNames();
        this.outputFields = program.getOutputRowType().getFieldNames();
        this.isRoot = isRoot;
        this.queryExecutor = queryExecutor;
    }

    public void project(){
        List<RexLocalRef> projects = program.getProjectList();
        for (int i = 0; i < projects.size(); i++) {
            RexLocalRef localRef = projects.get(i);
            RexNode node = program.getExprList().get(localRef.getIndex());
            if(node.isA(SqlKind.INPUT_REF)){
                RexInputRef inputRef = (RexInputRef) node;
                String alias = outputFields.get(i);
                String originalName = inputFields.get(inputRef.getIndex());
                tableContainer.addQueryColumn(originalName, alias, true, 0);
            }
            else if(node instanceof RexCall){
                RexCall call = (RexCall) node;
                SqlFunction sqlFunction;
                List<IQueryColumn> queryColumns = new ArrayList<>();
                switch (call.getKind()) {
                    case OTHER_FUNCTION:
                        sqlFunction = (SqlFunction) call.op;
                        addQueryColumns(call, queryColumns);
                        IQueryColumn functionCallColumn = new FunctionCallColumn(queryColumns, sqlFunction.getName(), sqlFunction.toString(), null, isRoot, -1);
                        if(isRoot)
                            tableContainer.getVisibleColumns().add(functionCallColumn);
                        else
                            tableContainer.getInvisibleColumns().add(functionCallColumn);
                        break;
                    case CAST:
                        sqlFunction = (SqlCastFunction) call.op;
                        addQueryColumns(call, queryColumns);
                        IQueryColumn functionCallColumn2 = new CastFunctionCallColumn(queryColumns, sqlFunction.toString(), sqlFunction.getName(), null, isRoot, -1, call.getType().getFullTypeString());
                        if(isRoot)
                            tableContainer.getVisibleColumns().add(functionCallColumn2);
                        else
                            tableContainer.getInvisibleColumns().add(functionCallColumn2);
                        break;
                    case CASE:
                        SqlCaseOperator sqlCaseOperator = (SqlCaseOperator) call.getOperator(); //TODO: @sagiv needed?
                        CaseColumn caseColumn = new CaseColumn(outputFields.get(i), CalciteUtils.getJavaType(call), i);
                        addCaseCondition(call, caseColumn);
                        queryExecutor.addSqlCaseColumn(caseColumn);
                        break;
                    default:
                        throw new UnsupportedOperationException("call of kind " + call.getKind() + " is not supported");

                }
            }
            else if(node.isA(SqlKind.LITERAL)){
                RexLiteral literal = (RexLiteral) node;
            }

        }
    }

    private void addQueryColumns(RexCall call, List<IQueryColumn> queryColumns) {
        for (RexNode operand : call.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode rexNode = program.getExprList().get(((RexLocalRef) operand).getIndex());
                if (rexNode.isA(SqlKind.INPUT_REF)) {
                    RexInputRef rexInputRef = (RexInputRef) rexNode;
                    String column = inputFields.get(rexInputRef.getIndex());
                    queryColumns.add(tableContainer.addQueryColumn(column, null, false, -1));
                }
                if (rexNode.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) rexNode;
                    queryColumns.add(new LiteralColumn(CalciteUtils.getValue(literal)));
                }
            }
        }
    }

    private void addCaseCondition(RexCall call, CaseColumn caseColumn) {
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
//            handleRowNumber(sqlKind, value);
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

    private RexNode getNode(RexLocalRef localRef){
        return program.getExprList().get(localRef.getIndex());
    }

    public boolean isRoot(){
        return isRoot;
    }
}
