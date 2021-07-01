package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.calcite.handlers.CalciteUtils;
import com.gigaspaces.jdbc.calcite.handlers.ConditionHandler;
import com.gigaspaces.jdbc.calcite.handlers.SingleTableProjectionHandler;
import com.gigaspaces.jdbc.calcite.pg.PgCalciteTable;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectHandler extends RelShuttleImpl {
    private final QueryExecutor queryExecutor;
    private final Map<RelNode, GSCalc> childToCalc = new HashMap<>();
    private RelNode root = null;

    public SelectHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    // TODO check inserting of same table
    public RelNode visit(TableScan scan) {
        RelNode result = super.visit(scan);
        RelOptTable relOptTable = scan.getTable();
        GSTable gsTable = relOptTable.unwrap(GSTable.class);
        TableContainer tableContainer;
        if (gsTable != null) {
            tableContainer = new ConcreteTableContainer(gsTable.getName(), null, queryExecutor.getSpace());
        } else {
            PgCalciteTable schemaTable = relOptTable.unwrap(PgCalciteTable.class);
            tableContainer = new SchemaTableContainer(schemaTable, null, queryExecutor.getSpace());
        }
        queryExecutor.getTables().add(tableContainer);
        if (!childToCalc.containsKey(scan)) {
            List<String> columns = tableContainer.getAllColumnNames();
            queryExecutor.addFieldCount(columns.size());
            for (String col : columns) {
                tableContainer.addQueryColumn(col, null, true, 0);
            }
        }
        else{
            handleCalc(childToCalc.get(scan), tableContainer);
        }
        return result;
    }

    @Override
    public RelNode visit(RelNode other) {
        if(root == null){
            root = other;
        }
        if(other instanceof GSCalc){
            GSCalc calc = (GSCalc) other;
            childToCalc.put(calc.getInput(), calc);
        }
        RelNode result = super.visit(other);
        if(other instanceof GSJoin){
            handleJoin((GSJoin) other);
        }
        if( other instanceof GSValues ){
            GSValues gsValues = (GSValues) other;
            handleValues(gsValues);
        }

//        else {
//            throw new UnsupportedOperationException("RelNode of type " + other.getClass().getName() + " are not supported yet");
//        }
        return result;
    }

    private void handleValues(GSValues gsValues) {
        if (childToCalc.containsKey(gsValues)) {
            GSCalc gsCalc = childToCalc.get(gsValues);
            RexProgram program = gsCalc.getProgram();

            List<RexLocalRef> projectList = program.getProjectList();
            for (RexLocalRef project : projectList) {
                RexNode node = program.getExprList().get(project.getIndex());
                if (node instanceof RexCall) {
                    RexCall rexCall = (RexCall) node;
                    SqlFunction sqlFunction = (SqlFunction) rexCall.op;
                    List<IQueryColumn> params = new ArrayList();
                    for (RexNode operand : rexCall.getOperands()) {
                        if (operand.isA(SqlKind.LOCAL_REF)) {
                            RexNode funcArgument = program.getExprList().get(((RexLocalRef) operand).getIndex());
                            if (funcArgument.isA(SqlKind.LITERAL)) {
                                RexLiteral literal = (RexLiteral) funcArgument;
                                params.add(new LiteralColumn(CalciteUtils.getValue(literal)));
                            }
                        }

                    }
                    queryExecutor.addColumn(new FunctionCallColumn(params, null, sqlFunction.getName(), null, true, -1));
                }
            }
        }
        else{
//            queryExecutor.addColumn(new LiteralColumn(gsValues.get));
        }
    }

    private void handleJoin(GSJoin join) {
        RexCall rexCall = (RexCall) join.getCondition();
        if(rexCall.getKind() != SqlKind.EQUALS){
            throw new UnsupportedOperationException("Only equal joins are supported");
        }
        int left = join.getLeft().getRowType().getFieldCount();
        int leftIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
        int rightIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
        String lColumn = join.getLeft().getRowType().getFieldNames().get(leftIndex);
        String rColumn = join.getRight().getRowType().getFieldNames().get(rightIndex - left);
        TableContainer rightContainer = queryExecutor.getTableByColumnIndex(rightIndex);
        TableContainer leftContainer = queryExecutor.getTableByColumnIndex(leftIndex);
        IQueryColumn rightColumn = rightContainer.addQueryColumn(rColumn, null, false, 0);
        IQueryColumn leftColumn = leftContainer.addQueryColumn(lColumn, null, false, 0);
        rightContainer.setJoinInfo(new JoinInfo(leftColumn, rightColumn, JoinInfo.JoinType.getType(join.getJoinType())));
        if (leftContainer.getJoinedTable() == null) {
            if (!rightContainer.isJoined()) {
                leftContainer.setJoinedTable(rightContainer);
                rightContainer.setJoined(true);
            }
        }
        if(!childToCalc.containsKey(join)) {
            if(join.equals(root)) {
                for (TableContainer tableContainer : queryExecutor.getTables()) {
                    queryExecutor.getVisibleColumns().addAll(tableContainer.getVisibleColumns());
                }
            }
        }
        else{
            handleCalc(childToCalc.get(join), join);
        }
    }

    private void handleCalc(GSCalc other, TableContainer tableContainer) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        queryExecutor.addFieldCount(outputFields.size());
        new SingleTableProjectionHandler(program, tableContainer, other.equals(root)).project();
        ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
        if (program.getCondition() != null) {
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }

    private void handleCalc(GSCalc other, GSJoin join) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        for (int i = 0; i < outputFields.size(); i++) {
            IQueryColumn qc = queryExecutor.getColumnByColumnIndex(program.getSourceField(i));
            queryExecutor.getVisibleColumns().add(qc);
        }
        if (program.getCondition() != null) {
            ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }
}
