package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.jsql.handlers.QueryColumnHandler;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;

import java.util.*;

import static java.lang.String.format;

public class CalciteRootVisitor extends RelShuttleImpl {
    private final QueryExecutor queryExecutor;
    private final List<TableContainer> tableContainerList = new ArrayList<>();
    private final Map<RelNode, GSCalc> childToCalc = new HashMap<>();
    private RelNode root = null;

    public CalciteRootVisitor(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public RelNode visit(TableScan scan) {
        RelNode result = super.visit(scan);
        GSTable table = scan.getTable().unwrap(GSTable.class);
        TableContainer tableContainer = new ConcreteTableContainer(table.getTypeDesc().getTypeName(), null, queryExecutor.getSpace());
        queryExecutor.getTables().add(tableContainer);
        if (!childToCalc.containsKey(scan)) {
            for (String col : tableContainer.getAllColumnNames()) {
                tableContainer.addQueryColumn(col, null, true, 0);
                tableContainerList.add(tableContainer);
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
//        else {
//            throw new UnsupportedOperationException("RelNode of type " + other.getClass().getName() + " are not supported yet");
//        }
        return result;
    }

    private void handleJoin(GSJoin join) {
        RexCall rexCall = (RexCall) join.getCondition();
        if(rexCall.getKind() != SqlKind.EQUALS){
            throw new UnsupportedOperationException("Only equi joins are supported");
        }
        int total = join.getRowType().getFieldCount();
        int left = join.getLeft().getRowType().getFieldCount();
        int right = join.getRight().getRowType().getFieldCount();
        int leftIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
        int rightIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
        String lColumn = join.getLeft().getRowType().getFieldNames().get(leftIndex);
        String rColumn = join.getRight().getRowType().getFieldNames().get(rightIndex - left);
        TableContainer rightContainer = tableContainerList.get(rightIndex);
        TableContainer leftContainer = tableContainerList.get(leftIndex);
        System.out.println(format("join: %s leftColumn: %s rightColumn: %s left table: %s right table: %s", join.getCondition(), lColumn, rColumn, leftContainer.getTableNameOrAlias(), rightContainer.getTableNameOrAlias()));
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
                TableContainer prev = null;
                for (TableContainer tableContainer : tableContainerList) {
                    if(prev == null || prev != tableContainer) {
                        queryExecutor.getVisibleColumns().addAll(tableContainer.getVisibleColumns());
                        prev = tableContainer;
                    }
                }
            }
        }
        else{
            handleCalc(childToCalc.get(join), join, leftContainer, rightContainer);
        }
    }

    private void handleCalc(GSCalc other, TableContainer tableContainer) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        for (int i = 0; i < outputFields.size(); i++) {
            String alias = outputFields.get(i);
            String originalName = inputFields.get(program.getSourceField(i));
            tableContainer.addQueryColumn(originalName, alias, true, 0);
            tableContainerList.add(tableContainer);
        }
        ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
        if (program.getCondition() != null) {
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }

    private void handleCalc(GSCalc other, GSJoin join, TableContainer leftContainer, TableContainer rightContainer) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        int total = outputFields.size();
        int left = join.getLeft().getRowType().getFieldCount();
        int right = join.getRight().getRowType().getFieldCount();
        if(total == left + right){
            for (int i = 0; i < total; i++) {
                if(i < left){
                    IQueryColumn qc = leftContainer.getAllQueryColumns().get(i);
                    queryExecutor.getVisibleColumns().add(qc);
                }
                else{
                    IQueryColumn qc = rightContainer.getAllQueryColumns().get(i - left);
                    queryExecutor.getVisibleColumns().add(qc);
                }
            }
        }
        else{
            for (int i = 0; i < outputFields.size(); i++) {
                String alias = outputFields.get(i);
                String originalName = inputFields.get(program.getSourceField(i));
                IQueryColumn qc = QueryColumnHandler.getColumn(originalName, alias, queryExecutor.getTables());
                queryExecutor.getVisibleColumns().add(qc);
            }
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
