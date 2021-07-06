package com.gigaspaces.jdbc.calcite.experimental.handlers;

import com.gigaspaces.jdbc.calcite.*;
import com.gigaspaces.jdbc.calcite.experimental.JoinResultSupplier;
import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;
import com.gigaspaces.jdbc.calcite.experimental.SingleResultSupplier;
import com.gigaspaces.jdbc.calcite.experimental.model.join.JoinInfo;
import com.gigaspaces.jdbc.calcite.experimental.result.QueryResult;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;

import java.sql.SQLException;
import java.util.Map;
import java.util.Stack;

public class SelectHandler extends RelShuttleImpl {
    private final Stack<ResultSupplier> stack = new Stack<>();
    private final IJSpace space;
    private final QueryExecutionConfig config;
    private final Object[] preparedValues;

    public SelectHandler(IJSpace space, QueryExecutionConfig config, Object[] preparedValues) {
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
    }

    public SelectHandler(IJSpace space, Object[] preparedValues) {
        this(space, new QueryExecutionConfig(), preparedValues);
    }

    @Override
    public RelNode visit(TableScan scan) {
        RelNode result = super.visit(scan);
        GSTable table = scan.getTable().unwrap(GSTable.class);
        ResultSupplier resultSupplier = new SingleResultSupplier(table.getTypeDesc(), space, preparedValues);
        stack.push(resultSupplier);
        return result;
    }

    @Override
    public RelNode visit(RelNode other) {
        RelNode result = super.visit(other);
        if(other instanceof GSCalc){
            handleCalc((GSCalc) other);
        }
        if(other instanceof GSJoin){
            handleJoin((GSJoin) other);
        }
        if(other instanceof GSAggregate){
            handleAggregate((GSAggregate) other);
        }
        if(other instanceof GSSort){
            handleSort((GSSort) other);
        }
        return result;
    }

    private void handleCalc(GSCalc gsCalc) {
        ResultSupplier resultSupplier = stack.pop();
        RexProgram rexProgram = gsCalc.getProgram();
        if(rexProgram.getCondition() != null){
            ConditionHandler conditionHandler = new ConditionHandler(rexProgram, resultSupplier);
            rexProgram.getCondition().accept(conditionHandler);
            for (Map.Entry<ResultSupplier, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
        ProjectionHandler.instance().project(rexProgram, resultSupplier);
        stack.push(resultSupplier);
    }

    private void handleSort(GSSort sort) {
        ResultSupplier resultSupplier = stack.pop();
        OrderByHandler.instance().apply(resultSupplier, sort);
        stack.push(resultSupplier);
    }

    private void handleAggregate(GSAggregate gsAggregate) {
        ResultSupplier resultSupplier = stack.pop();
        AggregateHandler.instance().apply(gsAggregate, resultSupplier);
        stack.push(resultSupplier);
    }

    private void handleJoin(GSJoin join) {
        ResultSupplier rightSupplier = stack.pop();
        ResultSupplier leftSupplier = stack.pop();
        RexCall rexCall = (RexCall) join.getCondition();
        if(rexCall.getKind() != SqlKind.EQUALS){
            throw new UnsupportedOperationException("Only equal joins are supported");
        }
        int left = join.getLeft().getRowType().getFieldCount();
        int leftIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
        int rightIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
        String leftColumn = join.getLeft().getRowType().getFieldNames().get(leftIndex);
        String rightColumn = join.getRight().getRowType().getFieldNames().get(rightIndex - left);
        rightSupplier.getOrCreatePhysicalColumn(rightColumn);
        leftSupplier.getOrCreatePhysicalColumn(leftColumn);
        final JoinInfo joinInfo = new JoinInfo(leftColumn, rightColumn, JoinInfo.JoinType.getType(join.getJoinType()));
        ResultSupplier joinResultSupplier = new JoinResultSupplier(leftSupplier, rightSupplier, space, preparedValues, joinInfo, stack.empty());
        stack.push(joinResultSupplier);
    }

//    private void handleCalc(GSCalc other, TableContainer tableContainer) {
//        RexProgram program = other.getProgram();
//        List<String> inputFields = program.getInputRowType().getFieldNames();
//        List<String> outputFields = program.getOutputRowType().getFieldNames();
//        queryExecutor.addFieldCount(outputFields.size());
//        for (int i = 0; i < outputFields.size(); i++) {
//            String alias = outputFields.get(i);
//            String originalName = inputFields.get(program.getSourceField(i));
//            tableContainer.addQueryColumn(originalName, alias, true, 0);
//        }
//        ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
//        if (program.getCondition() != null) {
//            program.getCondition().accept(conditionHandler);
//            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
//                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
//            }
//        }
//    }
//
//    private void handleCalc(GSCalc other, GSJoin join) {
//        RexProgram program = other.getProgram();
//        List<String> inputFields = program.getInputRowType().getFieldNames();
//        List<String> outputFields = program.getOutputRowType().getFieldNames();
//        for (int i = 0; i < outputFields.size(); i++) {
//            IQueryColumn qc = queryExecutor.getColumnByColumnIndex(program.getSourceField(i));
//            queryExecutor.getVisibleColumns().add(qc);
//        }
//        if (program.getCondition() != null) {
//            ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
//            program.getCondition().accept(conditionHandler);
//            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
//                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
//            }
//        }
//    }

    public QueryResult execute() throws SQLException {
        if(stack.size() != 1){
            throw new SQLException("Error when traversing query call tree");
        }
        return stack.pop().executeRead(config);
    }
}
