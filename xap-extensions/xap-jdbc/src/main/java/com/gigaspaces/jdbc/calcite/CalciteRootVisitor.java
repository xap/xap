package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Map;
import java.util.Stack;

public class CalciteRootVisitor extends RelShuttleImpl {
    private final QueryExecutor queryExecutor;
    private final Stack<TableContainer> containerStack = new Stack<>();
    private RelNode parent = null;

    public CalciteRootVisitor(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public RelNode visit(TableScan scan) {
        RelNode result = super.visit(scan);
        handleTable(scan.getTable().unwrap(GSTable.class));
        return result;
    }

    private void handleTable(GSTable table) {
        TableContainer tableContainer = new ConcreteTableContainer(table.getTypeDesc().getTypeName(), null, queryExecutor.getSpace());
        queryExecutor.getTables().add(tableContainer);
        if (parent == null || !(parent instanceof GSCalc)) {
            for (String col : tableContainer.getAllColumnNames()) {
                tableContainer.addQueryColumn(col, null, true);
            }
        }
        containerStack.push(tableContainer);
    }

    @Override
    public RelNode visit(RelNode other) {
        if(other instanceof GSCalc)
            parent = other;
        RelNode result = super.visit(other);
        if(other instanceof GSCalc){
            handleCalc((GSCalc) other);
        }
        else if(other instanceof GSJoin){
            handleJoin((GSJoin) other);
        }
        else {
            throw new UnsupportedOperationException("RelNode of type " + other.getClass().getName() + " are not supported yet");
        }
        return result;
    }

    private void handleJoin(GSJoin join) {
        RexCall rexCall = (RexCall) join.getCondition();
        if(rexCall.getKind() != SqlKind.EQUALS){
            throw new UnsupportedOperationException("Only equi joins are supported");
        }
        String lColumn = join.getRowType().getFieldNames().get(((RexInputRef) rexCall.getOperands().get(0)).getIndex());
        String rColumn = join.getRowType().getFieldNames().get(((RexInputRef) rexCall.getOperands().get(1)).getIndex());
        TableContainer rightContainer = containerStack.pop();
        TableContainer leftContainer = containerStack.pop();
        QueryColumn rightColumn = rightContainer.addQueryColumn(rColumn, null, false);
        QueryColumn leftColumn = leftContainer.addQueryColumn(lColumn, null, false);
        rightContainer.setJoinInfo(new JoinInfo(leftColumn, rightColumn, JoinInfo.JoinType.getType(join.getJoinType())));
        if (leftContainer.getJoinedTable() == null) {
            if (!rightContainer.isJoined()) {
                leftContainer.setJoinedTable(rightContainer);
                rightContainer.setJoined(true);
            }
        }
        if(parent == null) {
            queryExecutor.getQueryColumns().addAll(leftContainer.getVisibleColumns());
            queryExecutor.getQueryColumns().addAll(rightContainer.getVisibleColumns());
        }
        containerStack.push(leftContainer);
        containerStack.push(rightContainer);
    }

    private void handleCalc(GSCalc other) {
        if(other.getInput() instanceof GSTableScan) {
            TableContainer tableContainer = containerStack.pop();
            RexProgram program = other.getProgram();
            List<String> inputFields = program.getInputRowType().getFieldNames();
            List<String> outputFields = program.getOutputRowType().getFieldNames();
            for (int i = 0; i < outputFields.size(); i++) {
                String alias = outputFields.get(i);
                String originalName = inputFields.get(program.getSourceField(i));
                QueryColumn qc = tableContainer.addQueryColumn(originalName, alias, true);
                queryExecutor.getQueryColumns().add(qc);
            }
            ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
            if (program.getCondition() != null) {
                program.getCondition().accept(conditionHandler);
                for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                    tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
                }
            }
            containerStack.push(tableContainer);
        }
    }
}
