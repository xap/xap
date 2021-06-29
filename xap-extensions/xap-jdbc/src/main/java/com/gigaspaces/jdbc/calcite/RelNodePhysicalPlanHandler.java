package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.PhysicalPlanHandler;
import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public class RelNodePhysicalPlanHandler implements PhysicalPlanHandler<GSRelNode> {
    private final QueryExecutor queryExecutor;

    public RelNodePhysicalPlanHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public QueryExecutor prepareForExecution(GSRelNode relNode) {
        relNode.accept(new RelShuttleImpl() {
            private final Deque<Object> stack = new ArrayDeque<>();
            @Override
            public RelNode visit(TableScan scan) {
                System.out.println(">>> SCAN " + scan);
                // TODO: Extract type and column info, put to stack.
                RelOptTable relOptTable = scan.getTable();
                Object table = relOptTable.unwrap(GSTable.class);
                stack.push(table);
                return scan;
            }

            @Override
            public RelNode visit(RelNode other) {
                RelNode res = super.visit(other);
                System.out.println(">>> OTHER " + other);
                if (other instanceof GSCalc) {
                    GSCalc calc = (GSCalc) other;
                    Object pop = stack.pop();
                    TableContainer tableContainer;
                    if (pop instanceof GSTable) {
                        tableContainer = ((GSTable) pop).createTableContainer(queryExecutor.getSpace());
                    } else {
                        throw new UnsupportedOperationException("Got unsupported table type: " + pop);
                    }
                    queryExecutor.getTables().add(tableContainer);
                    RexProgram program = calc.getProgram();
                    List<String> inputFields = program.getInputRowType().getFieldNames();
                    List<String> outputFields = program.getOutputRowType().getFieldNames();
                    for (int i = 0; i < outputFields.size(); i++) {
                        String alias = outputFields.get(i);
                        String originalName = inputFields.get(program.getSourceField(i));
                        tableContainer.addQueryColumn(originalName, alias, true);
                    }
                    RexHandler rexHandler = new RexHandler(program);
                    for (RexNode expr : program.getExprList()) {
                        expr.accept(rexHandler);
                    }
                    ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, rexHandler.getFields());
                    if (program.getCondition() != null) {
                        program.getCondition().accept(conditionHandler);
                        for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                            tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
                        }
                    }

                }
                return res;
            }
        });

        return queryExecutor;
    }
}
