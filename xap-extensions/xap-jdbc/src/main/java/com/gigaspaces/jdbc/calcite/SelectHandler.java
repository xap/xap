package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.ConcreteColumn;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;

import java.util.*;

public class SelectHandler extends RelShuttleImpl {
    protected final Stack<ISQLOperator> gstack = new Stack<>();
    private final Map<RelNode, GSCalc> childToCalc = new HashMap<>();
    private final QueryExecutor queryExecutor;
    private RelNode root = null;
    private int columnOrdinalCounter = 0;

    public SelectHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    // TODO check inserting of same table
    public RelNode visit(TableScan scan) {
        if (scan instanceof GSTableScan) {
            handleTableScan((GSTableScan) scan);
        }
        return super.visit(scan);
    }

    private void handleTableScan(GSTableScan tableScan) {
        GSTable table = tableScan.getTable().unwrap(GSTable.class);
        TableScanOperator tableScanOperator = new TableScanOperator(
                new QueryTemplatePacketsHolder(queryExecutor.getPreparedValues(), queryExecutor.getSpace()), table.getTypeDesc());
        gstack.push(tableScanOperator);
    }

    private void handleCalc(GSCalc calc) {
        RexProgram program = calc.getProgram();
        List<String> projection = new ArrayList<>();
        Map<String, String> columnNameToAliasMap = new HashMap<>();// TODO: @sagiv use later

        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        for (int i = 0; i < outputFields.size(); i++) {
            String columnAlias = outputFields.get(i);
            String columnOriginalName = inputFields.get(program.getSourceField(i));
            columnNameToAliasMap.put(columnOriginalName, columnAlias);
            projection.add(columnOriginalName);
        }
        ISQLOperator iSqlOperator = gstack.pop();
        QueryTemplatePacketsHolder qtpHolder = iSqlOperator.build();
        WhereHandler whereHandler = new WhereHandler(program, qtpHolder, program.getInputRowType().getFieldList());
        if (program.getCondition() != null) {
            program.getCondition().accept(whereHandler);
            Map<String, QueryTemplatePacket> qtpMap = whereHandler.getQTPMap();
            qtpMap.forEach((k, v) -> qtpHolder.addQueryTemplatePacket(v));
        }
        CalcOperator calcOperator = new CalcOperator(qtpHolder, projection);
        gstack.push(calcOperator);
    }

    @Override
    public RelNode visit(RelNode other) {
        if (root == null) {
            root = other;
        }
        RelNode result = super.visit(other);
//        if (other instanceof GSTableScan) {
//            handleTableScan((GSTableScan) other);
//        }
        if (other instanceof GSCalc) {
            handleCalc((GSCalc) other);
        }
        if (other instanceof GSJoin) {
            handleJoin((GSJoin) other);
        }
        if (other instanceof GSSort) {
            handleSort((GSSort) other);
        }
        return result;
    }


    private void handleSort(GSSort sort) {
        int columnCounter = 0;
        List<OrderColumn> orderColumns = new ArrayList<>();
        ISQLOperator iSqlOperator = gstack.pop();
        QueryTemplatePacketsHolder qtpHolder = iSqlOperator.build();
        for (RelFieldCollation relCollation : sort.getCollation().getFieldCollations()) {
            int fieldIndex = relCollation.getFieldIndex();
            RelFieldCollation.Direction direction = relCollation.getDirection();
            RelFieldCollation.NullDirection nullDirection = relCollation.nullDirection;
            String columnAlias = sort.getRowType().getFieldNames().get(fieldIndex);
//            TableContainer table = queryExecutor.getTableByColumnIndex(fieldIndex);
            String columnName = columnAlias;
            boolean isVisible = false;
//            RelNode parent = this.stack.peek(); //TODO: @sagiv needed?
//            if (parent instanceof GSCalc) {
//                RexProgram program = ((GSCalc) parent).getProgram();
//                RelDataTypeField field = program.getOutputRowType().getField(columnAlias, true, false);
//                if (field != null) {
//                    isVisible = true;
//                    columnName = program.getInputRowType().getFieldNames().get(program.getSourceField(field.getIndex()));
//                }
//            }
            //TODO: @sagiv not so sure about this, because 'columnName' can be alias from sub-query, or
            // Ambiguous when using join for example..
//            TableContainer table = getTableByColumnName(columnName);
            //TODO: @sagiv need to inject the table at the 'init' method in 'queryExecutor'!
            OrderColumn orderColumn = new OrderColumn(new ConcreteColumn(columnName, null, columnAlias,
                    isVisible, null, columnCounter++), !direction.isDescending(),
                    nullDirection == RelFieldCollation.NullDirection.LAST);
            orderColumns.add(orderColumn);
        }
        SortOperator sortOperator = new SortOperator(qtpHolder, orderColumns);
        gstack.push(sortOperator);
    }


    private void handleJoin(GSJoin join) {
        RexCall rexCall = (RexCall) join.getCondition();
        if (rexCall.getKind() != SqlKind.EQUALS) {
            throw new UnsupportedOperationException("Only equi joins are supported");
        }
        List<IQueryColumn> joinColumns = new ArrayList<>();
        ISQLOperator leftIsqlOperator = gstack.pop(); //TODO: validate first pop is left child?
        ISQLOperator rightIsqlOperator = gstack.pop();

        QueryTemplatePacketsHolder rightQtpHolder = rightIsqlOperator.build();
        QueryTemplatePacketsHolder leftQtpHolder = leftIsqlOperator.build();
        //TODO: @sagiv! we stop here!, need to continue from this point.... create JoinOperator.

        int left = join.getLeft().getRowType().getFieldCount();
        int leftIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
        int rightIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
        String lColumn = join.getLeft().getRowType().getFieldNames().get(leftIndex);
        String rColumn = join.getRight().getRowType().getFieldNames().get(rightIndex - left);
        joinColumns.add(new ConcreteColumn(rColumn, null, null, false, null, -1));
        joinColumns.add(new ConcreteColumn(lColumn, null, null, false, null, -1));

        TableContainer rightContainer = queryExecutor.getTableByColumnIndex(rightIndex);
        TableContainer leftContainer = queryExecutor.getTableByColumnIndex(leftIndex);
        //TODO: @sagiv needed?- its already in the tables.
        IQueryColumn rightColumn = rightContainer.addQueryColumn(rColumn, null, false, -1);
        IQueryColumn leftColumn = leftContainer.addQueryColumn(lColumn, null, false, -1);
        rightContainer.setJoinInfo(new JoinInfo(leftColumn, rightColumn, JoinInfo.JoinType.getType(join.getJoinType())));
        if (leftContainer.getJoinedTable() == null) {
            if (!rightContainer.isJoined()) {
                leftContainer.setJoinedTable(rightContainer);
                rightContainer.setJoined(true);
            }
        }
        if (!childToCalc.containsKey(join)) { // it is SELECT *
            if (join.equals(root)
                    || ((root instanceof GSSort) && ((GSSort) root).getInput().equals(join))) { // root is GSSort and its child is join
                for (TableContainer tableContainer : queryExecutor.getTables()) {
                    queryExecutor.getVisibleColumns().addAll(tableContainer.getVisibleColumns());
                }
            }
        } else {
            handleCalcFromJoin(childToCalc.get(join));
        }
    }

    private void handleCalcFromJoin(GSCalc other) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        for (int i = 0; i < outputFields.size(); i++) {
            IQueryColumn qc = queryExecutor.getColumnByColumnIndex(program.getSourceField(i));
            queryExecutor.addColumn(qc);
        }
        if (program.getCondition() != null) {
            ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }

    public Stack<ISQLOperator> getGStack() {
        return gstack;
    }
}
