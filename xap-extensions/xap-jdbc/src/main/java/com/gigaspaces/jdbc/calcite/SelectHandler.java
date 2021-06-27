package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelectHandler extends RelShuttleImpl {
    private final QueryExecutor queryExecutor;
    private final Map<RelNode, GSCalc> childToCalc = new HashMap<>();
    private RelNode root = null;
    private boolean isAllColumnSelected = false;
    private int columnOrdinalCounter = 0;

    public SelectHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    // TODO check inserting of same table
    public RelNode visit(TableScan scan) {
        RelNode result = super.visit(scan);
        GSTable table = scan.getTable().unwrap(GSTable.class);
        TableContainer tableContainer = new ConcreteTableContainer(table.getTypeDesc().getTypeName(), null, queryExecutor.getSpace());
        queryExecutor.getTables().add(tableContainer);
        if (!childToCalc.containsKey(scan)) {
            //TODO: @sagiv arrive here only if has Select *.
            this.isAllColumnSelected = true;
            List<String> columns = tableContainer.getAllColumnNames();
            queryExecutor.addFieldCount(columns.size());
            for (String col : columns) {
                tableContainer.addQueryColumn(col, null, true, 0);//TODO: @sagiv columnOrdinal
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
            RelNode input = calc.getInput();
            while (!(input instanceof GSJoin)
                    && !(input instanceof GSTableScan)) {
                if(input.getInputs().isEmpty()) {
                    break;
                }
                input =  input.getInput(0);
            }
            childToCalc.putIfAbsent(input, calc);
        }
        RelNode result = super.visit(other);
        if(other instanceof GSJoin){
            handleJoin((GSJoin) other);
        }
        if(other instanceof GSSort){
            handleSort((GSSort) other);
        }
//        else {
//            throw new UnsupportedOperationException("RelNode of type " + other.getClass().getName() + " are not supported yet");
//        }
        return result;
    }

    private void handleSort(GSSort sort) {
        int columnCounter = 0;
        for (RelFieldCollation relCollation : sort.getCollation().getFieldCollations()) {
            int fieldIndex = relCollation.getFieldIndex();
            RelFieldCollation.Direction direction = relCollation.getDirection();
            RelFieldCollation.NullDirection nullDirection = relCollation.nullDirection;
            String columnAlias = sort.getRowType().getFieldNames().get(fieldIndex);
//            TableContainer table = queryExecutor.getTableByColumnIndex(fieldIndex);
//            table.addQueryColumn(columnName, null, false, -1);
            String columnName = columnAlias;
            boolean isVisible = false;
            RelNode parent = this.stack.peek();
            if(parent instanceof GSCalc) {
                RexProgram program = ((GSCalc) parent).getProgram();
                RelDataTypeField field = program.getOutputRowType().getField(columnAlias, true, false);
                if(field != null) {
                    isVisible = true;
                    columnName = program.getInputRowType().getFieldNames().get(program.getSourceField(field.getIndex()));
                }
            }
            //TODO: @sagiv not so sure about this, because 'columnName' can be alias from sub-query, or
            // Ambiguous when using join for example..
            TableContainer table = getTableByColumnName(columnName);
            OrderColumn orderColumn = new OrderColumn(new ConcreteColumn(columnName,null, columnAlias,
                    isVisible, table, columnCounter++), !direction.isDescending(),
                    nullDirection == RelFieldCollation.NullDirection.LAST);
            table.addOrderColumns(orderColumn);
        }
    }

    private TableContainer getTableByColumnName(String name) {
        TableContainer toReturn = null;
        for(TableContainer tableContainer : this.queryExecutor.getTables()) {
            if(tableContainer.hasColumn(name)) {
                if (toReturn == null) {
                    toReturn = tableContainer;
                } else {
                    throw new IllegalArgumentException("Ambiguous column name [" + name + "]");
                }
            }
        }
        return toReturn;
    }


    private void handleJoin(GSJoin join) {
        RexCall rexCall = (RexCall) join.getCondition();
        if(rexCall.getKind() != SqlKind.EQUALS){
            throw new UnsupportedOperationException("Only equi joins are supported");
        }
        int left = join.getLeft().getRowType().getFieldCount();
        int leftIndex = ((RexInputRef) rexCall.getOperands().get(0)).getIndex();
        int rightIndex = ((RexInputRef) rexCall.getOperands().get(1)).getIndex();
        String lColumn = join.getLeft().getRowType().getFieldNames().get(leftIndex);
        String rColumn = join.getRight().getRowType().getFieldNames().get(rightIndex - left);
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
        if(!childToCalc.containsKey(join)) { // it is SELECT *
            if(join.equals(root)
                    || ((root instanceof GSSort) && ((GSSort) root).getInput().equals(join))) { // root is GSSort and its child is join
                for (TableContainer tableContainer : queryExecutor.getTables()) {
                    queryExecutor.getVisibleColumns().addAll(tableContainer.getVisibleColumns());
                }
            }
        }
        else{
            handleCalcFromJoin(childToCalc.get(join));
        }
    }

    private void handleCalc(GSCalc other, TableContainer tableContainer) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        queryExecutor.addFieldCount(outputFields.size());
        for (int i = 0; i < outputFields.size(); i++) {
            String alias = outputFields.get(i);
            String originalName = inputFields.get(program.getSourceField(i));
            tableContainer.addQueryColumn(originalName, alias, true, columnOrdinalCounter++);
        }
        ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields);
        if (program.getCondition() != null) {
            program.getCondition().accept(conditionHandler);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : conditionHandler.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }
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
}
