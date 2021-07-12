/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

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
            tableContainer = new ConcreteTableContainer(gsTable.getName(), gsTable.getShortName(), queryExecutor.getSpace());
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
            childToCalc.remove(scan); // visited, not needed anymore
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
        if( other instanceof GSValues ){
            GSValues gsValues = (GSValues) other;
            handleValues(gsValues);
        }

        if(other instanceof GSSort){
            handleSort((GSSort) other);
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
                    FunctionCallColumn functionCallColumn = getFunctionCallColumn(program, (RexCall) node);
                    queryExecutor.addColumn(functionCallColumn);
                }
            }
        }
    }

    private FunctionCallColumn getFunctionCallColumn(RexProgram program, RexCall rexCall) {
        SqlOperator sqlFunction = rexCall.op;
        List<IQueryColumn> params = new ArrayList<>();
        for (RexNode operand : rexCall.getOperands()) {
            if (operand.isA(SqlKind.LOCAL_REF)) {
                RexNode funcArgument = program.getExprList().get(((RexLocalRef) operand).getIndex());
                if (funcArgument.isA(SqlKind.LITERAL)) {
                    RexLiteral literal = (RexLiteral) funcArgument;
                    params.add(new LiteralColumn(CalciteUtils.getValue(literal)));
                } else if (funcArgument instanceof RexCall) { //operator
                    RexCall function= (RexCall) funcArgument;
                    params.add(getFunctionCallColumn(program, function));
                }
            }

        }
        return new FunctionCallColumn(params, sqlFunction.getName(), null, null, true, -1);
    }

    private void handleSort(GSSort sort) {
        int columnCounter = 0;
        for (RelFieldCollation relCollation : sort.getCollation().getFieldCollations()) {
            int fieldIndex = relCollation.getFieldIndex();
            RelFieldCollation.Direction direction = relCollation.getDirection();
            RelFieldCollation.NullDirection nullDirection = relCollation.nullDirection;
            String columnAlias = sort.getRowType().getFieldNames().get(fieldIndex);
//            TableContainer table = queryExecutor.getTableByColumnIndex(fieldIndex);
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
            throw new UnsupportedOperationException("Only equal joins are supported");
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
                if (join.isSemiJoin()) {
                    queryExecutor.getVisibleColumns().addAll(leftContainer.getVisibleColumns());
                } else {
                    for (TableContainer tableContainer : queryExecutor.getTables()) {
                        queryExecutor.getVisibleColumns().addAll(tableContainer.getVisibleColumns());
                    }
                }
            }
        }
        else{
            handleCalcFromJoin(childToCalc.get(join));
            childToCalc.remove(join); // visited, not needed anymore
        }
    }

    private void handleCalc(GSCalc other, TableContainer tableContainer) {
        RexProgram program = other.getProgram();
        List<String> inputFields = program.getInputRowType().getFieldNames();
        List<String> outputFields = program.getOutputRowType().getFieldNames();
        queryExecutor.addFieldCount(outputFields.size());
        new SingleTableProjectionHandler(program, tableContainer, other.equals(root)).project();
        ConditionHandler conditionHandler = new ConditionHandler(program, queryExecutor, inputFields, tableContainer);
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
