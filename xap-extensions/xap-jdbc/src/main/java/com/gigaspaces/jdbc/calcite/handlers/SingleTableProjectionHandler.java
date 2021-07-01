package com.gigaspaces.jdbc.calcite.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.table.FunctionCallColumn;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.LiteralColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

public class SingleTableProjectionHandler extends RexShuttle {
    private final RexProgram program;
    private final TableContainer tableContainer;
    private final List<String> inputFields;
    private final List<String> outputFields;
    private final boolean isRoot;

    public SingleTableProjectionHandler(RexProgram program, TableContainer tableContainer, boolean isRoot) {
        this.program = program;
        this.tableContainer = tableContainer;
        this.inputFields = program.getInputRowType().getFieldNames();
        this.outputFields = program.getOutputRowType().getFieldNames();
        this.isRoot = isRoot;
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
                switch (call.getKind()) {
                    case OTHER_FUNCTION:
                        SqlFunction sqlFunction = (SqlFunction) call.op;
                        List<IQueryColumn> queryColumns = new ArrayList<>();
                        for (RexNode operand : call.getOperands()) {
                            if(operand.isA(SqlKind.LOCAL_REF)){
                                RexNode rexNode = program.getExprList().get(((RexLocalRef) operand).getIndex());
                                if(rexNode.isA(SqlKind.INPUT_REF)){
                                    RexInputRef rexInputRef = (RexInputRef) rexNode;
                                    String column = inputFields.get(rexInputRef.getIndex());
                                    queryColumns.add(tableContainer.addQueryColumn(column, null, false, -1));
                                }
                                if(rexNode.isA(SqlKind.LITERAL)){
                                    RexLiteral literal = (RexLiteral) rexNode;
                                    queryColumns.add(new LiteralColumn(CalciteUtils.getValue(literal)));
                                }
                            }

                        }
                        IQueryColumn functionCallColumn = new FunctionCallColumn(queryColumns, sqlFunction.toString(), sqlFunction.getName(), null, isRoot, -1);
                        if(isRoot)
                            tableContainer.getVisibleColumns().add(functionCallColumn);
                        else
                            tableContainer.getInvisibleColumns().add(functionCallColumn);
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

    public boolean isRoot(){
        return isRoot;
    }
}
