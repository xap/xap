package com.gigaspaces.jdbc.calcite.experimental.result;


import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;

public class ExplainPlanTableRow extends TableRow {
    public ExplainPlanTableRow(IQueryColumn[] columns, Object[] values) {
        super(columns, values);
    }
}
