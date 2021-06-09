package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.IQueryColumn;

public class ExplainPlanTableRow extends TableRow {
    public ExplainPlanTableRow(IQueryColumn[] columns, Object[] values) {
        super(columns, values);
    }
}
