package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;

public class ExplainPlanTableRow extends TableRow {
    public ExplainPlanTableRow(QueryColumn[] columns, Object[] values) {
        super(columns, values);
    }
}
