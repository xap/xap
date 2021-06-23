package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

public class GSOptimizerValidationResult {
    private final SqlNode validatedAst;
    private final RelDataType rowType;
    private final RelDataType parameterRowType;

    public GSOptimizerValidationResult(SqlNode validatedAst, RelDataType rowType, RelDataType parameterRowType) {
        this.validatedAst = validatedAst;
        this.rowType = rowType;
        this.parameterRowType = parameterRowType;
    }

    public SqlNode getValidatedAst() {
        return validatedAst;
    }

    public RelDataType getRowType() {
        return rowType;
    }

    public RelDataType getParameterRowType() {
        return parameterRowType;
    }
}
