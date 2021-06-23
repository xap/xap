package com.gigaspaces.sql.aggregatornode.netty.query;

public class StatementDescription {
    public static final StatementDescription EMPTY =
            new StatementDescription(ParametersDescription.EMPTY, RowDescription.EMPTY);

    private final ParametersDescription parametersDescription;
    private final RowDescription rowDescription;

    public StatementDescription(ParametersDescription parametersDescription, RowDescription rowDescription) {
        this.parametersDescription = parametersDescription;
        this.rowDescription = rowDescription;
    }

    public ParametersDescription getParametersDescription() {
        return this.parametersDescription;
    }

    public RowDescription getRowDescription() {
        return this.rowDescription;
    }
}
