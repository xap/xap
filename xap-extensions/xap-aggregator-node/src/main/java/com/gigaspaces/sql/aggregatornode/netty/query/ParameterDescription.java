package com.gigaspaces.sql.aggregatornode.netty.query;

public class ParameterDescription {
    private final int type;

    public ParameterDescription(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
