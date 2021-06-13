package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.Convention;

public class GSConvention extends Convention.Impl {

    public static final GSConvention INSTANCE = new GSConvention();

    private GSConvention() {
        super("GIGASPACES", GSRelNode.class);
    }
}
