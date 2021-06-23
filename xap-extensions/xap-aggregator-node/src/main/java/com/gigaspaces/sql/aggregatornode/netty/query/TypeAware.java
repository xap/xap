package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;

class TypeAware {
    protected final PgType type;

    protected TypeAware(PgType type) {
        this.type = type;
    }

    public int getTypeId() {
        return type.getId();
    }

    public PgType getType() {
        return type;
    }
}
