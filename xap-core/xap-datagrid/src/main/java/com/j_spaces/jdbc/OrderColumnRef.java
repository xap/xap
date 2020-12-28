package com.j_spaces.jdbc;

public class OrderColumnRef extends OrderColumn {
    private final Integer refIndex;
    public OrderColumnRef(Integer refIndex) {
        super(null, null);
        this.refIndex = refIndex;
    }

    public Integer getRefIndex() {
        return refIndex;
    }
}
