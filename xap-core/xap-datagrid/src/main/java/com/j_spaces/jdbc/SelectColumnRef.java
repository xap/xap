package com.j_spaces.jdbc;

public class SelectColumnRef extends SelectColumn {
    private final Integer refIndex;
    public SelectColumnRef(Integer refIndex) {
        this.refIndex = refIndex;
    }

    public Integer getRefIndex() {
        return refIndex;
    }
}
