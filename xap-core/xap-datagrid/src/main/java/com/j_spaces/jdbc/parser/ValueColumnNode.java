package com.j_spaces.jdbc.parser;

public class ValueColumnNode extends ColumnNode {
    private final Object value;

    public ValueColumnNode(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}
