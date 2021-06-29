package com.gigaspaces.jdbc.calcite.schema.type;

public class PgType {
    protected final int id;
    protected final String name;
    protected final int length;
    protected final int arrayType;
    protected final int elementType;

    protected PgType(int id, String name, int length, int arrayType, int elementType) {
        this.id = id;
        this.name = name;
        this.length = length;
        this.arrayType = arrayType;
        this.elementType = elementType;
    }

    public final int getId() {
        return id;
    }

    public final String getName() {
        return name;
    }

    public final int getLength() {
        return length;
    }

    public int getElementType() {
        return elementType;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PgType pgType = (PgType) o;

        return id == pgType.id;
    }

    @Override
    public final int hashCode() {
        return id;
    }

}
