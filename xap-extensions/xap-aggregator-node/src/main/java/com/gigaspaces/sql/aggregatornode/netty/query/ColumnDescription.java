package com.gigaspaces.sql.aggregatornode.netty.query;

public class ColumnDescription {
    private final String name;
    private final int type;
    private final int typeLen;
    private final int typeModifier;
    private final int formatCode;
    private final int tableId;
    private final int tableIndex;

    public ColumnDescription(String name, int type) {
        this(name, type, -1, -1);
    }

    public ColumnDescription(String name, int type, int typeLen, int typeModifier) {
        this(name, type, typeLen, typeModifier, 0, 0, 0);
    }

    public ColumnDescription(String name, int type, int typeLen, int typeModifier, int formatCode) {
        this(name, type, typeLen, typeModifier, formatCode, 0, 0);
    }

    public ColumnDescription(String name, int type, int typeLen, int typeModifier, int formatCode, int tableId, int tableIndex) {
        this.name = name;
        this.type = type;
        this.typeLen = typeLen;
        this.typeModifier = typeModifier;
        this.formatCode = formatCode;
        this.tableId = tableId;
        this.tableIndex = tableIndex;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public int getTypeLen() {
        return typeLen;
    }

    public int getTypeModifier() {
        return typeModifier;
    }

    public int getFormatCode() {
        return formatCode;
    }

    public int getTableId() {
        return tableId;
    }

    public int getTableIndex() {
        return tableIndex;
    }
}
