package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;
import io.netty.buffer.ByteBuf;

public class ColumnDescription extends TypeAware {
    private final String name;
    private final int typeLen;
    private final int typeModifier;
    private final int format;
    private final int tableId;
    private final int tableIndex;

    public ColumnDescription(String name, PgType type) {
        this(name, type, type.getLength(), -1, 0, 0, 0);
    }

    public ColumnDescription(String name, PgType type, int typeLen, int typeModifier, int format, int tableId, int tableIndex) {
        super(type);
        this.name = name;
        this.typeLen = typeLen;
        this.typeModifier = typeModifier;
        this.format = format;
        this.tableId = tableId;
        this.tableIndex = tableIndex;
    }

    public String getName() {
        return name;
    }

    public int getTypeLen() {
        return typeLen;
    }

    public int getTypeModifier() {
        return typeModifier;
    }

    public int getFormat() {
        return format;
    }

    public int getTableId() {
        return tableId;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public void write(Session session, ByteBuf dst, Object value) throws ProtocolException {
        PgType.writeColumn(session, dst, value, this);
    }
}
