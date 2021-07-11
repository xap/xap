/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;
import com.gigaspaces.sql.aggregatornode.netty.utils.PgType;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
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
        TypeUtils.writeColumn(session, dst, value, this);
    }
}
