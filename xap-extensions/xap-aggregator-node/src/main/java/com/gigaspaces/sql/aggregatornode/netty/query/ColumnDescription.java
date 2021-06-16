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
