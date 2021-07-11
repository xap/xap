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
package com.gigaspaces.jdbc.calcite.pg;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

public class PgTypeDescriptor {
    public static final PgTypeDescriptor ANY = new PgTypeDescriptor(2276, "any", 4, 0, 0);
    public static final PgTypeDescriptor BOOL = new PgTypeDescriptor(16, "bool", 1, 1000, 0);
    public static final PgTypeDescriptor BPCHAR = new PgTypeDescriptor(1042, "bpchar", -1, 1014, 0);
    public static final PgTypeDescriptor BYTEA = new PgTypeDescriptor(17, "bytea", -1, 1001, 0);
    public static final PgTypeDescriptor CHAR = new PgTypeDescriptor(18, "char", 1, 1002, 0);
    public static final PgTypeDescriptor CURSOR = new PgTypeDescriptor(1790, "refcursor", -1, 2201, 0);
    public static final PgTypeDescriptor DATE = new PgTypeDescriptor(1082, "date", 4, 1182, 0);
    public static final PgTypeDescriptor FLOAT4 = new PgTypeDescriptor(700, "float4", 4, 1021, 0);
    public static final PgTypeDescriptor FLOAT8 = new PgTypeDescriptor(701, "float8", 8, 1022, 0);
    public static final PgTypeDescriptor INT2 = new PgTypeDescriptor(21, "int2", 2, 1005, 0);
    public static final PgTypeDescriptor INT2VECTOR = new PgTypeDescriptor(22, "int2vector", -1, 1006, INT2.id);
    public static final PgTypeDescriptor INT4 = new PgTypeDescriptor(23, "int4", 4, 1007, 0);
    public static final PgTypeDescriptor INT8 = new PgTypeDescriptor(20, "int8", 8, 1016, 0);
    public static final PgTypeDescriptor INTERVAL = new PgTypeDescriptor(1186, "interval", 16, 1187, 0);
    public static final PgTypeDescriptor NAME = new PgTypeDescriptor(19, "name", 63, 1003, 0);
    public static final PgTypeDescriptor NODE_TREE = new PgTypeDescriptor(194, "pg_node_tree", -1, 0, 0);
    public static final PgTypeDescriptor NUMERIC = new PgTypeDescriptor(1700, "numeric", -1, 1231, 0);
    public static final PgTypeDescriptor TEXT = new PgTypeDescriptor(25, "text", -1, 1009, 0);
    public static final PgTypeDescriptor OID = new PgTypeDescriptor(26, "oid", 4, 1028, 0);
    public static final PgTypeDescriptor OID_VECTOR = new PgTypeDescriptor(30, "oidvector", -1, 0, OID.id);
    public static final PgTypeDescriptor REGPROC = new PgTypeDescriptor(24, "regproc", 4, 1008, 0);
    public static final PgTypeDescriptor TIME = new PgTypeDescriptor(1083, "time", 8, 1183, 0);
    public static final PgTypeDescriptor TIME_WITH_TIME_ZONE = new PgTypeDescriptor(1266, "timetz", 12, 1270, 0);
    public static final PgTypeDescriptor TIMESTAMP = new PgTypeDescriptor(1114, "timestamp", 8, 1115, 0);
    public static final PgTypeDescriptor TIMESTAMP_WITH_TIME_ZONE = new PgTypeDescriptor(1184, "timestamptz", 8, 1185, 0);
    public static final PgTypeDescriptor VARCHAR = new PgTypeDescriptor(1043, "varchar", -1, 1015, 0);
    public static final PgTypeDescriptor UNKNOWN = new PgTypeDescriptor(705, "unknown", -2, 0, 0);

    public static final List<PgTypeDescriptor> ALL_DESCRIPTORS;

    protected final int id;
    protected final String name;
    protected final int length;
    protected final int arrayType;
    protected final int elementType;

    static {
        ALL_DESCRIPTORS = new ArrayList<>();

        try {
            for (Field field : PgTypeDescriptor.class.getDeclaredFields()) {
                if (field.getType() != PgTypeDescriptor.class) {
                    continue;
                }

                int modifiers = field.getModifiers();
                if (!Modifier.isPublic(modifiers) || !Modifier.isStatic(modifiers)) {
                    continue;
                }

                PgTypeDescriptor descriptor = (PgTypeDescriptor) field.get(null);
                ALL_DESCRIPTORS.add(descriptor);
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to collect defined descriptors.", e);
        }
    }

    public PgTypeDescriptor(int id, String name, int length, int arrayType, int elementType) {
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

    public int getArrayType() {
        return arrayType;
    }

    public int getElementType() {
        return elementType;
    }

    public PgTypeDescriptor asArray() {
        return new PgTypeDescriptor(arrayType, name + "[]", -1, 0, id);
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PgTypeDescriptor pgType = (PgTypeDescriptor) o;

        return id == pgType.id;
    }

    @Override
    public final int hashCode() {
        return id;
    }
}
