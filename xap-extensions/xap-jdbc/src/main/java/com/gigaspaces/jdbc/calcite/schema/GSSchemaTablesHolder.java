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
package com.gigaspaces.jdbc.calcite.schema;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.HashMap;
import java.util.Map;

import static com.gigaspaces.jdbc.calcite.schema.GSSchemaTable.SchemaProperty;

public class GSSchemaTablesHolder {
    private static final Map<String, GSSchemaTable> schemaTables = new HashMap<>();

    static {
        for (PGSystemTable pgSystemTable : PGSystemTable.values()) {
            add(new GSSchemaTable(pgSystemTable));
        }
    }

    private static void add(GSSchemaTable gsSchemaTable) {
        schemaTables.put(gsSchemaTable.getName(), gsSchemaTable);
    }

    public static GSSchemaTable getTable(String name) {
        if (name.startsWith("pg_catalog."))
            name = name.substring(("pg_catalog.").length());
        return schemaTables.get(name);
    }
}
