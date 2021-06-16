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
package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.jdbc.calcite.schema.GSSchemaTablesHolder;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;

import java.util.*;

public class GSSchema implements Schema {

    private final IJSpace space;
    private final Map<String, GSTable> tableMap = new HashMap<>();

    public GSSchema(IJSpace space) {
        this.space = space;
    }

    @Override
    public Table getTable(String name) {
        //TODO this is temporary, should have another Schema class specifically for metadata
        if (name.startsWith("pg_")) {
            return GSSchemaTablesHolder.getTable(name);
        }

        GSTable table = tableMap.get(name);
        if (table == null) {
            try {
                ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
                table = new GSTable(name, typeDesc);
                tableMap.put(name, table);
            } catch (Exception e) {
                return null;
            }
        }
        return table;
    }

    @Override
    public Set<String> getTableNames() {
        return tableMap.keySet();
    }

    @Override
    public RelProtoDataType getType(String name) {
        return null;
    }

    @Override
    public Set<String> getTypeNames() {
        return Collections.emptySet();
    }

    @Override
    public Collection<Function> getFunctions(String name) {
        return Collections.emptySet();
    }

    @Override
    public Set<String> getFunctionNames() {
        return Collections.emptySet();
    }

    @Override
    public Schema getSubSchema(String name) {
        return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
        return Collections.emptySet();
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return null;
    }

    @Override
    public boolean isMutable() {
        return false;
    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        return this;
    }
}
