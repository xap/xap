package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.jdbc.exceptions.TypeNotFoundException;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;

import java.sql.SQLException;
import java.util.*;

public class GSSchema implements Schema {

    private final IJSpace space;
    private final Map<String, GSTable> tableMap = new HashMap<>();

    public GSSchema(IJSpace space) {
        this.space = space;
    }

    @Override
    public Table getTable(String name) {
        GSTable table = tableMap.get(name);
        if (table == null) {
            try {
                ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
                table = new GSTable(typeDesc);
                tableMap.put(name, table);
            } catch (SQLException e) {
                throw new TypeNotFoundException("Unknown table [" + name + "]", e);
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
