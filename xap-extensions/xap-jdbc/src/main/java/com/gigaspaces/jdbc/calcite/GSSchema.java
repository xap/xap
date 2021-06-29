package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.*;

import java.util.*;

public class GSSchema implements Schema {

    private final IJSpace space;
    private final GSTypeResolver typeResolver;
    private final Map<String, GSTableImpl> tableMap = new HashMap<>();

    public GSSchema(IJSpace space, GSTypeResolver typeResolver) {
        this.space = space;
        this.typeResolver = typeResolver;
    }

    @Override
    public Table getTable(String name) {
        GSTableImpl table = tableMap.get(name);
        if (table == null) {
            try {
                ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
                table = new GSTableImpl(name, typeDesc);
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
        return typeResolver == null ? null : typeResolver.resolveType(name);
    }

    @Override
    public Set<String> getTypeNames() {
        return typeResolver == null ? Collections.emptySet() : typeResolver.registeredTypeNames();
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
