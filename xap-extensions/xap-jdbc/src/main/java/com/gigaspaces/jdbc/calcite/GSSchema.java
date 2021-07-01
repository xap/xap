package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.schema.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class GSSchema extends GSAbstractSchema {

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
}
