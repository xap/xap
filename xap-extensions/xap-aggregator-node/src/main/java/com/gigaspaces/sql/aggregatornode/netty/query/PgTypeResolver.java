package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.jdbc.calcite.GSTypeResolver;
import com.gigaspaces.sql.aggregatornode.netty.utils.TypeUtils;
import org.apache.calcite.rel.type.RelProtoDataType;

import java.util.Set;

public class PgTypeResolver implements GSTypeResolver {
    public static final GSTypeResolver INSTANCE = new PgTypeResolver();

    private PgTypeResolver() {}

    @Override
    public Set<String> registeredTypeNames() {
        return TypeUtils.typeNames();
    }

    @Override
    public RelProtoDataType resolveType(String name) {
        return TypeUtils.resolveType(name);
    }
}
