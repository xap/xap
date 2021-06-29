package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.rel.type.RelProtoDataType;

import java.util.Set;

public interface GSTypeResolver {
    Set<String> registeredTypeNames();
    RelProtoDataType resolveType(String name);
}
