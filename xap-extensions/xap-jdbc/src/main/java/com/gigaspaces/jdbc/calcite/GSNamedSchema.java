package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.schema.Schema;

public interface GSNamedSchema extends Schema {
    String getName();
    boolean isDefault();
}
