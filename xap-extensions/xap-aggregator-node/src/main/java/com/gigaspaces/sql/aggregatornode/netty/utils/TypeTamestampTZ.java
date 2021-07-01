package com.gigaspaces.sql.aggregatornode.netty.utils;

import com.gigaspaces.jdbc.calcite.pg.PgTypeDescriptor;

// TODO implement type encoder/decoder
public class TypeTamestampTZ extends PgType {
    public static final PgType INSTANCE = new TypeTamestampTZ();

    public TypeTamestampTZ() {
        super(PgTypeDescriptor.TIMESTAMP_WITH_TIME_ZONE);
    }
}
