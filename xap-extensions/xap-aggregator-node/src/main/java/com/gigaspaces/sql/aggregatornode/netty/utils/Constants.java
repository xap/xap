package com.gigaspaces.sql.aggregatornode.netty.utils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.TimeZone;

public class Constants {
    public static final int BINARY = 1;
    public static final int TEXT = 0;

    public static final String DEFAULT_DATE_STYLE = "ISO, MDY";
    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    public static final int SSL_REQUEST = 80877103;
    public static final int CANCEL_REQUEST = 80877102;

    public static final int PROTOCOL_3_0 = 196608;

    public static final int BATCH_SIZE = 1000;

    public static final int PG_TYPE_BOOL = 16;
    public static final int PG_TYPE_CHAR = 18;
    public static final int PG_TYPE_BPCHAR = 1042;
    public static final int PG_TYPE_INT8 = 20;
    public static final int PG_TYPE_INT2 = 21;
    public static final int PG_TYPE_INT4 = 23;
    public static final int PG_TYPE_TEXT = 25;
    public static final int PG_TYPE_VARCHAR = 1043;
    public static final int PG_TYPE_BYTE_ARRAY = 17;
    public static final int PG_TYPE_FLOAT4 = 700;
    public static final int PG_TYPE_FLOAT8 = 701;
    public static final int PG_TYPE_NUMERIC = 1700;
    public static final int PG_TYPE_UNKNOWN = 705;
    public static final int PG_TYPE_DATE = 1082;
    public static final int PG_TYPE_TIME = 1083;
    public static final int PG_TYPE_TIMETZ = 1266;
    public static final int PG_TYPE_TIMESTAMP = 1114;
    public static final int PG_TYPE_TIMESTAMPTZ = 1184;
    public static final int PG_TYPE_INTERVAL = 1186;
    public static final int PG_TYPE_ANY = 2276;
    public static final int PG_TYPE_CURSOR = 1790;

    private Constants() {}
}
