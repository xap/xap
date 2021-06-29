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

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final String EMPTY_STRING = "";

    public static final char DELIMITER = ',';

    private Constants() {}
}
