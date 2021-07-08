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
