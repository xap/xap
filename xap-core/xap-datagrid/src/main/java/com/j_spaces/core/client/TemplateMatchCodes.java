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


package com.j_spaces.core.client;

/**
 * @author Yechiel Fefer
 * @version 4.0
 * @deprecated Since 8.0 - Use {@link SQLQuery} instead.
 */
@Deprecated

public class TemplateMatchCodes {
    /**
     * equal operation
     */
    public static final short EQ = 0;
    /**
     * not equal operation
     */
    public static final short NE = 1;
    /**
     * greater than operation
     */
    public static final short GT = 2;
    /**
     * grater-equal operation
     */
    public static final short GE = 3;
    /**
     * less than operation
     */
    public static final short LT = 4;
    /**
     * less-equal operation
     */
    public static final short LE = 5;
    /**
     * entry field is null operation (template field not relevant)
     */
    public static final short IS_NULL = 6;
    /**
     * entry field is not null operation (template field not relevant)
     */
    public static final short NOT_NULL = 7;
    /**
     * regular-expression matching operation (of a string field)
     */
    public static final short REGEX = 8;
    /**
     * collection/array contains matching
     */
    public static final short CONTAINS_TOKEN = 9;
    /**
     * regular-expression NOT matching operation (of a string field)
     */
    public static final short NOT_REGEX = 10;

    /**
     * IN operation
     */
    public static final short IN = 11;

    /**
     * RELATION operation
     */
    public static final short RELATION = 12;

    public static boolean supportFifoOrder(short matchCode) {
        return matchCode == EQ || matchCode == IS_NULL;
    }
}