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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public enum QueryOperator {
    EQ("="),
    NE("!="),
    GT(">"),
    GE(">="),
    LT("<"),
    LE("<="),
    IS_NULL("IS NULL"),
    NOT_NULL("IS NOT NULL"),
    REGEX("REGEX"),
    CONTAINS_TOKEN("CONTAINS_TOKEN"),
    NOT_REGEX("NOT_REGEX"),
    IN("IN"),
    RELATION("RELATION"),
    INTERSECTS("INTERSECTS"),
    WITHIN("WITHIN"),
    NOT_SUPPORTED("NOT_SUPPORTED"),
    BETWEEN("=");

    private final String operatorString;

    QueryOperator(String operatorString) {
        this.operatorString = operatorString;

    }

    public String getOperatorString() {
        return operatorString;
    }
}

