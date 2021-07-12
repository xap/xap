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
package com.gigaspaces.internal.server.space.tiered_storage;

public class SingleQueryParameter {
    final private String column;
    final private Object value;
    final private Class<?> type;

    public SingleQueryParameter(String column, Object value, Class<?> type) {
        this.column = column;
        this.value = value;
        this.type = type;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public Class<?> getType() {
        return type;
    }
}