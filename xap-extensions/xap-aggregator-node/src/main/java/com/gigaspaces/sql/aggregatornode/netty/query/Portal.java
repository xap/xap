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
package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;

import java.util.Iterator;

public interface Portal<T> extends Iterator<T>, AutoCloseable {
    /**
     * @return Portal name.
     */
    String name();

    /**
     * @return Bounded statement name.
     */
    Statement getStatement();

    /**
     * @return Row description.
     */
    RowDescription getDescription();

    /**
     * @return Operation tag.
     */
    String tag();

    /**
     * Executes portal operation
     */
    void execute() throws ProtocolException;

    /**
     * Indicates whether the portal is empty.
     */
    default boolean empty() {
        return false;
    }
}
