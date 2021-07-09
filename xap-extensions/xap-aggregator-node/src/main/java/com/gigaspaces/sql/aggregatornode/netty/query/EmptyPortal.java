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

import com.gigaspaces.sql.aggregatornode.netty.utils.Constants;

import java.util.NoSuchElementException;

class EmptyPortal<T> implements Portal<T> {
    private final QueryProviderImpl queryProvider;
    private final String name;
    private final Statement statement;

    EmptyPortal(QueryProviderImpl queryProvider, String name, Statement statement) {
        this.queryProvider = queryProvider;
        this.name = name;
        this.statement = statement;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public void close() {
        queryProvider.closeP(name);
    }

    @Override
    public boolean empty() {
        return true;
    }

    @Override
    public RowDescription getDescription() {
        return RowDescription.EMPTY;
    }

    @Override
    public String tag() {
        return Constants.EMPTY_STRING;
    }

    @Override
    public void execute() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }
}
