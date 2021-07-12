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

import java.util.NoSuchElementException;

class DmlPortal<T> implements Portal<T> {
    private final QueryProviderImpl queryProvider;
    private final String name;
    private final Statement statement;
    private final PortalCommand command;
    private final ThrowingSupplier<Integer, ProtocolException> op;

    private Integer processed;

    DmlPortal(QueryProviderImpl queryProvider, String name, Statement statement, PortalCommand command, ThrowingSupplier<Integer, ProtocolException> op) {
        this.queryProvider = queryProvider;
        this.name = name;
        this.statement = statement;
        this.command = command;
        this.op = op;
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
    public RowDescription getDescription() {
        return RowDescription.EMPTY;
    }

    @Override
    public String tag() {
        if (command == PortalCommand.SET)
            return String.format("%s %d", command.tag(), processed);
        return String.format("%s 0 %d", command.tag(), processed);
    }

    @Override
    public void execute() throws ProtocolException {
        processed = op.apply();
    }

    @Override
    public void close() {
        queryProvider.closeP(name);
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
