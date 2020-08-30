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

package com.gigaspaces.query.extension;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.SpaceServerEntry;

import java.io.Closeable;
import java.io.IOException;

/**
 * handler of foreign indexes and query-executers
 *
 * @author yechielf
 * @since 11.0
 */
public abstract class QueryExtensionManager implements Closeable {

    protected QueryExtensionManager(QueryExtensionRuntimeInfo info) {
    }

    @Override
    public void close() throws IOException {
    }

    public void registerType(SpaceTypeDescriptor typeDescriptor) {
    }

    public abstract boolean accept(String typeName, String path, String operation, Object leftOperand, Object rightOperand);

    public abstract boolean insertEntry(SpaceServerEntry entry, boolean hasPrevious);

    public abstract void removeEntry(SpaceTypeDescriptor typeDescriptor, String uid, int version);

    public abstract QueryExtensionEntryIterator queryByIndex(String typeName, String path, String operation, Object operand);
}
