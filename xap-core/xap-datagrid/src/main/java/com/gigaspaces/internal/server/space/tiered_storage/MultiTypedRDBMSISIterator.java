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

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

public class MultiTypedRDBMSISIterator implements ISAdapterIterator<IEntryHolder> {

    private final InternalRDBMSManager internalRDBMS;
    private final Context context;
    private final IServerTypeDesc[] types;
    private final ITemplateHolder templateHolder;
    private ISAdapterIterator<IEntryHolder> currentTypeIterator;
    private int currentTypeIndex;
    private boolean finished;

    public MultiTypedRDBMSISIterator(InternalRDBMSManager internalRDBMS, Context context, IServerTypeDesc[] types, ITemplateHolder templateHolder) {
        this.internalRDBMS = internalRDBMS;
        this.context = context;
        this.types = types;
        this.templateHolder = templateHolder;
        this.currentTypeIndex = 0;
    }

    @Override
    public IEntryHolder next() throws SAException {
        if (finished) {
            return null;
        }

        if (currentTypeIterator == null) {
            currentTypeIterator = createNextTypeIterator();
            if (currentTypeIterator == null) {
                finished = true;
                return null;
            }
        }

        IEntryHolder next = currentTypeIterator.next();
        if (next == null) {
            if (currentTypeIndex == types.length - 1) {
                finished = true;
                return null;
            } else {
                currentTypeIterator = null;
                currentTypeIndex++;
                return next();
            }
        } else {
            return next;
        }
    }

    private ISAdapterIterator<IEntryHolder> createNextTypeIterator() throws SAException {
        boolean foundType = false;
        while (!foundType && currentTypeIndex < types.length) {
            foundType = internalRDBMS.isKnownType(types[currentTypeIndex].getTypeName());
            if (!foundType) {
                currentTypeIndex++;
            }
        }

        if (foundType) {
            return internalRDBMS.makeEntriesIter(context, types[currentTypeIndex].getTypeName(), templateHolder);
        } else {
            return null;
        }
    }

    @Override
    public void close() throws SAException {
        if (currentTypeIterator != null) {
            currentTypeIterator.close();
        }
    }
}
