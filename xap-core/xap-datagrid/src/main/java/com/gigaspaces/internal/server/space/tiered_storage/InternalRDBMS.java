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

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.InitialLoadInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.io.IOException;

@InternalApi
public interface InternalRDBMS {


    /***
     *
     * @param spaceName
     * @param fullMemberName
     * @param typeManager
     * @return true if RDBMS is not empty on startup
     * @throws SAException
     */

    boolean initialize(String spaceName, String fullMemberName, SpaceTypeManager typeManager, boolean isBackup) throws SAException;

    void setLogger(String fullMemberName);

    long getDiskSize() throws SAException, IOException;

    long getFreeSpaceSize() throws SAException, IOException;

    void createTable(ITypeDesc typeDesc) throws SAException;

    /**
     * Inserts a new entry to the internalDiskStorage
     *
     * @param entryHolder entry to insert
     */
    void insertEntry(Context context,  IEntryHolder entryHolder) throws SAException;

    /**
     * updates an entry.
     *
     * @param updatedEntry new content, same UID and class
     */
    void updateEntry(Context context, IEntryHolder updatedEntry) throws SAException;

    /**
     * Removes an entry from the  internalDiskStorage
     *
     * @param entryHolder entry to remove
     */
    boolean removeEntry(Context context, IEntryHolder entryHolder) throws SAException;

    IEntryHolder getEntryById(Context context, String typeName, Object id) throws SAException;

    IEntryHolder getEntryByUID(Context context, String typeName, String uid) throws SAException;

    ISAdapterIterator<IEntryHolder> makeEntriesIter(Context context, String typeName, ITemplateHolder templateHolder) throws SAException;

    boolean isKnownType(String name);

    void shutDown();

    void deleteData() throws SAException;

    void persistType(ITypeDesc typeDesc) throws SAException;

    void initialLoad(Context context, SpaceEngine engine, InitialLoadInfo initialLoadInfo) throws SAException;

    SpaceTypeManager getTypeManager();
}

