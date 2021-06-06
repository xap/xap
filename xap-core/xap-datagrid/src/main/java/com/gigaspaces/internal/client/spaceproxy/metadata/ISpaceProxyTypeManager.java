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

package com.gigaspaces.internal.client.spaceproxy.metadata;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.StorageTypeDeserialization;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.j_spaces.core.IGSEntry;
import com.j_spaces.core.LeaseContext;

import net.jini.core.entry.UnusableEntryException;

/**
 * @author anna
 * @since 6.5
 */
public interface ISpaceProxyTypeManager {
    // Type descriptor related methods:
    ITypeDesc getTypeDescIfExistsInProxy(String typeName);

    ITypeDesc getTypeDescByName(String typeName);

    ITypeDesc getTypeDescByName(String typeName, String codebase);

    ITypeDesc getTypeDescByNameIfExists(String typeName);

    void loadTypeDescToPacket(ITransportPacket packet);

    void registerTypeDesc(ITypeDesc typeDesc);

    void deleteTypeDesc(String className);

    void deleteAllTypeDescs();

    String getCommonSuperTypeName(ITypeDesc typeDesc1, ITypeDesc typeDesc2);

    SpaceTypeInfoRepository getSpaceTypeInfoRepository();

    // Methods to convert input arguments to entry/template packets
    IEntryPacket getEntryPacketFromObject(Object object, ObjectType objectType);

    ITemplatePacket getTemplatePacketFromObject(Object object, ObjectType objectType);

    // Methods to convert results/exceptions from entry/template packets
    LeaseContext<?> convertWriteResult(Object entry, IEntryPacket entryPacket, LeaseContext<?> writeResult);

    public LeaseContext<?> convertWriteOrUpdateResult(LeaseContext<?> result, Object entry,
                                                      IEntryPacket entryPacket, int modifiers);

    Object convertQueryResult(IEntryPacket result, ITemplatePacket query, boolean returnEntryPacket);

    Object convertQueryResult(IEntryPacket result, ITemplatePacket query, boolean returnEntryPacket, AbstractProjectionTemplate projectionTemplate);

    Object[] convertQueryResults(IEntryPacket[] results, ITemplatePacket query, boolean returnEntryPacket, AbstractProjectionTemplate projectionTemplate);

    Object[] createResultArray(ITemplatePacket templatePacket, int length);

    // Other methods
    Object getLockObject();

    void close();

    Object getObjectFromIGSEntry(IGSEntry entry) throws UnusableEntryException;

    Object getObjectFromEntryPacket(IEntryPacket entryPacket, QueryResultTypeInternal resultType, boolean returnPacket);

    Object getObjectFromEntryPacket(IEntryPacket entryPacket, QueryResultTypeInternal resultType, boolean returnPacket, AbstractProjectionTemplate projectionTemplate);

    Object getObjectFromEntryPacket(IEntryPacket entryPacket, QueryResultTypeInternal resultType, boolean returnPacket, StorageTypeDeserialization storageTypeDeserialization, AbstractProjectionTemplate projectionTemplate);

    LeaseContext<?> processWriteResult(WriteEntryResult writeResult, Object entry, IEntryPacket entryPacket);
}