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

package com.gigaspaces.internal.cluster.node.impl.packets.data;


import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.j_spaces.core.cluster.IReplicationFilterEntry;


/**
 * Consumes {@link IReplicationPacketData} and delegate it to the corresponding replication in
 * handler.
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationPacketDataConsumer<T extends IReplicationPacketData<?>> {
    /**
     * Consumes a {@link IReplicationPacketData} and delegate the operation is represents to the
     * corresponding method at the given {@link IReplicationInFacade}
     *
     * @param data                operation to consume and translate to an operation
     * @param replicationInFacade facade to delegate the data as incoming replication
     * @return consumption result that indicates success consumption or error
     */
    IDataConsumeResult consume(IReplicationInContext context, T data, IReplicationInFacade replicationInFacade, IReplicationInFilterCallback filterInCallback);

    /**
     * Apply a fix to a data, the fix is generated due to a failed consumption of {@link
     * IReplicationPacketData}
     *
     * @param data data to fix
     * @param fix  fix to apply
     * @return fixed data
     */
    T applyFix(IReplicationInContext context, T data, IDataConsumeFix fix) throws Exception;

    /**
     * Transform generated data to filter entry in order to be passed to a filter
     *
     * @return collection of replication filter entries representing the data
     */
    Iterable<IReplicationFilterEntry> toFilterEntries(IReplicationInContext context, IReplicationPacketData<?> data);

    /**
     * Extracts participants meta data for the provided replication packet data
     */
    IReplicationParticipantsMetadata extractParticipantsMetadata(
            IReplicationPacketData<?> data);

    /**
     * Merges data of multiple participants operations to a single operation data which will
     * eventually be executed as one.
     *
     * @param participantsMetadata TODO
     */
    IReplicationPacketData<?> merge(IReplicationPacketData<?>[] allParticipantsData, IReplicationParticipantsMetadata participantsMetadata);

    SpaceTypeManager getTypeManager();

}
