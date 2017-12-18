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

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;


/**
 * Convert logical operation to a representing {@link IReplicationPacketData}
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationPacketDataProducer<T extends IReplicationPacketData<?>>
        extends IReplicationPacketEntryDataContentExtractor {
    /**
     * Convert a single entry operation to a representing {@link IReplicationPacketData}
     *
     * @param entryHolder   operation entry
     * @param operationType operation type
     * @return representing {@link IReplicationPacketData}
     */
    T createSingleOperationData(IEntryHolder entryHolder,
                                ReplicationSingleOperationType operationType,
                                ReplicationOutContext replicationOutContext);

    /**
     * Convert a transaction operation to a representing {@link IReplicationPacketData}
     */
    T createTransactionOperationData(ServerTransaction transaction,
                                     ArrayList<IEntryHolder> lockedEntries,
                                     ReplicationOutContext replicationOutContext, ReplicationMultipleOperationType operationType);

    T createGenericOperationData(Object operationData,
                                 ReplicationSingleOperationType operationType,
                                 ReplicationOutContext entireContext);

    /**
     * Creates a fix for the given error result, that fixed should be applied at the corresponding
     * {@link IReplicationPacketDataConsumer}
     *
     * @param errorResult the result that needs to be fixed
     * @param errorData   the data that created the error while being consumed
     * @return the fix for the error result
     */
    IDataConsumeFix createFix(IDataConsumeResult errorResult, T errorData);

    /**
     * Transform generated data to filter entry in order to be passed to a filter
     *
     * @return collection of replication filter entries representing the data
     */
    Iterable<IReplicationFilterEntry> toFilterEntries(T data);

    /**
     * Convert the given data that has a single entry ({@link IReplicationPacketData#isSingleEntryData()}
     * must be true) to the specified operation
     *
     * @param singleEntryData data to convert
     * @param metadata        conversion metadata
     * @return new data converted to the new operation
     * @throws ReplicationPacketEntryDataConversionException if the conversion is illegal
     */
    T convertSingleEntryData(T singleEntryData,
                             ReplicationEntryDataConversionMetadata metadata, PlatformLogicalVersion targetMemberVersion)
            throws ReplicationPacketEntryDataConversionException;

    /**
     * Converts the given data
     *
     * @param data               data to convert
     * @param convertToOperation which operation to convert to
     * @return new data converted to the new operation
     * @throws ReplicationPacketEntryDataConversionException if the conversion is illegal
     */
    T convertData(T data,
                  ReplicationMultipleOperationType convertToOperation, PlatformLogicalVersion targetMemberVersion)
            throws ReplicationPacketEntryDataConversionException;

    /**
     * Converts the given entry data to the specified operation
     *
     * @param entryData entry data to convert
     * @param metadata  conversion metadata
     * @return new entry data converted to the new operation
     * @throws ReplicationPacketEntryDataConversionException if the conversion is illegal
     */
    IReplicationPacketEntryData convertEntryData(
            IReplicationPacketEntryData entryData,
            ReplicationEntryDataConversionMetadata metadata,
            PlatformLogicalVersion targetMemberVersion)
            throws ReplicationPacketEntryDataConversionException;

    /**
     * create an empty data of the same type of the given data, this data must not be {@link
     * IReplicationPacketData#isSingleEntryData()}
     *
     * @param data template for data type creation
     * @return empty data of the same type of the given data,
     */
    T createEmptyMultipleEntryData(T data);

    void completePacketDataContent(T data);

    boolean isPrimary();
}
