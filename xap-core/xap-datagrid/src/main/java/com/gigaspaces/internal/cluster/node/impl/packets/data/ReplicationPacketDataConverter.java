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

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.CancelLeaseFullReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.ChangeReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.EntryLeaseExpiredFullReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.PartialUpdateReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.RemoveByUIDReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.RemoveReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.SingleReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.UpdateReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.WriteReplicationPacketData;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.internal.transport.TransportPacketType;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

/**
 * @author Dan Kilman
 * @since 9.0
 */
// TODO durable notifications : Refactor this class
@com.gigaspaces.api.InternalApi
public class ReplicationPacketDataConverter {

    public static AbstractReplicationPacketSingleEntryData convert(
            IReplicationPacketEntryData entryData,
            ReplicationEntryDataConversionMetadata metadata,
            PlatformLogicalVersion targetMemberVersion) throws ReplicationPacketEntryDataConversionException {

        AbstractReplicationPacketSingleEntryData result = null;

        switch (entryData.getOperationType()) {
            case UPDATE: {
                UpdateReplicationPacketData updateOperation = (UpdateReplicationPacketData) entryData;

                // serialize the previous entry to be included in the notification
                if (metadata.isRequiresPrevValue()) {
                    updateOperation.serializeFullContent();
                }
                //Convert update to update with full content, for now the only scenario where we convert update to update
                //is in special keeper filter where backup is in lru and central data source true.
                if (metadata.isRequiresReliableAsyncFullContent() &&
                        metadata.getConvertToOperationType() == ReplicationSingleOperationType.UPDATE) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromUpdateToUpdateReliableAsyncContentRequired(updateOperation);
                }

                // used in durable notifications
                // Convert update to update with modified entry data
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.UPDATE &&
                        metadata.getEntryData() != null) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromUpdateToUpdateWithEntryData(updateOperation, metadata);
                }

                // used in durable notifications
                // Convert update to update with full entry data
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.UPDATE &&
                        metadata.isRequiresFullEntryData() &&
                        metadata.getEntryData() == null &&
                        !entryData.containsFullEntryData()) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromUpdateToUpdateFullContentRequiredNoEntryData(updateOperation, metadata);
                }

                //Convert Update to Remove by UID
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.REMOVE_ENTRY &&
                        !metadata.isRequiresFullEntryData() &&
                        metadata.getEntryData() == null) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromUpdateToRemoveNoFullContentRequiredNoEntryData(updateOperation);
                }

                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.WRITE &&
                        metadata.getEntryData() == null) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromUpdateToWriteNoEntryData(updateOperation);
                }
                break;
            }
            case CHANGE: {
                ChangeReplicationPacketData changeOperation = (ChangeReplicationPacketData) entryData;

                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.UPDATE) {
                    validateNonCascadingConversion(entryData, result);
                    //Convert change to update with full content, for now the only scenario where we convert change to update with requires reliable async full content
                    //is in special keeper filter where backup is in lru and central data source true.
                    if (metadata.isRequiresReliableAsyncFullContent()) {
                        result = convertFromChangeToUpdateReliableAsyncContentRequired(changeOperation, metadata);
                    } else {
                        result = convertFromChangeToUpdate(changeOperation, metadata);
                    }
                }

                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.REMOVE_ENTRY &&
                        !metadata.isRequiresFullEntryData()) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromChangeToRemoveNoFullContentRequired(changeOperation);
                }

                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.WRITE) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromChangeToWrite(changeOperation, metadata);
                }

                break;
            }
            case WRITE: {
                WriteReplicationPacketData writeOperation = (WriteReplicationPacketData) entryData;

                // used in durable notifications
                // Convert write to write with modified entry data
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.WRITE &&
                        metadata.getEntryData() != null) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromWriteToWriteNoFullContentRequiredWithEntryData(writeOperation, metadata);
                }

                break;
            }
            case REMOVE_ENTRY: {
                // used in durable notifications
                // Convert remove to full remove possibly with modified entry data
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.REMOVE_ENTRY &&
                        metadata.isRequiresFullEntryData()) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromRemoveToRemoveFullContentRequired(entryData, metadata);
                }

                break;
            }
            case CANCEL_LEASE: {
                // used in durable notifications
                // Convert cancel lease and entry lease expired to full entry lease expired possibly with modified entry data
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.ENTRY_LEASE_EXPIRED &&
                        metadata.isRequiresFullEntryData()) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromLeaseOperationToLeaseExpiredFullContentRequired(entryData, metadata);
                }

                // used by lru keeper to convert a partial lease cancelled entry to a full one
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.CANCEL_LEASE &&
                        metadata.isRequiresFullEntryData()) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromLeaseCancelledToLeaseCancelledFullContentRequired(entryData);
                }

                break;
            }
            case ENTRY_LEASE_EXPIRED: {
                // used in durable notifications
                // Convert cancel lease and entry lease expired to full entry lease expired possibly with modified entry data
                if (metadata.getConvertToOperationType() == ReplicationSingleOperationType.ENTRY_LEASE_EXPIRED &&
                        metadata.isRequiresFullEntryData()) {
                    validateNonCascadingConversion(entryData, result);
                    result = convertFromLeaseOperationToLeaseExpiredFullContentRequired(entryData, metadata);
                }

                break;
            }
        }

        final boolean noConversionApplied = result == null || result == entryData;

        if (noConversionApplied &&
                entryData.containsFullEntryData() &&
                entryData instanceof SingleReplicationPacketData &&
                metadata.getRequiredQueryResultType() != QueryResultTypeInternal.NOT_SET) {
            SingleReplicationPacketData packetData = (SingleReplicationPacketData) entryData;
            if (metadata.getRequiredQueryResultType().isPbs() && packetData.getEntryPacket().getPacketType() != TransportPacketType.PBS) {
                IEntryPacket convertedPacket = EntryPacketFactory.createFullPacket(packetData.getMainEntryData(),
                        entryData.getOperationId(),
                        entryData.getUid(),
                        entryData.isTransient(),
                        metadata.getRequiredQueryResultType());

                result = packetData.cloneWithEntryPacket(convertedPacket);
            } else {
                result = packetData;
            }
        }

        //Apply projection template filtering
        if (metadata.getProjectionTemplate() != null) {
            IReplicationPacketEntryData returnedEntryData = result != null ? result : entryData;
            if (returnedEntryData instanceof SingleReplicationPacketData) {
                SingleReplicationPacketData returnedPacketEntryData = (SingleReplicationPacketData) returnedEntryData;
                SingleReplicationPacketData originalPacketData = (SingleReplicationPacketData) ((entryData instanceof SingleReplicationPacketData) ? entryData : null);

                //Minor optimization to avoid cloning entry packet if already cloned
                final boolean cloneEntryPacketRequired = originalPacketData == null ? false : returnedPacketEntryData.getEntryPacket() == originalPacketData.getEntryPacket();
                IEntryPacket entryPacket = cloneEntryPacketRequired ? returnedPacketEntryData.getEntryPacket().clone() : returnedPacketEntryData.getEntryPacket();

                metadata.getProjectionTemplate().filterOutNonProjectionProperties(entryPacket);
                if (returnedPacketEntryData instanceof UpdateReplicationPacketData &&
                        !NotificationReplicationChannelDataFilter.DurableNotificationConversionFlags.isUpdateToUnmatched(metadata.getFlags())) {
                    UpdateReplicationPacketData updateReplicationPacketData = (UpdateReplicationPacketData) returnedPacketEntryData;
                    IEntryData prevEntryData = updateReplicationPacketData.getSecondaryEntryData();
                    metadata.getProjectionTemplate().filterOutNonProjectionProperties(prevEntryData);
                }

                if (cloneEntryPacketRequired)
                    result = returnedPacketEntryData.cloneWithEntryPacket(entryPacket);
            }
        }

        if (result != null)
            return result;

        throw new ReplicationPacketEntryDataConversionException("Cannot convert [" + entryData + "] to [" + metadata + "]");

    }

    private static UpdateReplicationPacketData convertFromChangeToUpdateReliableAsyncContentRequired(
            ChangeReplicationPacketData changeOperation, ReplicationEntryDataConversionMetadata metadata) {
        UpdateReplicationPacketData updateReplicationPacketData = convertFromChangeToUpdate(changeOperation, metadata);
        updateReplicationPacketData.serializeFullContent();
        return updateReplicationPacketData;
    }

    private static UpdateReplicationPacketData convertFromChangeToUpdate(
            ChangeReplicationPacketData changeOperation, ReplicationEntryDataConversionMetadata metadata) {
        IEntryData entryData = metadata.getEntryData() != null ? metadata.getEntryData() : changeOperation.getMainEntryData();
        IEntryPacket fullPacket = EntryPacketFactory.createFullPacket(entryData,
                changeOperation.getOperationId(),
                changeOperation.getUid(),
                changeOperation.isTransient(),
                metadata.getRequiredQueryResultType());

        UpdateReplicationPacketData updateReplicationPacketData = new UpdateReplicationPacketData(fullPacket,
                changeOperation.isFromGateway(),
                false,
                changeOperation.getSecondaryEntryData(),
                metadata.getFlags(),
                changeOperation.getExpirationTime(),
                entryData);
        updateReplicationPacketData.serializeFullContent();
        return updateReplicationPacketData;
    }

    private static RemoveByUIDReplicationPacketData convertFromChangeToRemoveNoFullContentRequired(
            ChangeReplicationPacketData changeOperation) {
        return new RemoveByUIDReplicationPacketData(changeOperation.getTypeName(),
                changeOperation.getUid(),
                changeOperation.isTransient(),
                changeOperation.getMainEntryData(),
                changeOperation.getOperationId(),
                changeOperation.isFromGateway());
    }

    private static WriteReplicationPacketData convertFromChangeToWrite(
            ChangeReplicationPacketData changeOperation, ReplicationEntryDataConversionMetadata metadata) {
        IEntryData entryData = metadata.getEntryData() != null ? metadata.getEntryData() : changeOperation.getMainEntryData();
        IEntryPacket fullPacket = EntryPacketFactory.createFullPacket(entryData,
                changeOperation.getOperationId(),
                changeOperation.getUid(),
                changeOperation.isTransient(),
                metadata.getRequiredQueryResultType());
        return new WriteReplicationPacketData(fullPacket,
                changeOperation.isFromGateway(),
                changeOperation.getExpirationTime());
    }

    private static void validateNonCascadingConversion(
            IReplicationPacketEntryData entryData,
            AbstractReplicationPacketSingleEntryData result)
            throws ReplicationPacketEntryDataConversionException {
        if (result != null)
            throw new ReplicationPacketEntryDataConversionException("Packet data [" + entryData + "] was already converted to [" + result + "]");
    }

    private static AbstractReplicationPacketSingleEntryData convertFromRemoveToRemoveBackwardCompatible(
            IReplicationPacketEntryData entryData) {
        if (entryData.containsFullEntryData()) {
            RemoveReplicationPacketData removePacket = (RemoveReplicationPacketData) entryData;

            IEntryPacket entryPacket = TemplatePacketFactory.createFullPacket(removePacket.getEntryPacket());

            return new RemoveReplicationPacketData(entryPacket, entryData.isFromGateway(), removePacket.getMainEntryData(), true);
        }

        return (AbstractReplicationPacketSingleEntryData) entryData;
    }

    private static RemoveByUIDReplicationPacketData convertFromUpdateToRemoveNoFullContentRequiredNoEntryData(UpdateReplicationPacketData updateOperation) {
        IEntryPacket entryPacket = updateOperation.getEntryPacket();
        return new RemoveByUIDReplicationPacketData(entryPacket.getTypeName(),
                entryPacket.getUID(),
                entryPacket.isTransient(),
                updateOperation.getMainEntryData(),
                updateOperation.getEntryPacket().getOperationID(),
                updateOperation.isFromGateway());
    }


    private static WriteReplicationPacketData convertFromUpdateToWriteNoEntryData(
            UpdateReplicationPacketData updateOperation) {
        IEntryPacket fullEntryPacket = getFullEntryPacketFromUpdateOperation(updateOperation, QueryResultTypeInternal.NOT_SET);

        return new WriteReplicationPacketData(fullEntryPacket, updateOperation.isFromGateway(), updateOperation.getExpirationTime());
    }

    private static IEntryPacket getFullEntryPacketFromUpdateOperation(
            UpdateReplicationPacketData updateOperation,
            QueryResultTypeInternal queryResultTypeInternal) {
        IEntryPacket fullEntryPacket = updateOperation.getEntryPacket();
        if (updateOperation instanceof PartialUpdateReplicationPacketData) {
            //Construct full packet for write operation
            if (queryResultTypeInternal != QueryResultTypeInternal.NOT_SET)
                fullEntryPacket = EntryPacketFactory.createFullPacket(updateOperation.getMainEntryData(),
                        updateOperation.getOperationId(),
                        updateOperation.getUid(),
                        updateOperation.isTransient(),
                        queryResultTypeInternal);
            else
                fullEntryPacket = updateOperation.getEntryPacket().clone();


            IEntryData secondaryEntryDAta = updateOperation.getSecondaryEntryData();
            //Fill the entry packet with missing values taken from the previous server entry
            for (int i = 0; i < fullEntryPacket.getFieldValues().length; i++) {
                //Reinsert removed value
                if (fullEntryPacket.getFieldValue(i) == null)
                    fullEntryPacket.setFieldValue(i, secondaryEntryDAta.getFixedPropertyValue(i));

            }
        }
        return fullEntryPacket;
    }

    private static UpdateReplicationPacketData convertFromUpdateToUpdateFullContentRequiredNoEntryData(
            UpdateReplicationPacketData updateOperation,
            ReplicationEntryDataConversionMetadata metadata) {
        IEntryPacket fullEntryPacket = getFullEntryPacketFromUpdateOperation(updateOperation, metadata.getRequiredQueryResultType());

        UpdateReplicationPacketData updateReplicationPacketData = new UpdateReplicationPacketData(fullEntryPacket,
                updateOperation.isFromGateway(),
                updateOperation.isOverrideVersion(),
                updateOperation.getSecondaryEntryData(),
                metadata.getFlags(),
                updateOperation.getExpirationTime(),
                updateOperation.getMainEntryData());
        if (updateOperation.isSerializeFullContent()) {
            updateReplicationPacketData.serializeFullContent();
        }
        return updateReplicationPacketData;
    }

    private static UpdateReplicationPacketData convertFromUpdateToUpdateReliableAsyncContentRequired(
            UpdateReplicationPacketData entryData) {
        UpdateReplicationPacketData result = entryData.clone();
        result.serializeFullContent();
        return result;
    }


    private static AbstractReplicationPacketSingleEntryData convertFromUpdateToUpdateWithEntryData(
            UpdateReplicationPacketData entryData,
            ReplicationEntryDataConversionMetadata metadata) {
        boolean isUnmatched = NotificationReplicationChannelDataFilter.DurableNotificationConversionFlags.isUpdateToUnmatched(metadata.getFlags());
        IEntryData sourceEntryData;
        if (metadata.isModifiedByFilter()) {
            // ignoring unmatched - no logic in changing the old value
            sourceEntryData = metadata.getEntryData();
        } else {
            sourceEntryData = isUnmatched ? metadata.getEntryData() : entryData.getMainEntryData();
        }

        IEntryPacket entryPacket = EntryPacketFactory.createFullPacket(/*metadata.getEntryData()*/sourceEntryData,
                entryData.getOperationId(),
                entryData.getUid(),
                entryData.isTransient(),
                metadata.getRequiredQueryResultType());

        UpdateReplicationPacketData updateReplicationPacketData = new UpdateReplicationPacketData(entryPacket,
                entryData.isFromGateway(),
                entryData.isOverrideVersion(),
                isUnmatched ? entryData.getMainEntryData() : entryData.getSecondaryEntryData(),
                metadata.getFlags(),
                entryData.getExpirationTime(),
                sourceEntryData);
        if (entryData.isSerializeFullContent()) {
            updateReplicationPacketData.serializeFullContent();
        }
        return updateReplicationPacketData;

    }


    private static AbstractReplicationPacketSingleEntryData convertFromWriteToWriteNoFullContentRequiredWithEntryData(
            WriteReplicationPacketData writeOperation,
            ReplicationEntryDataConversionMetadata metadata) {
        IEntryPacket entryPacket = EntryPacketFactory.createFullPacket(metadata.getEntryData(),
                writeOperation.getOperationId(),
                writeOperation.getUid(),
                writeOperation.isTransient(),
                metadata.getRequiredQueryResultType());

        return new WriteReplicationPacketData(entryPacket, writeOperation.isFromGateway(), writeOperation.getExpirationTime(), false);
    }


    private static AbstractReplicationPacketSingleEntryData convertFromRemoveToRemoveFullContentRequired(
            IReplicationPacketEntryData entryData,
            ReplicationEntryDataConversionMetadata metadata) {
        AbstractReplicationPacketSingleEntryData entry = (AbstractReplicationPacketSingleEntryData) entryData;

        if (entry.containsFullEntryData() && metadata.getEntryData() == null)
            return entry;

        IEntryData iEntryData = metadata.getEntryData();

        if (iEntryData == null)
            iEntryData = entry.getMainEntryData();

        IEntryPacket entryPacket = EntryPacketFactory.createFullPacket(iEntryData,
                entryData.getOperationId(),
                entryData.getUid(),
                entryData.isTransient(),
                metadata.getRequiredQueryResultType());


        return new RemoveReplicationPacketData(entryPacket, entry.isFromGateway(), iEntryData, true);
    }

    private static AbstractReplicationPacketSingleEntryData convertFromLeaseOperationToLeaseExpiredFullContentRequired(
            IReplicationPacketEntryData entryData,
            ReplicationEntryDataConversionMetadata metadata) {
        if (entryData instanceof EntryLeaseExpiredFullReplicationPacketData && metadata.getEntryData() == null && metadata.getRequiredQueryResultType() == null)
            return (EntryLeaseExpiredFullReplicationPacketData) entryData;
        else {
            AbstractReplicationPacketSingleEntryData entry = (AbstractReplicationPacketSingleEntryData) entryData;

            IEntryData iEntryData = metadata.getEntryData();

            if (iEntryData == null)
                iEntryData = entry.getMainEntryData();

            IEntryPacket entryPacket = EntryPacketFactory.createFullPacket(iEntryData,
                    entryData.getOperationId(),
                    entryData.getUid(),
                    entryData.isTransient(),
                    metadata.getRequiredQueryResultType());


            return new EntryLeaseExpiredFullReplicationPacketData(entryPacket, entry.isFromGateway());
        }
    }

    private static AbstractReplicationPacketSingleEntryData convertFromLeaseCancelledToLeaseCancelledFullContentRequired(
            IReplicationPacketEntryData entryData) {
        if (entryData instanceof CancelLeaseFullReplicationPacketData)
            return (CancelLeaseFullReplicationPacketData) entryData;
        else {
            AbstractReplicationPacketSingleEntryData entry = (AbstractReplicationPacketSingleEntryData) entryData;

            IEntryPacket templatePacket = EntryPacketFactory.createFullPacket(entry.getMainEntryData(),
                    entryData.getOperationId(),
                    entryData.getUid(),
                    entryData.isTransient(),
                    QueryResultTypeInternal.NOT_SET);

            return new CancelLeaseFullReplicationPacketData(templatePacket, entry.isFromGateway());
        }
    }

}
