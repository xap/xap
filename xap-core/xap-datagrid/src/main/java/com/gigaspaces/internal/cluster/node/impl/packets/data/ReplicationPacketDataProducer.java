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

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.cluster.node.impl.DataTypeAddIndexPacketData;
import com.gigaspaces.internal.cluster.node.impl.DataTypeIntroducePacketData;
import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.data.errors.UnhandledErrorFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.CancelLeaseReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.ChangeReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.EntryLeaseExpiredReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.EvictReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.ExtendEntryLeaseReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.ExtendNotifyTemplateLeaseReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.InsertNotifyTemplateReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.NotifyTemplateLeaseExpiredReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.PartialUpdateReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.RemoveByUIDReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.RemoveNotifyTemplateReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.RemoveReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.SingleReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionAbortReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionCommitReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionOnePhaseReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionPrepareReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.TransactionReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.UpdateReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.WriteReplicationPacketData;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.ICustomTypeDescLoader;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.internal.transport.TransportPacketType;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.server.ServerEntry;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


@com.gigaspaces.api.InternalApi
public class ReplicationPacketDataProducer
        implements
        IReplicationPacketDataProducer<IExecutableReplicationPacketData<?>> {

    private final boolean _replicateToTargetWithExternalDatasource;
    private final boolean _replicateFullTake;
    private final SpaceEngine _spaceEngine;
    private final ReplicationPacketDataMediator _packetDataMediator;

    public ReplicationPacketDataProducer(SpaceEngine spaceEngine,
                                         boolean replicateToTargetWithExternalDatasource,
                                         boolean replicateFullTake, ReplicationPacketDataMediator packetDataMediator) {
        _spaceEngine = spaceEngine;
        _replicateToTargetWithExternalDatasource = replicateToTargetWithExternalDatasource;
        _replicateFullTake = replicateFullTake;
        _packetDataMediator = packetDataMediator;
    }

    public IExecutableReplicationPacketData<?> createSingleOperationData(
            IEntryHolder entryHolder,
            ReplicationSingleOperationType operationType,
            ReplicationOutContext replicationOutContext) {
        switch (operationType) {
            case WRITE:
                return createWriteReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID(),
                        replicationOutContext.isFromGateway(), replicationOutContext.isBackupOnly());
            case UPDATE:
                return createUpdateReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID(),
                        replicationOutContext.getPartialUpdatedValuesIndicators(),
                        replicationOutContext.isFromGateway(),
                        replicationOutContext.isOverrideVersion(entryHolder.getUID()),
                        replicationOutContext.getPreviousUpdatedEntryData());
            case CHANGE:
                return createChangeReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID(),
                        replicationOutContext.getSpaceEntryMutators(),
                        replicationOutContext.getPreviousUpdatedEntryData(),
                        replicationOutContext.isFromGateway());
            case REMOVE_ENTRY:
                return (IExecutableReplicationPacketData<?>) createRemoveReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID(),
                        replicationOutContext.isFromGateway(), replicationOutContext.isBackupOnly());

            case INSERT_NOTIFY_TEMPLATE:
                return createInsertNotifyReplicationPacket((NotifyTemplateHolder) entryHolder,
                        replicationOutContext.getOperationID());
            case REMOVE_NOTIFY_TEMPLATE:
                return createRemoveNotifyReplicationPacket((NotifyTemplateHolder) entryHolder,
                        replicationOutContext.getOperationID());

            case CANCEL_LEASE:
                return createCancelLeaseReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID(),
                        replicationOutContext.isFromGateway());
            case EVICT:
                return createEvictReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID());
            case EXTEND_ENTRY_LEASE:
                return createExtendEntryLeaseReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID(),
                        replicationOutContext.isFromGateway());
            case EXTEND_NOTIFY_TEMPLATE_LEASE:
                return createExtendNotifyTemplateLeaseReplicationPacket((NotifyTemplateHolder) entryHolder,
                        replicationOutContext.getOperationID());
            case ENTRY_LEASE_EXPIRED:
                return createEntryLeaseExpiredReplicationPacket(entryHolder,
                        replicationOutContext.getOperationID());
            case NOTIFY_TEMPLATE_LEASE_EXPIRED:
                return createNotifyTemplateLeaseExpiredReplicationPacket((NotifyTemplateHolder) entryHolder,
                        replicationOutContext.getOperationID());

            default:
                throw new UnsupportedOperationException("Unknown replication operation type "
                        + operationType);
        }

    }

    public IExecutableReplicationPacketData<?> createGenericOperationData(
            Object operationData, ReplicationSingleOperationType operationType,
            ReplicationOutContext entireContext) {
        switch (operationType) {
            case DATA_TYPE_INTRODUCE:
                return new DataTypeIntroducePacketData((ITypeDesc) operationData, entireContext.isFromGateway());
            case DATA_TYPE_ADD_INDEX:
                return new DataTypeAddIndexPacketData((AddTypeIndexesRequestInfo) operationData, entireContext.isFromGateway());
            default:
                throw new UnsupportedOperationException("Unknown replication operation type "
                        + operationType);
        }
    }

    public IExecutableReplicationPacketData<IReplicationTransactionalPacketEntryData> createTransactionOperationData(
            ServerTransaction transaction, ArrayList<IEntryHolder> entries,
            ReplicationOutContext replicationOutContext, ReplicationMultipleOperationType operationType) {
        AbstractTransactionReplicationPacketData transactionPacket = createTransactionPacket(operationType, transaction, replicationOutContext.getOperationID(), replicationOutContext.isFromGateway());

        //If this is a commit or abort we take the already existing prepared content from the mediator
        if (operationType == ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_COMMIT || operationType == ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_ABORT) {
            List<IReplicationTransactionalPacketEntryData> pendingTransactionData = _packetDataMediator.removePendingTransactionData(transaction);
            if (pendingTransactionData == null)
                throw new ReplicationInternalSpaceException("Attempt to create " + operationType + " packet while there is no pending transaction content");
            for (IReplicationTransactionalPacketEntryData entryData : pendingTransactionData)
                transactionPacket.add(entryData);
        } else {
            for (int i = 0; i < entries.size(); i++) {
                IEntryHolder entryHolder = entries.get(i);

                if (entryHolder == null
                        || !replicationOutContext.getShouldReplicate()[i]
                        || entryHolder.isDeleted())
                    continue;

                if (entryHolder.getWriteLockTransaction() == null
                        || !entryHolder.getWriteLockTransaction()
                        .equals(transaction))
                    continue;

                IReplicationTransactionalPacketEntryData singlePacket;
                switch (entryHolder.getWriteLockOperation()) {

                    case SpaceOperations.WRITE:
                        singlePacket = createWriteReplicationPacket(entryHolder,
                                replicationOutContext.getOperationIDs()[i],
                                replicationOutContext.isFromGateway(), false);
                        break;

                    case SpaceOperations.UPDATE:
                        Object updateInfo = replicationOutContext.getPartialUpdatesAndInPlaceUpdatesInfo()
                                .get(entryHolder.getUID());
                        if (updateInfo instanceof Collection<?>) {
                            singlePacket = createChangeReplicationPacket(entryHolder,
                                    replicationOutContext.getOperationIDs()[i],
                                    (Collection<SpaceEntryMutator>) updateInfo,
                                    entryHolder.getShadow()
                                            .getEntryData(),
                                    replicationOutContext.isFromGateway());
                        } else {
                            boolean[] partialUpdateIndicators = (boolean[]) updateInfo;
                            if (partialUpdateIndicators != null
                                    && partialUpdateIndicators.length == 0)
                                partialUpdateIndicators = null;
                            singlePacket = createUpdateReplicationPacket(entryHolder,
                                    replicationOutContext.getOperationIDs()[i],
                                    partialUpdateIndicators,
                                    replicationOutContext.isFromGateway(),
                                    replicationOutContext.isOverrideVersion(entryHolder.getUID()),
                                    entryHolder.getShadow().getEntryData());
                        }
                        break;
                    case SpaceOperations.TAKE:
                    case SpaceOperations.TAKE_IE:
                        singlePacket = (IReplicationTransactionalPacketEntryData) createRemoveReplicationPacket(entryHolder,
                                replicationOutContext.getOperationIDs()[i],
                                replicationOutContext.isFromGateway(), false);

                        break;

                    default: // take
                        throw new UnsupportedOperationException("Unsupported transactional operation - "
                                + entryHolder.getWriteLockOperation());

                }
                if(singlePacket.isTransient()){
                    transactionPacket.setHasTransientMembersFlag();
                } else {
                    transactionPacket.setHasPersistentMembersFlag();
                }
                transactionPacket.add(singlePacket);
            }

            if (operationType == ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_PREPARE)
                _packetDataMediator.setPendingTransactionData(transaction, transactionPacket);
        }

        return transactionPacket;
    }

    private AbstractTransactionReplicationPacketData createTransactionPacket(
            ReplicationMultipleOperationType operationType,
            ServerTransaction transaction, OperationID operationID, boolean fromGateway) {
        switch (operationType) {
            case TRANSACTION_ONE_PHASE:
                TransactionOnePhaseReplicationPacketData data = new TransactionOnePhaseReplicationPacketData(operationID, fromGateway);
                data.setMetaData(transaction.getMetaData());
                return data;
            case TRANSACTION_TWO_PHASE_PREPARE:
                return new TransactionPrepareReplicationPacketData(transaction, fromGateway);
            case TRANSACTION_TWO_PHASE_COMMIT:
                return new TransactionCommitReplicationPacketData(transaction, operationID, fromGateway);
            case TRANSACTION_TWO_PHASE_ABORT:
                return new TransactionAbortReplicationPacketData(transaction, operationID, fromGateway);
            default:
                throw new IllegalArgumentException("unknown operation type for transaction packet generation [" + operationType + "]");
        }
    }

    private CancelLeaseReplicationPacketData createCancelLeaseReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID, boolean isFromGateway) {

        return new CancelLeaseReplicationPacketData(entryHolder.getClassName(),
                entryHolder.getUID(),
                entryHolder.isTransient(),
                entryHolder.getEntryData(),
                operationID,
                entryHolder.getRoutingValue() != null ? entryHolder.getRoutingValue().hashCode() : -1,
                isFromGateway);

    }

    private EntryLeaseExpiredReplicationPacketData createEntryLeaseExpiredReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID) {

        return new EntryLeaseExpiredReplicationPacketData(entryHolder.getClassName(),
                entryHolder.getUID(),
                entryHolder.isTransient(),
                entryHolder.getEntryData(),
                operationID);

    }

    private NotifyTemplateLeaseExpiredReplicationPacketData createNotifyTemplateLeaseExpiredReplicationPacket(
            NotifyTemplateHolder templateHolder, OperationID operationID) {

        return new NotifyTemplateLeaseExpiredReplicationPacketData(templateHolder.getClassName(),
                templateHolder.getUID(),
                templateHolder.getEntryData(),
                operationID);

    }

    private EvictReplicationPacketData createEvictReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID) {

        return new EvictReplicationPacketData(entryHolder.getClassName(),
                entryHolder.getUID(),
                entryHolder.isTransient(),
                entryHolder.getEntryData(),
                operationID);

    }

    private WriteReplicationPacketData createWriteReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID,
            boolean fromGateway, boolean backupOnly) {
        IEntryPacket entryPacket = EntryPacketFactory.createFullPacketForReplication(entryHolder,
                operationID);

        return new WriteReplicationPacketData(entryPacket, fromGateway, entryHolder.getEntryData().getExpirationTime(), backupOnly);

    }

    private UpdateReplicationPacketData createUpdateReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID,
            boolean[] partialUpdatedValuesIndicators, boolean fromGateway,
            boolean overrideVersion, IEntryData previousEntryData) {
        if (isPartialUpdate(entryHolder, partialUpdatedValuesIndicators)) {
            final IEntryPacket entryPacket = EntryPacketFactory.createPartialUpdatePacketForReplication(entryHolder,
                    operationID,
                    partialUpdatedValuesIndicators);
            // Set previous entry version on entry packet for handling version conflicts on target
            if (entryHolder.hasShadow())
                entryPacket.setPreviousVersion(entryHolder.getShadow().getVersionID());

            return new PartialUpdateReplicationPacketData(entryPacket,
                    fromGateway,
                    overrideVersion,
                    previousEntryData,
                    entryHolder.getEntryData().getExpirationTime(),
                    entryHolder.getEntryData());
        } else {
            final IEntryPacket entryPacket = EntryPacketFactory.createFullPacketForReplication(entryHolder,
                    operationID);
            // Set previous entry version on entry packet for handling version conflicts on target
            if (entryHolder.hasShadow())
                entryPacket.setPreviousVersion(entryHolder.getShadow().getVersionID());

            return new UpdateReplicationPacketData(entryPacket,
                    fromGateway,
                    overrideVersion,
                    previousEntryData,
                    (short) 0,
                    entryHolder.getEntryData().getExpirationTime(),
                    entryHolder.getEntryData());
        }
    }

    private boolean isPartialUpdate(IEntryHolder entryHolder,
                                    boolean[] partialUpdatedValuesIndicators) {

        if (partialUpdatedValuesIndicators == null
                || partialUpdatedValuesIndicators.length == 0)
            return false;

        // persistent cluster always supports partial update on transient
        // entries
        if (entryHolder.isTransient())
            return true;

        // check if mirror space supports partial update
        if (_spaceEngine.getClusterPolicy()
                .getReplicationPolicy()
                .isMirrorServiceEnabled())
            return _spaceEngine.getClusterPolicy()
                    .getReplicationPolicy()
                    .getMirrorServiceConfig().supportsPartialUpdate;

        return true;
    }

    private ChangeReplicationPacketData createChangeReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID,
            Collection<SpaceEntryMutator> spaceEntryMutators,
            IEntryData previousUpdatedEntryData,
            boolean isFromGateway) {
        final int previousVersion = entryHolder.hasShadow() ? entryHolder.getShadow().getVersionID() : 0;
        final int currentVersion = entryHolder.getEntryData().getVersion();

        return new ChangeReplicationPacketData(entryHolder.getClassName(),
                entryHolder.getUID(),
                entryHolder.getEntryId(),
                currentVersion,
                previousVersion,
                entryHolder.isTransient(),
                entryHolder.getEntryData(),
                operationID,
                entryHolder.getRoutingValue() != null ? entryHolder.getRoutingValue().hashCode() : -1,
                spaceEntryMutators,
                previousUpdatedEntryData,
                entryHolder.getEntryData().getTimeToLive(false),
                entryHolder.getEntryData().getExpirationTime(),
                isFromGateway);
    }


    private IReplicationPacketEntryData createRemoveReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID,
            boolean fromGateway, boolean backupOnly) {
        if (_replicateFullTake)
            return createFullRemoveReplicationPacket(entryHolder, operationID, fromGateway);
        if (_replicateToTargetWithExternalDatasource
                && !entryHolder.isTransient())
            return createRemoveReplicationPacketForPersistency(entryHolder,
                    operationID,
                    fromGateway, backupOnly);

        return createRemoveByUIDReplicationPacket(entryHolder, operationID, fromGateway);

    }

    private RemoveByUIDReplicationPacketData createRemoveByUIDReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID, boolean fromGateway) {
        return new RemoveByUIDReplicationPacketData(entryHolder.getClassName(),
                entryHolder.getUID(),
                entryHolder.isTransient(),
                entryHolder.getEntryData(),
                operationID,
                fromGateway);
    }

    private RemoveReplicationPacketData createFullRemoveReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID, boolean fromGateway) {

        IEntryPacket entryPacket = EntryPacketFactory.createFullPacketForReplication(entryHolder, operationID);

        return new RemoveReplicationPacketData(entryPacket, fromGateway, entryHolder.getEntryData(), true);

    }

    private RemoveReplicationPacketData createRemoveReplicationPacketForPersistency(
            IEntryHolder entryHolder, OperationID operationID, boolean originGateway, boolean backupOnly) {

        IEntryPacket template = EntryPacketFactory.createRemovePacketForPersistency(entryHolder, operationID);

        return new RemoveReplicationPacketData(template, originGateway, entryHolder.getEntryData(), false, backupOnly);
    }

    private SingleReplicationPacketData createInsertNotifyReplicationPacket(
            NotifyTemplateHolder templateHolder, OperationID operationID) {
        ITemplatePacket templatePacket = TemplatePacketFactory.createFullPacketForReplication(templateHolder, operationID);
        final long expirationTime = templateHolder.getEntryData()
                .getExpirationTime();

        return new InsertNotifyTemplateReplicationPacketData(templatePacket,
                templateHolder.getUID(),
                templateHolder.getNotifyInfo(),
                expirationTime);

    }

    private RemoveNotifyTemplateReplicationPacketData createRemoveNotifyReplicationPacket(
            NotifyTemplateHolder templateHolder, OperationID operationID) {
        return new RemoveNotifyTemplateReplicationPacketData(templateHolder.getClassName(),
                templateHolder.getNotifyInfo(),
                templateHolder.getSpaceItemType(),
                templateHolder.getUID(),
                templateHolder.isTransient(),
                templateHolder.getEntryData(),
                operationID);
    }

    private ExtendEntryLeaseReplicationPacketData createExtendEntryLeaseReplicationPacket(
            IEntryHolder entryHolder, OperationID operationID, boolean isFromGateway) {
        final long expirationTime = entryHolder.getEntryData()
                .getExpirationTime();
        final long timeToLive = expirationTime - SystemTime.timeMillis();

        return new ExtendEntryLeaseReplicationPacketData(entryHolder.getClassName(),
                entryHolder.getUID(),
                entryHolder.isTransient(),
                entryHolder.getEntryData(),
                expirationTime,
                timeToLive,
                operationID,
                entryHolder.getRoutingValue() != null ? entryHolder.getRoutingValue().hashCode() : -1,
                isFromGateway);
    }

    private ExtendNotifyTemplateLeaseReplicationPacketData createExtendNotifyTemplateLeaseReplicationPacket(
            NotifyTemplateHolder entryHolder, OperationID operationID) {
        final long expirationTime = entryHolder.getEntryData()
                .getExpirationTime();
        final long timeToLive = expirationTime - SystemTime.timeMillis();
        return new ExtendNotifyTemplateLeaseReplicationPacketData(entryHolder.getClassName(),
                entryHolder.getUID(),
                entryHolder.isTransient(),
                entryHolder.getEntryData(),
                expirationTime,
                timeToLive,
                entryHolder.getSpaceItemType(),
                entryHolder.getNotifyInfo(),
                operationID);
    }

    public AbstractDataConsumeFix createFix(IDataConsumeResult errorResult, IExecutableReplicationPacketData<?> errorData) {
        // This prodocuer will always handle result of the corresponding
        // consumer class
        // Therefore this cast is safe since it is well known to both classes
        // that all error
        // results must be AbstractDataConsumeErrorResult
        AbstractDataConsumeErrorResult typedErrorResult = (AbstractDataConsumeErrorResult) errorResult;
        return typedErrorResult.createFix(_spaceEngine, this, errorData);
    }

    /**
     * Attempt to fix error that the target didn't identify as a typed error This way we can create
     * new fixes for older versions and they will dynamically load the fix
     */
    public AbstractDataConsumeFix createFixForUnknownError(Throwable error) {
        return new UnhandledErrorFix(error);
    }

    public Iterable<IReplicationFilterEntry> toFilterEntries(
            IExecutableReplicationPacketData<?> data) {
        return data.toFilterEntries(_spaceEngine.getTypeManager());
    }

    @Override
    public IEntryData getMainEntryData(IReplicationPacketEntryData data) {
        IEntryData mainEntryData = ((AbstractReplicationPacketSingleEntryData) data).getMainEntryData();
        return loadTypeDescIntoEntryIfNeeded(mainEntryData);
    }

    @Override
    public IEntryData getSecondaryEntryData(IReplicationPacketEntryData data) {
        IEntryData secondaryEntryData = ((AbstractReplicationPacketSingleEntryData) data).getSecondaryEntryData();
        return loadTypeDescIntoEntryIfNeeded(secondaryEntryData);
    }

    private IEntryData loadTypeDescIntoEntryIfNeeded(
            IEntryData entryData) {
        if (entryData == null)
            return null;
        if (entryData.getSpaceTypeDescriptor() == null) {
            if (entryData instanceof ICustomTypeDescLoader)
                ((ICustomTypeDescLoader) entryData).loadTypeDescriptor(_spaceEngine.getTypeManager());
            else
                throw new IllegalStateException("Received server entry is not of type " + ICustomTypeDescLoader.class + ", it is of type " + entryData.getClass());
        }
        return entryData;
    }

    @Override
    public String getMainTypeName(IReplicationPacketEntryData data) {
        ServerEntry mainServerEntry = getMainEntryData(data);
        if (mainServerEntry != null)
            return mainServerEntry.getSpaceTypeDescriptor().getTypeName();

        return ((AbstractReplicationPacketSingleEntryData) data).getMainTypeName();
    }

    @Override
    public <T> T getCustomContent(IReplicationPacketEntryData data) {
        return (T) ((AbstractReplicationPacketSingleEntryData) data).getCustomContent();
    }

    @Override
    public AbstractReplicationPacketSingleEntryData convertEntryData(
            IReplicationPacketEntryData entryData,
            ReplicationEntryDataConversionMetadata metadata, PlatformLogicalVersion targetMemberVersion)
            throws ReplicationPacketEntryDataConversionException {
        return ReplicationPacketDataConverter.convert(entryData, metadata, targetMemberVersion);
    }

    @Override
    public IExecutableReplicationPacketData<?> convertSingleEntryData(
            IExecutableReplicationPacketData<?> singleEntryData,
            ReplicationEntryDataConversionMetadata metadata, PlatformLogicalVersion targetMemberVersion)
            throws ReplicationPacketEntryDataConversionException {
        IReplicationPacketEntryData entryData = singleEntryData.getSingleEntryData();
        return convertEntryData(entryData, metadata, targetMemberVersion);
    }

    @Override
    public IExecutableReplicationPacketData<?> convertData(
            IExecutableReplicationPacketData<?> data,
            ReplicationMultipleOperationType convertToOperation, PlatformLogicalVersion targetMemberVersion) throws ReplicationPacketEntryDataConversionException {
        switch (data.getMultipleOperationType()) {
            case TRANSACTION_ONE_PHASE:
                break;
            case TRANSACTION_TWO_PHASE_COMMIT:
                if (convertToOperation == ReplicationMultipleOperationType.TRANSACTION_ONE_PHASE) {
                    TransactionCommitReplicationPacketData typedData = (TransactionCommitReplicationPacketData) data;
                    TransactionOnePhaseReplicationPacketData convertedData = new TransactionOnePhaseReplicationPacketData(typedData.getOperationID(), data.isFromGateway());
                    convertedData.setMetaData(typedData.getMetaData());
                    copyTransactionData(convertedData, typedData);
                    return convertedData;
                }
                break;
        }
        throw new ReplicationPacketEntryDataConversionException("Cannot convert [" + data.getMultipleOperationType() + "] to [" + convertToOperation + "]");
    }

    private IExecutableReplicationPacketData<?> createOldOnePhaseTransactionPacket(
            AbstractTransactionReplicationPacketData data) {
        TransactionReplicationPacketData convertedData = new TransactionReplicationPacketData();
        convertedData.setMetaData(data.getMetaData());
        copyTransactionData(convertedData, data);
        return convertedData;
    }

    private void copyTransactionData(
            List<IReplicationTransactionalPacketEntryData> convertedData,
            List<IReplicationTransactionalPacketEntryData> typedData) {
        for (IReplicationTransactionalPacketEntryData entryData : typedData)
            convertedData.add(entryData);
    }

    @Override
    public IExecutableReplicationPacketData<?> createEmptyMultipleEntryData(
            IExecutableReplicationPacketData<?> data) {
        return ((IExecutableReplicationPacketData) data).createEmptyMultipleEntryData();
    }

    @Override
    public void setSerializeWithFullContent(
            IReplicationPacketEntryData entryData) {
        if (entryData instanceof UpdateReplicationPacketData) {
            UpdateReplicationPacketData updateData = (UpdateReplicationPacketData) entryData;
            updateData.serializeFullContent();
        }

    }

    @Override
    public boolean requiresConversion(IReplicationPacketEntryData entryData, QueryResultTypeInternal queryResultType) {
        if (!queryResultType.isPbs())
            return false;
        if (!(entryData instanceof SingleReplicationPacketData))
            return false;
        return ((SingleReplicationPacketData) entryData).getEntryPacket().getPacketType() != TransportPacketType.PBS;
    }

    @Override
    public void completePacketDataContent(
            final IExecutableReplicationPacketData<?> data) {
        if (data.isSingleEntryData()) {
            getMainEntryData(data.getSingleEntryData());
            getSecondaryEntryData(data.getSingleEntryData());
        } else {
            for (IReplicationPacketEntryData entryData : data) {
                getMainEntryData(entryData);
                getSecondaryEntryData(entryData);
            }
        }
    }

    @Override
    public boolean isPrimary() {
        return _spaceEngine.getSpaceImpl().isPrimary();
    }
}
