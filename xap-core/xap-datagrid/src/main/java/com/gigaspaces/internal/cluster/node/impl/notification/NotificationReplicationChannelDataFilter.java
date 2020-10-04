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

package com.gigaspaces.internal.cluster.node.impl.notification;

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationUnreliableOperation;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationChannelEntryDataFilterResult;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryDataContentExtractor;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationEntryDataConversionMetadata;
import com.gigaspaces.internal.query.RegexCache;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.FlatEntryData;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.admin.TemplateInfo;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;
import com.j_spaces.core.filters.FilterManager;
import com.j_spaces.core.filters.FilterOperationCodes;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.ENTRY_LEASE_EXPIRED;
import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.REMOVE_ENTRY;
import static com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType.UPDATE;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class NotificationReplicationChannelDataFilter extends ReliableAsyncChannelDataFilter {

    public static class DurableNotificationConversionFlags {
        public static final short UPDATE_TO_UMATCHED = 1 << 0;
        public static final short UPDATE_TO_MATCHED_UPDATE = 1 << 1;
        public static final short UPDATE_TO_REMATCHED_UPDATE = 1 << 2;

        public static boolean isUpdateToUnmatched(short flag) {
            return (flag & UPDATE_TO_UMATCHED) == UPDATE_TO_UMATCHED;
        }

        public static boolean isUpdateToMatchedUpdate(short flag) {
            return (flag & UPDATE_TO_MATCHED_UPDATE) == UPDATE_TO_MATCHED_UPDATE;
        }

        public static boolean isUpdateToRematchedUpdate(short flag) {
            return (flag & UPDATE_TO_REMATCHED_UPDATE) == UPDATE_TO_REMATCHED_UPDATE;
        }

    }

    private static final Logger _spaceFilterLogger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_FILTERS);

    private enum MatchLevel {CURRENT, PREVIOUS, BOTH, NONE}

    private final NotifyActionType _notifyType;
    private final RegexCache _regexCache;
    private final SpaceTypeManager _typeManager;
    private final FilterManager _filterManager;
    private final NotifyTemplateHolder _tHolder;
    private final boolean _hasProjectionTemplate;

    // This cannot be final because template might be modified by a filter
    private TemplateEntryData _template = null;

    private final CacheManager _cacheManager;

    public NotificationReplicationChannelDataFilter(CacheManager cacheManager,
                                                    String groupName,
                                                    RegexCache regexCache,
                                                    SpaceTypeManager typeManager,
                                                    FilterManager filterManager,
                                                    NotifyTemplateHolder tHolder) {
        _cacheManager = cacheManager;
        _typeManager = typeManager;
        _tHolder = tHolder;
        _filterManager = filterManager;
        _notifyType = NotifyActionType.fromModifier(_tHolder.getNotifyType());
        _regexCache = regexCache;
        _hasProjectionTemplate = _tHolder.getProjectionTemplate() != null;
    }

    public IServerTypeDesc getTemplateTypeDesc() {
        return _tHolder.getServerTypeDesc();
    }

    public boolean isTemplateOfType(String typeName) {
        return typeName != null &&
                _tHolder.getServerTypeDesc() != null &&
                _tHolder.getServerTypeDesc().getTypeName() != null &&
                typeName.equals(_tHolder.getServerTypeDesc().getTypeName());
    }

    public TemplateInfo createTemplateInfo() {
        return new TemplateInfo(_tHolder.isFifoTemplate(),
                new Date(_tHolder.getExpirationTime()),
                _tHolder.getEntryData().getFixedPropertiesValues(),
                _tHolder.getUidToOperateBy(),
                _tHolder.getOperationModifiers());
    }

    @Override
    public ReplicationChannelEntryDataFilterResult filterBeforeReplicatingEntryData(IReplicationPacketEntryData packetEntryData, PlatformLogicalVersion targetLogicalVersion, IReplicationPacketEntryDataContentExtractor contentExtractor, Logger contextLogger, IReplicationPacketData<?> data) {

        if(packetEntryData.isBackupOnly()){
            return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
        }

        boolean isUnmatched = false;
        ReplicationChannelEntryDataFilterResult result = ReplicationChannelEntryDataFilterResult.FILTER_DATA;

        switch (packetEntryData.getOperationType()) {
            case CANCEL_LEASE:
            case ENTRY_LEASE_EXPIRED: {
                result = filterSingleEntry(contentExtractor.getMainEntryData(packetEntryData), packetEntryData.getOperationType())
                        // convert : flag for changing the entry packet to a full remove packet data later on
                        ? ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(
                        new ReplicationEntryDataConversionMetadata(ENTRY_LEASE_EXPIRED)
                                .requiresFullEntryData()
                                .resultType(_tHolder.getQueryResultType()))
                        : ReplicationChannelEntryDataFilterResult.FILTER_DATA;
                break;
            }
            case REMOVE_ENTRY: {
                boolean filterEntry = filterSingleEntry(contentExtractor.getMainEntryData(packetEntryData), packetEntryData.getOperationType());
                if (!filterEntry)
                    break;

                result = !packetEntryData.containsFullEntryData() ?
                        ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(
                                new ReplicationEntryDataConversionMetadata(REMOVE_ENTRY)
                                        .requiresFullEntryData()
                                        .resultType(_tHolder.getQueryResultType())) :
                        ReplicationChannelEntryDataFilterResult.PASS;

                break;
            }
            case WRITE: {
                result = filterSingleEntry(contentExtractor.getMainEntryData(packetEntryData),
                        packetEntryData.getOperationType()) ? ReplicationChannelEntryDataFilterResult.PASS
                        : ReplicationChannelEntryDataFilterResult.FILTER_DATA;
                break;
            }
            case UPDATE:
            case CHANGE: {
                final boolean isUpdate = packetEntryData.getOperationType() == ReplicationSingleOperationType.UPDATE;
                IEntryData entryBeforeUpdate = contentExtractor.getSecondaryEntryData(packetEntryData);
                IEntryData entryAfterUpdate = contentExtractor.getMainEntryData(packetEntryData);

                if (entryBeforeUpdate == null && contextLogger.isWarnEnabled()) {
                    String operation = isUpdate ? "update" : "change";
                    contextLogger.warn(operation + " operation of entry [" + packetEntryData + "] is missing previous entry state, this may result with inconsistent durable notification state");
                }

                ReplicationEntryDataConversionMetadata metadata = new ReplicationEntryDataConversionMetadata(UPDATE)
                        .resultType(_tHolder.getQueryResultType());

                MatchLevel matchLevel = filterUpdateEntry(entryAfterUpdate, entryBeforeUpdate, contextLogger, packetEntryData.getOperationType());

                switch (matchLevel) {
                    case BOTH:
                        if (_notifyType.isRematchedUpdate())
                            metadata.flags(DurableNotificationConversionFlags.UPDATE_TO_REMATCHED_UPDATE);

                        if (!packetEntryData.containsFullEntryData())
                            metadata.requiresFullEntryData();
                        else
                            metadata.entryData(entryAfterUpdate);

                        if (_tHolder.getNotifyInfo().isReturnPrevValue()) {
                            metadata.requiresPrevValue();
                            result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
                        } else if (!(packetEntryData.containsFullEntryData() && !_notifyType.isRematchedUpdate() && isUpdate)) {
                            result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
                        } else
                            result = ReplicationChannelEntryDataFilterResult.PASS;

                        break;
                    case CURRENT:
                        if (_notifyType.isMatchedUpdate())
                            metadata.flags(DurableNotificationConversionFlags.UPDATE_TO_MATCHED_UPDATE);

                        if (!packetEntryData.containsFullEntryData())
                            metadata.requiresFullEntryData();
                        else
                            metadata.entryData(entryAfterUpdate);

                        if (_tHolder.getNotifyInfo().isReturnPrevValue()) {
                            metadata.requiresPrevValue();
                            result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
                        } else if (!(packetEntryData.containsFullEntryData() && !_notifyType.isMatchedUpdate() && isUpdate)) {
                            result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
                        } else
                            result = ReplicationChannelEntryDataFilterResult.PASS;

                        break;
                    case PREVIOUS:

                        isUnmatched = true;

                        metadata.flags(DurableNotificationConversionFlags.UPDATE_TO_UMATCHED)
                                .entryData(entryBeforeUpdate);

                        if (_tHolder.getNotifyInfo().isReturnPrevValue()) {
                            metadata.requiresPrevValue();
                        }

                        result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
                        break;
                    case NONE:
                        result = ReplicationChannelEntryDataFilterResult.FILTER_DATA;
                        break;
                    default:
                        throw new ReplicationInternalSpaceException("Unknown match level handling " + matchLevel);

                }

                break;
            }
            // Ignore everything else:
            default:
                result = ReplicationChannelEntryDataFilterResult.FILTER_DATA;
        }

        // trigger filters
        if (result.getFilterOperation() != FilterOperation.FILTER_DATA &&
                _filterManager._isFilter[FilterOperationCodes.BEFORE_NOTIFY_TRIGGER]) {

            IEntryData entryData = isUnmatched ? contentExtractor.getSecondaryEntryData(packetEntryData)
                    : contentExtractor.getMainEntryData(packetEntryData);

            String uid = packetEntryData.getUid();
            boolean isTransient = packetEntryData.isTransient();

            NotificationReplicationSpaceFilterEntry filterEntry = null;

            if (_filterManager.hasFilterRequiresFullSpaceFilterEntry(FilterOperationCodes.BEFORE_NOTIFY_TRIGGER)) {
                if (packetEntryData.getOperationType() == ReplicationSingleOperationType.UPDATE &&
                        !packetEntryData.containsFullEntryData() &&
                        !isUnmatched)
                    filterEntry = new NotificationReplicationSpaceFilterEntry(entryData,
                            contentExtractor.getSecondaryEntryData(packetEntryData),
                            uid,
                            isTransient);
                else
                    filterEntry = new NotificationReplicationSpaceFilterEntry(entryData,
                            uid,
                            isTransient);
            }


            Object[] arguments = new Object[]{filterEntry, _tHolder};
            try {
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_NOTIFY_TRIGGER, null, arguments);
            } catch (RuntimeException e) {
                if (_spaceFilterLogger.isDebugEnabled())
                    _spaceFilterLogger.debug("Exception was thrown by filter on BEFORE_NOTIFY_TRIGGER.", e);

                return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
            }

            if (filterEntry != null && filterEntry.isModified()) {

                IEntryData modifiedEntryData = new FlatEntryData(filterEntry.getFieldsValues(),
                        filterEntry.getDynamicValues(),
                        entryData.getEntryTypeDesc(),
                        entryData.getVersion(),
                        entryData.getExpirationTime(),
                        null);


                ReplicationEntryDataConversionMetadata metadata;

                if (result.getFilterOperation() == FilterOperation.PASS)
                    metadata = new ReplicationEntryDataConversionMetadata(packetEntryData.getOperationType())
                            .entryData(modifiedEntryData)
                            .resultType(_tHolder.getQueryResultType());
                else
                    metadata = result.getConversionMetadata().entryData(modifiedEntryData);

                metadata.modifiedByFilter();

                result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
            }
        }

        // here we assume that if the packet is passed than we do not need
        // to convert it to a full packet meaning it is alreay full
        // and contains the entry packet which in turn will allow us
        // to extract the QueryResultTypeInternal
        if (result.getFilterOperation() == FilterOperation.PASS) {
            if (contentExtractor.requiresConversion(packetEntryData, _tHolder.getQueryResultType())) {
                ReplicationEntryDataConversionMetadata metadata = new ReplicationEntryDataConversionMetadata().resultType(_tHolder.getQueryResultType());
                result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
            }
        }
        //Apply template projection if needed
        if (_hasProjectionTemplate) {
            if (result.getFilterOperation() == FilterOperation.PASS) {
                ReplicationEntryDataConversionMetadata metadata = new ReplicationEntryDataConversionMetadata().projectionTemplate(_tHolder.getProjectionTemplate());
                result = ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
            } else if (result.getFilterOperation() == FilterOperation.CONVERT) {
                //This piece of code assumes that the metadata is not a cached type and therefore can be modified
                result.getConversionMetadata().projectionTemplate(_tHolder.getProjectionTemplate());
            }
        }

        return result;
    }

    private boolean filterSingleEntry(ServerEntry entry, ReplicationSingleOperationType operationType) {
        //We are in backup space that have recently became a primary after failover, we are replicating reliable async packets that are kept in the backup
        //for primary failure. this packets have no server entry set so we send them to the local view.
        //This will always be a UID packet, which means its either extend some lease or remove entry of some sort.
        //Which means if this UID is not present in the local cache, no damage will occur to the consistency only unpleasant warning message
        //which can be fixed later.
        if (entry == null)
            return true;

        if (!matchNotifyActionType(operationType))
            return false;

        SpaceTypeDescriptor spaceTypeDescriptor = entry.getSpaceTypeDescriptor();
        String typeName = spaceTypeDescriptor.getTypeName();
        IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(typeName);

        if (getTemplate().isAssignableFrom(typeDesc)) {
            if (getTemplate().match(_cacheManager, entry, -1, null, _regexCache, _cacheManager.getCacheContext()))
                return true;
        }
        return false;

    }

    private MatchLevel filterUpdateEntry(ServerEntry entry, ServerEntry prevEntry, Logger contextLogger, ReplicationSingleOperationType operationType) {
        if (!matchNotifyActionType(operationType))
            return MatchLevel.NONE;

        boolean matchPrevious = false;
        boolean matchCurrent = false;
        SpaceTypeDescriptor spaceTypeDescriptor = entry.getSpaceTypeDescriptor();
        String typeName = spaceTypeDescriptor.getTypeName();
        IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(typeName);

        if (getTemplate().isAssignableFrom(typeDesc)) {
            if (!matchPrevious && prevEntry != null && getTemplate().match(_cacheManager, prevEntry, -1, null, _regexCache, _cacheManager.getCacheContext()))
                matchPrevious = true;
            if (!matchCurrent && getTemplate().match(_cacheManager, entry, -1, null, _regexCache, _cacheManager.getCacheContext()))
                matchCurrent = true;
        }

        MatchLevel result = MatchLevel.NONE;

        if (matchPrevious && matchCurrent)
            result = MatchLevel.BOTH;
        else if (matchPrevious)
            result = MatchLevel.PREVIOUS;
        else if (matchCurrent)
            result = MatchLevel.CURRENT;

        if ((result == MatchLevel.PREVIOUS && !_notifyType.isUnmatched()) ||
                (result == MatchLevel.BOTH && !(_notifyType.isUpdate() || _notifyType.isRematchedUpdate())) ||
                (result == MatchLevel.CURRENT && !(_notifyType.isUpdate() || _notifyType.isMatchedUpdate())))
            result = MatchLevel.NONE;

        return result;
    }

    public synchronized void initTemplate() {
        if (_template != null)
            throw new IllegalStateException("Template has already been initiated");

        // TODO durable notifications : customQuery cannot be changed in filter 
        // filter only exposes the dynamic properties which are empty
        // the same behaviour also applies to regular notifications        
        ITemplatePacket templatePacket = _tHolder.getGenerationTemplate().clone();
        templatePacket.setFieldsValues(_tHolder.getEntryData().getFixedPropertiesValues());
        templatePacket.setCustomQuery(_tHolder.getCustomQuery());

        _template = new TemplateEntryData(templatePacket.getTypeDescriptor(), templatePacket, Long.MAX_VALUE /*expirationTime*/, false);

    }

    private TemplateEntryData getTemplate() {
        if (_template == null)
            synchronized (this) {
                // this could only mean we need to flush the cache
                return _template;
            }

        return _template;
    }

    private boolean matchNotifyActionType(ReplicationSingleOperationType operationType) {
        switch (operationType) {
            case REMOVE_ENTRY:
                return _notifyType.isTake();
            case UPDATE:
            case CHANGE:
                return _notifyType.isUpdate() || _notifyType.isUnmatched() || _notifyType.isMatchedUpdate() || _notifyType.isRematchedUpdate();
            case WRITE:
                return _notifyType.isWrite();
            case ENTRY_LEASE_EXPIRED:
            case CANCEL_LEASE:
                return _notifyType.isLeaseExpiration();
            default:
                return false;
        }
    }

    @Override
    public boolean filterBeforeReplicatingUnreliableOperation(IReplicationUnreliableOperation operation, PlatformLogicalVersion targetLogicalVersion) {
        return false;
    }

    @Override
    public void filterAfterReplicatedEntryData(
            IReplicationPacketEntryData data,
            PlatformLogicalVersion targetLogicalVersion,
            IReplicationPacketEntryDataContentExtractor contentExtractor,
            Logger contextLogger) {
        // trigger filters
        if (_filterManager._isFilter[FilterOperationCodes.AFTER_NOTIFY_TRIGGER]) {
            NotificationReplicationSpaceFilterEntry filterEntry = null;

            if (_filterManager.hasFilterRequiresFullSpaceFilterEntry(FilterOperationCodes.AFTER_NOTIFY_TRIGGER)) {
                filterEntry = new NotificationReplicationSpaceFilterEntry(contentExtractor.getMainEntryData(data),
                        data.getUid(),
                        data.isTransient());
            }

            Object[] arguments = new Object[]{filterEntry, _tHolder};
            try {
                _filterManager.invokeFilters(FilterOperationCodes.AFTER_NOTIFY_TRIGGER, null, arguments);
            } catch (RuntimeException e) {
                if (_spaceFilterLogger.isDebugEnabled())
                    _spaceFilterLogger.debug("Exception was thrown by filter on AFTER_NOTIFY_TRIGGER.", e);
            }
        }
    }

    @Override
    public boolean filterBeforeReplicatingEntryDataHasSideEffects() {
        return true;
    }

    @Override
    public Object[] getConstructionArgument() {
        ITemplatePacket generationTemplate = _tHolder.getGenerationTemplate().clone();
        generationTemplate.setSerializeTypeDesc(true);
        return new Object[]
                {
                        generationTemplate,
                        _tHolder.getNotifyInfo(),
                        _tHolder.getEventId()
                };
    }

    public NotifyTemplateHolder getNotifyTemplateHolder() {
        return _tHolder;
    }


}
