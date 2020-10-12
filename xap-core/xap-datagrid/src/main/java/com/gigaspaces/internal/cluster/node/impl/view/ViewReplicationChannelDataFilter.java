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

package com.gigaspaces.internal.cluster.node.impl.view;

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
import com.gigaspaces.internal.server.storage.TemplateEntryData;
import com.gigaspaces.internal.server.storage.TemplateHolder;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;


import org.slf4j.Logger;

/**
 * @author Niv
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class ViewReplicationChannelDataFilter extends ReliableAsyncChannelDataFilter {
    private final ITemplatePacket[] _templatePackets;
    private final TemplateEntryData[] _templates;
    private final RegexCache _regexCache;
    private final SpaceTypeManager _typeManager;
    private final String _groupName;
    private final CacheManager _cacheManager;

    private final static CopyOnUpdateMap<String, Integer> _createdGroupFilterIndicator = new CopyOnUpdateMap<String, Integer>();

    public static boolean hasCreatedFilters(String groupName) {
        return _createdGroupFilterIndicator.containsKey(groupName);
    }

    public ViewReplicationChannelDataFilter(CacheManager cacheManager, String groupName, ITemplatePacket[] templates, RegexCache regexCache, SpaceTypeManager typeManager) {
        _cacheManager = cacheManager;
        _groupName = groupName;
        synchronized (_createdGroupFilterIndicator) {
            Integer existingViews = _createdGroupFilterIndicator.get(_groupName);
            if (existingViews == null)
                existingViews = 0;

            existingViews += 1;
            _createdGroupFilterIndicator.put(_groupName,
                    existingViews);
        }
        _templatePackets = templates;
        _typeManager = typeManager;
        _templates = initTemplates(_templatePackets);
        //in case of blobstore- can we use optimized clear?
        if (_cacheManager.isBlobStoreCachePolicy())
        {
            for (int i=0; i < _templatePackets.length; i++)
            {
                if (!TemplateHolder.isOptimizedForBlobStoreClear(cacheManager,_templatePackets[i], _templates[i]))
                {
                    _cacheManager.getEngine().getLocalViewRegistrations().setBlobStoreClearOptimizationNotAllowed(_templates[i].getEntryTypeDesc().getTypeDesc().getTypeName());
                    break;
                }
            }
        }
        _regexCache = regexCache;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();

        synchronized (_createdGroupFilterIndicator) {
            Integer existingCount = _createdGroupFilterIndicator.get(_groupName);
            if (existingCount == 1)
                _createdGroupFilterIndicator.remove(_groupName);
            else
                _createdGroupFilterIndicator.put(_groupName,
                        existingCount - 1);
        }
    }

    private TemplateEntryData[] initTemplates(ITemplatePacket[] templates) {
        TemplateEntryData[] result = new TemplateEntryData[templates.length];
        for (int i = 0; i < templates.length; i++)
            result[i] = new TemplateEntryData(templates[i].getTypeDescriptor(),
                    templates[i],
                    Long.MAX_VALUE /*expirationTime*/,
                    false);
        return result;
    }

    @Override
    public ReplicationChannelEntryDataFilterResult filterBeforeReplicatingEntryData(IReplicationPacketEntryData entryData, PlatformLogicalVersion targetLogicalVersion, IReplicationPacketEntryDataContentExtractor contentExtractor, Logger contextLogger, IReplicationPacketData data) {
        if(entryData.isBackupOnly()){
            return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
        }

        switch (entryData.getOperationType()) {
            case WRITE:
            case REMOVE_ENTRY:
            case ENTRY_LEASE_EXPIRED:
            case CANCEL_LEASE:
            case EXTEND_ENTRY_LEASE: {
                return filterSingleEntry(contentExtractor.getMainEntryData(entryData),
                        entryData.getOperationType());
            }
            case UPDATE:
            case CHANGE: {
                ServerEntry prevEntry = contentExtractor.getSecondaryEntryData(entryData);
                ServerEntry entry = contentExtractor.getMainEntryData(entryData);

                if (prevEntry == null)
                    contextLogger.warn(
                            "update operation of entry [" + entryData + "] is missing previous entry state, this may result with inconsistent local view state");

                return filterUpdateEntry(entry,
                        prevEntry);
            }
            case DATA_TYPE_INTRODUCE:
            case DATA_TYPE_ADD_INDEX: {
                return filterType(contentExtractor.getMainTypeName(entryData)) ? ReplicationChannelEntryDataFilterResult.PASS : ReplicationChannelEntryDataFilterResult.FILTER_DATA;
            }

            // Ignore notify templates:
            case INSERT_NOTIFY_TEMPLATE:
            case REMOVE_NOTIFY_TEMPLATE:
            case EXTEND_NOTIFY_TEMPLATE_LEASE:
            case NOTIFY_TEMPLATE_LEASE_EXPIRED:
                return ReplicationChannelEntryDataFilterResult.FILTER_DATA;

            // Allow discarded packets:
            case DISCARD:
                return ReplicationChannelEntryDataFilterResult.PASS;

            // Ignore everything else:
            case EVICT:
                return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
            default:
                return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
        }
    }

    private ReplicationChannelEntryDataFilterResult filterSingleEntry(ServerEntry entry, ReplicationSingleOperationType operationType) {
        //We are in backup space that have recently became a primary after failover, we are replicating reliable async packets that are kept in the backup
        //for primary failure. this packets have no server entry set so we send them to the local view.
        //This will always be a UID packet, which means its either extend some lease or remove entry of some sort.
        //Which means if this UID is not present in the local cache, no damage will occur to the consistency only unpleasant warning message
        //which can be fixed later.
        if (entry == null)
            return ReplicationChannelEntryDataFilterResult.PASS;

        SpaceTypeDescriptor spaceTypeDescriptor = entry.getSpaceTypeDescriptor();
        String typeName = spaceTypeDescriptor.getTypeName();
        IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(typeName);

        for (int i = 0; i < _templates.length; i++) {
            if (_templates[i].isAssignableFrom(typeDesc)) {
                if (_templates[i].match(_cacheManager,
                        entry,
                        -1, null,
                        _regexCache, _cacheManager.getCacheContext())) {
                    if (_templatePackets[i].getProjectionTemplate() == null || operationType != ReplicationSingleOperationType.WRITE)
                        return ReplicationChannelEntryDataFilterResult.PASS;

                    ReplicationEntryDataConversionMetadata metadata = new ReplicationEntryDataConversionMetadata().projectionTemplate(
                            _templatePackets[i].getProjectionTemplate());
                    return ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
                }
            }
        }
        return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
    }

    private ReplicationChannelEntryDataFilterResult filterUpdateEntry(ServerEntry entry, ServerEntry prevEntry) {
        boolean matchPrevious = false;
        boolean matchCurrent = false;
        SpaceTypeDescriptor spaceTypeDescriptor = entry.getSpaceTypeDescriptor();
        String typeName = spaceTypeDescriptor.getTypeName();
        IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(typeName);
        ITemplatePacket templatePacket = null;

        for (int i = 0; i < _templates.length; i++) {
            if (matchPrevious && matchCurrent)
                break;
            if (_templates[i].isAssignableFrom(typeDesc)) {
                if (!matchPrevious && prevEntry != null && _templates[i].match(_cacheManager,
                        prevEntry,
                        -1, null,
                        _regexCache, _cacheManager.getCacheContext())) {
                    matchPrevious = true;
                }
                if (!matchCurrent && _templates[i].match(_cacheManager,
                        entry,
                        -1, null,
                        _regexCache, _cacheManager.getCacheContext())) {
                    templatePacket = _templatePackets[i];
                    matchCurrent = true;
                }
            }
        }

        if (matchCurrent) {
            if (matchPrevious) {
                if (templatePacket.getProjectionTemplate() == null)
                    return ReplicationChannelEntryDataFilterResult.PASS;

                ReplicationEntryDataConversionMetadata metadata = new ReplicationEntryDataConversionMetadata().projectionTemplate(templatePacket.getProjectionTemplate());
                return ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
            } else {
                if (templatePacket.getProjectionTemplate() == null)
                    return ReplicationChannelEntryDataFilterResult.CONVERT_WRITE;

                ReplicationEntryDataConversionMetadata metadata = new ReplicationEntryDataConversionMetadata(ReplicationSingleOperationType.WRITE)
                        .projectionTemplate(templatePacket.getProjectionTemplate());
                return ReplicationChannelEntryDataFilterResult.getConvertToOperationResult(metadata);
            }
        }

        if (matchPrevious)
            return ReplicationChannelEntryDataFilterResult.CONVERT_REMOVE;

        return ReplicationChannelEntryDataFilterResult.FILTER_DATA;
    }

    private boolean filterType(String typeName) {
        IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(typeName);
        for (TemplateEntryData template : _templates)
            if (template.isAssignableFrom(typeDesc))
                return true;

        return false;
    }

    @Override
    public boolean filterBeforeReplicatingUnreliableOperation(IReplicationUnreliableOperation operation, PlatformLogicalVersion targetLogicalVersion) {
        // Unreliable operations should never be replicated to the local view.
        return false;
    }

    @Override
    public Object[] getConstructionArgument() {
        for (ITemplatePacket templatePacket : _templatePackets) {
            templatePacket.setSerializeTypeDesc(true);
        }
        return _templatePackets;
    }

}
