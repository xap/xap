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
package com.j_spaces.core;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.TemplateHolderFactory;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.AbstractCacheManager;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.EntriesIter;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreRefCacheInfo;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import net.jini.space.InternalSpaceException;

import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_OFFHEAP_ENABLED_PROP;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_BLOBSTORE_OFFHEAP_MAXSIZE_PROP;

/**
 * @author Rotem Herzberg
 * @since 12.3
 */
public class OffHeapMemoryManager {

    private final CacheManager _cacheManager;
    private final boolean _enabled;
    private final Long _threshold;
    private final Logger _logger;

    private final String _spaceName;
    private final String _containerName;

    OffHeapMemoryManager(String spaceName, String containerName, AbstractCacheManager cacheManager, SpaceConfigReader configReader) {
        _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_OFFHEAP_MEMORYMANAGER + "." + spaceName);

        boolean _offHeapOptimizationEnabled = configReader.getBooleanSpaceProperty(CACHE_MANAGER_BLOBSTORE_OFFHEAP_ENABLED_PROP,"false");
        if (!cacheManager.isBlobStoreCachePolicy() && _offHeapOptimizationEnabled) {
            throw new RuntimeException("Can not enable Off Heap optimization when cache policy is not Blob Store");
        }
        if(_logger.isLoggable(Level.CONFIG)) {
            _logger.config("space-config.engine.blobstore.offheap.enabled=" + _offHeapOptimizationEnabled);
        }

        String offHeapThreshold = configReader.getSpaceProperty(CACHE_MANAGER_BLOBSTORE_OFFHEAP_MAXSIZE_PROP,"");
        Long _offHeapThreshold = (offHeapThreshold.isEmpty()) ? null : StringUtils.parseStringAsBytes(offHeapThreshold);

        if(_logger.isLoggable(Level.CONFIG)) {
            if (!offHeapThreshold.isEmpty()) {
                _logger.config("space-config.engine.blobstore.offheap.max_memory_size=" + _offHeapThreshold);
            }
        }
        if(offHeapThreshold.isEmpty() && _offHeapOptimizationEnabled) {
            throw new RuntimeException("Configuration exception: in order to enable off heap optimization you must define a threshold for the max off-heap size");
        }

        _spaceName = spaceName;
        _containerName = containerName;
        _threshold = _offHeapThreshold;
        _enabled = _offHeapOptimizationEnabled && (_threshold != null) && (cacheManager instanceof CacheManager);
        _cacheManager = (_enabled) ? (CacheManager) cacheManager : null;

        if(_logger.isLoggable(Level.CONFIG)) {
            if (_enabled) {
                _logger.config("Off Heap Memory Manager is enabled [threshold=" + _threshold + "]");
            }
        }
    }

    public boolean isEnabled() {
        return _enabled;
    }

    public void canAllocate() {
        if (!_enabled) {
            return;
        }
        long bytesUsed = _cacheManager.getBlobStoreInternalCache().getOffHeapByteCounter().getCount();
        if (bytesUsed >= _threshold) {
            long used = bytesUsed / (1024*1024);
            long max = _threshold / (1024*1024);
            String msg = "Off Heap Memory shortage at: host: " + SystemInfo.singleton().network().getHostId() + ", space " + _spaceName
                    + ", container " + _containerName + ", total off heap memory: " + max + " mb, used off heap memory: " + used + " mb";
            throw new OffHeapMemoryShortageException(msg, _spaceName, _containerName, SystemInfo.singleton().network().getHostId(), bytesUsed, _threshold);
        }
    }

    public void close() {
        if(!_enabled || _cacheManager.getBlobStoreInternalCache().getOffHeapByteCounter().getCount() == 0)
            return;

        Context context = null;
        EntriesIter entriesIter = null;
        try {
            context = _cacheManager.getCacheContext();
            for (IServerTypeDesc serverTypeDesc : _cacheManager.getTypeManager().getSafeTypeTable().values()) {
                try {
                    if (!serverTypeDesc.isRootType() && !serverTypeDesc.getTypeDesc().isInactive() && serverTypeDesc.getTypeDesc().isBlobstoreEnabled()) {
                        ITemplateHolder templateHolder = TemplateHolderFactory.createTemplateHolder(serverTypeDesc, new TemplatePacket(serverTypeDesc.getTypeDesc()), _cacheManager.getEngine().generateUid(), Long.MAX_VALUE);
                        entriesIter = (EntriesIter) _cacheManager.makeEntriesIter(context, templateHolder, serverTypeDesc, 0, SystemTime.timeMillis(), false);
                        entriesIter.setBringCacheInfoOnly(true);
                        while (true) {
                            IEntryCacheInfo entryCacheInfo = entriesIter.nextEntryCacheInfo();
                            if (entryCacheInfo == null) {
                                break;
                            }
                            if (entryCacheInfo.isBlobStoreEntry()) {
                                IBlobStoreRefCacheInfo entry = (IBlobStoreRefCacheInfo) entryCacheInfo;
                                entry.freeOffHeap(_cacheManager.getBlobStoreInternalCache().getOffHeapByteCounter());
                            }
                        }
                    }
                    if (entriesIter != null) {
                        entriesIter.close();
                        entriesIter = null;
                    }
                } catch (SAException ex) {
                    _logger.log(Level.WARNING,"caught exception while cleaning offheap entries during shutdown", ex);
                }
            }
            if(_cacheManager.getBlobStoreInternalCache().getOffHeapByteCounter().getCount() != 0){
             _logger.log(Level.WARNING,"offheap used bytes still consumes "+_cacheManager.getBlobStoreInternalCache().getOffHeapByteCounter().getCount()+" bytes");
            }
        }
        finally {
            if (context != null) {
                _cacheManager.freeCacheContext(context);
            }
        }
    }
}
