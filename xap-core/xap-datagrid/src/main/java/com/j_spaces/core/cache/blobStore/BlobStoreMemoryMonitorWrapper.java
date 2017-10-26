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


package com.j_spaces.core.cache.blobStore;

import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.cache.CacheManager;

import java.util.Properties;

/**
 * Wrapper for BlobStoreMemoryMonitor
 *
 * @author Yechiel
 * @since 10.1.0
 */

public class BlobStoreMemoryMonitorWrapper extends BlobStoreMemoryMonitor {
    private final BlobStoreMemoryMonitor _impl;
    private final String _spaceNameToUse;
    private final String _containerNameToUse;

    public BlobStoreMemoryMonitorWrapper(BlobStoreMemoryMonitor impl, CacheManager cm, String spaceName, Properties properties) {
        super();
        _impl = impl;
        _spaceNameToUse = cm.getEngine().getSpaceName();
        _containerNameToUse = cm.getEngine().getSpaceImpl().getContainerName();
    }

    @Override
    public void onMemoryAllocation(java.io.Serializable id) {
        try {
            if (_impl != null)
                _impl.onMemoryAllocation(id);
        } catch (BlobStoreMemoryShortageException ex) {
            throw new SpaceInternalBlobStoreMemoryShortageException(_spaceNameToUse, _containerNameToUse, SystemInfo.singleton().network().getHostId(), ex.getMemoryUsage(), ex.getMaxMemory(), ex);
        }
    }
}
