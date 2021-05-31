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

package com.gigaspaces.internal.server.space.recovery;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.recovery.strategy.*;
import com.j_spaces.core.SpaceRecoveryException;

import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_LRU;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_PROP;

/**
 * RecoveryManager is responsible for the memory recovery of the space
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class RecoveryManager {


    /**
     * Number of retries in case of recovery failure
     */
    public static final int RECOVERY_RETRIES = 3;

    public SpaceImpl _space;

    /**
     * @param space
     */
    public RecoveryManager(SpaceImpl space) {
        super();
        _space = space;

    }


    public SpaceRecoverStrategy getBackupSpaceRecovery() {
        boolean isCentralDB = _space.getEngine().getCacheManager().isCentralDB();
        int cachePolicy = Integer.parseInt((_space.getEngine().getConfigReader().getSpaceProperty(
                CACHE_POLICY_PROP,
                _space.getJspaceAttr().isPersistent() ? String.valueOf(CACHE_POLICY_LRU) : String.valueOf(CACHE_POLICY_ALL_IN_CACHE))));
        if (isCentralDB || _space.getEngine().getCacheManager().isBlobStoreCachePolicy()) {
            if (cachePolicy == com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE ||
                    cachePolicy == com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_BLOB_STORE)
                return new AllInCacheCentralDBBackupSpaceRecovery(_space);

            return new LRUCentralDBBackupSpaceRecovery(_space);
        }
        return new NonCentralDBBackupSpaceRecovery(_space);
    }

    /**
     * @param spaceMode
     * @return
     * @throws SpaceRecoveryException
     */
    public SpaceRecoverStrategy getRecoveryStrategy(SpaceMode spaceMode)
            throws SpaceRecoveryException {
        if(_space.getEngine().isTieredStorage()){
            return spaceMode.equals(SpaceMode.BACKUP) ? getBackupSpaceRecovery() : new TieredStoragePrimarySpaceRecovery(_space, spaceMode);
        } else {
            switch (spaceMode) {
                case PRIMARY: {
                    return new PrimarySpaceRecovery(_space);
                }
                case BACKUP: {
                    return getBackupSpaceRecovery();

                }
                default: {
                    return new NonPrimaryBackupSpaceRecovery(_space);
                }
            }
        }
    }
}
