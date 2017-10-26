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

//
package com.j_spaces.core.cache.blobStore.recovery;

import com.gigaspaces.internal.server.space.recovery.direct_persistency.StorageConsistencyModes;

/**
 * Created by yechielf on 09/03/2015.
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreRecoveryHelperMock extends BlobStoreRecoveryHelper {

    private String _spaceName;

    @Override
    public StorageConsistencyModes getStorageState() {
        int f = _spaceName.indexOf('_');
        f = _spaceName.indexOf('_', f + 1);
        return
                f >= 0 ? StorageConsistencyModes.Inconsistent : StorageConsistencyModes.Consistent;
    }


    @Override
    public boolean isPerInstancePersistency() {
        return true;
    }

    @Override
    public void initialize(String spaceName, int numberOfPartitions, int numberOfBackups) {
        _spaceName = spaceName;
    }

}
