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


import com.gigaspaces.internal.server.space.recovery.direct_persistency.IStorageConsistency;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.StorageConsistencyModes;

/**
 * helper functions in order to maintain blobstore recovery consistency
 *
 * @author yechiel
 * @since 10.1
 */
public abstract class BlobStoreRecoveryHelper implements IStorageConsistency {
    public static String BLOB_STORE_RECOVERY_HELPER_PROPERTY_NAME = "BlobStoreRecoveryHelper";

    public StorageConsistencyModes getStorageState() {
        return StorageConsistencyModes.Consistent;
    }

    public void setStorageState(StorageConsistencyModes s) {
    }

    public boolean isPerInstancePersistency() {
        return false;
    }

    public void initialize(String spaceName, int numberOfPartitions, int numberOfBackups) {

    }
}
