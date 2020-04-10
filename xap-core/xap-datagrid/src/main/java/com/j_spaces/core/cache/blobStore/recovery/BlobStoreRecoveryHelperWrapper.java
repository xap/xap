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

import com.gigaspaces.internal.server.space.recovery.direct_persistency.DefaultStorageConsistency;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.IStorageConsistency;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.StorageConsistencyModes;
import com.gigaspaces.server.blobstore.BlobStoreStorageHandler;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * helper functions in order to maintain blobstore recovery consistency
 *
 * @author yechiel
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreRecoveryHelperWrapper implements IStorageConsistency {
    private final Logger _logger;
    private final BlobStoreRecoveryHelper _original;
    private final IStorageConsistency _default;
    private final String _spaceName;

    public BlobStoreRecoveryHelperWrapper(Logger logger, String spaceName, BlobStoreStorageHandler driver, int numberOfPartitions, int numberOfBackups) {
        Properties p = driver.getProperties();
        _original = p != null ? ((BlobStoreRecoveryHelper) p.get(BlobStoreRecoveryHelper.BLOB_STORE_RECOVERY_HELPER_PROPERTY_NAME)) : null;
        //_original =new BlobStoreRecoveryHelperMock();
        _spaceName = spaceName;
        _logger = logger;
        if (_original != null)
            _original.initialize(_spaceName, numberOfPartitions, numberOfBackups);
        _default = new DefaultStorageConsistency();
    }

    @Override
    public StorageConsistencyModes getStorageState() {
        if (!isPerInstancePersistency())
            return _default.getStorageState();

        StorageConsistencyModes res = _original.getStorageState();
        if (_logger.isInfoEnabled()) {
            _logger.info("blob-store getStorageState result=" + res + " space=" + _spaceName);
        }
        return res;

    }

    @Override
    public void setStorageState(StorageConsistencyModes s) {
        if (!isPerInstancePersistency()) {
            _default.setStorageState(s);
            return;
        }

        _original.setStorageState(s);
        if (_logger.isInfoEnabled()) {
            _logger.info("blob-store setStorageState to " + s + " space=" + _spaceName);
        }
    }

    @Override
    public boolean isPerInstancePersistency() {
        return (_original == null) ? false : _original.isPerInstancePersistency();
    }

}
