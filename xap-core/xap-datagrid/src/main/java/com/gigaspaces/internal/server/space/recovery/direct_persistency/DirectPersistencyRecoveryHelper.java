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
package com.gigaspaces.internal.server.space.recovery.direct_persistency;

import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.attribute_store.PropertiesFileAttributeStore;
import com.gigaspaces.attribute_store.TransientAttributeStore;
import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.Constants;
import com.j_spaces.kernel.SystemProperties;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import org.slf4j.Logger;

/**
 * helper functions in order to maintain direct-persistency recovery consistency
 *
 * @author yechiel
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencyRecoveryHelper implements IStorageConsistency, ISpaceModeListener {

    private final static long WAIT_FOR_ANOTHER_PRIMARY_MAX_TIME = 5 * 60 * 1000;


    private final SpaceImpl _spaceImpl;
    private final IStorageConsistency _storageConsistencyHelper;
    private final boolean useZooKeeper;
    private volatile boolean _pendingBackupRecovery;
    private final Logger _logger;
    private AttributeStore _attributeStore;
    private final String _attributeStoreKey;
    private final String _fullSpaceName;
    private final int _recoverRetries = Integer.getInteger(SystemProperties.DIRECT_PERSISTENCY_RECOVER_RETRIES,
            SystemProperties.DIRECT_PERSISTENCY_RECOVER_RETRIES_DEFAULT);

    public DirectPersistencyRecoveryHelper(SpaceImpl spaceImpl, Logger logger) {
        _spaceImpl = spaceImpl;
        _logger = logger;

        Boolean isLastPrimaryStateKeeperEnabled = Boolean.parseBoolean((String) _spaceImpl.getCustomProperties().get(Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP));
        useZooKeeper = spaceImpl.getZookeeperLastPrimaryHandler() != null;
        final SpaceEngine spaceEngine = spaceImpl.getEngine();
        _storageConsistencyHelper = spaceEngine.getCacheManager().isBlobStoreCachePolicy() && isLastPrimaryStateKeeperEnabled
                ? spaceEngine.getCacheManager().getBlobStoreRecoveryHelper()
                : new DefaultStorageConsistency();
        _fullSpaceName = spaceEngine.getFullSpaceName();
        _attributeStoreKey = spaceEngine.getSpaceName() + "." + spaceEngine.getPartitionIdOneBased() + ".primary";
        boolean isPersistent = spaceEngine.getCacheManager().isBlobStoreCachePolicy() && _storageConsistencyHelper.isPerInstancePersistency();
        if (isPersistent) {
            AttributeStore attributeStoreImpl = (AttributeStore) _spaceImpl.getCustomProperties().get(Constants.DirectPersistency.DIRECT_PERSISTENCY_ATTRIBURE_STORE_PROP);
            if (attributeStoreImpl == null) {
                if (useZooKeeper) {
                    // TODO clean this code
                    // do nothing, ZookeeperLastPrimaryHandler already started on space constructor
                } else {
                    String attributeStorePath = System.getProperty(Constants.StorageAdapter.DIRECT_PERSISTENCY_LAST_PRIMARY_STATE_PATH_PROP);
                    if (attributeStorePath == null)
                        attributeStorePath = SystemInfo.singleton().locations().work() + File.separator + this.getClass().getSimpleName();
                    _attributeStore = new PropertiesFileAttributeStore(attributeStorePath);
                }
            } else {
                _attributeStore = attributeStoreImpl;
            }
        } else {
            _attributeStore = new TransientAttributeStore();
        }
        // add DirectPersistencyRecoveryHelper as a listener to spaceMode changed events to set last primary when afterSpaceModeChange occurs
        _spaceImpl.addSpaceModeListener(this);
    }

    @Override
    public StorageConsistencyModes getStorageState() {
        return _storageConsistencyHelper.getStorageState();
    }

    @Override
    public void setStorageState(StorageConsistencyModes s) {
        _storageConsistencyHelper.setStorageState(s);
    }

    @Override
    public boolean isPerInstancePersistency() {
        return _storageConsistencyHelper.isPerInstancePersistency();
    }


    public void beforePrimaryElectionProcess() {
        if (!_storageConsistencyHelper.isPerInstancePersistency())
            return;
        StorageConsistencyModes res = _storageConsistencyHelper.getStorageState();
        boolean validStorageState = res != StorageConsistencyModes.Inconsistent;
        if (_logger.isInfoEnabled())
            _logger.info("space tested for storageconsistency - result=" + res);

        String latestPrimary = getLastPrimaryName();
        if (_logger.isInfoEnabled())
            _logger.info("space tested for latest-primary - result=" + latestPrimary);

        boolean iWasPrimary = isMeLastPrimary(latestPrimary);
        boolean iMayBePrimary = ((iWasPrimary || latestPrimary == null) && validStorageState);
        if (iMayBePrimary)
            return; //passed ok)

        if (!validStorageState && iWasPrimary) {
            if (_logger.isErrorEnabled())
                _logger.error("Inconsistent storage state but space was primary]");
            throw new DirectPersistencyRecoveryException("Failed to start [" + (_spaceImpl.getEngine().getFullSpaceName())
                    + "] inconsistent storage state but space was primary]");

        }
        if (_logger.isInfoEnabled()) {
            _logger.info("Waiting for any other space to become primary");
        }
        GSDirectPersistencyLusWaiter waiter;
        //initiate a thread that scans the lus waiting for primary to be - temp solution until election is changed
        try {
            waiter = new GSDirectPersistencyLusWaiter(_spaceImpl,
                    _spaceImpl.getClusterPolicy(),
                    _logger, _spaceImpl.getJoinManager(),
                    new Object());
        } catch (Exception ex) {
            _logger.error("Exception while initiating waiter-for-primary thread " + ex);
            if (ex instanceof DirectPersistencyRecoveryException)
                throw (DirectPersistencyRecoveryException) ex;
            throw new DirectPersistencyRecoveryException("space " + _spaceImpl.getEngine().getFullSpaceName() + " got exception while initiating waiter-for-primary thread", ex);
        }
        waiter.start();
        waiter.waitForAnotherPrimary(WAIT_FOR_ANOTHER_PRIMARY_MAX_TIME);

    }

    public boolean isInconsistentStorage() {
        return (_storageConsistencyHelper.getStorageState() == StorageConsistencyModes.Inconsistent);
    }


    private String getLastPrimaryName() {
        try {
            if(useZooKeeper){
                return _spaceImpl.getZookeeperLastPrimaryHandler().getLastPrimaryNameMemoryXtend();
            }
            else {
                return _attributeStore.get(_attributeStoreKey);
            }
        } catch (IOException e) {
            throw new DirectPersistencyAttributeStoreException("Failed to get last primary", e);
        }
    }

    private void setMeAsLastPrimary() {
        try {
            if(useZooKeeper){
                 _spaceImpl.getZookeeperLastPrimaryHandler().setMeAsLastPrimary();
            }
            else {
                _attributeStore.set(_attributeStoreKey, _fullSpaceName);
            }

            if (_logger.isInfoEnabled())
                _logger.info("Set as last primary");
        } catch (IOException e) {
            throw new DirectPersistencyAttributeStoreException("Failed to set last primary", e);
        }
    }

    public boolean isMeLastPrimary() {
        String lastPrimary = getLastPrimaryName();
        return isMeLastPrimary(lastPrimary);
    }

    private boolean isMeLastPrimary(String lastPrimary) {
        if(useZooKeeper){
            return _spaceImpl.getInstanceId().equals(lastPrimary);
        }
        else {
            return _fullSpaceName.equals(lastPrimary);
        }
    }

    @Override
    public void beforeSpaceModeChange(SpaceMode newMode) throws RemoteException {
        if (newMode == SpaceMode.PRIMARY && isPendingBackupRecovery()) {
            throw DirectPersistencyRecoveryException.createBackupNotFinishedRecoveryException(_fullSpaceName);
        }
        // mark backup as started recovery but not yet finished
        else if (newMode == SpaceMode.BACKUP) {
            setPendingBackupRecovery(true);
        }
        if (newMode == SpaceMode.PRIMARY && !useZooKeeper) {
            setMeAsLastPrimary();
        }
    }

    @Override
    public void afterSpaceModeChange(SpaceMode newMode) throws RemoteException {
    }

    public void setPendingBackupRecovery(boolean pendingRecovery) {
        this._pendingBackupRecovery = pendingRecovery;
    }

    private boolean isPendingBackupRecovery() {
        return _pendingBackupRecovery;
    }

    public void handleDirectPersistencyRecoverFailure(int retryCount) {
        if (_logger.isWarnEnabled()) {
            _logger.warn("failed during recover, retrying for the " + retryCount + " time");
        }
        if (isPendingBackupRecovery() && _recoverRetries == retryCount) {
            throw DirectPersistencyRecoveryException.createBackupNotFinishedRecoveryException(_fullSpaceName);
        }
    }

}
