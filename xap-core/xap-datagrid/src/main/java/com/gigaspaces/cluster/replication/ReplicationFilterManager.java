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

package com.gigaspaces.cluster.replication;

import com.gigaspaces.cluster.activeelection.ISpaceComponentsHandler;
import com.gigaspaces.cluster.activeelection.SpaceComponentsInitializeException;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.ReplicationFilterProvider;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.ReplicationPolicy.ReplicationPolicyDescription;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Replication Filter Manager.
 *
 * @author Yechiel Fefer
 * @version 1.0
 **/
@com.gigaspaces.api.InternalApi
public class ReplicationFilterManager implements ISpaceComponentsHandler {
    private ReplicationFilterWrapper _inputFilterWrapper;
    private ReplicationFilterWrapper _outputWrapper;
    private IJSpace _space;
    protected ReplicationPolicy _replicationPolicy;
    private ReplicationPolicy.ReplicationPolicyDescription _replicationDescription;

    // If set to true - filters will be initialized only on primary space
    private boolean _startIfPrimaryOnly;
    // If set to true space will be shutdown in case of filter init() failure
    private boolean _shutdownSpaceOnInitFailure;

    //logger
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_REPLICATION);

    public ReplicationFilterManager(ReplicationPolicy replPolicy, SpaceURL spaceURL, IJSpace spaceProxy) {
        _replicationPolicy = replPolicy;
        _replicationDescription = replPolicy.m_ReplMemberPolicyDescTable.get(replPolicy.m_OwnMemberName);
        _space = spaceProxy;

        //check if filters are built from ReplicationFiltertProvider or from cluster schema
        if (spaceURL != null) {
            ReplicationFilterProvider replicationFilterProvider = (ReplicationFilterProvider) spaceURL.getCustomProperties()
                    .get(Constants.ReplicationFilter.REPLICATION_FILTER_PROVIDER);

            if (replicationFilterProvider != null)
                buildFilters(replicationFilterProvider);
            else
                buildFilters(_replicationDescription);
        } else {
            buildFilters(_replicationDescription);
        }
    }

    private void buildFilters(
            ReplicationFilterProvider replicationFilterProvider) {
        _logger.log(Level.FINE, "Using replication filters injected using replication filter provider, will not use definitions in cluster schema");
        if (replicationFilterProvider.getInputFilter() != null) {
            _inputFilterWrapper = new ReplicationFilterWrapper(replicationFilterProvider.getInputFilter());
        }
        if (replicationFilterProvider.getOutputFilter() != null) {
            if (replicationFilterProvider.getInputFilter() == replicationFilterProvider.getOutputFilter())
                _outputWrapper = _inputFilterWrapper;
            else
                _outputWrapper = new ReplicationFilterWrapper(replicationFilterProvider.getOutputFilter());
        }
        _startIfPrimaryOnly = !replicationFilterProvider.isActiveWhenBackup();
        _shutdownSpaceOnInitFailure = replicationFilterProvider.isShutdownSpaceOnInitFailure();
        return;
    }

    private void buildFilters(ReplicationPolicyDescription replicationPolicyDescription) {
        if (replicationPolicyDescription.inputReplicationFilterClassName != null) {
            // input replication filter
            _inputFilterWrapper = new ReplicationFilterWrapper(_replicationDescription.inputReplicationFilterClassName);
        }

        if (_replicationDescription.outputReplicationFilterClassName != null) {
            if (isInputFilterEnabled() && _inputFilterWrapper.getClassName().equals(_replicationDescription.outputReplicationFilterClassName)) {
                // same filter object for both
                _outputWrapper = _inputFilterWrapper;
            } else {
                // output replication filter
                _outputWrapper = new ReplicationFilterWrapper(_replicationDescription.outputReplicationFilterClassName);
            }
        }

        _startIfPrimaryOnly = !_replicationDescription.activeWhenBackup;
        _shutdownSpaceOnInitFailure = _replicationDescription.shutdownSpaceOnInitFailure;
    }

    public void close() {
        if (isInputFilterEnabled()) {
            _inputFilterWrapper.close();
        }

        if (isOutputFilterEnabled()) {
            _outputWrapper.close();
        }
    }

    /**
     * @return <code>true</code> if Replication Output Filter enabled
     */
    public boolean isOutputFilterEnabled() {
        return _outputWrapper != null;
    }

    /**
     * @return <code>true</code> if Replication Input Filter enabled
     */
    public boolean isInputFilterEnabled() {
        return _inputFilterWrapper != null;
    }


    /*
     * @see com.j_spaces.core.ISpaceComponentsHandler#initComponents(boolean)
     */
    public void initComponents(boolean primaryOnly)
            throws SpaceComponentsInitializeException {
        if (primaryOnly != _startIfPrimaryOnly)
            return;

        if (isInputFilterEnabled())
            initFilter(_inputFilterWrapper, _replicationDescription.inputReplicationFilterParamUrl);

        if (isOutputFilterEnabled())
            initFilter(_outputWrapper, _replicationDescription.outputReplicationFilterParamUrl);
    }

    /**
     * initialize given replication filter
     */
    private void initFilter(ReplicationFilterWrapper filterHolder, String url) throws SpaceComponentsInitializeException {
        try {
            filterHolder.init(_space, url, _replicationPolicy);

        } catch (RuntimeException re) {
            // Check if space needs to be closed 
            if (_shutdownSpaceOnInitFailure) {
                throw new SpaceComponentsInitializeException("Failed to initialize replication filter - " + filterHolder.getClassName(), re);
            }
        }

    }

    /*
     * @see com.j_spaces.core.ISpaceComponentsHandler#startComponents(boolean)
     */
    public void startComponents(boolean primaryOnly) {
    }

    /*
     * @see com.gigaspaces.cluster.activeelection.ISpaceComponentsHandler#isRecoverySupported()
     */
    public boolean isRecoverySupported() {
        return true;
    }

    @Override
    public void close(boolean primaryOnly) {
        if (primaryOnly != _startIfPrimaryOnly)
            return;

        close();
    }


    public ReplicationFilterWrapper getInputFilter() {
        return _inputFilterWrapper;
    }

    public ReplicationFilterWrapper getOutputFilter() {
        return _outputWrapper;
    }


}