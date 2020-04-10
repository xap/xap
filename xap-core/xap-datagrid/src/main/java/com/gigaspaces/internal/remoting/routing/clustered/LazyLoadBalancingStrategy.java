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

package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;



/**
 * @author Niv Ingberg
 * @since 9.1.2
 */
@com.gigaspaces.api.InternalApi
public class LazyLoadBalancingStrategy extends SpaceProxyLoadBalancingStrategy {
    private volatile RemoteOperationsExecutorProxy _activeProxy;
    private volatile boolean _activeProxyInitialized;

    public LazyLoadBalancingStrategy(RemoteOperationsExecutorsCluster cluster, RemoteOperationsExecutorProxy defaultMember) {
        super(cluster);
        this._activeProxy = defaultMember;
    }

    @Override
    public RemoteOperationsExecutorProxy getCandidate(RemoteOperationRequest<?> request) {
        return _activeProxy;
    }

    @Override
    protected void updateActiveProxy(RemoteOperationsExecutorProxy newActiveProxy) {
        _activeProxy = newActiveProxy;
        if (_activeProxy != null) {
            if (_activeProxyInitialized) {
                if (_logger.isInfoEnabled())
                    _logger.info("Active server" + _cluster.getPartitionDesc() + " is updated to " + _activeProxy.getName());
            } else {
                if (_logger.isDebugEnabled())
                    _logger.debug("Active server" + _cluster.getPartitionDesc() + " is initialized to " + _activeProxy.getName());
                _activeProxyInitialized = true;
            }
        }
    }
}
