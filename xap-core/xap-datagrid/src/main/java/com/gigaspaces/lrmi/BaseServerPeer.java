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

package com.gigaspaces.lrmi;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.lrmi.ConnectionUrlDescriptor;
import com.gigaspaces.lrmi.nio.PAdapter;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.XapNetworkInfo;
import com.gigaspaces.start.kubernetes.KuberntesClusterInfo;

import java.rmi.RemoteException;

/**
 * BaseServerPeer is a convenient superclass for Server Peer classes of Protocol Adapters.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class BaseServerPeer
        implements ServerPeer {
    final private PAdapter _protocolAdapter;
    final private long _objectId;
    final private String _serviceDetails;
    final private long _objectClassLoader;
    private boolean _connected;
    private int _timeout;

    public BaseServerPeer(PAdapter protocolAdapter, long objectId, ClassLoader objectClassLoader, String serviceDetails) {
        _protocolAdapter = protocolAdapter;
        _objectId = objectId;
        _serviceDetails = serviceDetails;
        try {
            _objectClassLoader = protocolAdapter.getClassProvider().putClassLoader(objectClassLoader);
        } catch (RemoteException e) {
            throw new RuntimeException("exception caught while inserting the exported object class loader to the class provider list", e);
        }
    }

    public boolean isConnected() {
        synchronized (this) {
            return _connected;
        }
    }

    public void disconnect() {
        synchronized (this) {
            _connected = false;
        }
    }

    public Object getSecurityContext() {
        return null;
    }

    public void setSecurityContext(Object securityContext) {
        // do nothing
    }

    public int getTimeout() {
        synchronized (this) {
            return _timeout;
        }
    }

    public void setTimeout(int timeout) {
        synchronized (this) {
            _timeout = timeout;
        }
    }

    public PAdapter getProtocolAdapter() {
        return _protocolAdapter;
    }

    public long getObjectId() {
        return _objectId;
    }

    public long getObjectClassLoaderId() {
        return _objectClassLoader;
    }

    public void beforeExport(ITransportConfig config) throws RemoteException {
    }

    public void afterUnexport(boolean force) throws RemoteException {
    }

    public String getConnectionURL() {
        String internalUrl = new ConnectionUrlDescriptor(
                _protocolAdapter.getName(),
                SystemInfo.singleton().network().getPublicHostId(),
                _protocolAdapter.getPort(),
                SystemInfo.singleton().os().processId(),
                _objectId,
                _objectClassLoader,
                LRMIRuntime.getRuntime().getID(),
                _serviceDetails).toUrl();
        KuberntesClusterInfo kuberntesClusterInfo = SystemInfo.singleton().kubernetes();
        if(!kuberntesClusterInfo.isKubernetesServiceConfigured()) {
            return internalUrl;
        }
        String externalUrl = new ConnectionUrlDescriptor(
                _protocolAdapter.getName(),
                kuberntesClusterInfo.getKubernetesServiceHost(),
                _protocolAdapter.getPort(),
                SystemInfo.singleton().os().processId(),
                _objectId,
                _objectClassLoader,
                LRMIRuntime.getRuntime().getID(),
                _serviceDetails).toUrl();
        String clusterId = kuberntesClusterInfo.getKubernetesClusterId();

        return internalUrl + ";" + externalUrl + ";" + clusterId;
    }
}