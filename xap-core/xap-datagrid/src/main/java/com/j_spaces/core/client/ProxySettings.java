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

package com.j_spaces.core.client;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.lrmi.stubs.LRMISpaceImpl;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.RemoteStub;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.IJSpaceContainer;
import com.j_spaces.core.IStubHandler;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.cluster.LBProxyHolder;
import com.j_spaces.kernel.SystemProperties;
import net.jini.id.Uuid;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

/**
 * Essential proxy settings. This class is not serializable and is not passed between the space and
 * the proxy
 *
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class ProxySettings implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private static final int SERIAL_VERSION = 1;

    private static final String _codebase = System.getProperty("java.rmi.server.codebase");
    /**
     * time-provider for ITimeProvider implementation (TODO [@author moran] use properties object
     * when introduced).
     */
    private static final String _timeProvider = System.getProperty(SystemProperties.SYSTEM_TIME_PROVIDER);

    private final IRemoteSpace _remoteSpace;
    private final SpaceSettings _spaceSettings;
    private final boolean _isCollocated;
    private final SpaceImpl _spaceImpl;
    private final int _uuidHashCode;
    private final String _memberName;
    private final Properties _customProperties;
    private final SpaceClusterInfo _clusterInfo;
    private final SpaceClusterInfo _nonClusterInfo;

    private SpaceURL _finderURL;
    private transient int _updateModifiers;
    private transient int _readModifiers;
    private transient boolean _fifo;
    private transient boolean _isVersioned;
    private transient boolean _gatewayProxy;
    private transient LBProxyHolder _oldProxyHolder;

    public ProxySettings(IRemoteSpace remoteSpace, SpaceSettings spaceSettings) {
        this._remoteSpace = remoteSpace;
        this._spaceSettings = spaceSettings;
        this._memberName = _spaceSettings.getMemberName();
        this._uuidHashCode = _spaceSettings.getUuid().hashCode();
        this._clusterInfo = spaceSettings.getSpaceConfig().getClusterInfo();
        this._nonClusterInfo = _clusterInfo.isClustered() ? new SpaceClusterInfo(null, _memberName) : _clusterInfo;
        this._nonClusterInfo.setTopology(_clusterInfo.getTopology());
        this._isCollocated = RemoteStub.isCollocatedStub(_remoteSpace);
        if (_remoteSpace instanceof LRMISpaceImpl) {
            LRMISpaceImpl lrmiSpaceImpl = (LRMISpaceImpl) _remoteSpace;
            _spaceImpl = lrmiSpaceImpl.isEmbedded() ? (SpaceImpl) lrmiSpaceImpl.getProxy() : null;
        } else
            _spaceImpl = null;
        _customProperties = new Properties();
        Properties serverProperties = getSpaceURL().getCustomProperties();
        if (serverProperties != null)
            _customProperties.putAll(serverProperties);
    }

    public Uuid getUuid() {
        return _spaceSettings.getUuid();
    }

    public int getSerializationType() {
        return _spaceSettings.getSerializationType();
    }

    public String getContainerName() {
        return _spaceSettings.getContainerName();
    }

    public String getSpaceName() {
        return _spaceSettings.getSpaceName();
    }

    public IStubHandler getStubHandler() {
        return _spaceSettings.getStubHandler();
    }

    public IJSpaceContainer getContainerProxy() {
        return _spaceSettings.getContainerProxy();
    }

    public SpaceConfig getSpaceAttributes() {
        return _spaceSettings.getSpaceConfig();
    }

    public SpaceURL getSpaceURL() {
        return _spaceSettings.getSpaceURL();
    }

    public boolean isSecuredService() {
        return _spaceSettings.isSecuredService();
    }

    public ITransportConfig getExportedTransportConfig() {
        return _spaceSettings.getExportedTransportConfig();
    }

    public SpaceURL getFinderURL() {
        return _finderURL;
    }

    public ProxySettings setFinderURL(SpaceURL finderURL) {
        _finderURL = finderURL;
        if (finderURL != null && finderURL.getCustomProperties() != null)
            _customProperties.putAll(finderURL.getCustomProperties());
        return this;
    }

    public String getTimeProvider() {
        return _timeProvider;
    }

    public String getCodebase() {
        return _codebase;
    }

    public String getMemberName() {
        return _memberName;
    }

    public int getUUIDHashCode() {
        return _uuidHashCode;
    }

    public IRemoteSpace getRemoteSpace() {
        return _remoteSpace;
    }

    public SpaceImpl getSpaceImplIfEmbedded() {
        return _spaceImpl;
    }

    public LBProxyHolder getOldProxyHolder() {
        if (_oldProxyHolder == null)
            _oldProxyHolder = new LBProxyHolder(_memberName, getSpaceURL(), _remoteSpace);
        return _oldProxyHolder;
    }

    public boolean isCollocated() {
        return _isCollocated;
    }

    public int getUpdateModifiers() {
        return _updateModifiers;
    }

    public void setUpdateModifiers(int updateModifiers) {
        _updateModifiers = updateModifiers;
    }

    public int getReadModifiers() {
        return _readModifiers;
    }

    public void setReadModifiers(int readModifiers) {
        _readModifiers = readModifiers;
    }

    public boolean isVersioned() {
        return _isVersioned;
    }

    public void setVersioned(boolean isVersioned) {
        _isVersioned = isVersioned;
    }

    public boolean isFifo() {
        return _fifo;
    }

    public void setFifo(boolean fifo) {
        _fifo = fifo;
    }

    public boolean isGatewayProxy() {
        return _gatewayProxy;
    }

    public void setGatewayProxy(boolean gatewayProxy) {
        _gatewayProxy = gatewayProxy;
    }

    public Properties getCustomProperties() {
        return _customProperties;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            throw new IllegalStateException("Class " + this.getClass().getName() + " should no longer be serialized");
        out.writeInt(SERIAL_VERSION);

        _spaceSettings.writeExternal(out);

        if (_finderURL == null)
            out.writeBoolean(false);
        else {
            out.writeBoolean(true);
            _finderURL.writeExternal(out);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new IllegalStateException("Class " + this.getClass().getName() + " should no longer be serialized");
    }

    public SpaceClusterInfo getSpaceClusterInfo(boolean clustered) {
        return clustered ? _clusterInfo : _nonClusterInfo;
    }

    public ProxySettings cloneAndUpdate(ClusterTopology chunksMap) {
        SpaceSettings newSpaceSettings = this._spaceSettings.cloneAndUpdate(chunksMap);
        return new ProxySettings(this._remoteSpace, newSpaceSettings);
    }

    public ProxySettings cloneAndUpdate(SpaceClusterInfo newClusterInfo) {
        SpaceSettings newSpaceSettings = this._spaceSettings.cloneAndUpdate(newClusterInfo);
        return new ProxySettings(this._remoteSpace, newSpaceSettings);
    }
}
