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

/**
 *
 */
package com.j_spaces.core.client;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.IJSpaceContainer;
import com.j_spaces.core.IStubHandler;
import com.j_spaces.core.admin.SpaceConfig;

import net.jini.id.Uuid;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.UnmarshalException;

/**
 * SpaceSettings is a set of properties passed from the space to its proxy
 *
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceSettings implements SmartExternalizable, Cloneable {
    private static final long serialVersionUID = 1L;
    private static final int SERIAL_VERSION = 1;

    /**
     * The unique identifier for this proxy.
     */
    private Uuid _uuid;
    private boolean _secondary;
    /**
     * how are the entry value (fields) serialized.
     */
    private int _serializationType;
    /**
     * the space name this proxy belong to.
     */
    private String _containerName;
    /**
     * the container name this proxy belong to.
     */
    private String _spaceName;
    /**
     * stub handler of this proxy.
     */
    private IStubHandler _stubHandler;
    /**
     * reference to container proxy.
     */
    private IJSpaceContainer _containerProxy;
    private SpaceConfig _spaceConfig;
    /**
     * reference to the transport config that was used when exporting this space
     */
    private ITransportConfig _exportedTransportConfig;
    /**
     * Returns the {@link SpaceURL} instance which was used to initialize the space. <p> Notice: The
     * {@link SpaceURL} object contains information on the space and container configuration/setup
     * such as space url used, space/container/cluster schema used and other attributes.<p> The
     * {@link IJSpaceContainer} keeps also its reference of the SpaceURL which launched the
     * container.
     */
    private SpaceURL _spaceURL;
    private boolean securedService;
    //if true- unused embedded-mahalo xtns are cleaned without abort 
    private boolean _cleanUnusedEmbeddedGlobalXtns;

    public SpaceSettings() {
    }

    public SpaceSettings(String containerName, IJSpaceContainer containerProxy, Uuid uuid, boolean secondary,
                         int serializationType, SpaceConfig spaceConfig, String spaceName, SpaceURL spaceURL,
                         IStubHandler stubHandler, boolean securedService, ITransportConfig exportedTransportConfig,
                         boolean cleanUnusedEmbeddedGlobalXtns) {
        _containerName = containerName;
        _containerProxy = containerProxy;
        _uuid = uuid;
        _secondary = secondary;
        _serializationType = serializationType;
        _spaceConfig = spaceConfig;
        _spaceName = spaceName;
        _spaceURL = spaceURL;
        _stubHandler = stubHandler;
        this.securedService = securedService;
        _exportedTransportConfig = exportedTransportConfig;
        _cleanUnusedEmbeddedGlobalXtns = cleanUnusedEmbeddedGlobalXtns;
    }

    @Override
    public SpaceSettings clone() {
        try {
            SpaceSettings copy = (SpaceSettings) super.clone();
            copy._spaceConfig = _spaceConfig.clone();
            return copy;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable object");
        }
    }

    public Uuid getUuid() {
        return _uuid;
    }

    public int getSerializationType() {
        return _serializationType;
    }

    public String getContainerName() {
        return _containerName;
    }

    public String getSpaceName() {
        return _spaceName;
    }

    public String getMemberName() {
        return _containerName + ":" + _spaceName;
    }

    public IStubHandler getStubHandler() {
        return _stubHandler;
    }

    public IJSpaceContainer getContainerProxy() {
        return _containerProxy;
    }

    public SpaceConfig getSpaceConfig() {
        return _spaceConfig;
    }

    public SpaceURL getSpaceURL() {
        return _spaceURL;
    }

    public boolean isSecuredService() {
        return securedService;
    }

    public ITransportConfig getExportedTransportConfig() {
        return _exportedTransportConfig;
    }

    SpaceSettings cloneAndUpdate(ClusterTopology chunksMap) {
        try {
            SpaceSettings newSpaceSettings = (SpaceSettings) super.clone();
            newSpaceSettings._spaceConfig = _spaceConfig.cloneAndUpdate(chunksMap);
            String totalMembers = newSpaceSettings._spaceURL.getProperty("total_members");
            String[] split = totalMembers.split(",");
            newSpaceSettings._spaceURL.setProperty("total_members",chunksMap.getNumberOfInstances()+","+split[1]);
            return newSpaceSettings;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable object");
        }
    }

    SpaceSettings cloneAndUpdate(SpaceClusterInfo clusterInfo) {
        try {
            SpaceSettings newSpaceSettings = (SpaceSettings) super.clone();
            newSpaceSettings._spaceConfig.setClusterInfo(clusterInfo);
            String totalMembers = newSpaceSettings._spaceURL.getProperty("total_members");
            String[] split = totalMembers.split(",");
            newSpaceSettings._spaceURL.setProperty("total_members",clusterInfo.getNumberOfPartitions()+","+split[1]);
            return newSpaceSettings;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable object");
        }
    }

    /* Bit map for serialization */
    private interface BitMap {
        short _UUID = 1 << 0;
        short _SECONDARY = 1 << 1;
        short _ENFORCESECURITY = 1 << 2; //TODO for backwards - remove in next major version after 7.0
        short _CONTAINERNAME = 1 << 3;
        short _SPACENAME = 1 << 4;
        short _STUBHANDLER = 1 << 5;
        short _CONTAINERPROXY = 1 << 6;
        short _SPACEATTRIBUTES = 1 << 7;
        short _SPACEURL = 1 << 8;
        short _SECURED_SERVICE = 1 << 9;
        short _EXPORTEDCONFIG = 1 << 10;
        short _CLEAN_GLOBAL_XTNS = 1 << 11;

    }

    private short buildFlags() {
        short flags = 0;
        if (_uuid != null)
            flags |= BitMap._UUID;
        if (_secondary)
            flags |= BitMap._SECONDARY;
        if (_containerName != null)
            flags |= BitMap._CONTAINERNAME;
        if (_spaceName != null)
            flags |= BitMap._SPACENAME;
        if (_stubHandler != null)
            flags |= BitMap._STUBHANDLER;
        if (_containerProxy != null)
            flags |= BitMap._CONTAINERPROXY;
        if (_spaceConfig != null)
            flags |= BitMap._SPACEATTRIBUTES;
        if (_spaceURL != null)
            flags |= BitMap._SPACEURL;
        if (securedService)
            flags |= BitMap._SECURED_SERVICE;
        if (_exportedTransportConfig != null)
            flags |= BitMap._EXPORTEDCONFIG;
        if (_cleanUnusedEmbeddedGlobalXtns)
            flags |= BitMap._CLEAN_GLOBAL_XTNS;

        return flags;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(SERIAL_VERSION);

        final short flags = buildFlags();
        out.writeShort(flags);

        out.writeInt(_serializationType);
        out.writeInt(-1 /*_implicitNumOfIndexes*/);

        if (flags == 0)
            return;

        if (_uuid != null)
            out.writeObject(_uuid);
        if (_containerName != null)
            out.writeUTF(_containerName);
        if (_spaceName != null)
            out.writeUTF(_spaceName);
        if (_stubHandler != null)
            out.writeObject(_stubHandler);
        if (_containerProxy != null)
            out.writeObject(_containerProxy);
        if (_spaceConfig != null)
            out.writeObject(_spaceConfig);
        if (_spaceURL != null)
            out.writeObject(_spaceURL);
        if (_exportedTransportConfig != null)
            out.writeObject(_exportedTransportConfig);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int version = in.readInt();

        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Class [" + getClass().getName() + "] received version [" + version
                    + "] does not match local version [" + SERIAL_VERSION
                    + "]. Please make sure you are using the same version on both ends.");

        final short flags = in.readShort();

        _serializationType = in.readInt();
        /*_implicitNumOfIndexes = */
        in.readInt();

        if (flags == 0)
            return;
        _secondary = ((flags & BitMap._SECONDARY) != 0);
        securedService = ((flags & BitMap._SECURED_SERVICE) != 0);
        _cleanUnusedEmbeddedGlobalXtns = ((flags & BitMap._CLEAN_GLOBAL_XTNS) != 0);

        if ((flags & BitMap._UUID) != 0)
            _uuid = (Uuid) in.readObject();
        if ((flags & BitMap._CONTAINERNAME) != 0)
            _containerName = in.readUTF();
        if ((flags & BitMap._SPACENAME) != 0)
            _spaceName = in.readUTF();
        if ((flags & BitMap._STUBHANDLER) != 0)
            _stubHandler = (IStubHandler) in.readObject();
        if ((flags & BitMap._CONTAINERPROXY) != 0)
            _containerProxy = (IJSpaceContainer) in.readObject();
        if ((flags & BitMap._SPACEATTRIBUTES) != 0)
            _spaceConfig = (SpaceConfig) in.readObject();
        if ((flags & BitMap._SPACEURL) != 0)
            _spaceURL = (SpaceURL) in.readObject();
        if ((flags & BitMap._EXPORTEDCONFIG) != 0)
            _exportedTransportConfig = (ITransportConfig) in.readObject();
    }
}
