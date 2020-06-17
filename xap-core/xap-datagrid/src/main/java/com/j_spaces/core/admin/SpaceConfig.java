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


package com.j_spaces.core.admin;

import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.server.space.IClusterInfoChangedListener;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.Constants;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.kernel.JSpaceUtilities;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.UnmarshalException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This structure contains all information about space configuration. <code>SpaceConfig</code>
 * builds inside of Server and transfered to the side of client.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see com.j_spaces.core.admin.IRemoteJSpaceAdmin#getConfig()
 **/

public class SpaceConfig extends JSpaceAttributes implements IClusterInfoChangedListener {
    private String _spaceName;
    private String _containerName;
    private String _fullSpaceName;
    private String _schemaPath = "";

    private final ConcurrentMap extraProperties = new ConcurrentHashMap<String, Object>();

    private static final long serialVersionUID = 1L;
    private static final int SERIAL_VERSION = 1;

    /**
     *
     */
    public SpaceConfig() {
        super();
    }

    /**
     * Creates SpaceConfig with the provided space name.
     *
     * @param spaceName     the space name
     * @param containerName container owner name
     */
    public SpaceConfig(String spaceName, String containerName) {
        _spaceName = spaceName;
        _containerName = containerName;
        _fullSpaceName = JSpaceUtilities.createFullSpaceName(_containerName, _spaceName);
    }

    /**
     * Creates SpaceConfig with the provided space name, using the given {@link java.util.Properties
     * Properties}.
     *
     * @param spaceName     the space name
     * @param prop          properties for the SpaceConfig
     * @param containerName container owner name
     * @param schemaPath    path to space schema file
     */
    public SpaceConfig(String spaceName, Properties prop, String containerName, String schemaPath) {
        super(prop);
        _spaceName = spaceName;
        _containerName = containerName;
        _schemaPath = schemaPath;
        _fullSpaceName = JSpaceUtilities.createFullSpaceName(_containerName, _spaceName);
    }

    @Override
    public SpaceConfig clone() {
        return (SpaceConfig) super.clone();
    }

    /**
     * Returns the space name.
     *
     * @return the space name
     */
    public String getSpaceName() {
        return _spaceName;
    }

    /**
     * Set the space name.
     *
     * @param spaceName the new space name
     */
    public void setSpaceName(String spaceName) {
        _spaceName = spaceName;
    }

    public String getFullSpaceName() {
        return _fullSpaceName;
    }

    /**
     * Returns space schema file path
     *
     * @return path to space schema file ( including host IP address )
     */
    public String getSchemaPath() {
        return _schemaPath;
    }

    /**
     * Sets space schema file path
     *
     * @param schemaName path to space schema file ( including host IP address )
     */
    public void setSchemaPath(String schemaName) {
        _schemaPath = schemaName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProperty(String key) {
        return super.getProperty(prepareProperty(key));
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        return super.getProperty(prepareProperty(key), defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object setProperty(String key, String value) {
        return super.setProperty(prepareProperty(key), value);
    }

    /**
     * Prepare a property adding the space name before the key.
     *
     * @param key is Xpath as following: Constants.SPACE_CONFIG_PREFIX + key
     * @return key with space name at the beginning of X path
     */
    private String prepareProperty(String key) {
        if (!key.startsWith(Constants.SPACE_CONFIG_PREFIX)) {
            return key;
        }

        return (_fullSpaceName + '.' + key);
    }

    /**
     * Returns the property without using the space name.
     *
     * @param key the property key.
     * @return the value in this property list with the specified key value.
     */
    public String getPropertyFromSuper(String key) {
        return super.getProperty(key);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("\n\t ============================================================\n");
        sb.append("\n\t Space configuration for space \"" + _fullSpaceName + "\" \n");
        sb.append("\n\t ============================================================\n");
        sb.append("\n\t DCache configuration \n\t ");
        sb.append(getDCacheProperties());
        sb.append('\n');
        sb.append("\n\t Cluster Info -\t");
        sb.append(getClusterInfo());
        sb.append('\n');
        sb.append("\n\t Filters configuration -\t");
        sb.append(Arrays.toString(getFiltersInfo()));
        sb.append('\n');
        sb.append("\n\t Custom properties -\t ");
        StringUtils.appendProperties(sb, getCustomProperties());
        sb.append('\n');
        sb.append("\n---------------------------------------------------------------------------------------- \n");
        sb.append("Space configuration elements for space < ");
        sb.append(_fullSpaceName);
        sb.append(" >");
        sb.append("\n---------------------------------------------------------------------------------------- \n\n");
        StringUtils.appendProperties(sb, this);

        return sb.toString();
    }

    @Override
    public synchronized boolean containsKey(Object key) {
        //This method is implemented in such way ( by calling to getProperty())
        //because method super.containsKey() does not affect for elements that
        //were added NOT by setProperty() method, but were added by calling
        //to constructor new Properties(  prop )
        String preparedKey = prepareProperty(key.toString());
        return super.getProperty(preparedKey) == null ? false : true;
    }

    /**
     * Return container owner name
     *
     * @return container name
     */
    public String getContainerName() {
        return _containerName;
    }

    @Override
    public void afterClusterInfoChange(SpaceClusterInfo clusterInfo) {
        this.setClusterInfo(clusterInfo);
    }

    /* Bit map for serialization */
    private interface BitMap {
        byte _SPACENAME = 1 << 0;
        byte _CONTAINERNAME = 1 << 1;
        byte _FULLSPACENAME = 1 << 2;
        byte _SCHEMAPATH = 1 << 3;
    }

    private byte buildFlags() {
        byte flags = 0;
        if (_spaceName != null)
            flags |= BitMap._SPACENAME;
        if (_containerName != null)
            flags |= BitMap._CONTAINERNAME;
        if (_fullSpaceName != null)
            flags |= BitMap._FULLSPACENAME;
        if (_schemaPath != null)
            flags |= BitMap._SCHEMAPATH;
        return flags;
    }

    public ConcurrentMap getExtraProperties() {
        return extraProperties;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        int version = in.readInt();

        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Class [" + getClass().getName() + "] received version [" + version + "] does not match local version [" + SERIAL_VERSION + "]. Please make sure you are using the same product version on both ends.");

        final byte flags = in.readByte();

        if (flags == 0)
            return;
        if ((flags & BitMap._SPACENAME) != 0)
            _spaceName = (String) in.readObject();

        if ((flags & BitMap._CONTAINERNAME) != 0)
            _containerName = (String) in.readObject();

        if ((flags & BitMap._FULLSPACENAME) != 0)
            _fullSpaceName = (String) in.readObject();

        if ((flags & BitMap._SCHEMAPATH) != 0)
            _schemaPath = (String) in.readObject();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(SERIAL_VERSION);

        final byte flags = buildFlags();
        out.writeByte(flags);

        if (flags == 0)
            return;

        if (_spaceName != null) {
            out.writeObject(_spaceName);
        }

        if (_containerName != null) {
            out.writeObject(_containerName);
        }

        if (_fullSpaceName != null) {
            out.writeObject(_fullSpaceName);
        }

        if (_schemaPath != null) {
            out.writeObject(_schemaPath);
        }

    }
}