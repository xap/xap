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

package com.gigaspaces.internal.version;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.UnmarshalException;

/**
 * Represents the logical version of the jar
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class PlatformLogicalVersion implements Externalizable {
    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VERSION = Byte.MIN_VALUE + 1;

    private final static PlatformLogicalVersion LOGICAL_VERSION = new PlatformLogicalVersion(PlatformVersion.getInstance());

    private byte _majorVersion;
    private byte _minorVersion;
    private byte _servicePackVersion;
    private int _buildNumber;
    private int _subBuildNumber;

    /**
     * @return this jar platform logical version
     */
    public static PlatformLogicalVersion getLogicalVersion() {
        return LOGICAL_VERSION;
    }

    //Externalizable
    public PlatformLogicalVersion() {
    }

    private PlatformLogicalVersion(PlatformVersion version) {
        this(version.getMajorVersion(), version.getMinorVersion(), version.getServicePackVersion(), version.getShortBuildNumber(), version.getSubBuildNumber());
    }

    private PlatformLogicalVersion(byte majorVersion, byte minorVersion, byte servicePackVersion, int buildNumber, int subBuildNumber) {
        _majorVersion = majorVersion;
        _minorVersion = minorVersion;
        _servicePackVersion = servicePackVersion;
        _buildNumber = buildNumber;
        _subBuildNumber = subBuildNumber;
    }

    public PlatformLogicalVersion(int majorVersion, int minorVersion, int servicePackVersion, int buildNumber, int subBuildNumber) {
        this((byte) majorVersion, (byte) minorVersion, (byte) servicePackVersion, buildNumber, subBuildNumber);
    }

    public boolean exactEquals(PlatformLogicalVersion otherVersion) {
        return this._buildNumber == otherVersion._buildNumber &&
                this._subBuildNumber == otherVersion._subBuildNumber;
    }

    /**
     * Returns true if this logical version is less than other ( < )
     *
     * @return true if this logical version is less than other ( < )
     */
    public boolean lessThan(PlatformLogicalVersion otherVersion) {
        Boolean specialLessThan = specialLessThan(otherVersion);
        if (specialLessThan != null)
            return specialLessThan;

        return (_buildNumber < otherVersion._buildNumber) || (_buildNumber == otherVersion._buildNumber && _subBuildNumber < otherVersion._subBuildNumber);
    }

    /**
     * Returns true if this logical version is greater or equals to the other ( >= )
     *
     * @return true if this logical version is greater or equals to the other ( >= )
     */
    public boolean greaterOrEquals(PlatformLogicalVersion otherVersion) {
        return !lessThan(otherVersion);
    }

    /**
     * Handle special cases of version, by default return null to mark a non special case
     */
    private Boolean specialLessThan(PlatformLogicalVersion otherVersion) {
        return null;
    }


    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        byte version = in.readByte();
        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Requested version [" + version + "] does not match local version [" + SERIAL_VERSION + "]. Please make sure you are using the same version on both ends, local version is " + PlatformVersion.getOfficialVersion());
        _majorVersion = in.readByte();
        _minorVersion = in.readByte();
        _servicePackVersion = in.readByte();
        _buildNumber = in.readInt();
        _subBuildNumber = in.readInt();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(SERIAL_VERSION);
        out.writeByte(_majorVersion);
        out.writeByte(_minorVersion);
        out.writeByte(_servicePackVersion);
        out.writeInt(_buildNumber);
        out.writeInt(_subBuildNumber);
    }

    @Override
    public String toString() {
        return "" + _majorVersion + "." + _minorVersion + "." + _servicePackVersion + "." + _buildNumber + "-" + _subBuildNumber;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + _buildNumber;
        result = prime * result + _majorVersion;
        result = prime * result + _minorVersion;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PlatformLogicalVersion other = (PlatformLogicalVersion) obj;
        if (_buildNumber != other._buildNumber)
            return false;
        if (_majorVersion != other._majorVersion)
            return false;
        if (_minorVersion != other._minorVersion)
            return false;
        if (_servicePackVersion != other._servicePackVersion)
            return false;
        if (_subBuildNumber != other._subBuildNumber)
            return false;
        return true;
    }

    public static PlatformLogicalVersion minimum(
            PlatformLogicalVersion version1,
            PlatformLogicalVersion version2) {
        if (version1.lessThan(version2))
            return version1;

        return version2;
    }


    public byte getMajorVersion() {
        return _majorVersion;
    }

    public byte getMinorVersion() {
        return _minorVersion;
    }

    public byte getServicePackVersion() {
        return _servicePackVersion;
    }

    public int getBuildNumber() {
        return _buildNumber;
    }

    public int getSubBuildNumber() {
        return _subBuildNumber;
    }

    //All marked version
    //public static final PlatformLogicalVersion v7_1_1 = new PlatformLogicalVersion(7, 1, 1, 4500, 0);
    //public static final PlatformLogicalVersion v7_1_2 = new PlatformLogicalVersion(7, 1, 2, 4601, 0);
    //public static final PlatformLogicalVersion v7_1_3 = new PlatformLogicalVersion(7, 1, 3, 4670, 0);
    //public static final PlatformLogicalVersion v7_1_4 = new PlatformLogicalVersion(7, 1, 4, 4750, 0);
    //public static final PlatformLogicalVersion v8_0_0 = new PlatformLogicalVersion(8, 0, 0, 5000, 0);
    //public static final PlatformLogicalVersion v8_0_1 = new PlatformLogicalVersion(8, 0, 1, 5200, 0);
    //public static final PlatformLogicalVersion v8_0_2 = new PlatformLogicalVersion(8, 0, 2, 5400, 0);
    //public static final PlatformLogicalVersion v8_0_3 = new PlatformLogicalVersion(8, 0, 3, 5600, 0);
    //public static final PlatformLogicalVersion v8_0_4 = new PlatformLogicalVersion(8, 0, 4, 5800, 0);
    //public static final PlatformLogicalVersion v8_0_5 = new PlatformLogicalVersion(8, 0, 5, 6000, 0);
    //public static final PlatformLogicalVersion v8_0_5_PATCH1 = new PlatformLogicalVersion(8, 0, 5, 6010, 0);
    //public static final PlatformLogicalVersion v8_0_6 = new PlatformLogicalVersion(8, 0, 6, 6200, 0);
    //public static final PlatformLogicalVersion v8_0_7 = new PlatformLogicalVersion(8, 0, 7, 6350, 0);
    //public static final PlatformLogicalVersion v8_0_8 = new PlatformLogicalVersion(8, 0, 8, 6380, 0);
    //public static final PlatformLogicalVersion v9_0_0 = new PlatformLogicalVersion(9, 0, 0, 6500, 0);
    //public static final PlatformLogicalVersion v9_0_1 = new PlatformLogicalVersion(9, 0, 1, 6700, 0);
    //public static final PlatformLogicalVersion v9_0_2 = new PlatformLogicalVersion(9, 0, 2, 6900, 0);
    public static final PlatformLogicalVersion v9_1_0 = new PlatformLogicalVersion(9, 1, 0, 7500, 0);
    public static final PlatformLogicalVersion v9_1_1 = new PlatformLogicalVersion(9, 1, 1, 7700, 0);
    public static final PlatformLogicalVersion v9_1_2 = new PlatformLogicalVersion(9, 1, 2, 7920, 0);
    public static final PlatformLogicalVersion v9_5_0 = new PlatformLogicalVersion(9, 5, 0, 8500, 0);
    public static final PlatformLogicalVersion v9_5_1 = new PlatformLogicalVersion(9, 5, 1, 8700, 0);
    public static final PlatformLogicalVersion v9_5_2 = new PlatformLogicalVersion(9, 5, 2, 8900, 0);
    public static final PlatformLogicalVersion v9_5_2_PATCH3 = new PlatformLogicalVersion(9, 5, 2, 8933, 0);
    public static final PlatformLogicalVersion v9_6_0 = new PlatformLogicalVersion(9, 6, 0, 9500, 0);
    public static final PlatformLogicalVersion v9_6_1 = new PlatformLogicalVersion(9, 6, 1, 9700, 0);
    public static final PlatformLogicalVersion v9_6_2_PATCH3 = new PlatformLogicalVersion(9, 6, 2, 9930, 0);
    public static final PlatformLogicalVersion v9_7_0 = new PlatformLogicalVersion(9, 7, 0, 10496, 0);
    public static final PlatformLogicalVersion v9_7_1 = new PlatformLogicalVersion(9, 7, 1, 10800, 0);
    public static final PlatformLogicalVersion v9_7_2 = new PlatformLogicalVersion(9, 7, 2, 11000, 0);
    public static final PlatformLogicalVersion v10_0_0 = new PlatformLogicalVersion(10, 0, 0, 11600, 0);
    public static final PlatformLogicalVersion v10_0_1 = new PlatformLogicalVersion(10, 0, 1, 11800, 0);
    public static final PlatformLogicalVersion v10_1_0 = new PlatformLogicalVersion(10, 1, 0, 12600, 0);
    public static final PlatformLogicalVersion v10_1_1 = new PlatformLogicalVersion(10, 1, 1, 12800, 0);
    public static final PlatformLogicalVersion v10_2_0 = new PlatformLogicalVersion(10, 2, 0, 13800, 0);
    public static final PlatformLogicalVersion v10_2_0_PATCH2 = new PlatformLogicalVersion(10, 2, 0, 13820, 0);
    public static final PlatformLogicalVersion v11_0_0 = new PlatformLogicalVersion(11, 0, 0, 14800, 0);
    public static final PlatformLogicalVersion v11_0_1 = new PlatformLogicalVersion(11, 0, 1, 14890, 0);
    public static final PlatformLogicalVersion v12_0_0 = new PlatformLogicalVersion(12, 0, 0, 15790, 0);
    public static final PlatformLogicalVersion v12_0_1 = new PlatformLogicalVersion(12, 0, 1, 16600, 0);
    public static final PlatformLogicalVersion v12_1_0 = new PlatformLogicalVersion(12, 1, 0, 16701, 0);

    //DOCUMENT BACKWARD BREAKING CHANGES, EACH CHANGE IN A LINE
    //GS-XXXX: Short backward breaking description and classes
    //GS-7725: Partial update replication
    //GS-7753: ReplicationPolicy new parameter, replicate full take
    //GS-8130: TypeDesc and SpaceTypeInfo have new non-transient field isSystemType.
    //END DOCUMENT BACKWARD BREAKING CHANGES    
}
