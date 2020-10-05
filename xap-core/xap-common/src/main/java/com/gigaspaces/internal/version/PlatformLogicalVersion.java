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
public class PlatformLogicalVersion implements Externalizable, Comparable<PlatformLogicalVersion> {
    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VERSION = Byte.MIN_VALUE + 1;
    private static final int LAST_BUILD_NUMBER = 30_000;

    private final static PlatformLogicalVersion LOGICAL_VERSION = fromVersion(PlatformVersion.getInstance());

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

    private PlatformLogicalVersion(int majorVersion, int minorVersion, int servicePackVersion, int buildNumber, int subBuildNumber) {
        _majorVersion = (byte) majorVersion;
        _minorVersion = (byte) minorVersion;
        _servicePackVersion = (byte) servicePackVersion;
        _buildNumber = buildNumber;
        _subBuildNumber = subBuildNumber;
    }

    static PlatformLogicalVersion fromBuild(int majorVersion, int minorVersion, int servicePackVersion, int buildNumber) {
        return fromBuild(majorVersion,  minorVersion, servicePackVersion, buildNumber, 0);
    }

    static PlatformLogicalVersion fromBuild(int majorVersion, int minorVersion, int servicePackVersion, int buildNumber, int subBuildNumber) {
        return new PlatformLogicalVersion(majorVersion,  minorVersion, servicePackVersion, buildNumber, subBuildNumber);
    }

    static PlatformLogicalVersion fromVersion(int majorVersion, int minorVersion, int servicePackVersion) {
        return fromVersion(majorVersion,  minorVersion, servicePackVersion, "", 0);
    }

    static PlatformLogicalVersion fromVersion(int majorVersion, int minorVersion, int servicePackVersion, String patchId, int patchNum) {
        return new PlatformLogicalVersion(majorVersion,  minorVersion, servicePackVersion, LAST_BUILD_NUMBER + patchNum, patchId.hashCode());
    }

    private static PlatformLogicalVersion fromVersion(PlatformVersion version) {
        return fromVersion(version.getMajorVersion(), version.getMinorVersion(), version.getServicePackVersion(),
                version.getPatchId(), version.getPatchNumber());
    }

    @Override
    public int compareTo(PlatformLogicalVersion other) {
        int code;
        if (_buildNumber >= LAST_BUILD_NUMBER && _majorVersion >= 14) {
            if ((code = Integer.compare(this._majorVersion, other._majorVersion)) != 0)
                return code;
            if ((code = Integer.compare(this._minorVersion, other._minorVersion)) != 0)
                return code;
            return Integer.compare(this._servicePackVersion, other._servicePackVersion);
        } else {
            if ((code = Integer.compare(this._buildNumber, other._buildNumber)) != 0)
                return code;
            return Integer.compare(this._subBuildNumber, other._subBuildNumber);
        }
    }

    public boolean patchSameOrGreater(PlatformLogicalVersion other) {
        return _majorVersion == other._majorVersion &&
                _minorVersion == other._minorVersion &&
                _servicePackVersion == other._servicePackVersion &&
                _subBuildNumber == other._subBuildNumber && // Patch ID
                _buildNumber >= other._buildNumber;         // Patch Number
    }

    /**
     * Returns true if this logical version is less than other ( < )
     *
     * @return true if this logical version is less than other ( < )
     */
    public boolean lessThan(PlatformLogicalVersion otherVersion) {
        return compareTo(otherVersion) < 0;
    }

    /**
     * Returns true if this logical version is greater or equals to the other ( >= )
     *
     * @return true if this logical version is greater or equals to the other ( >= )
     */
    public boolean greaterOrEquals(PlatformLogicalVersion otherVersion) {
        return compareTo(otherVersion) >=0;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte version = in.readByte();
        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Requested version [" + version + "] does not match local version [" + SERIAL_VERSION + "]. Please make sure you are using the same version on both ends, local version is " + PlatformVersion.getOfficialVersion());
        read(in);
    }

    public void read(ObjectInput in) throws IOException {
        _majorVersion = in.readByte();
        _minorVersion = in.readByte();
        _servicePackVersion = in.readByte();
        _buildNumber = in.readInt();
        _subBuildNumber = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(SERIAL_VERSION);
        write(out);
    }

    public void write(ObjectOutput out) throws IOException {
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

    //All marked version
    //public static final PlatformLogicalVersion v7_1_1 = fromBuild(7, 1, 1, 4500);
    //public static final PlatformLogicalVersion v7_1_2 = fromBuild(7, 1, 2, 4601);
    //public static final PlatformLogicalVersion v7_1_3 = fromBuild(7, 1, 3, 4670);
    //public static final PlatformLogicalVersion v7_1_4 = fromBuild(7, 1, 4, 4750);
    //public static final PlatformLogicalVersion v8_0_0 = fromBuild(8, 0, 0, 5000);
    //public static final PlatformLogicalVersion v8_0_1 = fromBuild(8, 0, 1, 5200);
    //public static final PlatformLogicalVersion v8_0_2 = fromBuild(8, 0, 2, 5400);
    //public static final PlatformLogicalVersion v8_0_3 = fromBuild(8, 0, 3, 5600);
    //public static final PlatformLogicalVersion v8_0_4 = fromBuild(8, 0, 4, 5800);
    //public static final PlatformLogicalVersion v8_0_5 = fromBuild(8, 0, 5, 6000);
    //public static final PlatformLogicalVersion v8_0_5_PATCH1 = fromBuild(8, 0, 5, 6010);
    //public static final PlatformLogicalVersion v8_0_6 = fromBuild(8, 0, 6, 6200);
    //public static final PlatformLogicalVersion v8_0_7 = fromBuild(8, 0, 7, 6350);
    //public static final PlatformLogicalVersion v8_0_8 = fromBuild(8, 0, 8, 6380);
    //public static final PlatformLogicalVersion v9_0_0 = fromBuild(9, 0, 0, 6500);
    //public static final PlatformLogicalVersion v9_0_1 = fromBuild(9, 0, 1, 6700);
    //public static final PlatformLogicalVersion v9_0_2 = fromBuild(9, 0, 2, 6900);
    public static final PlatformLogicalVersion v9_1_0 = fromBuild(9, 1, 0, 7500);
    public static final PlatformLogicalVersion v9_1_1 = fromBuild(9, 1, 1, 7700);
    public static final PlatformLogicalVersion v9_1_2 = fromBuild(9, 1, 2, 7920);
    public static final PlatformLogicalVersion v9_5_0 = fromBuild(9, 5, 0, 8500);
    public static final PlatformLogicalVersion v9_5_1 = fromBuild(9, 5, 1, 8700);
    public static final PlatformLogicalVersion v9_5_2 = fromBuild(9, 5, 2, 8900);
    public static final PlatformLogicalVersion v9_5_2_PATCH3 = fromBuild(9, 5, 2, 8933);
    public static final PlatformLogicalVersion v9_6_0 = fromBuild(9, 6, 0, 9500);
    public static final PlatformLogicalVersion v9_6_1 = fromBuild(9, 6, 1, 9700);
    public static final PlatformLogicalVersion v9_6_2_PATCH3 = fromBuild(9, 6, 2, 9930);
    public static final PlatformLogicalVersion v9_7_0 = fromBuild(9, 7, 0, 10496);
    public static final PlatformLogicalVersion v9_7_1 = fromBuild(9, 7, 1, 10800);
    public static final PlatformLogicalVersion v9_7_2 = fromBuild(9, 7, 2, 11000);
    public static final PlatformLogicalVersion v10_0_0 = fromBuild(10, 0, 0, 11600);
    public static final PlatformLogicalVersion v10_0_1 = fromBuild(10, 0, 1, 11800);
    public static final PlatformLogicalVersion v10_1_0 = fromBuild(10, 1, 0, 12600);
    public static final PlatformLogicalVersion v10_1_1 = fromBuild(10, 1, 1, 12800);
    public static final PlatformLogicalVersion v10_2_0 = fromBuild(10, 2, 0, 13800);
    public static final PlatformLogicalVersion v10_2_0_PATCH2 = fromBuild(10, 2, 0, 13820);
    public static final PlatformLogicalVersion v11_0_0 = fromBuild(11, 0, 0, 14800);
    public static final PlatformLogicalVersion v11_0_1 = fromBuild(11, 0, 1, 14890);
    public static final PlatformLogicalVersion v12_0_0 = fromBuild(12, 0, 0, 15790);
    public static final PlatformLogicalVersion v12_0_1 = fromBuild(12, 0, 1, 16600);
    public static final PlatformLogicalVersion v12_1_0 = fromBuild(12, 1, 0, 17000);
    public static final PlatformLogicalVersion v12_1_1 = fromBuild(12, 1, 1, 17100);
    public static final PlatformLogicalVersion v12_2_0 = fromBuild(12, 2, 0, 18000);
    public static final PlatformLogicalVersion v12_3_0 = fromBuild(12, 3, 0, 19000);
    public static final PlatformLogicalVersion v12_3_0_PATCH4 = fromBuild(12, 3, 0, 19040);
    public static final PlatformLogicalVersion v12_3_1 = fromBuild(12, 3, 1, 19300);
    public static final PlatformLogicalVersion v14_0_0 = fromBuild(14, 0, 0, 20000);
    public static final PlatformLogicalVersion v14_0_1 = fromBuild(14, 0, 1, 20100);
    public static final PlatformLogicalVersion v14_2_0 = fromBuild(14, 2, 0, 20400);
    /* Starting 14.5 build numbers are no longer used. */
    public static final PlatformLogicalVersion v14_5_0 = fromVersion(14, 5, 0);
    public static final PlatformLogicalVersion v15_0_0 = fromVersion(15, 0, 0);
    public static final PlatformLogicalVersion v15_2_0 = fromVersion(15, 2, 0);
    public static final PlatformLogicalVersion v15_5_0 = fromVersion(15, 5, 0);
    public static final PlatformLogicalVersion v15_8_0 = fromVersion(15, 8, 0);
    //DOCUMENT BACKWARD BREAKING CHANGES, EACH CHANGE IN A LINE
    //GS-XXXX: Short backward breaking description and classes
    //GS-7725: Partial update replication
    //GS-7753: ReplicationPolicy new parameter, replicate full take
    //GS-8130: TypeDesc and SpaceTypeInfo have new non-transient field isSystemType.
    //END DOCUMENT BACKWARD BREAKING CHANGES    
}
