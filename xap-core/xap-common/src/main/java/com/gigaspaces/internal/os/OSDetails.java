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

package com.gigaspaces.internal.os;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.NetworkInterface;
import java.net.SocketException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class OSDetails implements SmartExternalizable {

    //compatible with serialVersionUID of 7.1.1
    private static final long serialVersionUID = -290262929223386658L;

    private String uid = "";

    private String name = "";

    private String arch = "";

    private String version = "";

    private int availableProcessors = -1;

    private long totalSwapSpaceSize = -1;

    private long totalPhysicalMemorySize = -1;

    private String hostName = "";

    private String hostAddress = "";

    private OSNetInterfaceDetails[] netInterfaceConfigs;

    private OSDriveDetails[] driveConfigs;

    private OSVendorDetails vendorDetails;

    public OSDetails() {
    }

    public OSDetails(String uid, String name, String arch, String version, int availableProcessors,
                     long totalSwapSpaceSize, long totalPhysicalMemorySize, String hostName,
                     String hostAddress, OSNetInterfaceDetails[] netInterfaceConfigs, OSDriveDetails[] driveConfigs, OSVendorDetails vendorDetails) {
        this.uid = uid;
        this.name = name;
        this.arch = arch;
        this.version = version;
        this.availableProcessors = availableProcessors;
        this.totalSwapSpaceSize = totalSwapSpaceSize;
        this.totalPhysicalMemorySize = totalPhysicalMemorySize;
        this.hostName = hostName;
        this.hostAddress = hostAddress;
        this.netInterfaceConfigs = netInterfaceConfigs;
        this.driveConfigs = driveConfigs;
        this.vendorDetails = vendorDetails;
    }

    public boolean isNA() {
        return uid.length() == 0;
    }

    public String getUID() {
        return uid;
    }

    public String getName() {
        return name;
    }

    public String getArch() {
        return arch;
    }

    public String getVersion() {
        return version;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public long getTotalSwapSpaceSize() {
        return totalSwapSpaceSize;
    }

    public long getTotalPhysicalMemorySize() {
        return totalPhysicalMemorySize;
    }

    public String getHostName() {
        return hostName;
    }

    public String getHostAddress() {
        return hostAddress;
    }


    public OSNetInterfaceDetails[] getNetInterfaceConfigs() {
        return netInterfaceConfigs;
    }

    public OSDriveDetails[] getDriveConfigs() {
        return driveConfigs;
    }

    /**
     * @since 8.0.4
     */
    public OSVendorDetails getVendorDetails() {
        return vendorDetails;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeUTF(name);
        out.writeUTF(arch);
        out.writeUTF(version);
        out.writeInt(availableProcessors);
        out.writeLong(totalSwapSpaceSize);
        out.writeLong(totalPhysicalMemorySize);
        out.writeUTF(hostName);
        out.writeUTF(hostAddress);

        if (netInterfaceConfigs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(netInterfaceConfigs.length);
            for (OSNetInterfaceDetails netInterfaceDetails : netInterfaceConfigs) {
                netInterfaceDetails.writeExternal(out);
            }
        }

        if (driveConfigs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(driveConfigs.length);
            for (OSDriveDetails driveDetails : driveConfigs) {
                driveDetails.writeExternal(out);
            }
        }

        if (vendorDetails == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            vendorDetails.writeExternal(out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uid = in.readUTF();
        name = in.readUTF();
        arch = in.readUTF();
        version = in.readUTF();
        availableProcessors = in.readInt();
        totalSwapSpaceSize = in.readLong();
        totalPhysicalMemorySize = in.readLong();
        hostName = in.readUTF();
        hostAddress = in.readUTF();

        if (in.readBoolean()) {
            netInterfaceConfigs = new OSNetInterfaceDetails[in.readInt()];
            for (int i = 0; i < netInterfaceConfigs.length; i++) {
                netInterfaceConfigs[i] = new OSNetInterfaceDetails();
                netInterfaceConfigs[i].readExternal(in);
            }
        }

        if (in.readBoolean()) {
            driveConfigs = new OSDriveDetails[in.readInt()];
            for (int i = 0; i < driveConfigs.length; i++) {
                driveConfigs[i] = new OSDriveDetails();
                driveConfigs[i].readExternal(in);
            }
        }

        if (in.readBoolean()) {
            vendorDetails = new OSVendorDetails();
            vendorDetails.readExternal(in);
        }
    }


    public static class OSNetInterfaceDetails implements SmartExternalizable {
        private static final long serialVersionUID = 6720043955636646719L;

        private String address;
        private String name;
        private String description;

        public OSNetInterfaceDetails() {

        }

        public OSNetInterfaceDetails(String addr, String name, String description) {
            this.address = addr;
            this.name = name;
            this.description = description;
        }

        public static OSNetInterfaceDetails of(NetworkInterface nic) throws SocketException {
            byte[] hardwareAddress = nic.getHardwareAddress();
            return hardwareAddress != null ? new OSNetInterfaceDetails(parseHardwareAddress(hardwareAddress), nic.getName(), nic.getDisplayName()) : null;
        }

        public String getAddress() {
            return address;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(address);
            out.writeUTF(name);
            out.writeUTF(description);
        }

        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            address = in.readUTF();
            name = in.readUTF();
            description = in.readUTF();
        }

        private static String parseHardwareAddress(byte[] hardwareAddress) {
            String hardwareAddressStr = "";

            for (int index = 0; index < hardwareAddress.length; index++) {
                int val = hardwareAddress[index];
                if (val < 0) {
                    val = 256 + val;
                }

                String hexStr = Integer.toString(val, 16).toUpperCase();
                //add zero if this is only one char
                if (hexStr.length() == 1) {
                    hexStr = 0 + hexStr;
                }

                hardwareAddressStr += hexStr;
                if (index < hardwareAddress.length - 1) {
                    hardwareAddressStr += ":";
                }
            }
            return hardwareAddressStr;
        }
    }

    public static class OSDriveDetails implements SmartExternalizable {
        private static final long serialVersionUID = -1579157666784550069L;

        private String name;
        private Long capacityInKB;

        public OSDriveDetails() {

        }

        public OSDriveDetails(String name, long capacityInKB) {
            this.name = name;
            this.capacityInKB = capacityInKB;
        }

        public Long getCapacityInBytes() {
            return capacityInKB * 1024;
        }

        public Long getCapacityInKB() {
            return capacityInKB;
        }

        public Long getCapacityInMB() {
            return capacityInKB / 1024;
        }

        public Long getCapacityInGB() {
            return getCapacityInMB() / 1024;
        }

        public String getName() {
            return name;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(capacityInKB);
            out.writeUTF(name);
        }

        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            capacityInKB = in.readLong();
            name = in.readUTF();
        }

    }

    /**
     * @since 8.0.4
     */
    public static class OSVendorDetails implements SmartExternalizable {
        private static final long serialVersionUID = 422639202428429664L;

        private String vendor;
        private String vendorCodeName;
        private String vendorName;
        private String vendorVersion;

        public OSVendorDetails() {
        }

        public OSVendorDetails(String vendor, String vendorCodeName, String vendorName, String vendorVersion) {
            final String NA = "N/A";
            this.vendor = vendor != null ? vendor : NA;
            this.vendorCodeName = vendorCodeName != null ? vendorCodeName : NA;
            this.vendorName = vendorName != null ? vendorName : NA;
            this.vendorVersion = vendorVersion!= null ? vendorVersion : NA;
        }

        public String getVendor() {
            return vendor;
        }

        public String getVendorCodeName() {
            return vendorCodeName;
        }

        public String getVendorName() {
            return vendorName;
        }

        public String getVendorVersion() {
            return vendorVersion;
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            vendor = in.readUTF();
            vendorCodeName = in.readUTF();
            vendorName = in.readUTF();
            vendorVersion = in.readUTF();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(vendor);
            out.writeUTF(vendorCodeName);
            out.writeUTF(vendorName);
            out.writeUTF(vendorVersion);
        }
    }
}