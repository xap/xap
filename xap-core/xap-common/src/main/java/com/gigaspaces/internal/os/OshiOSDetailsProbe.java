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

import com.gigaspaces.internal.oshi.OshiUtils;
import com.gigaspaces.start.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;
import oshi.util.FormatUtil;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class OshiOSDetailsProbe implements OSDetailsProbe  {
    private static final String uid = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostAddress = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostName = SystemInfo.singleton().network().getHost().getHostName();
    private static final OperatingSystem operatingSystem = OshiUtils.getOperatingSystem();
    private static final HardwareAbstractionLayer hardware = OshiUtils.getHardware();

    @Override
    public OSDetails probeDetails() throws Exception {
        if (uid == null) {
            return new OSDetails();
        }

        GlobalMemory memory = hardware.getMemory();
        VirtualMemory virtualMemory = memory.getVirtualMemory();

        return new OSDetails(uid,
                operatingSystem.getManufacturer(),
                FormatUtil.formatBytes(operatingSystem.getBitness()),
                operatingSystem.getVersionInfo().getBuildNumber(),
                hardware.getProcessor().getLogicalProcessorCount(),
                virtualMemory.getSwapTotal(),
                memory.getTotal(),
                localHostName, localHostAddress,
                getOSNetDetails(),
                getOSDriveDetailsArray(),
                getVendorDetails());

    }

    private OSDetails.OSNetInterfaceDetails[] getOSNetDetails() throws SocketException {
        List<OSDetails.OSNetInterfaceDetails> result = new ArrayList<>();
        for (NetworkIF networkIF : OshiUtils.getNetworkIFs()) {
            OSDetails.OSNetInterfaceDetails nicDetails = OSDetails.OSNetInterfaceDetails.of(networkIF.queryNetworkInterface());
            if (nicDetails != null)
                result.add(nicDetails);

        }
        return result.toArray(new OSDetails.OSNetInterfaceDetails[0]);
    }

    private OSDetails.OSDriveDetails[] getOSDriveDetailsArray(){
        List<OSDetails.OSDriveDetails> drives = new ArrayList<>();
        for (OSFileStore drive : operatingSystem.getFileSystem().getFileStores()) {
            if(drive.getDescription().equals("Local Disk") ||
                    drive.getDescription().equals("Network Drive")) {

                drives.add(new OSDetails.OSDriveDetails(drive.getName(), drive.getTotalSpace() / 1024));//changes from bytes to KB
            }
        }
        return drives.toArray(new OSDetails.OSDriveDetails[drives.size()]);
    }

    private OSDetails.OSVendorDetails getVendorDetails(){
        return new OSDetails.OSVendorDetails(
                operatingSystem.getFamily(),
                operatingSystem.getVersionInfo().getCodeName(),
                operatingSystem.getManufacturer(),
                operatingSystem.getVersionInfo().getVersion());
    }
}
