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
        List<NetworkIF> networkIFs = hardware.getNetworkIFs();
        OSDetails.OSNetInterfaceDetails[] interfacesList = new OSDetails.OSNetInterfaceDetails[networkIFs.size()];

        for(int i=0;i<interfacesList.length;i++) {
            NetworkInterface networkInterface = networkIFs.get(i).queryNetworkInterface();

            byte[] hwAddress = networkInterface.getHardwareAddress();
            String hardwareAddressStr=translateByteArrayToHwAddress(hwAddress);
            String name = networkInterface.getName();
            String displayName = networkInterface.getDisplayName();

            OSDetails.OSNetInterfaceDetails osNetInterfaceDetails =
                    new OSDetails.OSNetInterfaceDetails(hardwareAddressStr, name, displayName);

            //add network interface details to list
            interfacesList[i] =osNetInterfaceDetails;
        }
        return interfacesList;
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

    private String translateByteArrayToHwAddress(byte[] hardwareAddress){
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
