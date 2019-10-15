package com.gigaspaces.internal.os;

import com.gigaspaces.internal.oshi.OshiChecker;
import com.gigaspaces.start.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;
import oshi.util.FormatUtil;
import oshi.util.ParseUtil;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class OshiOSDetailsProbe implements OSDetailsProbe  {
    private static final String uid = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostAddress = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostName = SystemInfo.singleton().network().getHost().getHostName();
    private static final oshi.SystemInfo oshiSystemInfo = OshiChecker.getSystemInfo();

    @Override
    public OSDetails probeDetails() throws Exception {
        if (uid == null) {
            return new OSDetails();
        }

        OperatingSystem operatingSystem = oshiSystemInfo.getOperatingSystem();
        HardwareAbstractionLayer hardwareAbstractionLayer = oshiSystemInfo.getHardware();

        GlobalMemory memory = hardwareAbstractionLayer.getMemory();
        VirtualMemory virtualMemory = memory.getVirtualMemory();

        return new OSDetails(uid,
                operatingSystem.getManufacturer(),
                FormatUtil.formatBytes(operatingSystem.getBitness()),
                operatingSystem.getVersion().getBuildNumber(),
                hardwareAbstractionLayer.getProcessor().getLogicalProcessorCount(),
                virtualMemory.getSwapTotal(),
                memory.getTotal(),
                localHostName, localHostAddress,
                getOSNetDetails(),
                getOSDriveDetailsArray(),
                getVendorDetails());

    }

    private OSDetails.OSNetInterfaceDetails[] getOSNetInterfacesDetailsArray() throws SocketException {

        NetworkIF[] networkIFs = oshiSystemInfo.getHardware().getNetworkIFs();
        OSDetails.OSNetInterfaceDetails[] netInterfaceConfigArray = new
                OSDetails.OSNetInterfaceDetails[networkIFs.length];

        for (int index = 0; index < networkIFs.length; index++) {
            NetworkIF networkIF = networkIFs[index];
            NetworkInterface netInterface = networkIF.queryNetworkInterface();

            String addr = ParseUtil.byteArrayToHexString(netInterface.getHardwareAddress());
            String name = netInterface.getName();
            String description = netInterface.getDisplayName();
            if (description == null) {
                //can happen on Windows when the network interface has no description
                description = String.valueOf(name);
            }
            OSDetails.OSNetInterfaceDetails netInterfaceConfig =
                    new OSDetails.OSNetInterfaceDetails(addr, name, description);
            netInterfaceConfigArray[index] = netInterfaceConfig;
        }

        return netInterfaceConfigArray;
    }

    private OSDetails.OSNetInterfaceDetails[] getOSNetDetails() throws SocketException {
        NetworkIF[] networkIFs = oshiSystemInfo.getHardware().getNetworkIFs();
        OSDetails.OSNetInterfaceDetails[] interfacesList = new OSDetails.OSNetInterfaceDetails[networkIFs.length];

        for(int i=0;i<networkIFs.length;i++) {
            NetworkInterface networkInterface = networkIFs[i].queryNetworkInterface();

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

        OperatingSystem operatingSystem = oshiSystemInfo.getOperatingSystem();

        for (OSFileStore drive :operatingSystem.getFileSystem().getFileStores()) {
            if(drive.getDescription().equals("Local Disk") ||
                    drive.getDescription().equals("Network Drive")) {

                drives.add(new OSDetails.OSDriveDetails(drive.getName(), drive.getTotalSpace() / 1024));//changes from bytes to KB
            }
        }
        return drives.toArray(new OSDetails.OSDriveDetails[drives.size()]);
    }

    private OSDetails.OSVendorDetails getVendorDetails(){
        return new OSDetails.OSVendorDetails(
                oshiSystemInfo.getOperatingSystem().getFamily(),
                oshiSystemInfo.getOperatingSystem().getVersion().getCodeName(),
                oshiSystemInfo.getOperatingSystem().getManufacturer(),
                oshiSystemInfo.getOperatingSystem().getVersion().getVersion());
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
