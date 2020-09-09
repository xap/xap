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

package com.gigaspaces.internal.os.jmx;

import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSDetails.OSNetInterfaceDetails;
import com.gigaspaces.internal.os.OSDetailsProbe;
import com.gigaspaces.start.SystemInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class JMXOSDetailsProbe implements OSDetailsProbe {

    private static final String uid = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostAddress = SystemInfo.singleton().network().getHost().getHostAddress();
    private static final String localHostName = SystemInfo.singleton().network().getHost().getHostName();
    private static final OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();

    private static Method getTotalSwapSpaceSize;
    private static Method getTotalPhysicalMemorySize;

    static {

        try {
            Class sunOperatingSystemMXBeanClass = JMXOSDetailsProbe.class.getClassLoader().loadClass("com.sun.management.OperatingSystemMXBean");
            try {
                Method method = sunOperatingSystemMXBeanClass.getMethod("getTotalSwapSpaceSize");
                method.setAccessible(true);
                getTotalSwapSpaceSize = method;
            } catch (NoSuchMethodException e) {
                // no method, bail
            }
            try {
                Method method = sunOperatingSystemMXBeanClass.getMethod("getTotalPhysicalMemorySize");
                method.setAccessible(true);
                getTotalPhysicalMemorySize = method;
            } catch (NoSuchMethodException e) {
                // no method, bail
            }
        } catch (ClassNotFoundException e) {
            // not using sun. can't get the information
        }
    }

    public OSDetails probeDetails() {
        if (uid == null) {
            return new OSDetails();
        }
        long totalSwapSpaceSize = -1;
        if (getTotalSwapSpaceSize != null) {
            try {
                totalSwapSpaceSize = (Long) getTotalSwapSpaceSize.invoke(operatingSystemMXBean);
            } catch (Exception e) {
                // ignore
            }
        }
        long totalPhysicalMemorySize = -1;
        if (getTotalPhysicalMemorySize != null) {
            try {
                totalPhysicalMemorySize = (Long) getTotalPhysicalMemorySize.invoke(operatingSystemMXBean);
            } catch (Exception e) {
                // ignore
            }
        }

        OSNetInterfaceDetails[] osNetInterfaceDetailsArray = retrieveOSNetInterfaceDetails();

        return new OSDetails(uid, operatingSystemMXBean.getName(), operatingSystemMXBean.getArch(),
                operatingSystemMXBean.getVersion(), operatingSystemMXBean.getAvailableProcessors(),
                totalSwapSpaceSize, totalPhysicalMemorySize,
                localHostName, localHostAddress, osNetInterfaceDetailsArray, null, null);
    }

    private OSNetInterfaceDetails[] retrieveOSNetInterfaceDetails() {
        try {
            Enumeration<NetworkInterface> networkInterfacesEnum = NetworkInterface.getNetworkInterfaces();
            List<OSNetInterfaceDetails> interfacesList = new ArrayList<>();
            while (networkInterfacesEnum.hasMoreElements()) {
                NetworkInterface nic = networkInterfacesEnum.nextElement();
                OSNetInterfaceDetails nicDetails = OSNetInterfaceDetails.of(nic);
                if (nicDetails != null)
                    interfacesList.add(nicDetails);
            }
            return interfacesList.toArray(new OSNetInterfaceDetails[0]);
        } catch (SocketException se) {
            return null;
        }
    }
}