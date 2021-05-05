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

package com.gigaspaces.internal.utils;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.kernel.SystemProperties;

import org.jini.rio.boot.BootUtil;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.*;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class XapRuntimeReporter {
    private final List<String> lines = new ArrayList<String>();
    private int width = 0;

    protected void append(String line) {
        lines.add(line);
        if (line.length() > width)
            width = line.length();
    }

    private void clear() {
        lines.clear();
        width = 0;
    }

    public String generate(boolean verbose, String title, char sepChar, int maxWidth) {
        clear();
        appendRuntimeInformation(verbose);
        return format(title, sepChar, maxWidth);
    }

    protected void appendRuntimeInformation(boolean verbose) {
        appendGigaSpacesPlatformInfo();
        appendJavaDetails();
        appendSystemDetails();
        appendNetworkInfo();
        if (verbose) {
            appendEnvironmentVariables();
            appendSystemProperties();
        }
    }

    protected String format(String title, char sepChar, int maxWidth) {
        final StringBuilder sb = new StringBuilder();
        final int width = Math.min(this.width, maxWidth);
        sb.append("\n");
        appendSeparator(sb, sepChar, " " + title + " ", width);
        for (String line : lines)
            sb.append(line).append("\n");
        append(sb, sepChar, width);
        return sb.toString();
    }

    protected void appendGigaSpacesPlatformInfo() {
        SystemInfo systemInfo = SystemInfo.singleton();
        append(PlatformVersion.getOfficialVersion());
        append("    Home: " + SystemLocations.singleton().home());
        append("    Lookup Groups: " + systemInfo.lookup().groups());
        if (systemInfo.getManagerClusterInfo().isEmpty()) {
            String locators = systemInfo.lookup().locators();
            append("    Lookup Locators: " + (locators != null ? locators : ""));
        }
        else {
            append("    Manager Servers: " + systemInfo.getManagerClusterInfo().toString());
        }

        String communicationFilterFactory = System.getProperty(SystemProperties.LRMI_NETWORK_FILTER_FACTORY, null);
        if (communicationFilterFactory != null)
            append("    Communication Filter Factory: " + communicationFilterFactory);
    }

    protected void appendJavaDetails() {
        append("Java:");
        append("    Java Runtime: " + System.getProperty("java.runtime.name") + " " + System.getProperty("java.runtime.version") + " (" + JavaUtils.getVendor() + ")");
        append("    Java VM: " + System.getProperty("java.vm.name") + " " + System.getProperty("java.vm.version") + " (" + System.getProperty("java.vm.vendor") + ")");
        append("    Java Home: " + System.getProperty("java.home"));

        Runtime rt = Runtime.getRuntime();
        // See http://stackoverflow.com/questions/3571203/what-is-the-exact-meaning-of-runtime-getruntime-totalmemory-and-freememory
        final long maxHeapBytes = rt.maxMemory();   // Maximum heap size (== xmx)
        final long currHeapBytes = rt.totalMemory();// curr heap size (initially == xms, later grows)
        final long freeHeapBytes = rt.freeMemory(); // Free space in *current* heap
        final long usedHeapBytes = currHeapBytes - freeHeapBytes;
        append("    Memory: " +
                bytesToString("Currently used ", usedHeapBytes) + ", " +
                bytesToString("Current heap size ", currHeapBytes) + ", " +
                bytesToString("Max heap size ", maxHeapBytes));
    }

    protected void appendSystemDetails() {
        append("Operating System: " + System.getProperty("os.name") +
                " [version=" + System.getProperty("os.version") +
                ", architecture=" + System.getProperty("os.arch") +
                ", processors=" + Runtime.getRuntime().availableProcessors() + "]");
        append("Process Id: " + SystemInfo.singleton().os().processId());
        append("Username: " + SystemInfo.singleton().os().getUsername());
    }

    protected void appendNetworkInfo() {
        append("Network:");
        try {
            append("    Local Host: [" + SystemInfo.singleton().network().getHost() + "] ");
            if (!SystemInfo.singleton().network().getPublicHost().getHostAddress().equals(SystemInfo.singleton().network().getHost().getHostAddress())) {
                append("    Public Host: [" + SystemInfo.singleton().network().getPublicHost() + "] ");
            }
            NetworkInterface[] networkInterfaces = BootUtil.getNetworkInterfaces();
            for (NetworkInterface networkInterface : networkInterfaces) {
                String desc = toString(networkInterface);
                if (desc != null)
                    append(desc);
            }
        } catch (Exception e) {
            append("Failed to get Network Interface Info: " + e.getMessage());
        }
    }

    protected void appendSystemProperties() {
        appendMap(System.getProperties(), "System properties");
    }

    protected void appendEnvironmentVariables() {
        appendMap(System.getenv(), "Environment variables");
    }

    protected void appendMap(Map map, String title) {
        append(title + " (total=" + map.size() + "):");
        ArrayList<String> keys = new ArrayList<String>(map.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            append("    " + key + "=" + map.get(key));
        }
    }

    protected String bytesToString(String prefix, long sizeInBytes) {
        return prefix + (sizeInBytes / 1000000) + "MB";
    }

    protected String toString(NetworkInterface networkInterface) {
        Enumeration<InetAddress> addressesEnum = networkInterface.getInetAddresses();
        if (addressesEnum.hasMoreElements()) {
            StringBuilder sb = new StringBuilder();
            sb.append("    " + networkInterface.getName() + ": " + networkInterface.getDisplayName());
            sb.append(" [IP addresses: ");
            while (addressesEnum.hasMoreElements())
                sb.append(addressesEnum.nextElement().getHostAddress()).append(" | ");
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]");
            return sb.toString();
        }
        return null;
    }

    private static void appendSeparator(StringBuilder sb, char c, String message, int length) {
        int tempLength = (length - message.length()) / 2;
        append(sb, c, tempLength);
        sb.append(message);
        append(sb, c, tempLength);
        if (tempLength + message.length() + tempLength < length)
            sb.append(c);
        sb.append("\n");
    }

    private static void append(StringBuilder sb, char c, int times) {
        for (int i = 0; i < times; i++)
            sb.append(c);
    }
}
