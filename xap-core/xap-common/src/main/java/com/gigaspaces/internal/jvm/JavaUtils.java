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
package com.gigaspaces.internal.jvm;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.utils.LazySingleton;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Optional;

@InternalApi
public class JavaUtils {
    private static final String VENDOR = System.getProperty("java.vendor", "");
    private static final String VERSION = System.getProperty("java.version", "");
    private static final int JAVA_VERSION_MAJOR = parseJavaMajorVersion(VERSION);
    private static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
    private static final boolean isOsx = System.getProperty("os.name").toLowerCase().startsWith("mac os x");
    private static final String username = System.getProperty("user.name");
    private static final LazySingleton<JVMDiagnosticWrapper> jvmDiagnosticWrapper = new LazySingleton<>(JavaUtils::initJVMDiagnosticWrapper);
    private static final LazySingleton<Long> pid = new LazySingleton<>(JavaUtils::findProcessId);

    /**
     * Starting Java 9, format is: MAJOR.MINOR.SECURITY, where trailing 0 are omitted.
     * For example: 9, 9.0.1, etc.
     * Older versions format is 1.MAJOR.suffix
     * For example: 1.8.144, 1.7, etc.
     */
    private static int parseJavaMajorVersion(String version) {
        final String EA_SUFFIX = "-ea";
        // Remove "1." if exists (versions before 9):
        if (version.startsWith("1."))
            version = version.substring(2);
        // Supports early access versions (ends with '-ea'):
        if (version.endsWith(EA_SUFFIX))
            version = version.substring(0, version.length() - EA_SUFFIX.length());
        // Find and remove everything after major, if any:
        int pos = version.indexOf('.');
        if (pos != -1)
            version = version.substring(0, pos);
        try {
            return Integer.parseInt(version);
        } catch (NumberFormatException e) {
            // throw a user-friendly exception
            throw new IllegalArgumentException("Failed to parse java version: " + version);
        }
    }

    public static String getVersion() {
        return VERSION;
    }

    public static int getMajorJavaVersion() {
        return JAVA_VERSION_MAJOR;
    }

    public static boolean greaterOrEquals(int version) {
        return JAVA_VERSION_MAJOR >= version;
    }

    public static boolean isWindows() {
        return isWindows;
    }

    public static boolean isOsx() {
        return isOsx;
    }

    /**
     * Dumps the heap, note generally you only want live objects (does garage collection first)
     * @param filename - name of the file
     * @param live - only live or include uncollected objects
     * @throws IOException - thrown if there is an error
     */
    public static void dumpHeap(String filename, boolean live) throws IOException {
        jvmDiagnosticWrapper.getOrCreate().dumpHeap(filename, live);
    }

    public static boolean useCompressedOopsAsBoolean() {
        return jvmDiagnosticWrapper.getOrCreate().useCompressedOopsAsBoolean();

    }

    private static JVMDiagnosticWrapper initJVMDiagnosticWrapper()  {
        return JVMUtils.getJVMDiagnosticWrapper();
    }

    public static void main(String[] args) {
        try {
            String request = args.length != 0 ? args[0].toLowerCase() : "version";
            switch (request) {
                case "version":
                    System.out.println(JavaUtils.getMajorJavaVersion());
                    break;
                default:
                    System.out.println("Unsupported: [" + request + "]");
                    break;
            }
        } catch (Exception e) {
            System.out.println("Error: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String getVendor() {
        return VENDOR;
    }

    public static long getPid() {
        return pid.getOrCreate();
    }

    public static String getUsername() {
        return username;
    }

    private static long findProcessId() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int pos = name.indexOf('@');
        if (pos < 1)
            return -1;
        try {
            return Long.parseLong(name.substring(0, pos));
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
