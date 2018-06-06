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

/**
 * Returns the default JVM options for the current JVM vendor and version
 *
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class OutputJVMOptions {

    public static void main(String[] args) {
        try {
            String result = getJvmOptions();
            System.out.println(result);
            System.exit(0);
        } catch (Exception e) {
            System.out.println("Failed to generate XAP java options");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String getJvmOptions() {
        final String vmName = getJvmName();
        if (vmName.equals("HOTSPOT")) {
            String result = "-server -XX:+AggressiveOpts -XX:+HeapDumpOnOutOfMemoryError";
            if (!JdkVersion.isAtLeastJava18()) { // < 8
                result += " -XX:MaxPermSize=256m";
            } else if (JdkVersion.getMajorJavaVersion() == JdkVersion.JAVA_9) {
                result += " --add-modules=ALL-SYSTEM";
            }
            return result;
        }

        if (vmName.equals("J9"))
            return "-XX:MaxPermSize=256m";

        return "";
    }

    private static String getJvmName() {
        String vmName = System.getProperty("java.vm.name");
        if (vmName == null)
            return null;
        String[] split = vmName.split(" |\\(");
        String result;
        if (split.length > 1)
            result = split[1];// e.g. IBM J9 VM --> J9 or Oracle HotSpot(R) --> HotSpot
        else
            result = split[0];
        return result.toUpperCase();
    }
}
