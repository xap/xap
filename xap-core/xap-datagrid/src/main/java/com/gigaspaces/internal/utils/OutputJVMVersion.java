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
 * Returns the JVM version short prefixed number e.g. 1.4 1.5 1.6
 *
 * @author gershond
 * @version 6.5
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class OutputJVMVersion {
    public static final String JVM_VERSION;

    static {
        JVM_VERSION = System.getProperty("java.version");
    }

    public static void main(String[] args) throws Exception {
        try {
            if (JVM_VERSION != null) {
                String shortJvmVersion = JVM_VERSION.substring(0, JVM_VERSION.lastIndexOf('.'));
                String majorVersion;
                if(shortJvmVersion.startsWith("1.")){
                    majorVersion = String.valueOf(shortJvmVersion.charAt(shortJvmVersion.indexOf(".") + 1));
                }
                else{
                    majorVersion = shortJvmVersion.substring(0, shortJvmVersion.indexOf("."));
                }
                System.out.println(majorVersion);
                System.exit(0);
            }
        } catch (Exception e) {
            System.out.println("Could not parse java.version property: " + JVM_VERSION);
        }
        System.exit(1);
    }
}