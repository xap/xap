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
package com.gigaspaces.logger;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.start.Locator;
import com.gigaspaces.start.XapNetworkInfo;

import java.io.File;
import java.lang.management.ManagementFactory;

public class LoggerSystemInfo {

    public static final String xapHome = findXapHome();
    public static final long processId = findProcessId();
    public static final XapNetworkInfo networkInfo = new XapNetworkInfo();

    private static String findXapHome() {
        String result = System.getProperty(CommonSystemProperties.GS_HOME);
        if (result == null)
            result = System.getenv("XAP_HOME");
        if (result == null)
            result = Locator.deriveDirectories().getProperty(Locator.GS_HOME);
        if (result == null)
            result = ".";
        if (result.endsWith(File.separator))
            result = result + File.separator;

        result = trimSuffix(result, File.separator);
        System.setProperty(CommonSystemProperties.GS_HOME, result);
        return result;
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


    private static String trimSuffix(String s, String suffix) {
        return s.endsWith(suffix) ? s.substring(0, s.length() - suffix.length()) : s;
    }
}
