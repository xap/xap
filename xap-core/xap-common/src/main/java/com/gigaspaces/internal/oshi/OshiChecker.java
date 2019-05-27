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
package com.gigaspaces.internal.oshi;

import com.gigaspaces.CommonSystemProperties;
import oshi.SystemInfo;

import java.util.logging.Level;
import java.util.logging.Logger;

public class OshiChecker {

    private static final Logger logger = Logger.getLogger(OshiChecker.class.getName());
    private static final SystemInfo systemInfo = initSystemInfo();

    private static SystemInfo initSystemInfo() {
        String enabledProperty = System.getProperty(CommonSystemProperties.OSHI_ENABLED, "");
        boolean enabled = enabledProperty.isEmpty() || Boolean.parseBoolean(enabledProperty);
        if (!enabled) {
            logger.info("Oshi is disabled");
            return null;
        }

        logger.log(enabledProperty.isEmpty() ? Level.FINE : Level.INFO, "Oshi is enabled");

        try {
            SystemInfo systemInfo = new SystemInfo();
            logger.log(enabledProperty.isEmpty() ? Level.FINE : Level.INFO, "Oshi is available");
            return systemInfo;
        } catch (Throwable t) {
            logger.log(enabledProperty.isEmpty() ? Level.FINE : Level.WARNING, "Oshi is not available: " + t.toString());
            return null;
        }
    }

    public static boolean isAvailable() {
        return systemInfo != null;
    }

    public static SystemInfo getSystemInfo(){
        return systemInfo;
    }
}
