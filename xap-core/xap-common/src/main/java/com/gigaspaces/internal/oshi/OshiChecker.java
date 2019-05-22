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
import java.util.logging.Logger;

public class OshiChecker {

    private static final Logger logger = Logger.getLogger(OshiChecker.class.getName());
    private static final boolean enabled = initEnabled();

    private static final SystemInfo systemInfo = initSystemInfo();

    private static boolean initEnabled() {
        String enabled = System.getProperty(CommonSystemProperties.OSHI_ENABLED, "");

        if(enabled.isEmpty() || Boolean.parseBoolean(enabled)){
            logger.fine("Oshi is enabled");
            return true;
        }
        return false;
    }

    private static SystemInfo initSystemInfo() {
        try {
            return new SystemInfo();
        }catch (Throwable t) {
            logger.warning("Oshi is not available "+t.getMessage());
        }
        return null;
    }

    public static boolean isAvailable() {

        return enabled && (systemInfo != null);
    }

    public static SystemInfo getSystemInfo(){
        return systemInfo;
    }
}
