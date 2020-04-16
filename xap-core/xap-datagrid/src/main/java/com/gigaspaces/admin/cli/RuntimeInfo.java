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

/*
 * @(#)RuntimeInfo.java 1.0  02.11.2004  22:33:20
 */

package com.gigaspaces.admin.cli;

import com.gigaspaces.internal.extension.XapExtensions;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Igor Goldenberg
 * @version 4.0
 **/
@com.gigaspaces.api.InternalApi
public class RuntimeInfo {

    private static boolean isFirstTime = true;

    public static void logRuntimeInfo(Logger logger, String prefix) {
        if (logger.isInfoEnabled()) {
            if (logger.isDebugEnabled())
                logger.debug(prefix + getEnvironmentInfoIfFirstTime(true));
            else
                logger.info(prefix + getEnvironmentInfoIfFirstTime(false));
        }
    }

    public static String getEnvironmentInfoIfFirstTime() {
        return getEnvironmentInfoIfFirstTime(false);
    }

    public static String getEnvironmentInfoIfFirstTime(boolean verbose) {
        return isFirstTime ? getEnvironmentInfo(verbose) : "";
    }


    public static String getEnvironmentInfo() {
        return getEnvironmentInfo(false);
    }

    public static String getEnvironmentInfo(boolean verbose) {
        isFirstTime = false;
        return XapExtensions.getInstance().getXapRuntimeReporter().generate(verbose, "System Report", '*', 120);
    }

    public static void main(String[] args) {
        System.out.println(RuntimeInfo.getEnvironmentInfo(true));
    }
}
