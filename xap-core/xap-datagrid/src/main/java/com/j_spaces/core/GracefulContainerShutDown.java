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
 * @(#)GracefulContainerShutDown.java 1.0  Nov 15, 2005 5:06:07 PM
 */

package com.j_spaces.core;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class GracefulContainerShutDown {
    final static String FILENAME_POSTFIX = "-shutdown.gcf";
    final static String GRACEFUL_SHUTDOWN = "com.gs.graceful-shutDown";

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONTAINER);


    public static void gracefulShutDown(String containerName) {
        try {
            new File(containerName + FILENAME_POSTFIX).createNewFile();
        } catch (IOException e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(" Failed to create graceful shutDown for " + containerName + FILENAME_POSTFIX + " Exception: " + e.toString(), e);
            }
        }
    }

    public static boolean isGracefulShutDown(String containerName) {
        return new File(containerName + FILENAME_POSTFIX).delete();
    }
}
