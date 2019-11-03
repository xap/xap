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

import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class for loading native libraries in GigaSpaces.
 *
 * @author Niv Ingberg
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class NativeLibraryLoader {
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_COMMON);
    private static final Set<String> _loadedLibs = new HashSet<String>();
    private static final String _defaultNativeLibPath;

    static {
        _defaultNativeLibPath = SystemLocations.singleton().libPlatform("native").toString();
    }

    public static void loadNativeLibrary(String libraryName) {
        loadNativeLibrary(libraryName, null);
    }

    public static synchronized void loadNativeLibrary(String libraryName, String customProperty) {
        // If already loaded abort:
        if (_loadedLibs.contains(libraryName))
            return;

        String path = null;
        // If custom property is specified, try it first:
        if (customProperty != null)
            path = System.getProperty(customProperty);
        // If not initialized yet, try default path:
        if (path == null)
            path = _defaultNativeLibPath + File.separator + libraryName + ".dll";

        if (_logger != null && _logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "Loading " + libraryName + " library from " + path);

        System.load(path);

        _loadedLibs.add(libraryName);
    }
}
