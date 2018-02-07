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

package com.gigaspaces.start;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.logger.LogHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Locator class derives the location of GigaSpaces Technologies system directories and provides
 * various utilities for finding files and directories
 */
@com.gigaspaces.api.InternalApi
public class Locator {

    public static final String GS_HOME_DIR_SYSTEM_PROP = "com.gs.home.dir";

    /**
     * Configuration and logger property
     */
    static final String COMPONENT = "com.gigaspaces.start";
    /* Property key indicating the GigaSpaces EAG home */
    public static final String GS_HOME = CommonSystemProperties.GS_HOME;
    /* Property key indicating the GigaSpaces library location */
    private static final String GS_LIB = "com.gigaspaces.lib";
    /* Property key indicating the GigaSpaces location of either SystemJars.DATA_GRID_JAR or SystemJars.CORE_COMMON_JAR */
    public static final String GS_BOOT_LIB = "com.gigaspaces.lib.boot";
    /* Property key indicating the GigaSpaces library gigaspaces location */
    private static final String GS_LIB_REQUIRED = "com.gigaspaces.lib.required";
    /* Property key indicating the GigaSpaces library platform location */
    private static final String GS_LIB_PLATFORM = "com.gigaspaces.lib.platform";
    /* Property key indicating the GigaSpaces library opt location */
    private static final String GS_LIB_OPTIONAL = "com.gigaspaces.lib.opt";
    /* Property key indicating the GigaSpaces library ext location */
    public static final String GS_LIB_PLATFORM_EXT = "com.gigaspaces.lib.platform.ext";
    /* Property key indicating the GigaSpaces library security location */
    public static final String GS_LIB_OPTIONAL_SECURITY = "com.gigaspaces.lib.opt.security";

    public static String getLib(Properties properties) {
        return getLocation(properties, GS_LIB);
    }

    public static String getLibRequired() {
        return System.getProperty(GS_LIB_REQUIRED);
    }

    public static String getLibRequired(Properties properties) {
        return getLocation(properties, GS_LIB_REQUIRED);
    }

    public static String getLibPlatform() {
        return System.getProperty(GS_LIB_PLATFORM);
    }

    public static String getLibPlatform(Properties properties) {
        return getLocation(properties, GS_LIB_PLATFORM);
    }

    public static String getLibOptional() {
        return System.getProperty(GS_LIB_OPTIONAL);
    }

    public static String getLib() {
        return System.getProperty(GS_LIB);
    }

    public static String getLibOptional(Properties properties) {
        return getLocation(properties, GS_LIB_OPTIONAL);
    }

    public static String getLibOptionalSecurity() {
        return System.getProperty(GS_LIB_OPTIONAL_SECURITY, getLibOptional() + "security");
    }

    public static String getLocation(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value != null)
            System.setProperty(key, value);
        return value;
    }

    public static Properties deriveDirectories() {

        Properties locationProps = new Properties();

        // the GS_HOME was externally defined
        if (System.getProperty(GS_HOME_DIR_SYSTEM_PROP) != null) {
            // it got specified externally
            String path = System.getProperty(GS_HOME_DIR_SYSTEM_PROP) + File.separator;
            locationProps.put(GS_HOME, path);
            locationProps.put(GS_LIB, path);
            locationProps.put(GS_BOOT_LIB, path);
            return locationProps;
        }

        try {
            String path = getClassLocation(Locator.class);
            if (path == null) {
                String workDir = path(System.getProperty("user.dir"), "gigaspaces");
                LogHelper.log(COMPONENT, Level.FINE, "Setting Home to [" + workDir + "] since can't derive location from jar file", null);
                //noinspection ResultOfMethodCallIgnored
                new File(workDir).mkdirs();
                locationProps.put(GS_HOME, workDir);
                locationProps.put(GS_LIB, workDir);
                locationProps.put(GS_BOOT_LIB, workDir);
            } else {
                final File location = getLocation(path);
                final File gsLib = findGsLib(path, location);
                locationProps.setProperty(GS_HOME, new File(gsLib, "..").getCanonicalPath() + File.separator);
                locationProps.setProperty(GS_LIB, gsLib.getCanonicalPath() + File.separator);
                locationProps.setProperty(GS_BOOT_LIB, location.getCanonicalPath() + File.separator);
                initDefaultLocations(locationProps, gsLib);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to derive product directories", e);
        }
        LogHelper.log(COMPONENT, Level.FINE, "Derived product directories " + locationProps, null);
        return locationProps;
    }

    private static File getLocation(String path) {
        int ndx = path.lastIndexOf("/");
        if (ndx != -1)
            path = checkDirFormat(path.substring(0, ndx));
        return new File(path);
    }

    private static void initDefaultLocations(Properties properties, File gsLib) throws IOException {
        properties.setProperty(GS_LIB, gsLib.getCanonicalPath() + File.separator);
        properties.setProperty(GS_LIB_REQUIRED, new File(gsLib, "required").getCanonicalPath() + File.separator);
        properties.setProperty(GS_LIB_PLATFORM, new File(gsLib, "platform").getCanonicalPath() + File.separator);
        properties.setProperty(GS_LIB_OPTIONAL, new File(gsLib, "optional").getCanonicalPath() + File.separator);
    }

    /**
     * Derive the location of a file or subdirectory, starting from the GigaSpaces base directory
     * and scanning sub-directories.
     *
     * @param fileName The file name to locate. This parameter must not be null
     * @return The first occurance of the file will be returned, as a cannonical path. If not found,
     * <code>null</code> is returned
     */
    public static String derivePath(String fileName) {
        Properties props = deriveDirectories();
        String baseDir = (String) props.get(GS_HOME);
        Logger logger = Logger.getLogger(COMPONENT);
        if (logger.isLoggable(Level.FINE))
            logger.log(Level.FINE, "Derive path for file: " + fileName + " | locations: " + props.toString());
        return (derivePath(baseDir, fileName));
    }

    /**
     * Derive the location of a file or subdirectory, starting from the GigaSpaces base directory
     * and scanning sub-directories.
     *
     * @param fileName The file name to locate. This parameter must not be null
     * @param baseDir  The directory to base/start the location from. Must not be null. If the value
     *                 start wih a "$", the value is interpreted as a System property. If the
     * @return The first occurance of the file will be returned, as a cannonical path. If not found,
     * <code>null</code> is returned
     */
    public static String derivePath(String baseDir, String fileName) {
        Logger logger = Logger.getLogger(COMPONENT);
        if (logger.isLoggable(Level.FINE))
            logger.log(Level.FINE, "Derive path for file: " + fileName + " | baseDir: " + baseDir);
        if (baseDir == null)
            throw new NullPointerException("baseDir is null");
        if (fileName == null)
            throw new NullPointerException("fileName is null");
        if (baseDir.startsWith("$")) {
            String sysProp = baseDir.substring(1);
            baseDir = System.getProperty(sysProp);
            if (baseDir == null)
                throw new IllegalArgumentException("baseDir [" + sysProp + "] " +
                        "does not reference a valid " +
                        "System Property");
        }
        File base = new File(baseDir);
        if (!base.exists())
            throw new IllegalArgumentException(baseDir + " does not exist");
        if (!base.isDirectory())
            throw new IllegalArgumentException(baseDir + " is not a directory");
        if (!base.canRead())
            throw new IllegalArgumentException("cannot read from " + baseDir);
        String path = locate(base, fileName);
        if (path != null) {
            File checkDir = new File(path);
            if (checkDir.isDirectory())
                path = checkDirFormat(path);
        }
        return (path);
    }

    /**
     * Make sure the directory exists, that read access is permitted and the name has a {@link
     * java.io.File#separator} as it's last character
     *
     * @param dir A non-null String
     * @return A String appended with {@link java.io.File#separator} as it's last character
     * @throws NullPointerException     if the dir param is null
     * @throws IllegalArgumentException if the dir directory does not exist or cannot be read from
     */
    private static String checkDirFormat(String dir) {
        if (dir == null)
            throw new NullPointerException("dir is null");
        File directory = new File(dir);
        if (!directory.exists())
            throw new IllegalArgumentException("directory [" + dir + "] " +
                    "does not exist");
        if (!directory.canRead())
            throw new IllegalArgumentException("no read access to directory " +
                    "[" + dir + "]");
        if (!dir.endsWith(File.separator))
            dir = dir + File.separator;

        return (dir);
    }

    /**
     * Locate a file using the starting base directory, traversing down the directory tree as
     * sub-directories are encountered
     *
     * @param baseDir  Directory base to start from
     * @param fileName Filename to locate
     * @return The cannonical path name of the file being sought
     * @throws NullPointerException if the baseDir or fileName parameters are <code>null</code>
     */
    private static String locate(File baseDir, String fileName) {
        if (baseDir == null)
            throw new IllegalArgumentException("baseDir is null");
        if (fileName == null)
            throw new IllegalArgumentException("subDirName is null");
        String foundFile = null;
        File[] files = BootIOUtils.listFiles(baseDir);
        List<File> dirList = new ArrayList<File>();
        for (File file1 : files) {
            if (file1.getName().equals(fileName)) {
                try {
                    foundFile = file1.getCanonicalPath();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            } else {
                if (file1.isDirectory() && file1.canRead())
                    dirList.add(file1);
            }
        }
        if (foundFile == null) {
            File[] dirs = dirList.toArray(new File[dirList.size()]);
            for (File dir : dirs) {
                String file = locate(dir,
                        fileName);
                if (file != null) {
                    foundFile = file;
                    break;
                }
            }
        }
        return (foundFile);
    }

    public static String getProductHomeDirectoryName() {
        return path(System.getProperty("user.home"), ".gigaspaces");
    }

    private static String path(String path, String element) {
        return path + File.separator + element;
    }

    private static String path(String path, String... elements) {
        for (String element : elements)
            path = path(path, element);
        return path;
    }

    private static String getClassLocation(Class<?> clazz) throws URISyntaxException {
        CodeSource cs = clazz.getProtectionDomain().getCodeSource();
        return cs != null && cs.getLocation() != null ? cs.getLocation().toURI().getPath() : null;
    }

    private static File findGsLib(String path, File location) {
        if (path.contains("platform/ui/xap-ui.jar"))
            return new File(new File(location, ".."), "..");
        if (path.contains("lib" + XapModules.DATA_GRID.getJarFilePath()))
            return new File(location, "..");
        if (path.contains("lib" + XapModules.CORE_COMMON.getJarFilePath()))
            return new File(location, "..");
        return new File(location, "..");
    }
}