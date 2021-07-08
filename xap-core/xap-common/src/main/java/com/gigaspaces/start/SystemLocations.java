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
import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.io.FileUtils;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.utils.GsEnv;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@InternalApi
public class SystemLocations {

    private static final SystemLocations instance = new SystemLocations();
    public static SystemLocations singleton() {
        return instance;
    }

    private final Path home;
    private final String homeFwdSlash;
    private final Path xapNetHome;
    private final Path bin;
    private final Path config;
    private final Path lib;
    private final Path libRequired;
    private volatile Map<ClassLoaderType, Collection<Path>> libRequiredMap;
    private final Path libOptional;
    private final Path libOptionalSecurity;
    private final Path libPlatform;
    private final Path libPlatformExt;
    private final Path logs;
    private final Path work;
    private final Path deploy;
    private final Path sparkHome;
    private final Path tools;

    private SystemLocations() {
        this.home = initHome();
        this.homeFwdSlash = initHomeFwdSlash(home);
        this.xapNetHome = GsEnv.propertyPath("com.gs.xapnet.home").get();
        this.bin = xapNetHome != null ? xapNetHome.resolve("bin") : home.resolve("bin");
        this.config = home.resolve("config");
        this.lib = GsEnv.propertyPath("com.gigaspaces.lib").get(home.resolve("lib"));
        this.libRequired = GsEnv.propertyPath("com.gigaspaces.lib.required").get(lib.resolve("required"));
        this.libOptional = GsEnv.propertyPath("com.gigaspaces.lib.opt").get(lib.resolve("optional"));
        this.libOptionalSecurity = GsEnv.propertyPath("com.gigaspaces.lib.opt.security").get(libOptional.resolve("security"));
        this.libPlatform = GsEnv.propertyPath("com.gigaspaces.lib.platform").get(lib.resolve("platform"));
        this.libPlatformExt = GsEnv.propertyPath("com.gigaspaces.lib.platform.ext").get(libPlatform.resolve("ext"));
        this.logs = GsEnv.propertyPath("com.gs.logs").get(home.resolve("logs"));
        this.work = GsEnv.propertyPath("com.gs.work").getAndInit(home.resolve("work"));
        this.deploy = GsEnv.propertyPath("com.gs.deploy").getAndInit(home.resolve("deploy"));
        this.sparkHome = fromEnvVar("SPARK_HOME", home.resolve("insightedge").resolve("spark"));
        this.tools = GsEnv.propertyPath("com.gigaspaces.tools").get(home.resolve("tools"));
        System.setProperty("spark.home", sparkHome.toString());
    }

    private static Map<ClassLoaderType, Collection<Path>> categorize(Path libRequired) {
        Map<ClassLoaderType, Collection<Path>> result = new HashMap<>();
        for (ClassLoaderType type : ClassLoaderType.values())
            result.put(type, new ArrayList<>());

        try {
            FileUtils.forEach(libRequired, FileUtils.Filters.jarFiles(), f -> result.get(getClassLoaderType(f)).add(f));
        } catch (IOException e) {
            throw new RuntimeException("Failed to categorize lib/required files", e);
        }
        return result;
    }

    private static ClassLoaderType getClassLoaderType(Path file) {
        // NOTE: this code intentionally uses startsWith and not equals, because fileName may include version suffix.
        final String fileName = file.getFileName().toString();

        // If file is a built-in module, get its class loader type
        for (XapModules module : XapModules.getRequiredModules()) {
            if (fileName.startsWith(module.getArtifactName()))
                return module.getClassLoaderType();
        }

        if (fileName.startsWith("xap-")) {
            if (fileName.startsWith("xap-openspaces"))
                return ClassLoaderType.SERVICE;
            if (fileName.startsWith("xap-slf4j"))
                return ClassLoaderType.SYSTEM;
            // Sensible default, but should not happen (should warn, but we cannot use logs here...)
            return ClassLoaderType.SYSTEM;
        }

        // Spring files are Service, except for spring-jcl which is system:
        if (fileName.startsWith("spring-")) {
            if (fileName.startsWith("spring-jcl-"))
                return ClassLoaderType.SYSTEM;
            return ClassLoaderType.SERVICE;
        }

        return ClassLoaderType.SYSTEM;
    }

    private static Path initHome() {
        String result = System.getProperty(CommonSystemProperties.GS_HOME);
        if (result == null) {
            result = GsEnv.get("HOME", System.getProperty("user.dir"));
            System.setProperty(CommonSystemProperties.GS_HOME, result);
        }

        return Paths.get(result).toAbsolutePath();
    }

    private static String initHomeFwdSlash(Path home) {
        String result = home.toFile().toString().replace("\\", "/");
        System.setProperty(CommonSystemProperties.GS_HOME + ".fwd-slash", result);
        return result;
    }

    private static Path fromEnvVar(String key, Path defaultValue) {
        final String result = System.getenv(key);
        return result != null ? Paths.get(result) : defaultValue;
    }

    public Path home() {
        return home;
    }

    public Path home(String subpath) {
        return home.resolve(subpath);
    }

    public Path home(String subpath, String ... more) {
        return home.resolve(Paths.get(subpath, more));
    }

    public String homeFwdSlash() {
        return homeFwdSlash;
    }

    public Path config() {
        return config;
    }

    public Path config(String subdir) {
        return config.resolve(subdir);
    }

    public Path logs() {
        return logs;
    }

    public Path work() {
        return work;
    }

    public Path work(String subpath) {
        return work.resolve(subpath);
    }

    public Path deploy(){
        return deploy;
    }

    public Path deploy(String subpath) {
        return deploy.resolve(subpath);
    }

    public Path sparkHome() {
        return sparkHome;
    }

    public boolean isXapNet() {
        return xapNetHome != null;
    }

    public Path xapNetHome() {
        return xapNetHome;
    }

    public Path bin() {
        return bin;
    }

    public Path bin(String scriptName) {
        String suffix;
        if (xapNetHome != null)
            suffix = ".exe";
        else
            suffix = (JavaUtils.isWindows() ? ".bat" : ".sh");
        return bin.resolve(scriptName + suffix);
    }

    public Path lib() {
        return lib;
    }

    public Path lib(String subpath) {
        return lib.resolve(subpath);
    }

    public Path lib(XapModules module) {
        return lib.resolve(module.getJarFilePath());
    }

    public Path libRequired() {
        return libRequired;
    }

    public Collection<Path> libRequired(ClassLoaderType clType) {
        // Lazy implementation is required because SystemLocations is also used in non-product-structure scenarios (e.g. maven tests)
        Map<ClassLoaderType, Collection<Path>> snapshot = libRequiredMap;
        if (snapshot == null) {
            synchronized (instance) {
                if (libRequiredMap == null)
                    libRequiredMap = categorize(libRequired);
                snapshot = libRequiredMap;
            }
        }
        return snapshot.get(clType);
    }

    public Path libOptional() {
        return libOptional;
    }

    public Path libOptional(String subpath) {
        return libOptional.resolve(subpath);
    }

    public Path libOptionalSecurity() {
        return libOptionalSecurity;
    }

    public Path libPlatform() {
        return libPlatform;
    }

    public Path libPlatform(String subpath) {
        return libPlatform.resolve(subpath);
    }

    public Path libPlatformExt() {
        return libPlatformExt;
    }

    public Path tools() {
        return tools;
    }

    public Path tools(String subpath) {
        return tools.resolve(subpath);
    }

    public Path tools(String subpath, String ... more) {
        return tools.resolve(Paths.get(subpath, more));
    }
    
    public Path dataGatewayJar() {
        return tools().resolve("data-gateway").resolve("xap-data-gateway.jar");
    }



}
