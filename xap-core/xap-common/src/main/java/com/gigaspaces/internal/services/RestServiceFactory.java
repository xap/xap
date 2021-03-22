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
package com.gigaspaces.internal.services;

import com.gigaspaces.admin.ManagerClusterType;
import com.gigaspaces.start.*;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class RestServiceFactory extends ServiceFactory {
    @Override
    public String getServiceName() {
        return "REST";
    }

    @Override
    protected String getServiceClassName() {
        return "org.openspaces.launcher.JettyManagerRestLauncher";
    }

    @Override
    protected void initializeClasspath(ClasspathBuilder classpath) {
        classpath.appendJar(XapModules.ADMIN)
                .appendJar(XapModules.SERVICE_GRID)
                .appendAny(SystemLocations.singleton().libOptionalSecurity())
                .appendPlatformJars("scala")
                .appendPlatformJars("blueprints")
                .appendLibRequiredJars(ClassLoaderType.COMMON)
                .appendLibRequiredJars(ClassLoaderType.SERVICE)
                .appendOptionalJars("jetty")
                .appendOptionalJars("spring")
                .appendOptionalJars("jetty/xap-jetty")
                .appendOptionalJars("jackson")
                .appendOptionalJars("metrics")
                .appendOptionalJars("jdbc")
                .appendPlatformJars("jdbc")
                .appendPlatformJars("zookeeper");

        // Prevents ClassDef Exception while using kubernetes because he uses okhttp3 client (3.14.3) while we use okhttp2 (2.7.5) in other places.
        if (SystemInfo.singleton().getManagerClusterInfo().getManagerClusterType() == ManagerClusterType.ELASTIC_GRID) {
            classpath.appendJars(SystemLocations.singleton().tools().resolve("cli"), path ->
                    path.toFile().getName().startsWith("okhttp")
                            || path.toFile().getName().startsWith("okio")
                            || path.toFile().getName().startsWith("gson"));
        }
    }
}
