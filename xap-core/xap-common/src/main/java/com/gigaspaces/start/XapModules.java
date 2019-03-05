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

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
public enum XapModules {
    // System modules
    CORE_COMMON("/required/xap-common", ClassLoaderType.SYSTEM),
    // Common Modules
    DATA_GRID("/required/xap-datagrid", ClassLoaderType.COMMON),
    CORE_REFLECTIONS_ASM("/required/xap-asm", ClassLoaderType.COMMON),
    CORE_COLLECTIONS_TROVE("/required/xap-trove", ClassLoaderType.COMMON),
    LICENSE("/required/xap-premium-common", ClassLoaderType.COMMON),
    MAP("/optional/map/xap-map", ClassLoaderType.COMMON),
    NEAR_CACHE("/optional/near-cache/xap-near-cache", ClassLoaderType.COMMON),
    INTEROP("/optional/interop/xap-interop", ClassLoaderType.COMMON),
    WAN("/optional/wan-gateway/xap-wan-gateway", ClassLoaderType.COMMON),
    SERVICE_GRID("/platform/service-grid/xap-service-grid", ClassLoaderType.COMMON),
    // Service modules
    HIBERNATE_SPRING("/optional/hibernate/xap-hibernate-spring", ClassLoaderType.SERVICE),
    JPA_SPRING("/optional/jpa/xap-jpa-spring", ClassLoaderType.SERVICE),
    MAP_SPRING("/optional/map/xap-map-spring", ClassLoaderType.SERVICE),
    NEAR_CACHE_SPRING("/optional/near-cache/xap-near-cache-spring", ClassLoaderType.SERVICE),
    INTEROP_SPRING("/optional/interop/xap-interop-spring", ClassLoaderType.SERVICE),
    WAN_SPRING("/optional/wan-gateway/xap-wan-gateway-spring", ClassLoaderType.SERVICE),
    ADMIN("/platform/service-grid/xap-admin", ClassLoaderType.SERVICE);

    private static final Collection<XapModules> REQUIRED_NON_SERVICE_CL_MODULES = initRequiredNonServiceModules();

    private static Collection<XapModules> initRequiredNonServiceModules() {
        ArrayList<XapModules> result = new ArrayList<XapModules>();
        for (XapModules module : XapModules.values()) {
            if (module.getClassLoaderType() != ClassLoaderType.SERVICE && module.getJarFilePath().startsWith("/required/"))
                result.add(module);
        }
        return result;
    }

    public static Collection<XapModules> getByClassLoaderType(ClassLoaderType classLoaderType) {
        ArrayList<XapModules> result = new ArrayList<XapModules>();
        for (XapModules module : XapModules.values()) {
            if (module.getClassLoaderType().equals(classLoaderType))
                result.add(module);
        }

        return result;
    }

    private final String artifactName;
    private final String jarFileName;
    private final String jarFilePath;
    private final ClassLoaderType classLoaderType;

    XapModules(String path, ClassLoaderType classLoaderType) {
        this.classLoaderType = classLoaderType;
        int pos = path.lastIndexOf('/');
        this.artifactName = pos == -1 ? path : path.substring(pos + 1);
        this.jarFileName = artifactName + ".jar";
        this.jarFilePath = path + ".jar";
    }

    public String getArtifactName() {
        return artifactName;
    }

    public String getJarFileName() {
        return jarFileName;
    }

    public String getJarFilePath() {
        return jarFilePath;
    }

    public ClassLoaderType getClassLoaderType() {
        return classLoaderType;
    }

    public static boolean isRequiredCommonOrBoot(String filename) {
        // NOTE: this code intentionally uses startsWith and not equals,
        // because when maven is used filename includes version info.
        for (XapModules module : REQUIRED_NON_SERVICE_CL_MODULES) {
            if (filename.startsWith(module.artifactName))
                return true;
        }
        return false;
    }
}
