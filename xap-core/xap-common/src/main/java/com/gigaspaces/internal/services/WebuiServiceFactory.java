package com.gigaspaces.internal.services;

import com.gigaspaces.start.ClassLoaderType;
import com.gigaspaces.start.ClasspathBuilder;
import com.gigaspaces.start.SystemLocations;

public class WebuiServiceFactory extends ServiceFactory {
    @Override
    public String getServiceName() {
        return "WEBUI";
    }

    @Override
    protected String getServiceClassName() {
        return "org.openspaces.launcher.Launcher";
    }

    @Override
    protected void initializeClasspath(ClasspathBuilder classpath) {
        SystemLocations locations = SystemLocations.singleton();
        classpath
                // $GS_JARS
                .appendLibRequiredJars(ClassLoaderType.COMMON)
                .appendLibRequiredJars(ClassLoaderType.SERVICE)
                .appendJars(locations.libPlatformExt())
                .appendOptionalJars("spring").appendJars(locations.libOptionalSecurity())        // $SPRING_JARS
                .appendOptionalJars("jetty").appendOptionalJars("jetty/xap-jetty")
                .appendOptionalJars("interop")
                .appendOptionalJars("memoryxtend/off-heap")
                .appendOptionalJars("memoryxtend/rocksdb")
                .appendPlatformJars("service-grid")
                .appendPlatformJars("zookeeper");
    }
}
