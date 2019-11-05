package com.gigaspaces.internal.services;

import com.gigaspaces.start.ClassLoaderType;
import com.gigaspaces.start.ClasspathBuilder;

/**
 * @author kobi on 01/01/17.
 * @since 12.1
 */
public class ZooKeeperServiceFactory extends ServiceFactory {
    @Override
    public String getServiceName() {
        return "ZK";
    }

    @Override
    protected String getServiceClassName() {
        return "org.openspaces.zookeeper.grid.XapZookeeperContainer";
    }

    @Override
    protected void initializeClasspath(ClasspathBuilder classpath) {
        classpath.appendPlatformJars("zookeeper")
                .appendPlatformJars("service-grid")
                .appendLibRequiredJars(ClassLoaderType.COMMON)
                .appendLibRequiredJars(ClassLoaderType.SERVICE);

    }
}
