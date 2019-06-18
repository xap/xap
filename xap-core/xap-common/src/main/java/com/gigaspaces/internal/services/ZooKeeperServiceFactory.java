package com.gigaspaces.internal.services;

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
        classpath.appendPlatform("zookeeper")
                .appendPlatform("service-grid")
                // Required jars: spring-context-*, spring-beans-*, spring-core-*, spring-jcl-*, xap-datagrid, xap-asm, xap-trove
                .appendRequired(ClasspathBuilder.startsWithFilter("slf4j-", "spring-", "xap-datagrid", "xap-openspaces", "xap-asm", "xap-trove"));

    }
}
