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
