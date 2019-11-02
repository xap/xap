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


package org.openspaces.pu.container.jee;

import com.gigaspaces.start.ClasspathBuilder;

import com.gigaspaces.start.SystemInfo;
import org.jini.rio.boot.SharedServiceData;
import org.openspaces.core.properties.BeanLevelProperties;
import org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider;
import org.openspaces.pu.container.support.ResourceApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * An extension to the {@link org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider}
 * that can handle JEE processing units.
 *
 * @author kimchy
 */
public abstract class JeeProcessingUnitContainerProvider extends ApplicationContextProcessingUnitContainerProvider {

    /**
     * The {@link javax.servlet.ServletContext} key under which the {@link
     * org.openspaces.core.cluster.ClusterInfo} is stored.
     */
    public static final String CLUSTER_INFO_CONTEXT = "clusterInfo";

    /**
     * The {@link javax.servlet.ServletContext} key under which the {@link
     * org.openspaces.core.properties.BeanLevelProperties} is stored.
     */
    public static final String BEAN_LEVEL_PROPERTIES_CONTEXT = "beanLevelProperties";


    public static final String JETTY_PORT_CONTEXT_PREFIX = "jetty";
    public static final String JETTY_PORT_CONTEXT_SUFFIX = ".port.actual";

    /**
     * @since 12.1
     */
    public static final String JETTY_PORT_ACTUAL_CONTEXT = JETTY_PORT_CONTEXT_PREFIX + JETTY_PORT_CONTEXT_SUFFIX;

    /**
     * The {@link javax.servlet.ServletContext} key under which the {@link
     * org.springframework.context.ApplicationContext} (loaded from the <code>pu.xml</code>) is
     * stored.
     */
    public static final String APPLICATION_CONTEXT_CONTEXT = "applicationContext";

    public static final String JEE_CONTAINER_PROPERTY_NAME = "jee.container";

    public static final String DEFAULT_JEE_CONTAINER = "jetty";

    private ClassLoader classLoader;

    private File deployPath;

    private final List<Resource> configResources = new ArrayList<Resource>();

    private Iterable<URL> manifestURLs;

    private ApplicationContext parentContext;

    public abstract String getJeeContainerType();

    public Iterable<URL> getManifestURLs() {
        return this.manifestURLs;
    }

    public void setManifestUrls(Iterable<URL> manifestURLs) {
        this.manifestURLs = manifestURLs;
    }

    /**
     * Sets the class loader this processing unit container will load the application context with.
     */
    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Sets the deploy path where the exploded war jetty will work with is located.
     */
    public void setDeployPath(File warPath) {
        this.deployPath = warPath;
    }

    public File getDeployPath() {
        return deployPath;
    }

    /**
     * Adds a config location using Springs {@link org.springframework.core.io.Resource}
     * abstraction. This config location represents a Spring xml context.
     *
     * <p>Note, once a config location is added that default location used when no config location
     * is defined won't be used (the default location is <code>classpath*:/META-INF/spring/pu.xml</code>).
     */
    public void addConfigLocation(Resource resource) {
        this.configResources.add(resource);
    }

    /**
     * Adds a config location based on a String description using Springs {@link
     * org.springframework.core.io.support.PathMatchingResourcePatternResolver}.
     *
     * @see org.springframework.core.io.support.PathMatchingResourcePatternResolver
     */
    public void addConfigLocation(String path) throws IOException {
        Resource[] resources = new PathMatchingResourcePatternResolver().getResources(path);
        for (Resource resource : resources) {
            addConfigLocation(resource);
        }
    }

    /**
     * Sets Spring parent {@link org.springframework.context.ApplicationContext} that will be used
     * when constructing this processing unit application context.
     */
    public void setParentContext(ApplicationContext parentContext) {
        this.parentContext = parentContext;
    }

    protected Iterable<String> getWebAppClassLoaderJars() {
        List<String> result = new ArrayList<String>();
        SystemInfo.XapLocations locations = SystemInfo.singleton().locations();
        result.add(System.getProperty("com.gs.pu-common", locations.libOptional("pu-common").toString()));
        result.add(System.getProperty("com.gs.web-pu-common", locations.libOptional("web-pu-common").toString()));
        return result;
    }

    protected Iterable<String> getWebAppClassLoaderClassPath() {
        return new ClasspathBuilder().append(getJeeContainerJarPath(getJeeContainerType())).toFilesNames();
    }

    protected ClassLoader getJeeClassLoader() throws Exception {
        return SharedServiceData.getJeeClassLoader(getJeeContainerType());
    }

    protected ResourceApplicationContext initApplicationContext() {
        Resource[] resources = configResources.toArray(new Resource[configResources.size()]);
        // create the Spring application context
        ResourceApplicationContext applicationContext = new ResourceApplicationContext(resources, parentContext,
                getConfig());
        if (classLoader != null) {
            applicationContext.setClassLoader(classLoader);
        }
        return applicationContext;
    }

    public static Path getJeeContainerJarPath(String jeeContainer) {
        return SystemInfo.singleton().locations().libOptional(jeeContainer).resolve("xap-" + jeeContainer);
    }

    public static String getJeeContainer(BeanLevelProperties properties) {
        return properties.getContextProperties().getProperty(JEE_CONTAINER_PROPERTY_NAME, DEFAULT_JEE_CONTAINER);
    }
}
