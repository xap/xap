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


package org.openspaces.pu.container.standalone;

import com.gigaspaces.internal.io.BootIOUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.pu.container.CannotCreateContainerException;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainerProvider;
import org.openspaces.pu.container.support.ClusterInfoParser;
import org.openspaces.pu.container.support.CompoundProcessingUnitContainer;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * A {@link StandaloneProcessingUnitContainer} provider. A standalone processing unit container is a
 * container that understands a processing unit archive structure (both when working with an
 * "exploded" directory and when working with a zip/jar archive of it). It is provided with the
 * location of the processing unit using {@link StandaloneProcessingUnitContainerProvider#StandaloneProcessingUnitContainerProvider(String)}.
 * The location itself follows Spring resource loader syntax.
 *
 * <p> When creating the container a thread is started with {@link org.openspaces.pu.container.standalone.StandaloneContainerRunnable}.
 * This is done since a custom class loader is created taking into account the processing unit
 * archive structure, and in order to allows using the standlone container within other
 * environments, the new class loader is only set on the newly created thread context.
 *
 * <p> At its core the integrated processing unit container is built around Spring {@link
 * org.springframework.context.ApplicationContext} configured based on a set of config locations.
 *
 * <p> The provider allows for programmatic configuration of different processing unit aspects. It
 * allows to configure where the processing unit Spring context xml descriptors are located (by
 * default it uses <code>classpath*:/META-INF/spring/pu.xml</code>). It also allows to set {@link
 * org.openspaces.core.properties.BeanLevelProperties} and {@link org.openspaces.core.cluster.ClusterInfo}
 * that will be injected to beans configured within the processing unit.
 *
 * <p> For a runnable "main" processing unit container please see {@link
 * StandaloneProcessingUnitContainer#main(String[])}.
 *
 * @author kimchy
 */
public class StandaloneProcessingUnitContainerProvider extends ApplicationContextProcessingUnitContainerProvider {

    private static Log logger = LogFactory.getLog(StandaloneProcessingUnitContainerProvider.class);

    private String location;

    private List<String> configLocations = new ArrayList<String>();

    private boolean addedSharedLibToClassLoader = false;

    /**
     * Constructs a new standalone container provider using the provided location as the location of
     * the processing unit archive (either an exploded archive or a jar/zip archive). The location
     * syntax follows Spring {@link org.springframework.core.io.Resource} syntax.
     *
     * @param location The location of the processing unit archive
     */
    public StandaloneProcessingUnitContainerProvider(String location) {
        this.location = location;
    }

    /**
     * Adds a config location based on a String description using Springs {@link
     * org.springframework.core.io.support.PathMatchingResourcePatternResolver}.
     *
     * @see org.springframework.core.io.support.PathMatchingResourcePatternResolver
     */
    public void addConfigLocation(String configLocation) {
        configLocations.add(configLocation);
    }

    public void addConfigLocation(Resource resource) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * <p> Creates a new {@link StandaloneProcessingUnitContainer} based on the configured
     * parameters. A standalone processing unit container is a container that understands a
     * processing unit archive structure (both when working with an "exploded" directory and when
     * working with a zip/jar archive of it). It is provided with the location of the processing
     * unit using {@link org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainerProvider#StandaloneProcessingUnitContainerProvider(String)}.
     * The location itself follows Spring resource loader syntax.
     *
     * <p> If {@link #addConfigLocation(String)} is used, the Spring xml context will be read based
     * on the provided locations. If no config location was provided the default config location
     * will be <code>classpath*:/META-INF/spring/pu.xml</code>.
     *
     * <p> If {@link #setBeanLevelProperties(org.openspaces.core.properties.BeanLevelProperties)} is
     * set will use the configured bean level properties in order to configure the application
     * context and specific beans within it based on properties. This is done by adding {@link
     * org.openspaces.core.properties.BeanLevelPropertyBeanPostProcessor} and {@link
     * org.openspaces.core.properties.BeanLevelPropertyPlaceholderConfigurer} to the application
     * context.
     *
     * <p> If {@link #setClusterInfo(org.openspaces.core.cluster.ClusterInfo)} is set will use it to
     * inject {@link org.openspaces.core.cluster.ClusterInfo} into beans that implement {@link
     * org.openspaces.core.cluster.ClusterInfoAware}.
     *
     * @return An {@link StandaloneProcessingUnitContainer} instance
     */
    public ProcessingUnitContainer createContainer() throws CannotCreateContainerException {
        File fileLocation = new File(location);
        if (!fileLocation.exists()) {
            throw new CannotCreateContainerException("Failed to locate pu location [" + location + "]");
        }

        // in case we don't have a cluster info specific members
        final ClusterInfo clusterInfo = getClusterInfo();
        if (clusterInfo != null && clusterInfo.getInstanceId() == null) {
            ClusterInfo origClusterInfo = clusterInfo;
            List<ProcessingUnitContainer> containers = new ArrayList<ProcessingUnitContainer>();
            for (int i = 0; i < clusterInfo.getNumberOfInstances(); i++) {
                ClusterInfo containerClusterInfo = clusterInfo.copy();
                containerClusterInfo.setInstanceId(i + 1);
                containerClusterInfo.setBackupId(null);
                setClusterInfo(containerClusterInfo);
                containers.add(createContainer());
                if (clusterInfo.getNumberOfBackups() != null) {
                    for (int j = 0; j < clusterInfo.getNumberOfBackups(); j++) {
                        containerClusterInfo = containerClusterInfo.copy();
                        containerClusterInfo.setBackupId(j + 1);
                        setClusterInfo(containerClusterInfo);
                        containers.add(createContainer());
                    }
                }
            }
            setClusterInfo(origClusterInfo);
            return new CompoundProcessingUnitContainer(containers.toArray(new ProcessingUnitContainer[containers.size()]));
        }

        if (clusterInfo != null) {
            ClusterInfoParser.guessSchema(clusterInfo);
        }

        if (logger.isInfoEnabled()) {
            logger.info("Starting a Standalone processing unit container " + (clusterInfo != null ? "with " + clusterInfo : ""));
        }

        List<URL> urls = new ArrayList<URL>();
        List<URL> sharedUrls = new ArrayList<URL>();
        if (fileLocation.isDirectory()) {
            if (fileLocation.exists()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding pu directory location [" + location + "] to classpath");
                }
                try {
                    urls.add(fileLocation.toURL());
                } catch (MalformedURLException e) {
                    throw new CannotCreateContainerException("Failed to add classes to class loader with location ["
                            + location + "]", e);
                }
            }
            addJarsLocation(fileLocation, urls, "lib");
            addJarsLocation(fileLocation, sharedUrls, "shared-lib");
        } else {
            try (JarFile jarFile = new JarFile(fileLocation)) {
                // add the root to the classpath
                try {
                    urls.add(new URL("jar:" + fileLocation.toURL() + "!/"));
                } catch (MalformedURLException e) {
                    throw new CannotCreateContainerException("Failed to add pu location [" + location + "] to classpath", e);
                }
                // add jars in lib and shared-lib to the classpath
                for (Enumeration<JarEntry> entries = jarFile.entries(); entries.hasMoreElements(); ) {
                    JarEntry jarEntry = entries.nextElement();
                    if (isWithinDir(jarEntry, "lib") || isWithinDir(jarEntry, "shared-lib")) {
                        // extract the jar into a temp location
                        if (logger.isDebugEnabled()) {
                            logger.debug("Adding jar [" + jarEntry.getName() + "] with pu location [" + location + "]");
                        }
                        File tempLocation = new File(System.getProperty("java.io.tmpdir") + "/openspaces");
                        tempLocation.mkdirs();
                        File tempJar;
                        String tempJarName = jarEntry.getName();
                        if (tempJarName.indexOf('/') != -1) {
                            tempJarName = tempJarName.substring(tempJarName.lastIndexOf('/') + 1);
                        }
                        try {
                            tempJar = File.createTempFile(tempJarName, ".jar", tempLocation);
                        } catch (IOException e) {
                            throw new CannotCreateContainerException("Failed to create temp jar at location ["
                                    + tempLocation + "] with name [" + tempJarName + "]", e);
                        }
                        tempJar.deleteOnExit();
                        if (logger.isTraceEnabled()) {
                            logger.trace("Extracting jar [" + jarEntry.getName() + "] to temporary jar ["
                                    + tempJar.getAbsolutePath() + "]");
                        }

                        FileOutputStream fos;
                        try {
                            fos = new FileOutputStream(tempJar);
                        } catch (FileNotFoundException e) {
                            throw new CannotCreateContainerException("Failed to find temp jar ["
                                    + tempJar.getAbsolutePath() + "]", e);
                        }
                        InputStream is = null;
                        try {
                            is = jarFile.getInputStream(jarEntry);
                            FileCopyUtils.copy(is, fos);
                        } catch (IOException e) {
                            throw new CannotCreateContainerException("Failed to create temp jar ["
                                    + tempJar.getAbsolutePath() + "]");
                        } finally {
                            if (is != null) {
                                try {
                                    is.close();
                                } catch (IOException e1) {
                                    // do nothing
                                }
                            }
                            try {
                                fos.close();
                            } catch (IOException e1) {
                                // do nothing
                            }
                        }

                        try {
                            if (isWithinDir(jarEntry, "lib")) {
                                urls.add(tempJar.toURL());
                            } else if (isWithinDir(jarEntry, "shared-lib")) {
                                sharedUrls.add(tempJar.toURL());
                            }
                        } catch (MalformedURLException e) {
                            throw new CannotCreateContainerException("Failed to add pu entry [" + jarEntry.getName()
                                    + "] with location [" + location + "]", e);
                        }
                    }
                }
            } catch (IOException e) {
                throw new CannotCreateContainerException("Failed to open pu file [" + location + "]", e);
            }
        }

        List<URL> allUrls = new ArrayList<URL>();
        allUrls.addAll(sharedUrls);
        allUrls.addAll(urls);

        addUrlsToContextClassLoader(allUrls.toArray(new URL[allUrls.size()]));

        StandaloneContainerRunnable containerRunnable = new StandaloneContainerRunnable(getBeanLevelProperties(),
                clusterInfo, configLocations);
        Thread standaloneContainerThread = new Thread(containerRunnable, "Standalone Container Thread");
        standaloneContainerThread.setDaemon(false);
        standaloneContainerThread.start();

        while (!containerRunnable.isInitialized()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for standalone container to initialize");
            }
        }

        if (containerRunnable.hasException()) {
            throw new CannotCreateContainerException("Failed to start container", containerRunnable.getException());
        }

        return new StandaloneProcessingUnitContainer(containerRunnable);
    }

    private boolean isWithinDir(JarEntry jarEntry, String dir) {
        return jarEntry.getName().startsWith(dir + "/") && jarEntry.getName().length() > (dir + "/").length();
    }

    private void addJarsLocation(File fileLocation, List<URL> urls, String dir) {
        File libLocation = new File(fileLocation, dir);
        if (libLocation.exists()) {
            File[] jarFiles = BootIOUtils.listFiles(libLocation);
            for (File jarFile : jarFiles) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding jar [" + jarFile.getAbsolutePath() + "] with pu directory location ["
                            + location + "] to classpath");
                }
                try {
                    urls.add(jarFile.toURL());
                } catch (MalformedURLException e) {
                    throw new CannotCreateContainerException("Failed to add jar file [" + jarFile.getAbsolutePath()
                            + "] to classs loader", e);
                }
            }
        }
    }

    /**
     * Adds the shared lib to the thread context class loader (they need to be added to where the
     * openspaces.jar class exists).
     */
    private void addUrlsToContextClassLoader(URL[] urls) {
        if (addedSharedLibToClassLoader) {
            return;
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class clazz = classLoader.getClass();
        while (clazz != Object.class && !URLClassLoader.class.equals(clazz)) {
            clazz = clazz.getSuperclass();
        }
        if (clazz == Object.class) {
            throw new CannotCreateContainerException("Failed to find URLClassLoader to add shared lib for " + classLoader.getClass());
        }

        try {
            Method addURL = clazz.getDeclaredMethod("addURL", URL.class);
            addURL.setAccessible(true);
            for (URL url : urls) {
                addURL.invoke(classLoader, url);
            }
        } catch (Exception e) {
            throw new CannotCreateContainerException("Failed to add shared lib to thread context class loader [" + classLoader + "]", e);
        }
        addedSharedLibToClassLoader = true;
    }
}
