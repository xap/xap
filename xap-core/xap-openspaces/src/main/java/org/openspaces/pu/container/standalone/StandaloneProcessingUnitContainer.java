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

import com.gigaspaces.admin.cli.RuntimeInfo;
import com.gigaspaces.internal.utils.JdkVersion;
import com.gigaspaces.logger.GSLogConfigLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.pu.container.CannotCloseContainerException;
import org.openspaces.pu.container.ProcessingUnitContainer;
import org.openspaces.pu.container.spi.ApplicationContextProcessingUnitContainer;
import org.openspaces.pu.container.support.BeanLevelPropertiesParser;
import org.openspaces.pu.container.support.ClusterInfoParser;
import org.openspaces.pu.container.support.CommandLineParser;
import org.openspaces.pu.container.support.ConfigLocationParser;
import org.openspaces.pu.container.support.ProcessingUnitPathParser;
import org.springframework.context.ApplicationContext;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;

/**
 * A {@link StandaloneProcessingUnitContainer} provider. A standalone processing unit container is a
 * container that understands a processing unit archive structure (both when working with an
 * "exploded" directory and when working with a zip/jar archive of it).
 *
 * <p> The standalone processing unit container also provides a a main method ({@link
 * #main(String[])} which uses the {@link StandaloneProcessingUnitContainerProvider} and the
 * provided parameters to create itself. Please see the javadoc for the main method for a full list
 * of the possible parameters values.
 *
 * @author kimchy
 */
public class StandaloneProcessingUnitContainer extends ApplicationContextProcessingUnitContainer {

    private static final Log logger = LogFactory.getLog(StandaloneProcessingUnitContainer.class);

    private StandaloneContainerRunnable containerRunnable;

    public StandaloneProcessingUnitContainer(StandaloneContainerRunnable containerRunnable) {
        this.containerRunnable = containerRunnable;
    }

    public ApplicationContext getApplicationContext() {
        return this.containerRunnable.getApplicationContext();
    }

    @Override
    public void close() throws CannotCloseContainerException {
        containerRunnable.stop();
        // TODO wait till it shuts down
        super.close();
    }

    /**
     * Allows to run the standalone processing unit container. Uses the {@link
     * StandaloneProcessingUnitContainerProvider} and the parameters provided in order to configure
     * it.
     *
     * <p> The following parameters are allowed: <ul> <li><b>[location]</b>: The location of the
     * processing unit archive. See {@link org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainerProvider#StandaloneProcessingUnitContainerProvider(String)}.
     * This parameter is required and must be at the end of the command line.</li> <li><b>-config
     * [configLocation]</b>: Allows to add a Spring application context config location. See {@link
     * org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainerProvider#addConfigLocation(String)}.
     * This is an optional parameter and it can be provided multiple times.</li> <li><b>-properties
     * [beanName] [properties]</b>: Allows to inject {@link org.openspaces.core.properties.BeanLevelProperties},
     * see {@link org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainerProvider#setBeanLevelProperties(org.openspaces.core.properties.BeanLevelProperties)}.
     * [beanName] is optional, if not used, the properties will set the {@link
     * org.openspaces.core.properties.BeanLevelProperties#setContextProperties(java.util.Properties)}.
     * If used, will inject properties only to the bean registered under the provided beanName
     * within the Spring context (see {@link org.openspaces.core.properties.BeanLevelProperties#setBeanProperties(String,
     * java.util.Properties)}). The [properties] can either start with <code>embed://</code> which
     * mean they will be provided within the command line (for example:
     * <code>embed://propName1=propVal1;propName2=propVal2</code>) or they can follow Spring {@link
     * org.springframework.core.io.Resource} lookup based on URL syntax or Spring extended
     * <code>classpath</code> prefix (see {@link org.springframework.core.io.DefaultResourceLoader}).</li>
     * <li><b>-cluster [cluster parameters]</b>: Allows to configure {@link
     * org.openspaces.core.cluster.ClusterInfo}, see {@link org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainerProvider#setClusterInfo(org.openspaces.core.cluster.ClusterInfo)}.</li>
     * The following parameters are allowed: <code>total_members=1,1</code> (1,1 is an example
     * value), <code>id=1</code> (1 is an example value), <code>backup_id=1</code> (1 is an example
     * value) and <code>schema=primary_backup</code> (primary_backup is an example value). No
     * parameter is required. For more information regarding the Space meaning of this parameters
     * please consult GigaSpaces reference documentation within the Space URL section. </ul>
     */
    public static void main(String[] args) throws Exception {

        ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
        if (! (currentClassLoader instanceof URLClassLoader)) {
            URLClassLoader urlClassLoader = new URLClassLoader(new URL[0], currentClassLoader);
            Thread.currentThread().setContextClassLoader(urlClassLoader);
        }



        GSLogConfigLoader.getLoader();
        showUsageOptionsOnHelpCommand(args);
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        logger.info("Starting with args: " + Arrays.toString(args) + "\n" + RuntimeInfo.getEnvironmentInfoIfFirstTime());
        try {
            final ProcessingUnitContainer container = createContainer(args);
            logger.info("Started successfully");

            // Use the MAIN thread as the non daemon thread to keep it alive
            final Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        logger.info("Shutdown hook triggered");
                        container.close();
                        logger.info("Shutdown complete");
                    } finally {
                        mainThread.interrupt();
                    }
                }
            });
            while (!mainThread.isInterrupted()) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    // do nothing, simply exit
                }
            }
        } catch (Exception e) {
            printUsage();
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void showUsageOptionsOnHelpCommand(String[] args) {
        for (String arg : args) {
            if (arg.equals("--help")) {
                printUsage();
                System.exit(0);
            }
        }
    }

    public static ProcessingUnitContainer createContainer(String[] args) throws Exception {
        CommandLineParser.Parameter[] params = CommandLineParser.parse(args);

        StandaloneProcessingUnitContainerProvider provider = new StandaloneProcessingUnitContainerProvider(ProcessingUnitPathParser.parse(params));
        provider.setBeanLevelProperties(BeanLevelPropertiesParser.parse(params));
        provider.setClusterInfo(ClusterInfoParser.parse(params));
        ConfigLocationParser.parse(provider, params);

        return provider.createContainer();
    }

    public static void printUsage() {
        System.out.println("usage options: [-path ...] [-cluster ...] [-properties ...]");
        System.out.println();
        System.out.println("    --help                                    : Shows this usage options menu");
        System.out.println();
        System.out.println("    -path [processing unit path]   (required) : The relative/absolute path of a processing unit directory or jar");
        System.out.println("    -cluster [cluster properties]             : Allows specify cluster parameters");
        System.out.println("             schema=partitioned               : The cluster schema to use (default is partitioned)");
        System.out.println("             total_members=1,1                : The number of instances and number of backups to use");
        System.out.println("             id=1                             : The instance id of this processing unit");
        System.out.println("             backup_id=1                      : The backup id of this processing unit");
        System.out.println("    -properties [properties-loc]              : Location of context level properties");
        System.out.println("    -properties [bean-name] [properties-loc]  : Location of properties used applied only for a specified bean");
        System.out.println();
        System.out.println();
        System.out.println("Processing Unit directory structure:");
        System.out.println("~/my-processing-unit/");
        System.out.println("  |- META-INF/ MANIFEST.MF                       (*)");
        System.out.println("  |            spring/ pu.xml");
        System.out.println("  |                    pu.properties             (*)");
        System.out.println("  |                    sla.xml                   (*)");
        System.out.println("  |- com/mycompany/myproject/ MyClass1.class");
        System.out.println("  |-                          MyClass2.class");
        System.out.println("  |- lib/ dependency1.jar                        (*)");
        System.out.println("  |-      dependency2.jar                        (*)");
        System.out.println();
        System.out.println(" (*) files that are optional");
        System.out.println();
        System.out.println("Some Examples:");
        System.out.println();
        System.out.println("1. -path ~/my-processing-unit/");
        System.out.println("    > Start a processing unit specified by a path to a processing unit directory structure");
        System.out.println();
        System.out.println("2. -path ~/my-processing-unit/target/my-processing-unit.jar");
        System.out.println("    > Starts a processing unit packaged as a jar file (assembled with a processing unit directory structure)");
        System.out.println();
        System.out.println("3. -path ~/my-processing-unit/ -cluster schema=partitioned total_members=2,1");
        System.out.println("    > Starts a processing unit with a partitioned cluster schema of two members (with one backup each) all in one process");
        System.out.println();
        System.out.println("4. -path ~/my-processing-unit/ -cluster schema=partitioned total_members=2,1 id=1");
        System.out.println("    > Starts only the first instance (id=1) of a processing unit belonging to a 2,1 partitioned cluster (two members with one backup each)");
        System.out.println();
        System.out.println("5. -path ~/my-processing-unit/ -cluster schema=partitioned total_members=2,1 id=1 backup_id=1");
        System.out.println("    > Starts only the backup of the first instance (id=1) of a processing unit belonging to a 2,1 partitioned cluster (two members with one backup each)");
        System.out.println();
        System.out.println("6. -path ~/my-processing-unit/ -properties file://config/context.properties -properties space1 file://config/space1.properties");
        System.out.println("    > Starts a processing unit using context level properties (context.properties) and bean level properties (space1.properties) applied to bean named space1");
        System.out.println();
        System.out.println("7. -path ~/my-processing-unit/ -properties embed://prop1=value1 -properties space1 embed://prop2=value2;prop3=value3");
        System.out.println("    > Starts a processing unit using context level properties with a single property called prop1 with value1 and bean level properties with two properties");
        System.out.println();
    }
}
