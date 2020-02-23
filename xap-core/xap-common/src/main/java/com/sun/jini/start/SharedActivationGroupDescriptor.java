/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.sun.jini.start;

import com.gigaspaces.logger.LogUtils;
import net.jini.config.Configuration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.rmi.MarshalledObject;
import java.rmi.activation.ActivationException;
import java.rmi.activation.ActivationGroupDesc;
import java.rmi.activation.ActivationGroupDesc.CommandEnvironment;
import java.rmi.activation.ActivationGroupID;
import java.rmi.activation.ActivationSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Class used to create a shared activation group. Clients construct this object with the details of
 * the activation group to be launched, then call <code>create</code> to register the activation
 * system group with the activation system <P> This class, in conjunction with the {@link
 * ActivateWrapper} class, creates an activation group suitable for hosting multiple service
 * objects, with each object maintaining a distinct codebase and policy.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */

@com.gigaspaces.api.InternalApi
public class SharedActivationGroupDescriptor
        implements ServiceDescriptor, Serializable {
    private static final long serialVersionUID = 1L;

    // Required Args
    /**
     * @serial <code>String</code> representing VM policy filename or URL
     */
    private final String policy;

    /**
     * @serial <code>String</code> representing the class path of the shared VM classes
     */
    private final String classpath;

    /**
     * @serial <code>String</code> representing the location where group identifier information will
     * be persisted
     */
    private final String log;

    // Optional Args
    /**
     * @serial <code>String</code> representing the VM command to use
     */
    private final String serverCommand;

    /**
     * @serial <code>String[]</code> representing array of command line options to pass to the VM's
     * command line
     */
    private final String[] serverOptions;

    /**
     * @serial <code>Properties</code> representing propperties to pass to the VM's command line
     */
    private final Properties serverProperties;

    /**
     * @serial <code>String</code> representing host name of the desired activation system
     */
    private final String host;

    /**
     * @serial <code>int</code> representing port of the desired activation system
     */
    private final int port;

    private static final String GROUP_COOKIE_FILE = "cookie";

    private static final Logger logger = ServiceStarter.logger;


    /**
     * Trivial constructor. Simply calls the other overloaded constructor with the <code>host</code>
     * and <code>port</code> parameters set to <code>null</code> and 0, respectively.
     */
    public SharedActivationGroupDescriptor(
            //Required Args
            String policy, String classpath, String log,
            //Optional Args
            String serverCommand, String[] serverOptions,
            String[] serverProperties) {
        this(policy, classpath, log, serverCommand, serverOptions,
                serverProperties, null,
                ServiceStarter.getActivationSystemPort());
    }

    /**
     * Trivial constructor. Simply assigns given parameters to their associated, internal fields.
     *
     * @param policy           location of VM policy filename or URL
     * @param classpath        location where shared VM classes can be found. Classpath components
     *                         must be separated by path separators.
     * @param log              location where group identifier information will be persisted
     * @param serverCommand    VM command to use
     * @param serverOptions    array of command line options to pass on the VM's command line
     * @param serverProperties array of property/value string pairs to pass on the VMs command line
     *                         (as in -D&lt;property&gt;=value). This array must have an even number
     *                         of elements.
     * @param host             hostname of desired activation system. If <code>null</code>, defaults
     *                         to the localhost.
     * @param port             port of desired activation system. If value is <= 0, then defaults to
     *                         {@link java.rmi.activation.ActivationSystem#SYSTEM_PORT
     *                         ActivationSystem.SYSTEM_PORT}.
     */
    public SharedActivationGroupDescriptor(
            //Required Args
            String policy, String classpath, String log,
            //Optional Args
            String serverCommand, String[] serverOptions,
            String[] serverProperties, String host, int port) {
        if (policy == null || classpath == null || log == null) {
            throw new NullPointerException(
                    "Policy, classpath, or log cannot be null");
        }
        this.policy = policy;
        this.classpath = classpath;
        this.log = log;
        this.serverCommand = serverCommand;
        this.serverOptions =
                customizeSharedGroupOptions(classpath, serverOptions);
        Properties props =
                convertToProperties(serverProperties);
        this.serverProperties =
                customizeSharedGroupProperties(policy, props);
        this.host = (host == null) ? "" : host;
        if (port <= 0) {
            this.port = ServiceStarter.getActivationSystemPort();
        } else {
            this.port = port;
        }
    }

    /**
     * Policy accessor method.
     *
     * @return the policy location associated with this service descriptor.
     */
    final public String getPolicy() {
        return policy;
    }

    /**
     * Classpath accessor method.
     *
     * @return classpath associated with this service descriptor.
     */
    final public String getClasspath() {
        return classpath;
    }

    /**
     * Shared group log accessor method.
     *
     * @return the shared group log associated with this service descriptor.
     */
    final public String getLog() {
        return log;
    }

    /**
     * Command accessor method.
     *
     * @return the path-qualified java command name associated with this service descriptor.
     */
    final public String getServerCommand() {
        return serverCommand;
    }

    /**
     * Command options accessor method.
     *
     * @return the command options associated with this service descriptor.
     */
    final public String[] getServerOptions() {
        return serverOptions.clone();
    }

    /**
     * Properties accessor method.
     *
     * @return the VM properties associated with this service descriptor.
     */
    final public Properties getServerProperties() {
        return (Properties) serverProperties.clone();
    }

    /**
     * Activation system host accessor method.
     *
     * @return the activation system host associated with this service descriptor.
     */
    final public String getActivationSystemHost() {
        return host;
    }

    /**
     * Activation system port accessor method.
     *
     * @return the activation system port associated with this service descriptor.
     */
    final public int getActivationSystemPort() {
        return port;
    }

    private static String[] customizeSharedGroupOptions(
            String classpath, String[] userOptions) {
        String[] customOpts = new String[]{"-cp", classpath};
        //Prepend classpath so user options can override later on
        if (userOptions != null) {
            String[] tmp = new String[customOpts.length + userOptions.length];
            System.arraycopy(customOpts, 0, tmp, 0, customOpts.length);
            System.arraycopy(userOptions, 0, tmp, customOpts.length,
                    userOptions.length);
            customOpts = tmp;
        }
        return customOpts;
    }

    private static Properties convertToProperties(String[] propertyValues) {
        Properties properties = new Properties();

        if (propertyValues == null || propertyValues.length == 0)
            return properties;

        if (propertyValues.length % 2 != 0) {
            throw new IllegalArgumentException(
                    "The service properties entry has an odd number of elements");
        }
        for (int i = 0; i < propertyValues.length; i += 2) {
            properties.setProperty(propertyValues[i], propertyValues[i + 1]);
        }
        return properties;
    }


    private static Properties customizeSharedGroupProperties(
            String policy, Properties userProperties) {
        // Avoid passing null properties
        if (userProperties == null) {
            userProperties = new Properties();
        }
        userProperties.put("java.security.policy", policy);

        return userProperties;
    }

    /**
     * Method that attempts to create a shared activation system group from the description
     * information provided via constructor parameters. <P> This method: <UL> <LI> creates a {@link
     * java.rmi.activation.ActivationGroupDesc} with the provided constructor parameter information
     * <LI> calls {@link java.rmi.activation.ActivationSystem#registerGroup(java.rmi.activation.ActivationGroupDesc)
     * ActivationSystem.registerGroup()} with the constructed <code>ActivationGroupDesc</code> <LI>
     * persists the returned {@link java.rmi.activation.ActivationGroupID activation group
     * identifier} to the shared group log. <LI> calls {@link java.rmi.activation.ActivationSystem#unregisterGroup(java.rmi.activation.ActivationGroupID)
     * ActivationSystem.unregisterGroup()} if an exception occurs while persisting the
     * <code>ActivationGroupID</code>. </UL> <EM>Notes:</EM> <OL> <LI>Prepends invoking VM's
     * classpath to the server command options. This allows subsequent classpath settings to
     * override. <LI>Adds a <code>"java.security.policy"</code> property with the provided policy
     * setting to server properties. </OL>
     *
     * @return the {@link java.rmi.activation.ActivationGroupID} for the newly created activation
     * system group instance.
     */
    public Object create(Configuration config) throws Exception {
        LogUtils.entering(logger, SharedActivationGroupDescriptor.class,"create", config);

        if (config == null) {
            throw new NullPointerException(
                    "Configuration argument cannot be null");
        }

//TODO - expand and/or canonicalize classpath components

//TODO - check for shared log existence prior to group creation

        ActivationSystem sys =
                ServiceStarter.getActivationSystem(
                        getActivationSystemHost(),
                        getActivationSystemPort(),
                        config);

        CommandEnvironment cmdToExecute
                = new CommandEnvironment(getServerCommand(),
                getServerOptions());
        ActivationGroupID gid = null;
        try {
            gid = sys.registerGroup(
                    new ActivationGroupDesc(getServerProperties(),
                            cmdToExecute));
            storeGroupID(getLog(), gid);
        } catch (Exception e) {
            try {
                if (gid != null) sys.unregisterGroup(gid);
            } catch (Exception ee) {
                // ignore - did the best we could
            }
            if (e instanceof IOException)
                throw (IOException) e;
            else if (e instanceof ActivationException)
                throw (ActivationException) e;
            else if (e instanceof ClassNotFoundException)
                throw (ClassNotFoundException) e;
            else
                throw new RuntimeException("Unexpected Exception", e);
        }

        LogUtils.exiting(logger, SharedActivationGroupDescriptor.class, "create", gid);
        return gid;
    }

    /**
     * Stores the <code>created</code> object to a well known file under the provided
     * <code>dir</code> path.
     */
    private static void storeGroupID(final String dir,
                                     final ActivationGroupID obj)
            throws IOException {
//TODO - create log dir as a separate step
        File log = new File(dir);
        String absDir = log.getAbsolutePath();
        if (log.exists()) {
            throw new IOException("Log " + absDir + " exists."
                    + " Please delete or select another path");
        }
        if (!log.mkdir()) {
            throw new IOException("Could not create directory: " + absDir);
// TODO - implement a lock out strategy
        }

        File cookieFile = new File(log, GROUP_COOKIE_FILE);
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(
                    new BufferedOutputStream(
                            new FileOutputStream(cookieFile)));
            oos.writeObject(new MarshalledObject(obj));
            oos.flush();
//TODO - file sync?
        } catch (IOException e) {
            cookieFile.delete();
            throw (IOException) e.fillInStackTrace();
        } finally {
            if (oos != null) oos.close();
        }
    }

    /**
     * Utility method that restores the object stored in a well known file under the provided
     * <code>dir</code> path.
     */
    static ActivationGroupID restoreGroupID(final String dir)
            throws IOException, ClassNotFoundException {
        File log = new File(dir);
        String absDir = log.getAbsolutePath();
        if (!log.exists() || !log.isDirectory()) {
            throw new IOException("Log directory ["
                    + absDir + "] does not exist.");
        }

        File cookieFile = new File(log, GROUP_COOKIE_FILE);
        ObjectInputStream ois = null;
        ActivationGroupID obj = null;
        try {
//TODO - lock out strategy for concurrent r/w file access
            ois = new ObjectInputStream(
                    new BufferedInputStream(
                            new FileInputStream(cookieFile)));
            MarshalledObject mo = (MarshalledObject) ois.readObject();
            obj = (ActivationGroupID) mo.get();
        } finally {
            if (ois != null) ois.close();
        }
        return obj;
    }

    public String toString() {
        ArrayList fields = new ArrayList(8);
        fields.add(policy);
        fields.add(classpath);
        fields.add(log);
        fields.add(serverCommand);
        fields.add(Arrays.asList(serverOptions));
        fields.add(serverProperties);
        fields.add(host);
        fields.add(new Integer(port));
        return fields.toString();
    }

    /**
     * Reads the default serializable field values for this object. Also, verifies that the
     * deserialized values are legal.
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Verify that serialized fields
        if (policy == null) {
            throw new InvalidObjectException("null policy");
        }
        if (classpath == null) {
            throw new InvalidObjectException("null class path");
        }
        if (log == null) {
            throw new InvalidObjectException("null log");
        }
        if (serverOptions == null) {
            throw new InvalidObjectException("null server options");
        }
        if (serverProperties == null) {
            throw new InvalidObjectException("null server properties");
        }
        if (host == null) {
            throw new InvalidObjectException("null activation host name");
        }
        if (port <= 0) {
            throw new InvalidObjectException("invalid activation port: " + port);
        }
    }

    /**
     * Throws InvalidObjectException, since data for this class is required.
     */
    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("no data");
    }

}


