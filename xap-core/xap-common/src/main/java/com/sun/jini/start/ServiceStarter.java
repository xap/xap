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

import com.sun.jini.action.GetIntegerAction;
import com.sun.jini.config.Config;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;

import java.rmi.Naming;
import java.rmi.activation.ActivationException;
import java.rmi.activation.ActivationSystem;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.MissingResourceException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * This class provides the main routine for starting shared groups, non-activatable services, and
 * activatable services.
 *
 * The following implementation-specific items are discussed below: <ul> <li><a
 * href="#configEntries">Configuring ServiceStarter</a> <li><a href="#logging">Logging</a> </ul>
 *
 * <a name="configEntries"> <h3>Configuring ServiceStarter</h3> </a>
 *
 * This implementation of <code>ServiceStarter</code> supports the following configuration entries,
 * with component <code>com.sun.jini.start</code>:
 *
 * <table summary="Describes the loginContext configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"><code> loginContext</code></font> <tr valign="top">
 * <td> &nbsp <th scope="row" align="right"> Type: <td> {@link javax.security.auth.login.LoginContext}
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>null</code> <tr
 * valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> If not
 * <code>null</code>, specifies the JAAS login context to use for performing a JAAS login and
 * supplying the {@link javax.security.auth.Subject} to use when running the service starter. If
 * <code>null</code>, no JAAS login is performed. </table>
 *
 * <table summary="Describes the serviceDescriptors configuration entry" border="0" cellpadding="2">
 * <tr valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th
 * scope="col" align="left" colspan="2"> <font size="+1"><code> serviceDescriptors</code></font> <tr
 * valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> {@link ServiceDescriptor}[]
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: no default <tr valign="top">
 * <td> &nbsp <th scope="row" align="right"> Description: <td> Array of service descriptors to
 * start. </table>
 *
 *
 * <a name="logging"> <h3>Loggers and Logging Levels</h3> </a>
 *
 * The implementation uses the {@link java.util.logging.Logger}, named
 * <code>com.sun.jini.start.service.starter</code>. The following table describes the type of
 * information logged as well as the levels of information logged. <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by service.starter at
 * different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> com.sun.jini.start.service.starter</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link java.util.logging.Level#SEVERE SEVERE} <td> for problems that prevent service
 * creation from proceeding <tr> <td> {@link java.util.logging.Level#WARNING WARNING} <td> for
 * problems with service creation that don't prevent further processing <tr> <td> {@link
 * java.util.logging.Level#FINER FINER} <td> for high level service creation operation tracing <tr>
 * <td> {@link java.util.logging.Level#FINEST FINEST} <td> for low level service creation operation
 * tracing
 *
 * </table> <p>
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class ServiceStarter {

    /**
     * Component name for service starter configuration entries
     */
    static final String START_PACKAGE = "com.sun.jini.start";


    /**
     * Configure logger
     */
    static /*final*/ Logger logger = null;

    static {
        try {
            logger =
                    Logger.getLogger(
                            START_PACKAGE + ".service.starter",
                            START_PACKAGE + ".resources.service");
        } catch (Exception e) {
            logger =
                    Logger.getLogger(START_PACKAGE + ".service.starter");
            if (e instanceof MissingResourceException) {
                logger.info("Could not load logger's ResourceBundle: "
                        + e);
            } else if (e instanceof IllegalArgumentException) {
                logger.info("Logger exists and uses another resource bundle: "
                        + e);
            }
            logger.info("Defaulting to existing logger");
        }
    }

    /**
     * Array of strong references to transient services
     */
    private static ArrayList transient_service_refs;

    /**
     * Prevent instantiation
     */
    private ServiceStarter() {
    }

    /**
     * Trivial class used as the return value by the <code>create</code> methods. This class
     * aggregates the results of a service creation attempt: proxy (if any), exception (if any),
     * associated descriptor object.
     */
    private static class Result {
        /**
         * Service proxy object, if any.
         */
        public final Object result;
        /**
         * Service creation exception, if any.
         */
        public final Exception exception;
        /**
         * Associated <code>ServiceDescriptor</code> object used to create the service instance
         */
        public final ServiceDescriptor descriptor;

        /**
         * Trivial constructor. Simply assigns each argument to the appropriate field.
         */
        Result(ServiceDescriptor d, Object o, Exception e) {
            descriptor = d;
            result = o;
            exception = e;
        }

        // javadoc inherited from super class
        public String toString() {
            return this.getClass() + ":[descriptor=" + descriptor + ", "
                    + "result=" + result + ", exception=" + exception + "]";
        }
    }

    /**
     * Generic service creation method that attempts to login via the provided
     * <code>LoginContext</code> and then call the <code>create</code> overload without a login
     * context argument.
     *
     * @param descs        The <code>ServiceDescriptor[]</code> that contains the descriptors for
     *                     the services to start.
     * @param config       The associated <code>Configuration</code> object used to customize the
     *                     service creation process.
     * @param loginContext The associated <code>LoginContext</code> object used to login/logout.
     * @return Returns a <code>Result[]</code> that is the same length as <code>descs</code>, which
     * contains the details for each service creation attempt.
     * @throws Exception If there was a problem logging in/out or a problem creating the service.
     * @see Result
     * @see ServiceDescriptor
     * @see net.jini.config.Configuration
     * @see javax.security.auth.login.LoginContext
     */

    private static Result[] createWithLogin(
            final ServiceDescriptor[] descs, final Configuration config,
            final LoginContext loginContext)
            throws Exception {
        logger.entering(ServiceStarter.class.getName(),
                "createWithLogin", new Object[]{descs, config, loginContext});
        loginContext.login();
        Result[] results = null;
        try {
            results = (Result[]) Subject.doAsPrivileged(
                    loginContext.getSubject(),
                    new PrivilegedExceptionAction() {
                        public Object run()
                                throws Exception {
                            return create(descs, config);
                        }
                    },
                    null);
        } catch (PrivilegedActionException pae) {
            throw pae.getException();
        } finally {
            try {
                loginContext.logout();
            } catch (LoginException le) {
                logger.log(Level.FINE, "service.logout.exception", le);
            }
        }
        logger.exiting(ServiceStarter.class.getName(),
                "createWithLogin", results);
        return results;
    }

    /**
     * Generic service creation method that attempts to start the services defined by the provided
     * <code>ServiceDescriptor[]</code> argument.
     *
     * @param descs  The <code>ServiceDescriptor[]</code> that contains the descriptors for the
     *               services to start.
     * @param config The associated <code>Configuration</code> object used to customize the service
     *               creation process.
     * @return Returns a <code>Result[]</code> that is the same length as <code>descs</code>, which
     * contains the details for each service creation attempt.
     * @throws Exception If there was a problem creating the service.
     * @see Result
     * @see ServiceDescriptor
     * @see net.jini.config.Configuration
     */
    private static Result[] create(final ServiceDescriptor[] descs,
                                   final Configuration config)
            throws Exception {
        logger.entering(ServiceStarter.class.getName(), "create",
                new Object[]{descs, config});
        ArrayList proxies = new ArrayList();

        Object result = null;
        Exception problem = null;
        ServiceDescriptor desc = null;
        for (int i = 0; i < descs.length; i++) {
            desc = descs[i];
            result = null;
            problem = null;
            try {
                if (desc != null) {
                    result = desc.create(config);
                }
            } catch (Exception e) {
                problem = e;
            } finally {
                proxies.add(new Result(desc, result, problem));
            }
        }

        logger.exiting(ServiceStarter.class.getName(), "create", proxies);
        return (Result[]) proxies.toArray(new Result[proxies.size()]);
    }

    /**
     * Utility routine that returns a "prepared" activation system proxy for a system at the given
     * <code>host</code> and <code>port</code>.
     *
     * @param host   The host of the desired activation system
     * @param port   The port of the desired activation system
     * @param config The <code>Configuration</code> used to prepare the system proxy.
     * @return A prepared activation system proxy
     * @throws ActivationException If there was a problem communicating with the activation system.
     * @see net.jini.config.Configuration
     */
    static ActivationSystem getActivationSystem(
            String host, int port, Configuration config)
            throws ActivationException, ConfigurationException {
        if (config == null) {
            throw new NullPointerException(
                    "Configuration argument cannot be null");
        }

        ActivationSystem sys = null;
        final String h = (host == null) ? "" : host;
        final int p = (port <= 0) ? getActivationSystemPort() : port;
        try {
            sys = (ActivationSystem)
                    Naming.lookup("//" + h + ":" + p +
                            "/java.rmi.activation.ActivationSystem");
            ProxyPreparer activationSystemPreparer =
                    (ProxyPreparer) Config.getNonNullEntry(config,
                            START_PACKAGE, "activationSystemPreparer",
                            ProxyPreparer.class, new BasicProxyPreparer());
            sys = (ActivationSystem) activationSystemPreparer.prepareProxy(sys);
        } catch (Exception e) {
            throw new ActivationException(
                    "ActivationSystem @ " + host + ":" + port +
                            " could not be obtained", e);
        }
        return sys;
    }

    /**
     * Utility routine that returns a "default" activation system port. The default port is
     * determined by: <UL> <LI> the value of the <code>java.rmi.activation.port</code> system
     * property, if set <LI> the value of <code>ActivationSystem.SYSTEM_PORT</code> </UL>
     *
     * @return The activation system port
     * @see java.rmi.activation.ActivationSystem
     */
    static int getActivationSystemPort() {
        return ((Integer) java.security.AccessController.doPrivileged(
                new GetIntegerAction("java.rmi.activation.port",
                        ActivationSystem.SYSTEM_PORT))).intValue();
    }

    /**
     * Utility routine that maintains strong references to any transient services in the provided
     * <code>Result[]</code>. This prevents the transient services from getting garbage collected.
     */
    private static void maintainNonActivatableReferences(Result[] results) {
        logger.entering(ServiceStarter.class.getName(),
                "maintainNonActivatableReferences", (Object[]) results);
        if (results.length == 0)
            return;
        transient_service_refs = new ArrayList();
        for (int i = 0; i < results.length; i++) {
            if (results[i] != null &&
                    results[i].result != null &&
                    NonActivatableServiceDescriptor.class.equals(
                            results[i].descriptor.getClass())) {
                logger.log(Level.FINEST, "Storing ref to: {0}",
                        results[i].result);
                transient_service_refs.add(results[i].result);
            }
        }
//TODO - kick off daemon thread to maintain refs via LifeCycle object	
        logger.exiting(ServiceStarter.class.getName(),
                "maintainNonActivatableReferences");
        return;
    }

    /**
     * Utility routine that prints out warning messages for each service descriptor that produced an
     * exception or that was null.
     */
    private static void checkResultFailures(Result[] results) {
        logger.entering(ServiceStarter.class.getName(),
                "checkResultFailures", (Object[]) results);
        if (results.length == 0)
            return;
        for (int i = 0; i < results.length; i++) {
            if (results[i].exception != null) {
                logger.log(Level.WARNING,
                        "service.creation.unknown",
                        results[i].exception);
                logger.log(Level.WARNING,
                        "service.creation.unknown.detail",
                        new Object[]{new Integer(i),
                                results[i].descriptor});
            } else if (results[i].descriptor == null) {
                logger.log(Level.WARNING,
                        "service.creation.null", new Integer(i));
            }
        }
        logger.exiting(ServiceStarter.class.getName(),
                "checkResultFailures");
    }


    /**
     * The main method for the <code>ServiceStarter</code> application. The <code>args</code>
     * argument is passed directly to <code>ConfigurationProvider.getInstance()</code> in order to
     * obtain a <code>Configuration</code> object. This configuration object is then queried for the
     * <code>com.sun.jini.start.serviceDescriptors</code> entry, which is assumed to be a
     * <code>ServiceDescriptor[]</code>. The <code>create()</code> method is then called on each of
     * the array elements.
     *
     * @param args <code>String[]</code> passed to <code>ConfigurationProvider.getInstance()</code>
     *             in order to obtain a <code>Configuration</code> object.
     * @see ServiceDescriptor
     * @see SharedActivatableServiceDescriptor
     * @see SharedActivationGroupDescriptor
     * @see NonActivatableServiceDescriptor
     * @see net.jini.config.Configuration
     * @see net.jini.config.ConfigurationProvider
     */
    public static void main(String[] args) {
        try {
            logger.entering(ServiceStarter.class.getName(),
                    "main", (Object[]) args);
            Configuration config = ConfigurationProvider.getInstance(args);
            ServiceDescriptor[] descs = (ServiceDescriptor[])
                    config.getEntry(START_PACKAGE, "serviceDescriptors",
                            ServiceDescriptor[].class, null);
            if (descs == null || descs.length == 0) {
                logger.warning("service.config.empty");
                return;
            }
            LoginContext loginContext = (LoginContext)
                    config.getEntry(START_PACKAGE, "loginContext",
                            LoginContext.class, null);
            Result[] results = null;
            if (loginContext != null)
                results = createWithLogin(descs, config, loginContext);
            else
                results = create(descs, config);
            checkResultFailures(results);
            maintainNonActivatableReferences(results);
        } catch (ConfigurationException cex) {
            logger.log(Level.SEVERE, "service.config.exception", cex);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "service.creation.exception", e);
        }
        logger.exiting(ServiceStarter.class.getName(),
                "main");
    }

}//end class ServiceStarter
