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

package com.j_spaces.jmx;

import com.gigaspaces.internal.jmx.JMXUtilities;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.management.local_time.LocalTime;
import com.gigaspaces.management.local_time.LocalTimeConstants;
import com.gigaspaces.management.local_time.LocalTimeMBean;
import com.gigaspaces.management.transport.TransportConstants;
import com.gigaspaces.management.transport.TransportProtocolMonitor;
import com.gigaspaces.management.transport.TransportProtocolMonitorMBean;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.IJSpaceContainer;
import com.j_spaces.jmx.util.ObjectNameFactory;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.log.JProperties;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.NameAlreadyBoundException;

import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_URL_DEFAULT;
import static com.j_spaces.core.Constants.LookupManager.LOOKUP_JNDI_URL_PROP;
import static com.j_spaces.core.Constants.Management.JMX_MBEAN_DESCRIPTORS_CONTAINER;
import static com.j_spaces.core.Constants.Management.JMX_MBEAN_DESCRIPTORS_JAVASPACE;
import static com.j_spaces.core.Constants.Management.JMX_MBEAN_DESCRIPTORS_JAVASPACE_EXT;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class JMXProvider {
    //TODO - later add a further check not to load from file system. Currently might not work in IDE only
    //if the src/resources is not in classpath causing to potential JMX crash. Hold this fix for now.
    private static final URL contURL = ResourceLoader.getResourceURL(JMX_MBEAN_DESCRIPTORS_CONTAINER, SystemLocations.singleton().home().toString());
    public static final String CONTAINER_MBEAN_DESCR_URL = (contURL != null ? contURL.toString() : null);

    private static final URL spaceURL = ResourceLoader.getResourceURL(JMX_MBEAN_DESCRIPTORS_JAVASPACE, SystemLocations.singleton().home().toString());
    public static final String JSPACE_MBEAN_DESCR_URL = (spaceURL != null ? spaceURL.toString() : null);

    private static final URL spaceExtURL = ResourceLoader.getResourceURL(JMX_MBEAN_DESCRIPTORS_JAVASPACE_EXT, SystemLocations.singleton().home().toString());
    public static final String JSPACE_EXT_MBEAN_DESCR_URL = (spaceExtURL != null ? spaceExtURL.toString() : null);

    public static final String DEFAULT_DOMAIN = "com.gigaspaces";

    private static MBeanServer m_MBeanServer;
    private static Map<String, JMXConnectorServer> _jmxConnectionsMap =
            new HashMap<String, JMXConnectorServer>(3);
    private static Hashtable<String, ObjectInstance> m_MBeansRepository = new Hashtable<String, ObjectInstance>();

    //logger
    final private static Logger _logger =
            Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_JMX);

    private static final String CONNECTION_FAILED_MESSAGE = "Failed to receive MBean Server.";

    private final static String failedTransportMBeanRegistrationMessagePrefix =
            "\nTransport MBean registration failed for container <";

    private final static String failedLocalTimeMBeanRegistrationMessagePrefix =
            "\nLocalTime MBean registration failed for container <";

    private final static String alreadyRegistredTransportMBeanMessage =
            "\nTransport MBean is already registered for container <";

    private final static String successfulTransportMBeanRegistrationMessage =
            "\nTransport MBean was registered successfully for container <";


    /**
     * Don't let anyone instantiate this class.
     */
    private JMXProvider() {
    }

    /**
     * Return the <code>MBeanServer</code>. If MBeanServer reference equals null, that is, it wasn't
     * initialized via <code>JMXProvider.startManagementAgent()</code> method, then try to obtain
     * initially created platform <tt>MBeanServer</tt>.
     */
    public synchronized static MBeanServer getMBeanServer(String containerName) {

        try {
            if (m_MBeanServer == null) {
                try {
                    //try to retrieve MBeanServer instance with JDK 1.5
                    m_MBeanServer =
                            java.lang.management.ManagementFactory.getPlatformMBeanServer();
                } catch (Throwable th) {
                    //if failed try to use in method MBeanServerFactory.createMBeanServer()
                    m_MBeanServer = MBeanServerFactory.createMBeanServer();
                }
            }

            if (System.getProperty(SystemProperties.CREATE_JMX_CONNECTOR_PROP, "true").equals("true")) {

                String jmxServiceURL = "";
                JMXConnectorServer jmxConn = _jmxConnectionsMap.get(containerName);
                if (jmxConn == null) {

                    String jndiUrl = JProperties.getContainerProperty(
                            containerName, LOOKUP_JNDI_URL_PROP, LOOKUP_JNDI_URL_DEFAULT);

                    jmxServiceURL = JMXUtilities.createJMXUrl(jndiUrl);
                    Map env = System.getProperties();
                    jmxConn = JMXConnectorServerFactory.newJMXConnectorServer(
                            new JMXServiceURL(jmxServiceURL), env, m_MBeanServer);

                    _jmxConnectionsMap.put(containerName, jmxConn);

                    if (!jmxConn.isActive()) {
                        try {
                            jmxConn.start();

                            if (_logger.isLoggable(Level.FINE)) {
                                _logger.fine("\nNew JMXConnectorServer was successfully registered" +
                                        " into the MBeanServer using service url: "
                                        + "\n" + jmxServiceURL + "\n");
                            }
                        } catch (IOException ioe) {
                            String message = JSpaceUtilities.getCauseExceptionMessageFromHierarchy(
                                    ioe, NameAlreadyBoundException.class);
                            if (message != null) {
                                if (_logger.isLoggable(Level.FINE)) {
                                    _logger.fine("\nUsing an already registered JMXConnectorServer " +
                                            "with service url:\n" + jmxServiceURL + "\n");
                                }
                            } else if (_logger.isLoggable(Level.WARNING)) {
                                _logger.log(Level.WARNING, CONNECTION_FAILED_MESSAGE + ioe.toString(), ioe);
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            String message = JSpaceUtilities.getCauseExceptionMessageFromHierarchy(
                    e, NameAlreadyBoundException.class);
            if (message != null) {
                if (_logger.isLoggable(Level.FINEST)) {
                    _logger.log(Level.FINEST, CONNECTION_FAILED_MESSAGE + e.toString(), e);
                }
            } else if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, CONNECTION_FAILED_MESSAGE + e.toString(), e);
            }
        } catch (Throwable th) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, CONNECTION_FAILED_MESSAGE + th.toString(), th);
            }
        }

        return m_MBeanServer;
    }

    /**
     * Create management bean for JSpace Container.
     */
    public static void registerContainerMBean(String containerName, IJSpaceContainer container) {
        MBeanServer mBeanServer = getMBeanServer(containerName);
        if (mBeanServer == null)
            return;

        if (m_MBeansRepository.containsKey(containerName))
            return;

        try {
            JMXSpaceContainer mbean = new JMXSpaceContainer(container, CONTAINER_MBEAN_DESCR_URL);
            //String domain = getJmxProperty(JMX_DOMAIN_PROP);
            ObjectName objName = ObjectNameFactory.buildObjectName(DEFAULT_DOMAIN, mbean.getType(), containerName);
            m_MBeansRepository.put(containerName, mBeanServer.registerMBean(mbean, objName));
        } catch (InstanceAlreadyExistsException e) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Container MBean is already registered for <" +
                        containerName + ">");
            }
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, "Failed to register MBean for <" +
                        containerName + "> container.", ex);
            }
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("\nContainer <" + containerName + "> MBean was registered successfully.\n");
        }
    }

    /**
     * Create management bean for JSpace Container.
     */
    public static void registerTransportMBean(String containerName) {
        MBeanServer mBeanServer = getMBeanServer(containerName);
        if (mBeanServer == null)
            return;

        TransportProtocolMonitorMBean transportConnectionsInfoMBean =
                new TransportProtocolMonitor();
        ObjectName objName = TransportConstants.createTransportMBeanObjectName(containerName);
        if (!mBeanServer.isRegistered(objName)) {
            try {
                mBeanServer.registerMBean(transportConnectionsInfoMBean, objName);
            } catch (InstanceAlreadyExistsException e) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine(alreadyRegistredTransportMBeanMessage + containerName + ">");
                }
            } catch (NotCompliantMBeanException e) {
                if (_logger.isLoggable(Level.WARNING)) {
                    _logger.log(Level.SEVERE, failedTransportMBeanRegistrationMessagePrefix +
                            containerName + ">", e);
                }
            } catch (MBeanRegistrationException e) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, failedTransportMBeanRegistrationMessagePrefix +
                            containerName + ">", e);
                }
            }

            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine(
                        successfulTransportMBeanRegistrationMessage + containerName + ">");
            }
        } else {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine(alreadyRegistredTransportMBeanMessage);
            }
        }
    }


    /**
     * Create management bean for JSpace Container.
     */
    public static void registerLocalTimeMBean(String containerName) {
        MBeanServer mBeanServer = getMBeanServer(containerName);
        if (mBeanServer == null)
            return;

        LocalTimeMBean localTimeMBean = new LocalTime();
        ObjectName objName = LocalTimeConstants.MBEAN_NAME;
        if (!mBeanServer.isRegistered(objName)) {
            try {
                mBeanServer.registerMBean(localTimeMBean, objName);
            } catch (InstanceAlreadyExistsException e) {
                /*                if( _logger.isLoggable( Level.CONFIG ))
                {
                    _logger.config( alreadyRegistredTransportMBeanMessage + containerName + ">"  );
                }
                 */
            } catch (NotCompliantMBeanException e) {
                if (_logger.isLoggable(Level.WARNING)) {
                    _logger.log(Level.SEVERE, failedLocalTimeMBeanRegistrationMessagePrefix +
                            containerName + ">", e);
                }
            } catch (MBeanRegistrationException e) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, failedLocalTimeMBeanRegistrationMessagePrefix +
                            containerName + ">", e);
                }
            }

            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine(
                        successfulTransportMBeanRegistrationMessage + containerName + ">");
            }
        }

    }


    /**
     * Release Transport MBean registration.
     */

    public static void unregisterTransportMBean(String containerName) throws Exception {
        MBeanServer mBeanServer = getMBeanServer(containerName);
        if (mBeanServer == null)
            return;

        ObjectName transportMBeanObjectName =
                TransportConstants.createTransportMBeanObjectName(containerName);
        if (mBeanServer.isRegistered(transportMBeanObjectName)) {
            mBeanServer.unregisterMBean(transportMBeanObjectName);
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Transport MBean was unregistered successfully.");

            }
        }
    }


    /**
     * Release mbean registration for JSpace Container.
     */
    public static void unregisterContainerMBean(IJSpaceContainer container)
            throws Exception {
        String containerName = container.getName();
        if (m_MBeanServer == null)
            return;


        try {
            ObjectInstance objInst = m_MBeansRepository.remove(containerName);
            if (objInst != null) {
                getMBeanServer(containerName).unregisterMBean(objInst.getObjectName());

                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("Container <" + containerName + "> MBean was unregistered successfully.");
                }
            }
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, "Failed to unregister MBean for <" + containerName + "> container", ex);
            }
        }

    }

    /**
     * Create and register management bean for specified JSpace.
     */
    public static void registerSpaceMBean(String spaceName, SpaceImpl spaceImpl) {

        MBeanServer mBeanServer = getMBeanServer(spaceImpl.getContainerName());
        if (mBeanServer == null)
            return;

        if (m_MBeansRepository.containsKey(spaceImpl.getServiceName()))
            return;

        try {
            IJSpace remoteSpaceProxy = spaceImpl.getSingleProxy();

            JMXSpace mbean = new JMXSpace(remoteSpaceProxy, JSPACE_MBEAN_DESCR_URL);
            ObjectName objName =
                    ObjectNameFactory.buildObjectName(DEFAULT_DOMAIN, mbean.getType(),
                            spaceImpl.getContainerName() + '-' + spaceName);

            m_MBeansRepository.put(spaceImpl.getServiceName(), mBeanServer.registerMBean(mbean, objName));
            // Create additional mbean for extended space attributes
            JMXSpaceExt mbeanExt = new JMXSpaceExt(remoteSpaceProxy, mbean.m_spaceConfig, JSPACE_EXT_MBEAN_DESCR_URL);
            ObjectName objNameExt = ObjectNameFactory.buildObjectName(DEFAULT_DOMAIN,
                    mbeanExt.getType(), spaceImpl.getContainerName() + '-' + spaceName);

            m_MBeansRepository.put(spaceImpl.getServiceName() + "Ext", mBeanServer.registerMBean(mbeanExt, objNameExt));

            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("\nSpace <" + spaceImpl.getServiceName() + "> MBean was registered successfully.\n");
            }
        } catch (InstanceAlreadyExistsException e) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Space MBean is already registered for <" + spaceImpl.getServiceName() + ">");
            }
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, "Failed to register MBean for <" + spaceImpl.getServiceName() + "> space.", ex);
            }
        }
    }

    /**
     * Release mbean registration for JSpace.
     */
    public static void unregisterSpaceMBean(String spaceName, String containerName) {
        if (m_MBeanServer == null)
            return;

        try {
            String fullSpaceName =
                    JSpaceUtilities.createFullSpaceName(containerName, spaceName);
            ObjectInstance objInst = m_MBeansRepository.remove(fullSpaceName);
            if (objInst != null) {
                m_MBeanServer.unregisterMBean(objInst.getObjectName());
            }

            objInst = m_MBeansRepository.remove(fullSpaceName + "Ext");
            if (objInst != null) {
                m_MBeanServer.unregisterMBean(objInst.getObjectName());
            }

            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Space <" + containerName + ":" +
                        spaceName + "> MBean was unregister successfully.");
            }
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, "Failed to unregister MBean for <" +
                        containerName + ":" + spaceName + "> space.", ex);
            }
        }
    }

    /*
    public synchronized static void stopJMXConnector( String containerName )
	{
		JMXConnectorServer connectorServer = _jmxConnectionsMap.get( containerName );

		if( connectorServer != null && connectorServer.isActive() )
		{
			try
			{
				connectorServer.stop();
				_jmxConnectionsMap.remove( containerName );
			}
			catch( Exception e )
			{
				if( _logger.isLoggable( Level.WARNING ))
				{
					_logger.log( Level.WARNING, "Failed to stop JMX Connector.", e );
				}
			}
			if( _logger.isLoggable( Level.CONFIG ))
			{
				_logger.config( "JMX Connector was successfully stopped." );
			}

		}
	}
     */

    public synchronized static void removeMBeanServer() {
        _jmxConnectionsMap.clear();
        m_MBeanServer = null;
    }

}
