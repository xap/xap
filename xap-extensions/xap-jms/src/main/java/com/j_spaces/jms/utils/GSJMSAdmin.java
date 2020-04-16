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

package com.j_spaces.jms.utils;

import com.gigaspaces.logger.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.jms.GSConnectionFactoryImpl;
import com.j_spaces.jms.GSQueueImpl;
import com.j_spaces.jms.GSTopicImpl;
import com.j_spaces.jms.GSXAConnectionFactoryImpl;

import net.jini.id.ReferentUuid;
import net.jini.id.Uuid;

import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnectionFactory;
import javax.jms.ResourceAllocationException;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnectionFactory;
import javax.naming.InitialContext;
import javax.naming.NamingException;


/**
 * Admin util for the GigaSpaces JMS implementation. It is used by client to obtain JMS resources
 * like connection factories and destinations. If an instance of the class already exists we return
 * the same static instance. It caches the JMS resources.
 *
 * @author Shai Wolf
 * @version 6.0
 */
public class GSJMSAdmin {

    /**
     * Singleton instance
     */
    private static GSJMSAdmin m_adminInstance = null;


    private static final String COMPRESSION_PROPERTY = "com.gs.jms.compressionMinSize";


    /**
     * The minimum size (in bytes) from which we start to compress messages body. E.g. if a 1 MB
     * Text JMS Message body is sent, and the compressionMinSize value is 500000 (0.5MB) then we
     * will compress that message body (only), otherwise we will send (write) it as is. TODO
     * Currently this configured via system property later will be part of the JMS-config.xml
     * default value is 0.5 MB TODO move constant to SystemProperties.java
     */
    private int compressionMinSize;


    /**
     * logger
     */
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);


    /**
     * A cache for connection factories.
     */
    //private HashMap<Uuid, ConnectionFactory> connectionfactories;
    private HashMap<String, ConnectionFactory> connectionfactories;


    /**
     * A cache for XA connection factories.
     */
    //private HashMap<Uuid, XAConnectionFactory> xaConnectionfactories;
    private HashMap<String, XAConnectionFactory> xaConnectionfactories;


    /**
     * A cache for queues.
     */
    private HashMap<String, Queue> queues;


    /**
     * A cache for topics.
     */
    private HashMap<String, Topic> topics;


    /**
     * A context for JNDI lookup. Initialized with the jndi.properties file.
     */
    private InitialContext jndiContext = null;


    /**
     * Private constructor
     */
    private GSJMSAdmin() {
        //connectionfactories   = new HashMap<Uuid, ConnectionFactory>();
        //xaConnectionfactories = new HashMap<Uuid, XAConnectionFactory>();
        connectionfactories = new HashMap<String, ConnectionFactory>();
        xaConnectionfactories = new HashMap<String, XAConnectionFactory>();
        queues = new HashMap<String, Queue>();
        topics = new HashMap<String, Topic>();
        String compression = System.getProperty(COMPRESSION_PROPERTY, "500000");
        compressionMinSize = Integer.valueOf(compression).intValue();
    }

    /**
     * Returns a GSJMSAdmin instance.
     *
     * @return a GSJMSAdmin instance.
     */
    synchronized public static GSJMSAdmin getInstance() {
        if (m_adminInstance == null) {
            m_adminInstance = new GSJMSAdmin();
        }
        return m_adminInstance;
    }


    public ConnectionFactory getConnectionFactory(String spaceURL) throws JMSException {
        return getConnectionFactory(spaceURL, null);
    }

    /**
     * Returns a <code>ConnectionFactory</code> that works with the space in the specified spaceURL.
     * First it tries to find a cached <code>ConnectionFactory</code>. If the required instance
     * doesn't exist in the cache, it finds the space and creates a new
     * <code>ConnectionFactory</code> to work with that space.
     *
     * @param spaceURL space URL
     * @return a <code>ConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized ConnectionFactory getConnectionFactory(String spaceURL, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getConnectionFactory(String): spaceURL="
                    + spaceURL + ", IMessageConverter=" + messageConverter);
        }

        IJSpace space = findSpace(spaceURL);

        // TODO: ?
        //space.setNOWriteLeaseMode(true);
        //space.setFifo(true);

        return getConnectionFactory(space, messageConverter);
    }


    public ConnectionFactory getConnectionFactory(IJSpace space) throws JMSException {
        return getConnectionFactory(space, null);
    }

    /**
     * Returns a <code>ConnectionFactory</code> that works with the specified space proxy. First it
     * tries to find a cached <code>ConnectionFactory</code>. If the required instance doesn't exist
     * in the cache, it creates a new <code>ConnectionFactory</code> to work with that space.
     *
     * @param space the space proxy
     * @return a <code>ConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    //public synchronized ConnectionFactory getConnectionFactory(IJSpace space)
    public synchronized ConnectionFactory getConnectionFactory(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        if (space == null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("GSJMSAdmin.getConnectionFactory(IJSpace): " +
                        "Space proxy is null");
            }
            throw new JMSException("Space proxy is null");
        }


        Uuid spaceUuid = ((ReferentUuid) space).getReferentUuid();
        //String url = space.getURL().getURL();

        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getConnectionFactory(IJSpace): " +
                    "Space URL=" + space.getURL().getURL() + ", Space UUID=" + spaceUuid +
                    ", IMessageConverter=" + messageConverter);
            //"Space URL="+url+"  |  Space UUID="+spaceUuid);
        }

        String key;
        if (messageConverter == null) {
            key = spaceUuid.toString();
        } else {
            key = spaceUuid.toString() + messageConverter.getClass().getName();
        }

        // try to receive a cached one
        //ConnectionFactory connectionFactory = connectionfactories.get(spaceUuid);
        ConnectionFactory connectionFactory = connectionfactories.get(key);
        if (connectionFactory != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("GSJMSAdmin.getConnectionFactory(IJSpace): " +
                        "Found ConnectionFactory in cache: " + connectionFactory);
            }
            return connectionFactory;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getConnectionFactory(IJSpace): " +
                    "ConnectionFactory not found in cache");
        }

        // create a new one
        //connectionFactory = new GSConnectionFactoryImpl(url, space);
        connectionFactory = new GSConnectionFactoryImpl(space, messageConverter);
        //connectionfactories.put(spaceUuid, connectionFactory);
        connectionfactories.put(key, connectionFactory);
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getConnectionFactory(IJSpace): " +
                    "Created a new ConnectionFactory: " + connectionFactory);
        }

        return connectionFactory;
    }


    public QueueConnectionFactory getQueueConnectionFactory(String spaceURL) throws JMSException {
        return getQueueConnectionFactory(spaceURL, null);
    }

    /**
     * Returns a <code>QueueConnectionFactory</code> that works with the space in the specified
     * spaceURL. First it tries to find a cached <code>QueueConnectionFactory</code>. If the
     * required instance doesn't exist in the cache, it finds the space and creates a new
     * <code>QueueConnectionFactory</code> to work with that space.
     *
     * @param spaceURL space URL
     * @return a <code>QueueConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized QueueConnectionFactory getQueueConnectionFactory(String spaceURL, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getQueueConnectionFactory(String): spaceURL="
                    + spaceURL + ", IMessageConverter=" + messageConverter);
        }
        return (QueueConnectionFactory) getConnectionFactory(spaceURL, messageConverter);
    }


    public TopicConnectionFactory getTopicConnectionFactory(String spaceURL) throws JMSException {
        return getTopicConnectionFactory(spaceURL, null);
    }


    /**
     * Returns a <code>TopicConnectionFactory</code> that works with the space in the specified
     * spaceURL. First it tries to find a cached <code>TopicConnectionFactory</code>. If the
     * required instance doesn't exist in the cache, it finds the space and creates a new
     * <code>TopicConnectionFactory</code> to work with that space.
     *
     * @param spaceURL space URL
     * @return a <code>TopicConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized TopicConnectionFactory getTopicConnectionFactory(String spaceURL, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getTopicConnectionFactory(String): spaceURL="
                    + spaceURL + ", IMessageConverter=" + messageConverter);
        }
        return (TopicConnectionFactory) getConnectionFactory(spaceURL, messageConverter);
    }


    public QueueConnectionFactory getQueueConnectionFactory(IJSpace space) throws JMSException {
        return getQueueConnectionFactory(space, null);
    }

    /**
     * Returns a <code>QueueConnectionFactory</code> that works with the specified space proxy.
     * First it tries to find a cached <code>QueueConnectionFactory</code>. If the required instance
     * doesn't exist in the cache, it creates a new <code>QueueConnectionFactory</code> to work with
     * that space.
     *
     * @param space the space proxy
     * @return a <code>QueueConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized QueueConnectionFactory getQueueConnectionFactory(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getQueueConnectionFactory(IJSpace): Space="
                    + space.getName() + ", IMessageConverter=" + messageConverter);
        }
        return (QueueConnectionFactory) getConnectionFactory(space, messageConverter);
    }


    public TopicConnectionFactory getTopicConnectionFactory(IJSpace space) throws JMSException {
        return getTopicConnectionFactory(space, null);
    }


    /**
     * Returns a <code>TopicConnectionFactory</code> that works with the specified space proxy.
     * First it tries to find a cached <code>TopicConnectionFactory</code>. If the required instance
     * doesn't exist in the cache, it creates a new <code>TopicConnectionFactory</code> to work with
     * that space.
     *
     * @param space the space proxy
     * @return a <code>TopicConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized TopicConnectionFactory getTopicConnectionFactory(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getTopicConnectionFactory(IJSpace): Space="
                    + space.getName() + ", IMessageConverter=" + messageConverter);
        }
        return (TopicConnectionFactory) getConnectionFactory(space, messageConverter);
    }


    public XAConnectionFactory getXAConnectionFactory(String spaceURL) throws JMSException {
        return getXAConnectionFactory(spaceURL, null);
    }

    /**
     * Returns a <code>XAConnectionFactory</code> that works with the space in the specified
     * spaceURL. First it tries to find a cached <code>XAConnectionFactory</code>. If the required
     * instance doesn't exist in the cache, it finds the space and creates a new
     * <code>XAConnectionFactory</code> to work with that space.
     *
     * @param spaceURL space URL
     * @return a <code>XAConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized XAConnectionFactory getXAConnectionFactory(String spaceURL, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXAConnectionFactory(String): spaceURL="
                    + spaceURL + ", IMessageConverter=" + messageConverter);
        }

        IJSpace space = findSpace(spaceURL);

        //TODO: ?
        //space.setNOWriteLeaseMode(true);
        //space.setFifo(true);

        return getXAConnectionFactory(space, messageConverter);
    }

    public XAConnectionFactory getXAConnectionFactory(IJSpace space) throws JMSException {
        return getXAConnectionFactory(space, null);
    }

    /**
     * Returns a <code>XAConnectionFactory</code> that works with the specified space proxy. First
     * it tries to find a cached <code>XAConnectionFactory</code>. If the required instance doesn't
     * exist in the cache, it creates a new <code>XAConnectionFactory</code> to work with that
     * space.
     *
     * @param space the space proxy
     * @return a <code>XAConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized XAConnectionFactory getXAConnectionFactory(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        if (space == null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("GSJMSAdmin.getXAConnectionFactory(IJSpace): " +
                        "Space proxy is null");
            }
            throw new JMSException("Space proxy is null");
        }


        Uuid spaceUuid = ((ReferentUuid) space).getReferentUuid();
        //String url = space.getURL().getURL();

        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXAConnectionFactory(IJSpace): " +
                    "Space URL=" + space.getURL().getURL() + ", Space UUID=" + spaceUuid +
                    ", IMessageConverter=" + messageConverter);
            //"Space URL="+url+"  |  Space UUID="+spaceUuid);
        }

        String key;
        if (messageConverter == null) {
            key = spaceUuid.toString();
        } else {
            key = spaceUuid.toString() + messageConverter.getClass().getName();
        }

        // try to receive a cached one
        //XAConnectionFactory connectionFactory = xaConnectionfactories.get(spaceUuid);
        XAConnectionFactory connectionFactory = xaConnectionfactories.get(key);
        if (connectionFactory != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("GSJMSAdmin.getXAConnectionFactory(IJSpace): " +
                        "Found XAConnectionFactory in cache: " + connectionFactory);
            }
            return connectionFactory;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXAConnectionFactory(IJSpace): " +
                    "XAConnectionFactory not found in cache");
        }

        // create a new one
        //connectionFactory = new GSXAConnectionFactoryImpl(url, space);
        connectionFactory = new GSXAConnectionFactoryImpl(space, messageConverter);
        //xaConnectionfactories.put(spaceUuid, connectionFactory);
        xaConnectionfactories.put(key, connectionFactory);
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXAConnectionFactory(IJSpace): " +
                    "Created a new XAConnectionFactory: " + connectionFactory);
        }

        return connectionFactory;
    }


    public XAQueueConnectionFactory getXAQueueConnectionFactory(String spaceURL) throws JMSException {
        return getXAQueueConnectionFactory(spaceURL, null);
    }


    /**
     * Returns a <code>XAQueueConnectionFactory</code> that works with the space in the specified
     * spaceURL. First it tries to find a cached <code>XAQueueConnectionFactory</code>. If the
     * required instance doesn't exist in the cache, it finds the space and creates a new
     * <code>XAQueueConnectionFactory</code> to work with that space.
     *
     * @param spaceURL space URL
     * @return a <code>XAQueueConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized XAQueueConnectionFactory getXAQueueConnectionFactory(String spaceURL, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXAQueueConnectionFactory(String): spaceURL=" +
                    spaceURL + ", IMessageConverter=" + messageConverter);
        }
        return (XAQueueConnectionFactory) getConnectionFactory(spaceURL, messageConverter);
    }


    public XATopicConnectionFactory getXATopicConnectionFactory(String spaceURL) throws JMSException {
        return getXATopicConnectionFactory(spaceURL, null);
    }


    /**
     * Returns a <code>XATopicConnectionFactory</code> that works with the space in the specified
     * spaceURL. First it tries to find a cached <code>XATopicConnectionFactory</code>. If the
     * required instance doesn't exist in the cache, it finds the space and creates a new
     * <code>XATopicConnectionFactory</code> to work with that space.
     *
     * @param spaceURL space URL
     * @return a <code>XATopicConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized XATopicConnectionFactory getXATopicConnectionFactory(String spaceURL, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXATopicConnectionFactory(String): spaceURL=" +
                    spaceURL + ", IMessageConverter=" + messageConverter);
        }
        return (XATopicConnectionFactory) getConnectionFactory(spaceURL, messageConverter);
    }


    public XAQueueConnectionFactory getXAQueueConnectionFactory(IJSpace space) throws JMSException {
        return getXAQueueConnectionFactory(space, null);
    }

    /**
     * Returns a <code>XAQueueConnectionFactory</code> that works with the specified space proxy.
     * First it tries to find a cached <code>XAQueueConnectionFactory</code>. If the required
     * instance doesn't exist in the cache, it creates a new <code>XAQueueConnectionFactory</code>
     * to work with that space.
     *
     * @param space the space proxy
     * @return a <code>XAQueueConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized XAQueueConnectionFactory getXAQueueConnectionFactory(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXAQueueConnectionFactory(IJSpace): Space="
                    + space.getName() + ", IMessageConverter=" + messageConverter);
        }
        return (XAQueueConnectionFactory) getXAConnectionFactory(space, messageConverter);
    }


    public XATopicConnectionFactory getXATopicConnectionFactory(IJSpace space) throws JMSException {
        return getXATopicConnectionFactory(space, null);
    }

    /**
     * Returns a <code>XATopicConnectionFactory</code> that works with the specified space proxy.
     * First it tries to find a cached <code>XATopicConnectionFactory</code>. If the required
     * instance doesn't exist in the cache, it creates a new <code>XATopicConnectionFactory</code>
     * to work with that space.
     *
     * @param space the space proxy
     * @return a <code>XATopicConnectionFactory</code> instance
     * @throws JMSException if the was a problem to return the factory
     */
    public synchronized XATopicConnectionFactory getXATopicConnectionFactory(IJSpace space, IMessageConverter messageConverter)
            throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSJMSAdmin.getXATopicConnectionFactory(IJSpace): Space="
                    + space.getName() + ", IMessageConverter=" + messageConverter);
        }
        return (XATopicConnectionFactory) getXAConnectionFactory(space, messageConverter);
    }


    /**
     * @param name the name of the <code>Queue</code>
     * @return a <code>Queue</code> with the specified name.
     */
    public synchronized Queue getQueue(String name) {
        Queue queue = queues.get(name);
        if (queue == null) {
            queue = new GSQueueImpl(name);
            queues.put(name, queue);
        }
        return queue;
    }

    /**
     * @param name the name of the <code>Topic</code>
     * @return a <code>Topic</code> with the specified name.
     */
    public synchronized Topic getTopic(String name) {
        Topic topic = topics.get(name);
        if (topic == null) {
            topic = new GSTopicImpl(name);
            topics.put(name, topic);
        }
        return topic;
    }


    /**
     * Finds a space proxy at the specified URL.
     *
     * @param spaceURL the space URL
     * @return A space proxy instance
     * @throws JMSException if failed to find the space
     */
    private IJSpace findSpace(String spaceURL) throws JMSException {
        IJSpace spaceProxy = null;
        try {
            spaceProxy = (IJSpace) SpaceFinder.find(spaceURL);
            if (_logger.isInfoEnabled()) {
                _logger.info("GSJMSAdmin.findSpace() Found space <" + spaceProxy.getName() + "> " + " using SpaceURL: " + spaceURL);
            }
        } catch (FinderException ex) {
            ResourceAllocationException rae = new ResourceAllocationException(
                    "GSJMSAdmin - ERROR: Could not find space: "
                            + spaceURL
                            + ex.toString() + "\n");
            rae.setLinkedException(ex);
            throw rae;
        }
        return spaceProxy;
    }


    /**
     * @return Returns the compressionMinSize. The minimum size (in bytes) which from where we start
     * to compress all the message body.
     *
     * e.g. if a 1 MB Text JMSMessage body is sent, and the compressionMinSize value is 500000
     * (0.5MB) then we will compress that message body (only), otherwise we will send (write) it as
     * is. TODO Currently this configured via system property later will be part of the
     * JMS-config.xml default value is 0.5 MB
     */
    public int getCompressionMinSize() {
        return compressionMinSize;
    }


    /**
     * Creates a JNDI API InitialContext object if none exists yet. Then looks up the string
     * argument and returns the associated JMS administrated object.
     *
     * @param name the name of the object to look for
     * @return the JMS administrated object bound to the name.
     * @throws NamingException if name cannot be found
     */
    public Object jndiLookup(String name) throws NamingException {

        //  We should lookup according to the following pattern:
        //  for Factories:
        //  -----------------
        //  GigaSpaces;ContainerName;SpaceName;GSConnectionFactoryImpl
        //  GigaSpaces;ContainerName;SpaceName;GSTopicConnectionFactoryImpl
        //  GigaSpaces;ContainerName;SpaceName;GSQueueConnectionFactoryImpl
        //  GigaSpaces;ContainerName;SpaceName;GSXATopicConnectionFactoryImpl
        //  GigaSpaces;ContainerName;SpaceName;GSXAQueueConnectionFactoryImpl
        //
        //  for Destinations:
        //  -----------------
        //  GigaSpaces;ContainerName;SpaceName;jms;destinations;MyQueue
        //  GigaSpaces;ContainerName;SpaceName;jms;destinations;MyTopic
        //
        //  The returned object from the lookup is the actual jms admin object
        //  e.g the requested jms Dest/Fac objects.

        return getInitialContext().lookup(name);
    }


    /**
     * Returns the InitialContext.
     *
     * @return the InitialContext instance
     * @throws NamingException if failed to return the InitialContext
     */
    private synchronized InitialContext getInitialContext() throws NamingException {
        if (jndiContext == null) {
            try {
                jndiContext = new InitialContext();

                // Assure we have an rmi registry InitialContext. If not we
                // might get NoInitialContextException during lookup.
                jndiContext.getEnvironment();
            } catch (javax.naming.NoInitialContextException ne) {
                Properties props = new Properties();

                // java.naming.factory.initial
                props.setProperty(InitialContext.INITIAL_CONTEXT_FACTORY,
                        "com.sun.jndi.rmi.registry.RegistryContextFactory");

                // java.naming.provider.url
                props.setProperty(InitialContext.PROVIDER_URL,
                        "rmi://localhost:10098");

                jndiContext = new InitialContext(props);
                if (_logger.isDebugEnabled()) {
                    _logger.debug("GSJMSAdmin.getInitialContext() Using fallback" +
                            " jndiContext environment: " + jndiContext.getEnvironment());
                }
            }
        }
        return jndiContext;
    }


    /**
     * Creates a Destination (GSTopicImpl or GSQueueImpl) and then binds it to the jndi registry.
     */
    public void createAndBindDestination(boolean isQueue,
                                         String destinationName) throws NamingException, JMSException {
        Object dest;
        try {
            dest = jndiLookup(destinationName);
        } catch (NamingException ne) {
            if (isQueue) {
                dest = getQueue(destinationName);
            } else {
                dest = getTopic(destinationName);
            }

            getInitialContext().rebind(destinationName, dest);
        }
    }


    /**
     * Destroys a Destination (GSTopicImpl or GSQueueImpl) and then unbinds from the jndi registry.
     */
    public void destroyAndUnBindDestination(String destinationName)
            throws JMSException {
        try {
            getInitialContext().unbind(destinationName);
        } catch (NamingException ne) {
            //TODO doing nothing. it might have not find the jndi name to remove
        }
    }

}
