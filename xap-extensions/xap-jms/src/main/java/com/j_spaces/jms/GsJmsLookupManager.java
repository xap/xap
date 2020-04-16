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

package com.j_spaces.jms;

import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.IJSpace;
import com.j_spaces.kernel.JSpaceUtilities;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingException;

import static com.j_spaces.core.Constants.Jms.JMS_CON_FAC_NAME;
import static com.j_spaces.core.Constants.Jms.JMS_DELIMITER;
import static com.j_spaces.core.Constants.Jms.JMS_DESTINATIONS_NAME;
import static com.j_spaces.core.Constants.Jms.JMS_JMS_NAME;
import static com.j_spaces.core.Constants.Jms.JMS_QUEUE_CON_FAC_NAME;
import static com.j_spaces.core.Constants.Jms.JMS_QUEUE_NAMES_DEFAULT;
import static com.j_spaces.core.Constants.Jms.JMS_QUEUE_NAMES_PROP;
import static com.j_spaces.core.Constants.Jms.JMS_RMI_PORT_DEFAULT;
import static com.j_spaces.core.Constants.Jms.JMS_RMI_PORT_PROP;
import static com.j_spaces.core.Constants.Jms.JMS_TOPIC_CON_FAC_NAME;
import static com.j_spaces.core.Constants.Jms.JMS_TOPIC_NAMES_DEFAULT;
import static com.j_spaces.core.Constants.Jms.JMS_TOPIC_NAMES_PROP;
import static com.j_spaces.core.Constants.Jms.JMS_XAQUEUE_CON_FAC_NAME;
import static com.j_spaces.core.Constants.Jms.JMS_XATOPIC_CON_FAC_NAME;
import static com.j_spaces.core.Constants.LookupManager.VENDOR;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
public class GsJmsLookupManager implements Closeable {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_LOOKUPMANAGER);

    private final String spaceName;
    private final IJSpace spaceProxy;
    private final GSJmsFactory jmsFactory;
    private final Context internalContext;
    private final Context externalContext;

    private final String connectionFactoryBindString;
    private final String topicConnectionFactoryBindString;
    private final String queueConnectionFactoryBindString;
    private final String xaTopicConnectionFactoryBindString;
    private final String xaQueueConnectionFactoryBindString;
    private final Collection<String> queueNames;
    private final Collection<String> topicNames;

    public GsJmsLookupManager(String containerName, IJSpace spaceProxy, GSJmsFactory jmsFactory,
                              Context internalJndiContext, Context externalJndiContext) {
        this.spaceName = spaceProxy.getName();
        this.spaceProxy = spaceProxy;
        this.jmsFactory = jmsFactory;
        this.internalContext = internalJndiContext;
        this.externalContext = externalJndiContext;

        final String prefix = VENDOR + JMS_DELIMITER + containerName + JMS_DELIMITER + spaceName + JMS_DELIMITER;
        this.connectionFactoryBindString = prefix + JMS_CON_FAC_NAME;
        this.queueConnectionFactoryBindString = prefix + JMS_QUEUE_CON_FAC_NAME;
        this.topicConnectionFactoryBindString = prefix + JMS_TOPIC_CON_FAC_NAME;
        this.xaQueueConnectionFactoryBindString = prefix + JMS_XAQUEUE_CON_FAC_NAME;
        this.xaTopicConnectionFactoryBindString = prefix + JMS_XATOPIC_CON_FAC_NAME;

        String fullSpaceName = JSpaceUtilities.createFullSpaceName(containerName, spaceName);
        SpaceConfigReader configReader = new SpaceConfigReader(fullSpaceName);
        final String destinationPrefix = prefix + JMS_JMS_NAME + JMS_DELIMITER + JMS_DESTINATIONS_NAME + JMS_DELIMITER;
        topicNames = toCollection(configReader.getSpaceProperty(JMS_TOPIC_NAMES_PROP, JMS_TOPIC_NAMES_DEFAULT), destinationPrefix);
        queueNames = toCollection(configReader.getSpaceProperty(JMS_QUEUE_NAMES_PROP, JMS_QUEUE_NAMES_DEFAULT), destinationPrefix);
        // TODO: Delete last usage of JMS_RMI_PORT_PROP and JMS_RMI_PORT_DEFAULT properly.
        String rmiPort = configReader.getSpaceProperty(JMS_RMI_PORT_PROP, JMS_RMI_PORT_DEFAULT);
    }

    public void initialize() throws Exception {
        rebind(connectionFactoryBindString, jmsFactory.createConnectionFactory(spaceProxy), "ConnectionFactory");
        rebind(queueConnectionFactoryBindString, jmsFactory.createQueueConnectionFactory(spaceProxy), "QueueConnectionFactory");
        rebind(topicConnectionFactoryBindString, jmsFactory.createTopicConnectionFactory(spaceProxy), "TopicConnectionFactory");
        rebind(xaQueueConnectionFactoryBindString, jmsFactory.createXAQueueConnectionFactory(spaceProxy), "XAQueueConnectionFactory");
        rebind(xaTopicConnectionFactoryBindString, jmsFactory.createXATopicConnectionFactory(spaceProxy), "XATopicConnectionFactory");
        for (String name : topicNames)
            rebind(name, jmsFactory.createTopic(name), "Topic");
        for (String name : queueNames)
            rebind(name, jmsFactory.createQueue(name), "Queue");
    }

    @Override
    public void close() {
        //unbind(connectionFactoryBindString, "ConnectionFactory");
        unbind(topicConnectionFactoryBindString, "TopicConnectionFactory");
        unbind(queueConnectionFactoryBindString, "QueueConnectionFactory");
        unbind(xaTopicConnectionFactoryBindString, "XATopicConnectionFactory");
        unbind(xaQueueConnectionFactoryBindString, "XAQueueConnectionFactory");
        for (String name : topicNames)
            unbind(name, "Topic");
        for (String name : queueNames)
            unbind(name, "Queue");
    }

    protected void rebind(String name, Object obj, String desc) {
        //binds to an internal jndi context
        if (internalContext != null) {
            try {
                internalContext.rebind(name, obj);
                if (_logger.isInfoEnabled())
                    _logger.info("JMS " + desc + " < " + name + " > bound successfully to the RMIRegistry lookup service.");
            } catch (NamingException e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Failed to register < " + name + " > JMS Admin Object, on space " +
                            "<" + spaceName + "> using the the RMIRegistry lookup service: "
                            + internalContext + " | ", e);
                }
            }
        }

        //binds to an External jndi context returns true if success , other wise error message.
        if (externalContext != null) {
            try {
                externalContext.rebind(name, obj);
                if (_logger.isInfoEnabled())
                    _logger.info("JMS " + desc + " < " + name + " > bound successfully to the External JNDI lookup service: " + externalContext.getEnvironment().toString());
            } catch (NamingException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error("Failed to register < " + name + " > JMS Admin Object, on space " +
                            "<" + spaceName + "> using the external JNDI lookup service: "
                            + externalContext + " | " + e.getRootCause().getMessage(), e);
                }
            }
        }
    }

    protected void unbind(String name, String desc) {
        if (StringUtils.isEmpty(name))
            return;

        //unbinds from internal jndi context
        if (internalContext != null) {
            try {
                internalContext.unbind(name);
                if (_logger.isDebugEnabled())
                    _logger.debug("JMS " + desc + " < " + name + " > unbound successfully from the RMIRegistry lookup service");
            } catch (NamingException e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Failed to unbind the < " + name + " > JMS Admin Object, from space " +
                            "<" + spaceName + "> using the the RMIRegistry lookup service: "
                            + internalContext + " | " + e.getRootCause().toString(), e);
                }
            }
        }

        //unbinds from an External jndi context returns true if success , other wise error message.
        if (externalContext != null) {
            try {
                externalContext.unbind(name);
                if (_logger.isDebugEnabled())
                    _logger.debug("JMS " + desc + " < " + name + " > unbound successfully from the External JNDI lookup service: " + externalContext.getEnvironment().toString());
            } catch (NamingException e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Failed to unbind the < " + name + " > JMS Admin Object, from space " +
                            "<" + spaceName + "> using the external JNDI lookup service: "
                            + externalContext + " | " + e.getRootCause().toString(), e);
                }
            }
        }
    }

    private static Collection<String> toCollection(String s, String prefix) {
        if (StringUtils.isEmpty(s))
            return Collections.emptyList();
        StringTokenizer st = new StringTokenizer(s, ",");
        ArrayList<String> result = new ArrayList<String>();
        while (st.hasMoreTokens())
            result.add(prefix + st.nextToken());
        return result;
    }
}
