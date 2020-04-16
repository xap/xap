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

import com.j_spaces.core.IJSpace;
import com.j_spaces.jms.utils.IMessageConverter;



import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XAQueueConnectionFactory;
import javax.jms.XATopicConnection;
import javax.jms.XATopicConnectionFactory;

public class GSXAConnectionFactoryImpl extends GSConnectionFactoryImpl implements XAConnectionFactory,
        XAQueueConnectionFactory, XATopicConnectionFactory {
    private static final long serialVersionUID = 1L;

    /**
     * Required for Externalizable
     */
    public GSXAConnectionFactoryImpl() {
    }

    public GSXAConnectionFactoryImpl(IJSpace space, IMessageConverter messageConverter) throws JMSException {
        super(space, messageConverter);
    }

    public XAConnection createXAConnection() throws JMSException {
        return createGSXAConnection("xac");
    }

    public XAConnection createXAConnection(String userName, String password) throws JMSException {
        return createGSXAConnection("xac", userName, password);
    }

    public XAQueueConnection createXAQueueConnection() throws JMSException {
        return createGSXAConnection("xaqc");
    }

    public XAQueueConnection createXAQueueConnection(String userName, String password) throws JMSException {
        return createGSXAConnection("xaqc", userName, password);
    }

    public XATopicConnection createXATopicConnection() throws JMSException {
        return createGSXAConnection("xatc");
    }

    public XATopicConnection createXATopicConnection(String userName, String password) throws JMSException {
        return createGSXAConnection("xatc", userName, password);
    }

    private GSXAConnectionImpl createGSXAConnection(String prefix)
            throws JMSException {
        GSXAConnectionImpl conn = new GSXAConnectionImpl(this);
        String connKey = nextCnxKey();
        conn.setCnxKey(connKey);
        conn.updateClientIDInternally(connKey + "_" + getSpaceName());
        addConnection(conn);
        if (_logger.isDebugEnabled())
            _logger.debug("GSConnectionFactoryImpl.createGSConnection() connKey: " + connKey);
        return conn;
    }

    private GSXAConnectionImpl createGSXAConnection(String prefix, String userName, String password) throws JMSException {
        setSpaceSecurityContext(userName, password);
        return createGSXAConnection(prefix);
    }
}
