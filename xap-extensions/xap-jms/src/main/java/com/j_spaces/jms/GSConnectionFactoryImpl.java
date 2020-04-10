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

import com.gigaspaces.client.DirectSpaceProxyFactory;
import com.gigaspaces.client.transaction.DistributedTransactionManagerProvider;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceSecurityException;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.LookupFinder;
import com.j_spaces.core.client.LookupRequest;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.jms.utils.GSJMSAdmin;
import com.j_spaces.jms.utils.IMessageConverter;

import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.TransactionManager;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.Random;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;

/**
 * Implements the <code>javax.jms.ConnectionFactory</code> interface.<br>
 *
 * It also in charge of the following:<br> - Finding a SpaceProxy according to te provided space
 * url.<br> - Generating connection id's.<br> - Managing the gigaspaces space urls used by the jms
 * connections.<br> - Creates the ParserManager for selector usage.<br>
 *
 * @author Gershon Diner
 * @version 4.0
 */
public class GSConnectionFactoryImpl implements ConnectionFactory,
        QueueConnectionFactory,
        TopicConnectionFactory,
        //XAConnectionFactory,
        Externalizable,
        Remote {
    private static final long serialVersionUID = 1L;

    protected static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);
    private static final Object distributedTransactionManagerProviderLock = new Object();
    private static final Random random = new Random();
    private static final String ID_SUFFIX = generateIdSuffix();
    private static final String AUTH_DEF_ANONYMOUSE_NAME = "anonymous";
    private static DistributedTransactionManagerProvider distributedTransactionManagerProvider;
    private static TransactionManager dtm; // distributed transaction manager (mahalo)

    private IJSpace m_space;
    private IMessageConverter messageConverter;
    private int connectionsCounter;
    private transient GSJMSAdmin admin;

    /**
     * Hashtable holds all the current available QueueConnection/TopicConnections with the
     * name/value pairs for spaceURL as the name and QueueConnection/TopicConnections as the value.
     * Evrytime new connection needs to be created, we first check if that conn is already been
     * created for that same space proxy, if true, then we return that same conn instance, otherwise
     * we create new conn. When calling to connection.close(), we also remove this conn from the
     * hashtable.
     */
    private Hashtable<String, GSConnectionImpl> connectionsHash;

    private static String generateIdSuffix() {
        String hostIPStr = SystemInfo.singleton().network().getHost().getHostAddress();

        //gets the host ip adress for later usage in the
        //clientID, and masking it from last to first, removing the .
        //e.g. 192.114.147.10 --> 10147114192
        StringBuilder sb = new StringBuilder();
        StringTokenizer st = new StringTokenizer(hostIPStr, ".");
        while (st.hasMoreTokens())
            sb.insert(0, st.nextToken());
        return sb.toString();
    }

    /**
     * Required for Externalizable
     */
    public GSConnectionFactoryImpl() {
    }

    public GSConnectionFactoryImpl(IJSpace space, IMessageConverter messageConverter) throws JMSException {
        this.m_space = space;
        this.messageConverter = messageConverter;
        this.connectionsHash = new Hashtable<String, GSConnectionImpl>();
    }

    protected IMessageConverter getMessageConverter() {
        return messageConverter;
    }

    protected IJSpace getSpace() {
        return this.m_space;
    }

    protected String getSpaceName() {
        return m_space.getName();
    }

    protected SpaceURL getSpaceURL() {
        return m_space.getURL();
    }

    protected GSJMSAdmin getAdmin() throws JMSException {
        if (admin == null)
            admin = GSJMSAdmin.getInstance();
        return admin;
    }

    protected void setSpaceSecurityContext(String username, String password) throws JMSException {
        if (username == null)
            throw new JMSSecurityException("Unauthenticated Username- Username must not be null");
        if (password == null)
            throw new JMSSecurityException("Unauthenticated Password- Password must not be null");

        try {
            //if anonymus user/pass are used then ignore the security
            if (username.equalsIgnoreCase(AUTH_DEF_ANONYMOUSE_NAME)
                    && password.equalsIgnoreCase(AUTH_DEF_ANONYMOUSE_NAME))
                return;

            if (!m_space.isSecured()) {
                throw new JMSSecurityException("The space < "
                        + getSpaceName() + " > must be defined as a Secured Space.");
            }
            ((ISpaceProxy) m_space).login(new DefaultCredentialsProvider(username, password));
        } catch (SpaceSecurityException se) {
            if (_logger.isInfoEnabled()) {
                _logger.info("exception inside setSpaceSecurityContext(user,pass): " + se.toString(), se);
            }
            JMSSecurityException e = new JMSSecurityException(
                    "SpaceSecurityException : " + se.toString());
            e.setLinkedException(se);
            throw e;
        } catch (RemoteException re) {
            if (_logger.isInfoEnabled()) {
                _logger.info("exception inside setSpaceSecurityContext(user,pass): " + re.toString(), re);
            }
            JMSException e = new JMSException("RemoteException : "
                    + re.toString());
            e.setLinkedException(re);
            throw e;
        }
    }

    /**
     * According to the m_connFactoryType we decide what would be the connection key prefix. Using
     * tc prefix for TopicConnection or qc prefix for QueueConnection, or Using xatc prefix for
     * XATopicConnection or xaqc prefix for XAQueueConnection.
     *
     * @return the next connection key.
     **/
    protected synchronized String nextCnxKey() {
        if (connectionsCounter == Integer.MAX_VALUE)
            connectionsCounter = 0;
        connectionsCounter++;
        int randLong = random.nextInt(Integer.MAX_VALUE);
        return connectionsCounter + (randLong + "_" + ID_SUFFIX);
    }

    public TransactionManager getLocalTransactionManager() throws JMSException {
        if (distributedTransactionManagerProvider == null) {
            try {
                synchronized (distributedTransactionManagerProviderLock) {
                    if (distributedTransactionManagerProvider == null)
                        distributedTransactionManagerProvider = new DistributedTransactionManagerProvider();
                }
            } catch (TransactionException re) {
                if (_logger.isErrorEnabled())
                    _logger.error("Exception inside GSConnectionFactoryImpl.createLocalTransactionManager() : " + re.toString(), re);
                JMSException e = new JMSException("RemoteException: " + re.toString());
                e.setLinkedException(re);
                throw e;
            }
        }
        return distributedTransactionManagerProvider.getTransactionManager();
    }

    public TransactionManager getDistributedTransactionManager() throws JMSException {
        if (dtm == null) {
            try {
                LookupRequest request = LookupRequest.TransactionManager()
                        .setLocators(getSpaceURL().getProperty(SpaceURL.LOCATORS))
                        .setGroups(getSpaceURL().getProperty(SpaceURL.GROUPS))
                        .setTimeout(10 * 1000);

                dtm = (TransactionManager) LookupFinder.find(request);

                if (_logger.isDebugEnabled())
                    _logger.debug("Created Distributed Transaction Manager: " + dtm.toString());
            } catch (FinderException fe) {
                if (_logger.isErrorEnabled())
                    _logger.error("Failed to create Exception Distributed Transaction Manager.", fe);
                JMSException e = new JMSException("FinderException: " + fe.toString());
                e.setLinkedException(fe);
                throw e;
            }
        }
        return dtm;
    }

    /**
     * Add the specified connection to the list of managed connections by this connection factory.
     *
     * @param connection connection to register
     */
    void addConnection(GSConnectionImpl connection) throws JMSException {
        connectionsHash.put(connection.connFacParent.getSpaceURL().getURL(), connection);
    }

    /**
     * Remove the specified connection from the list of managed connections by this connection
     * factory. If it doesn't exist then fail silently
     *
     * @param connection connection to remove
     */
    void removeConnection(GSConnectionImpl connection) {
        connectionsHash.remove(connection.connFacParent.getSpaceURL().getURL());
    }

    private GSConnectionImpl createGSConnection(String prefix) throws JMSException {
        GSConnectionImpl conn = new GSConnectionImpl(this);
        String connKey = prefix + nextCnxKey();
        conn.setCnxKey(connKey);
        conn.updateClientIDInternally(connKey + "_" + getSpaceName());
        addConnection(conn);
        if (_logger.isDebugEnabled())
            _logger.debug("GSConnectionFactoryImpl.createGSConnection() connKey: " + connKey);
        return conn;
    }

    private GSConnectionImpl createGSConnection(String prefix, String userName, String password) throws JMSException {
        setSpaceSecurityContext(userName, password);
        return createGSConnection(prefix);
    }

    @Override
    public Connection createConnection() throws JMSException {
        return createGSConnection("c");
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        return createGSConnection("c", userName, password);
    }

    @Override
    public QueueConnection createQueueConnection() throws JMSException {
        return createGSConnection("qc");
    }

    @Override
    public QueueConnection createQueueConnection(String userName, String password) throws JMSException {
        return createGSConnection("qc", userName, password);
    }

    @Override
    public TopicConnection createTopicConnection() throws JMSException {
        return createGSConnection("tc");
    }

    @Override
    public TopicConnection createTopicConnection(String userName, String password) throws JMSException {
        return createGSConnection("tc", userName, password);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(m_space.getDirectProxy().getFactory());
        out.writeObject(messageConverter);
        out.writeInt(connectionsCounter);
        out.writeObject(connectionsHash);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        DirectSpaceProxyFactory factory = (DirectSpaceProxyFactory) in.readObject();
        m_space = factory.createSpaceProxy();
        messageConverter = (IMessageConverter) in.readObject();
        connectionsCounter = in.readInt();
        connectionsHash = (Hashtable<String, GSConnectionImpl>) in.readObject();
    }
}
