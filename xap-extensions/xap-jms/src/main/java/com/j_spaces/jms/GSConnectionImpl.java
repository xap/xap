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


/*
 * Created on 11/03/2004
 *
 * @author		Gershon Diner
 * Title:      	The GigaSpaces Platform
 * Copyright:  	Copyright (c) GigaSpaces Team 2004
 * Company:   	GigaSpaces Technologies Ltd.
 * @version 	4.0
 */
package com.j_spaces.jms;

import com.gigaspaces.logger.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jms.utils.CyclicCounter;
import com.j_spaces.jms.utils.StringsUtils;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionFactory;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionManager;

import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.InvalidClientIDException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;

/**
 * Implements the <code>javax.jms.Connection</code> interface.
 *
 * GigaSpaces implementation of the JMS Connection Inerface. <p> <blockquote>
 *
 * <pre>
 *
 *  - It holds the list of the sessions that attached to this conn.
 *  - It returns a LocalTransaction instance
 *  - Most of the method implementations located at the sub-classes
 *  		GSTopicConnectionImpl and GSQueueConnectionImpl.
 *
 * </p>
 * &lt;/blockquote&gt;
 * </pre>
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
class GSConnectionImpl implements Connection, QueueConnection, TopicConnection {
    /**
     * client id
     */
    private String m_clientID;

    private ExceptionListener errorHandler;

    /**
     * <code>true</code> if the connection is closing.
     */
    protected boolean closing = false;

    /**
     * <code>true</code> if the connection is closed.
     */
    protected boolean closed = false;

    /**
     * This flag indicates whether the connection is in the started or stopped state
     */
    protected boolean stopped = true;

    volatile private CyclicCounter sessionsC = new CyclicCounter();

    /**
     * Connection key.
     */
    private String cnxKey;

    /*
     * Vector of the connection's sessions. TBD- according to the J2EE 1.4 there
     * should be only one session per one connection
     */
    private final Vector sessions;

    /* Vector of the connection's consumers. */
    //Vector connConsumers;

    /**
     * Connection MetaData
     */
    private GSConnectionMetaDataImpl m_metaData;

    /**
     * connFacParent instance
     */
    GSConnectionFactoryImpl connFacParent;

    /**
     * This flag indicates whether the connection has been modified. If so, subsequent attempts to
     * invoke {@link #setClientID}will cause an <code>IllegalStateException</code> being thrown
     */
    private boolean m_modified = false;

    /**
     * Gates the setting of the clientId more than once
     */
    private boolean m_clientIdSet = false;

    /**
     * The minimum size (in bytes) which from where we start to compress all the message body.
     *
     * e.g. if a 1 MB Text JMSMessage body is sent, and the compressionMinSize value is 500000
     * (0.5MB) then we will compress that message body (only), otherwise we will send (write) it as
     * is. TODO Currently this configured via system property later will be part of the
     * JMS-config.xml default value is 0.5 MB
     */
    private final int m_compressionMinSize;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);

    /**
     * Constructor:
     *
     * @param _connFacParent instance
     */
    public GSConnectionImpl(GSConnectionFactoryImpl _connFacParent) throws JMSException {
        this.connFacParent = _connFacParent;
        sessions = new Vector();//TODO To be change, as per J2EE 1.4, every
        // conn has ONLY one session
        this.m_compressionMinSize = connFacParent.getAdmin().getCompressionMinSize();
    }

    /**
     * Use the extended methods, from GSTopicConnectionImpl for Topic and from GSQueueConnectionImpl
     * for Queue.
     *
     * @return null
     * @see Connection#createSession(boolean, int)
     */
    public Session createSession(boolean transacted, int acknowledgeMode)
            throws JMSException {
        if (closed) {
            throw new IllegalStateException("Forbidden call on a closed connection.");
        }

        GSSessionImpl session = new GSSessionImpl(this, transacted, acknowledgeMode);
        if (!isStopped()) {
            session.start();
        }
        return session;
    }

    /**
     * The connection client ID contains the space name together with the Destination name, "space
     * name_destination name" e.g. "JavaSpaces_MyTopic"
     *
     * @return String m_clientID
     * @see Connection#getClientID()
     */
    public String getClientID() throws JMSException {
        if (closed)
            throw new IllegalStateException("Forbidden call on a closed"
                    + " connection.");
        setModified();
        return this.m_clientID;
    }

    /**
     * The connection client ID contains the random connection Key + hostname + space name +
     * destination name "space name_destination name" e.g. "JavaSpaces_MyTopic" Client cannot set a
     * clientID more then once.
     *
     * @see Connection#setClientID(String)
     */
    public void setClientID(final String clientID) throws JMSException {
        if (closed)
            throw new IllegalStateException(
                    "Forbidden call on a closed connection.");
        // check if the client id has already been set externally
        boolean isSuggestedIDValid = isValidExtClientID(clientID);
        if (m_clientIdSet
                || (isSuggestedIDValid && !isValidExtClientID(m_clientID))) {
            throw new IllegalStateException(
                    "The client id has already been set");
        }

        if (!isSuggestedIDValid) {
            throw new InvalidClientIDException(
                    "Attempt to set the invalid client id:  " + clientID);
        }

        if (m_modified) {
            throw new IllegalStateException(
                    "The client identifier must be set before any other operation is performed");
        }

        // gat the client id from being set more than once.
        this.m_clientID = clientID;
        m_clientIdSet = true;
    }

    //returns false if it is internal clientID or empty
    private boolean isValidExtClientID(String _clientID) {
        if (_clientID.startsWith("tc_") || _clientID.startsWith("qc_")
                || _clientID.startsWith("xatc_") || _clientID.startsWith("xaqc_")
                || StringsUtils.isEmpty(_clientID))
            return false;
        return true;
    }


    /**
     * This method called internally by the GSSessionImpl and GSTopicConnectionImpl and
     * GSQueueConnectionImpl in order to append another string to the existing clientID to have the
     * final clientID, in which after that no more client modifications can be done from outside.
     */
    protected void updateClientIDInternally(final String updatedClientID)
            throws JMSException {
        if (closed)
            throw new IllegalStateException(
                    "Forbidden call on a closed connection.");

        this.m_clientID = updatedClientID;
        //this.m_clientIdSet = true;
    }

    /**
     * Flags this connection as being modified. Subsequent attempts to invoke {@link
     * #setClientID}will result in an <code> IllegalStateException </code> being thrown
     */
    protected void setModified() {
        m_modified = true;
    }

    /**
     * @see Connection#getMetaData()
     * @see com.j_spaces.jms.GSConnectionMetaDataImpl
     */
    public ConnectionMetaData getMetaData() throws JMSException {
        if (closed)
            throw new IllegalStateException("Forbidden call on a closed"
                    + " connection.");
        if (m_metaData == null)
            m_metaData = new GSConnectionMetaDataImpl();
        setModified();
        return m_metaData;
    }

    /**
     * @see Connection#getExceptionListener()
     */
    public ExceptionListener getExceptionListener() throws JMSException {
        if (closed)
            throw new IllegalStateException("Forbidden call on a closed"
                    + " connection.");
        setModified();
        return errorHandler;
    }

    /**
     * @see Connection#setExceptionListener(ExceptionListener)
     */
    public void setExceptionListener(final ExceptionListener listener)
            throws JMSException {
        if (closed)
            throw new IllegalStateException("Forbidden call on a closed"
                    + " connection.");
        setModified();
        this.errorHandler = listener;
    }

    /**
     * Adding a child session into the current connection.
     */
    void addSession(GSSessionImpl childSession) {
        synchronized (this) {
            sessions.addElement(childSession);
        }
    }

    /**
     * Removing a child session from the current connection.
     */
    void removeSession(GSSessionImpl childSession) {
        synchronized (this) {
            sessions.remove(childSession);
            childSession = null;
        }
    }

    /**
     * Passes an asynchronous exception to the exception listener, if any.
     *
     * @param jE The asynchronous JMSException.
     */
    void onException(JMSException jE) {
        if (errorHandler != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("calling ExceptionListener.onException(): " + cnxKey);
            }
            errorHandler.onException(jE);
        } else {
            if (_logger.isDebugEnabled()) {
                _logger.debug("ExceptionListener is not set for connection: " + cnxKey);
            }
        }
    }

    /**
     * Overwrite of the API method for starting the Topic connection. Loops on all the sessions and
     * starts them.
     *
     * @throws IllegalStateException If the connection is closed or broken.
     */
    public void start() throws JMSException {
        synchronized (this) {
            //If closed, throwing an exception:
            ensureOpen();
            //		Ignoring the call if the connection is started:
            //		if (!stopped)
            //			return;
            setModified();

            try {
                if (stopped) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(
                                "GSConnectionImpl.start()  starting connection: " + toString());
                    }
                    // propagate the start to all the associated sessions. When
                    // that is complete transition the state of the connection to
                    // RUNNING
                    //Starting the sessions:
                    for (int i = 0; i < sessions.size(); i++) {
                        GSSessionImpl session = (GSSessionImpl) sessions.get(i);
                        session.start();
                    }
                    // set the state of the connection to start
                    stopped = false;
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("GSConnectionImpl.start()  connection was started: " + toString());
                    }
                }
            } catch (JMSException exception) {
                // TODO do we need to change stopped to true if the any of the
                // sessions fail to start ???
                throw exception;
            }
        }
    }

    /**
     * Overwrite of the API method for stoping the Topic connection.
     *
     * @throws IllegalStateException If the connection is closed or broken.
     * @see Connection#stop()
     */
    public void stop() throws JMSException {
        synchronized (this) {
            //		If closed, throwing an exception:
            ensureOpen();
            //		Ignoring the call if the connection is already stopped:
            //		if (! stopped)
            //			  return;
            setModified();

            if (!stopped) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("GSConnectionImpl.stop()  stopping connection: " + toString());
                }
                // propagate the stop to all the encapsulated sessions before
                // changing the state of the connection. Only when all the
                // sessions have successfully transitioned, without exception,
                // we change the state of the connection
                //Stoping the sessions:
                for (int i = 0; i < sessions.size(); i++) {
                    GSSessionImpl session = (GSSessionImpl) sessions.get(i);
                    session.stop();
                }
                if (_logger.isDebugEnabled()) {
                    _logger.debug("GSConnectionImpl.stop()  connection was stopped: " + getCnxKey());
                }
                // set the state of the connection to stopped before stopping
                // all the enclosed sessions
                stopped = true;
            }
        }
    }

    /**
     * API method for closing the connection; even if the connection appears to be broken, closes
     * the sessions. Loops on all the sessions and closes them. If one or more of the connections
     * sessions message listeners is processing a message at the point when connection close is
     * invoked, all the facilities of the connection and its sessions must remain available to those
     * listeners until they return control to the JMS provider. When connection close is invoked it
     * should not return until message processing has been shut down in an orderly fashion. This
     * means that all message listeners that may have been running have returned, and that all
     * pending receives have returned.
     *
     * @throws JMSException Actually never thrown.
     */
    public void close() throws JMSException {
        synchronized (this) {
            //Ignoring the call if the connection is already closed
            if (!closed) {
                closing = true;
                //before we close we should stop the connection and any
                // associated sessions
                if (_logger.isDebugEnabled()) {
                    _logger.debug("GSConnectionImpl.close()  closing connection: " + toString());
                }
                stop();
                //closing the sessions ??? DO WE NEED THIS in j2ee 1.4 must be only
                // 1 session per conn

                // propagate the close to all the encapsulated sessions before
                // changing the state of the connection. Only when all the
                // sessions have successfully transitioned, without exception,
                // do we change the state of the connection. All the sessions
                // are removed from the connection.
                while (!sessions.isEmpty()) {
                    GSSessionImpl session = (GSSessionImpl) sessions.elementAt(0);
                    try {
                        session.close();
                    }
                    // Catching a JMSException if the connection is broken:
                    catch (JMSException jE) {
                    }
                }
                //remove yourself from the list of connections managed by the
                // connection factory and then null the factory.
                getConnFacParent().removeConnection(this);
                //connFacParent = null;

                closed = true;
                closing = false;
                if (_logger.isDebugEnabled()) {
                    _logger.debug("GSConnectionImpl.close() connection was closed: " + toString());
                }
            }
        }
    }

    /**
     * Return the running state of the connection
     *
     * @return <code>true</code> if stopped, else started state.
     */
    protected boolean isStopped() {
        return stopped;
    }

    /**
     * Verifies that the connection is open
     *
     * @throws IllegalStateException if the connection is closed
     */
    protected void ensureOpen() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException(
                    "Forbidden call on a closed connection.");
        }
    }

    /**
     * @see Connection#createConnectionConsumer(Destination, String, ServerSessionPool, int)
     */
    public ConnectionConsumer createConnectionConsumer(Destination destination,
                                                       String messageSelector, ServerSessionPool sessionPool,
                                                       int maxMessages) throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSConnectionImpl.createConnectionConsumer() method is not implemented. ");
        }
        return null;
    }

    /**
     * @see Connection#createDurableConnectionConsumer(Topic, String, String, ServerSessionPool,
     * int)
     */
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic,
                                                              String subscriptionName, String messageSelector,
                                                              ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSConnectionImpl.createDurableConnectionConsumer() method is not implemented. ");
        }
        return null;
    }


    /**
     * Returns a new session identifier.
     *
     * @return a new session identifier.
     */
    String nextSessionId() {
        return cnxKey + "_sess_" + sessionsC.increment();
    }


    /**
     * @return IJSpace space proxy instance
     */
    IJSpace getSpace() {
        return connFacParent.getSpace();
    }

    ReentrantLock txnLock = new ReentrantLock();

    /**
     * create a new local transaction instance and use it in case of a transacted JMS session.
     *
     * @return tr
     */
    public Transaction getTransaction(boolean localTransaction, long leaseTime)
            throws TransactionCreateException {
        Transaction tr;
        Transaction.Created tCreated;
        txnLock.lock();
        try {
            if (localTransaction) {
                tCreated = TransactionFactory.create(
                        connFacParent.getLocalTransactionManager(),
                        leaseTime);
            } else {
                tCreated = TransactionFactory.create(
                        connFacParent.getDistributedTransactionManager(),
                        leaseTime);
            }
            tr = tCreated.transaction;
            if (_logger.isDebugEnabled()) {
                _logger.debug("GSConnectionImpl.getTransaction() leaseTime: "
                        + leaseTime + "  |  TX.id: " + ((ServerTransaction) tr).id
                        + "|  TX.mgr: " + ((ServerTransaction) tr).mgr.toString());
            }
        } catch (Exception e) {
            String msg = "Failed to create transaction: " + e;
            if (_logger.isErrorEnabled()) {
                _logger.error(msg);
            }
            throw new TransactionCreateException(msg, e);
        } finally {
            txnLock.unlock();
        }
        return tr;
    }


    TransactionManager getTransactionManager() throws JMSException {
        return connFacParent.getLocalTransactionManager();
    }

    /**
     * @return cnxKey
     */
    protected String getCnxKey() {
        return cnxKey;
    }

    /**
     * @param string
     */
    protected void setCnxKey(final String string) {
        cnxKey = string;
    }

    @Override
    public String toString() {
        return "GSConnectionImpl || client  ID: " + m_clientID
                + " || connection ID: " + getCnxKey();
    }

    /**
     * @return GSConnectionFactoryImpl
     */
    protected GSConnectionFactoryImpl getConnFacParent() {
        return connFacParent;
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
        return m_compressionMinSize;
    }


    /**
     * @see QueueConnection#createConnectionConsumer(Queue, String, ServerSessionPool, int)
     */
    public ConnectionConsumer createConnectionConsumer(Queue queue,
                                                       String messageSelector,
                                                       ServerSessionPool sessionPool,
                                                       int maxMessages)
            throws JMSException {
        return createConnectionConsumer((Destination) queue, messageSelector, sessionPool, maxMessages);
    }


    /**
     * @see QueueConnection#createQueueSession(boolean, int)
     */
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode)
            throws JMSException {
        if (closed) {
            throw new IllegalStateException("Forbidden call on a closed connection.");
        }

        GSQueueSessionImpl session = new GSQueueSessionImpl(this, transacted, acknowledgeMode);
        if (!isStopped()) {
            session.start();
        }
        return session;
    }


    /**
     * @see TopicConnection#createConnectionConsumer(Topic, String, ServerSessionPool, int)
     */
    public ConnectionConsumer createConnectionConsumer(Topic topic,
                                                       String messageSelector,
                                                       ServerSessionPool sessionPool,
                                                       int maxMessages)
            throws JMSException {
        return createConnectionConsumer((Destination) topic, messageSelector, sessionPool, maxMessages);
    }

    /**
     * @see TopicConnection#createTopicSession(boolean, int)
     */
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode)
            throws JMSException {
        if (closed) {
            throw new IllegalStateException("Forbidden call on a closed connection.");
        }

        GSTopicSessionImpl session = new GSTopicSessionImpl(this, transacted, acknowledgeMode);
        if (!isStopped()) {
            session.start();
        }
        return session;
    }

}//end of class
