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
 * Title:        	The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:    GigaSpaces Technologies Ltd.
 * @version 	 4.0
 */
package com.j_spaces.jms;

import com.gigaspaces.events.DataEventSession;
import com.gigaspaces.events.DataEventSessionFactory;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.jms.utils.StringsUtils;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;
import net.jini.core.lease.Lease;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.jms.TransactionRolledBackException;

/**
 * Implements the <code>javax.jms.MessageConsumer</code> interface.
 *
 * GigaSpaces implementation of the JMS MessageConsumer Interface and the Jini RemoteEventListener.
 *
 * <p><blockquote><pre>
 * - It holds MessageConsumer Destination, Topic or Queue
 * - It creates the GigaSpaces OnMessageNotifyDelegator and the notify() calls
 * the MessageListener.onMessage() method.
 * - It supports both Synchronized and Asynchronized message consumption.
 * One can use the method receive() for the Synchronized form,
 * or use the NotifyDelegator together with a MessageListener
 * implementation for the Asynchronized message receiving manner.
 *
 * </p></blockquote></pre>
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 */
public class GSMessageConsumerImpl
        implements MessageConsumer, QueueReceiver, TopicSubscriber {
    /**
     * The message listener in case of asynchronous delivery.
     */
    private MessageListener m_messageListener;

    /**
     * React to notifications in case of asynchronous delivery.
     */
    private OnMessageEventListener onMessageEventListener;

    /**
     * The control thread of the consumer in case of asynchronous delivery.
     */
    private AsyncPoller asyncPoller;


    /**
     * The destination the consumer gets its messages from
     */
    protected Destination m_dest;

    /**
     * The destination name the consumer gets its messages from GERSHON FIX 25042006 - The default
     * class name (dest name is dummy in case the destination is null in the beginning and that
     * later passed to the send(). We then set the extEntry className using the destName
     */
    protected String m_destName = "TempDestName";

    /**
     * The m_session the consumer belongs to
     */
    protected GSSessionImpl m_session;

    /**
     * Tells whether this is a queue or a topic consumer
     */
    protected volatile boolean m_isQueue = true;

    /**
     * The configuration of the eventSession.
     */
    private EventSessionConfig eventSessionConfig;

    /**
     * The message notificator in topic asynchronous mode.
     */
    private DataEventSession eventSession;

    /**
     * <code>true</code> if the session is closed.
     */
    protected volatile boolean m_closed = false;

    /**
     * The space proxy.
     */
    private transient IJSpace m_space;

	/*
     * <code>true</code> for a durable subscriber.
	 *
	private boolean m_durableSubscriber;*/

    /**
     * The durable consumer subscription name.
     */
    public String m_durableSubscriptionName;

    /**
     * The time we wait for messages when using receiveNoWait(). TODO: Make this property
     * configurable.
     */
    private final static long RECEIVE_NO_WAIT_TIMEOUT = 2000L;

    /**
     * Consumer unique ID.
     */
    private String m_consumerID;

    /**
     * The m_selector for filtering messages.
     */
    private String m_selector;

    /**
     * The template to match required messages in the space.
     */
    private GSMessageImpl m_jmsMessageTemplate = null;

    /**
     * Helps to synchronize delivery in synchronous delivery.
     */
    private Object synchTopicNotifyLock = new Object();

    /**
     * The waiting message in synchronous delivery.
     */
    private GSMessageImpl currentMessage = null;

    /**
     * Used to wake up a waiting async poller thread (topic only)
     */
    private GSMessageImpl topicWakeupObject = new GSMessageImpl();


    /**
     * If <code>true</code>, indicates that locally published messages are inhibited.
     */
    private boolean m_noLocal = false;


    private String connectionKey;

    /**
     * The maximum timeout to wait on a queue. The total timeout will be divided to smaller time
     * frames.
     */
    private long RECEIVE_TIME_FRAME = 3000;

    /**
     * The logger.
     */
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);


    /**
     * Constructs a message consumer.
     *
     * @param session         The m_session the consumer belongs to.
     * @param dest            The destination the consumer gets messages from.
     * @param subsriptionName The m_durableSubscriber subscription's name, if any.
     * @param selector        Selector string for filtering messages.
     * @throws IllegalStateException If the connection is broken.
     * @throws JMSException          If the creation fails for any other reason.
     */
    public GSMessageConsumerImpl(GSSessionImpl session,
                                 Destination dest,
                                 String consumerID,
                                 String subsriptionName,
                                 String selector,
                                 boolean noLocal)
            throws JMSException {
        if (session == null) {
            throw new IllegalArgumentException("Argument 'session' is null");
        }
        if (dest == null) {
            throw new InvalidDestinationException("Argument 'dest' is null");
        }

        m_session = session;
        m_dest = dest;
        m_destName = m_dest.toString();
        m_consumerID = consumerID;
        m_durableSubscriptionName = subsriptionName;
        m_selector = selector;
        m_space = m_session.getConn().getSpace();
        connectionKey = m_session.getConn().getCnxKey();
        m_noLocal = noLocal;

        initTemplates();

        try {
            m_space.snapshot(m_jmsMessageTemplate);
        } catch (RemoteException re) {
            JMSException e = new JMSException("RemoteException while space.snapshot(): ", re.toString());
            e.setLinkedException(re);
            throw e;
        } catch (Exception e) {
            JMSException jmse = new JMSException("Exception while space.snapshot(): ", e.toString());
            jmse.setLinkedException(e);
            throw jmse;
        }

        //If the destination is a topic, the consumer is a subscriber:
        if (m_dest instanceof GSTopicImpl) {
            m_isQueue = false;
            onMessageEventListener = new OnMessageEventListener();
            try {
                createEventSessionConfig();
            } catch (IOException e) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(toString2() + "IOException inside GSMessageConsumerImpl: Failed to create DataEventSession configuration", e);
                }
                JMSException e1 = new JMSException("Failed to create DataEventSession configuration: " + e.toString());
                e1.setLinkedException(e);
                throw e1;
            }
            registerToNotifications();
            //m_durableSubscriber = !StringsUtils.isEmpty(m_durableSubscriptionName);
//			if (m_durableSubscriber)
//			{
//			// deleted
//			}
        } else if (m_dest instanceof GSQueueImpl) {
            m_isQueue = true;
        }

        //If the destination is a temporary Queue
        if (m_dest instanceof GSTemporaryQueueImpl) {
            String tempQueueSrcID = ((GSTemporaryQueueImpl) m_dest).getSourceID();
            //we check that the temp Queue has the same src id as this consumer id,
            //otherwise  this temp queue was not created this consumer and cannot used by it.
            if (StringsUtils.isEmpty(tempQueueSrcID) || !tempQueueSrcID.equals(m_session.getSessionID()))
                throw new JMSSecurityException("Forbidden consumer with SessionID " + m_session.getSessionID() + " on this "
                        + "temporary queue with the session src id  " + tempQueueSrcID + " |  queueName: " + m_destName);
        }

        //If the destination is a temporary Topic
        else if (m_dest instanceof GSTemporaryTopicImpl) {
            String tempTopicSrcID = ((GSTemporaryTopicImpl) m_dest).getSourceID();
            //we check that the temp Topic has the same src id as this consumer id,
            //otherwise  this temp topic was not created this consumer and cannot used by it.
            if (StringsUtils.isEmpty(tempTopicSrcID) || !tempTopicSrcID.equals(m_session.getSessionID()))
                throw new JMSSecurityException("Forbidden consumer with SessionID " + m_session.getSessionID() + " on this "
                        + "temporary topic with the session src id  " + tempTopicSrcID + " |  topicName: " + m_destName);
            m_isQueue = false;
        }
    }


    /**
     * Constructs a message consumer
     *
     * @param session  The m_session the consumer belongs to.
     * @param dest  The destination the consumer gets messages from.
     * @param consumerID
     * @param selector Selector string for filtering messages.
     * @param pManager ParserManager used for the selector support
     * @exception InvalidSelectorException  If the m_selector syntax is invalid.
     * @exception IllegalStateException  If the connection is broken.
     * @exception JMSException  If the creation fails for any other reason.
     */
//	public GSMessageConsumerImpl(GSSessionImpl session, 
//			Destination dest,
//			String consumerID,
//			String selector,
//			boolean noLocal,
//			ParserManager pManager)
//	throws JMSException 
//	{
//		this(session, dest, consumerID, null, selector, noLocal, pManager);
//	}


    /**
     * Initializes the templates.
     */
    private void initTemplates() {
        m_jmsMessageTemplate = new GSMessageImpl();
        m_jmsMessageTemplate.setDestinationName(m_destName);
        m_jmsMessageTemplate.setProperties(null);

        //used as a template later for takeMultiple(), readMultiple() calls e.g. in setMessageListener()
//		m_batchOperTemplate = new ExternalEntry(m_destName, 
//		new Object[ GSMessageImpl.reservedNumOfValues ],
//		GSMessageImpl.m_ExtEntryFieldsNames);
//		m_batchOperTemplate.m_FieldsTypes = GSMessageImpl.m_ExtEntryFieldsTypes;
//		m_batchOperTemplate.setFifo( true );
//		m_batchOperTemplate.setNOWriteLeaseMode( true );
//		m_batchOperTemplate.setIndexIndicators( MessagePool.extEntryIndexes );
    }


    /**
     * Creates the DataEventSession configuration.
     */
    private void createEventSessionConfig() throws IOException {
        eventSessionConfig = new EventSessionConfig();
        eventSessionConfig.setFifo(true);
        eventSessionConfig.setReplicateNotifyTemplate(true);
        eventSessionConfig.setTriggerNotifyTemplate(false);
        //eventSessionConfig.setAutoRenew(renew, listener);
        //eventSessionConfig.setBatch(size, time);
    }


    /**
     * Returns the message template.
     *
     * @return the message template
     */
    GSMessageImpl getMessageTemplate() {
        return m_jmsMessageTemplate;
    }


    /**
     * Checks whether the message can be passed to the lsitener.
     *
     * @param message the message
     * @return true if the message can be passed to the listener
     */
    boolean checkMessageForConsumer(GSMessageImpl message) throws JMSException {
        // check expiration time
        if (!isValid(message)) {
            long ttl = message.getJMSExpiration();
            Time expTime = new Time(ttl);
            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "Not delivering JMS message < "
                        + message.JMSMessageID + " > due to expiration: " + expTime);
            }
            return false;
        }
        if (m_noLocal) {
            String prodConnection = message.getStringProperty(GSMessageImpl.JMS_GSCONNECTION_KEY_NAME);
            boolean pass = !connectionKey.equals(prodConnection);
            if (!pass && _logger.isDebugEnabled()) {
                _logger.debug(toString2() + "Not delivering JMS message < "
                        + message.JMSMessageID + " > due to noLocal attribute.");
            }
            return pass;
        }
        return true;
    }


    /**
     * Check the validity of the message.
     *
     * @param message The message
     * @return true if the message is valid
     */
    private boolean isValid(GSMessageImpl message) throws JMSException {
        long ttl = message.getJMSExpiration();
        return (ttl == Message.DEFAULT_TIME_TO_LIVE) ||
                (ttl - SystemTime.timeMillis() >= 0);
    }


    /**
     * Creates the NotifyDelegator to get notifications for messages.
     */
    synchronized void registerToNotifications() throws JMSException {
        if (m_closed) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "GSMessageConsumerImpl.registerToNotifications(): Called on a closed consumer.");
            }
            return;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug(toString2() + "GSMessageConsumerImpl.registerToNotifications(): registering for notifications");
        }

        try {
            // currently we don't use transactions with notify delegators
            eventSession = DataEventSessionFactory.create(m_space, eventSessionConfig);
            eventSession.addListener(m_jmsMessageTemplate, onMessageEventListener, NotifyActionType.NOTIFY_WRITE);
        } catch (IOException e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "GSMessageConsumerImpl.startAsynchronous(): IOException while creating event session" + e);
            }
            JMSException e1 = new JMSException("Failed to create event session.");
            e1.setLinkedException(e);
            throw e1;

        }
    }


    /**
     * Closes the NotifyDelegator so no more notifications will arrive.
     */
    synchronized void unregisterToNotifications() throws JMSException {
        if (eventSession == null) {
            return;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug(toString2() + "GSMessageConsumerImpl.unregisterToNotifications(): unregistering from notifications");
        }

        try {
            eventSession.close();
        } catch (IOException e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "GSMessageConsumerImpl.stopAsynchronous(): IOException while closing event session" + e);
            }
            JMSException e1 = new JMSException("Failed to close event session.");
            e1.setLinkedException(e);
            throw e1;
        } catch (UnknownLeaseException e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "GSMessageConsumerImpl.stopAsynchronous(): UnknownLeaseException while closing event session" + e);
            }
            JMSException e1 = new JMSException("Failed to close event session.");
            e1.setLinkedException(e);
            throw e1;
        } catch (Exception e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "GSMessageConsumerImpl.stopAsynchronous(): UnknownLeaseException while closing event session" + e);
            }
            JMSException e1 = new JMSException("Failed to close event session.");
            e1.setLinkedException(e);
            throw e1;
        } finally {
            eventSession = null;
        }
    }


    /**
     * API method.
     *
     * @throws IllegalStateException If the consumer is closed.
     */
    public String getMessageSelector()
            throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed consumer.");
        }

        return this.m_selector;
    }

    /**
     * API method.
     *
     * @return m_messageListener
     * @throws IllegalStateException If the consumer is closed.
     */
    public MessageListener getMessageListener()
            throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed consumer.");
        }

        return this.m_messageListener;
    }


    private void prepareMessageForConsumer(GSMessageImpl message)
            throws JMSException {
        message.setSession(m_session);
        message.setBodyReadOnly(true);
        message.setPropertiesReadOnly(true);

        // decompress if needed
        if (message instanceof TextMessage &&
                message.Body != null &&
                message.Body instanceof MarshObject) {
            if (_logger.isTraceEnabled()) {
                _logger.trace(toString2() +
                        "Decompressing message: " + message.JMSMessageID);
            }
            try {
                Object decompressed = m_session.decompressObject((MarshObject) message.Body);
                message.Body = decompressed.toString();
            } catch (IOException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(toString2()
                            + "IOException while decompressing message: "
                            + message.JMSMessageID);
                }
                JMSException e1 = new JMSException(
                        "Failed to decompress message: " + message.JMSMessageID);
                e1.setLinkedException(e);
                throw e1;
            } catch (ClassNotFoundException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(toString2() +
                            "ClassNotFoundException while decompressing message: "
                            + message.JMSMessageID);
                }
                JMSException e1 = new JMSException("Failed to decompress message: "
                        + message.JMSMessageID);
                e1.setLinkedException(e);
                throw e1;
            }
        }
    }


    /**
     * This method is called by the JMS Client, it sets the MessageListener implementation and It
     * creates a GigaSpaces NotifyDelegator for Asynchronic messaging. It also supports the case of
     * a Durable Subscriber, in which accumulated messages in the GigaSpaces space while it was not
     * up and listening. It performs a m_space.readMultiple() which gets all the pending entries, it
     * calls then the MessageListener.onMessage(), same as the notify() does, but it also keeps the
     * entries in the pendingEntriesHash so the notify() will check and wont execute duplicated
     * entries.
     *
     * This method must NOT be called if the connection the consumer belongs to is started, because
     * the session would then be accessed by the thread calling this method and by the thread
     * controlling asynchronous deliveries. This situation is clearly forbidden by the single
     * threaded nature of sessions. Moreover, unsetting a message listener without stopping the
     * connection may lead to the situation where asynchronous deliveries would arrive on the
     * connection, the session or the consumer without being able to reach their target listener!
     *
     * @throws IllegalStateException If the consumer is closed.
     * @see MessageConsumer#setMessageListener(MessageListener)
     **/
    public synchronized void setMessageListener(MessageListener listener)
            throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("GSMessageConsumerImpl.setMessageListener() -- Forbidden call on a closed consumer.");
        }

        innerSetMessageListener(listener);
    }


    private synchronized void innerSetMessageListener(MessageListener listener) {
        if (_logger.isDebugEnabled()) {
            _logger.debug(toString2() + "Setting MessageListener: " + listener);
        }

        if (listener != null) {
            if (m_messageListener == null) {
                m_messageListener = listener;
                m_session.m_msgListeners.increment();

                // starting the poller thread
                asyncPoller = new AsyncPoller(m_consumerID);
                asyncPoller.start();
            } else {
                // just switch the listeners
                m_messageListener = listener;
            }
        } else {
            if (m_messageListener != null) {
                synchronized (m_session.stopMonitor) {
                    // stopping the poller thread
                    if (asyncPoller != null) {
                        asyncPoller.setShutDown();
                        notifyStop();
                        if (m_session.m_stopped) {
                            m_session.stopMonitor.notifyAll();
                        }
                    }
                }
                // wait for the poller thread to end
                if (asyncPoller != null) {
                    try {
                        asyncPoller.join(60000);
                    } catch (InterruptedException e) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(toString2() +
                                    "Failed to wait for shutdown of thread " + asyncPoller.getName() + ".\n" + e);
                        }
                    }
                    asyncPoller = null;
                }
                m_messageListener = listener;
                m_session.m_msgListeners.decrement();
            }
        }
    }


    /**
     * API method. Using default readTimeout which is  Long.MAX_VALUE. This call blocks until a
     * message arrives, the timeout expires, or this message consumer is m_closed.
     *
     * @throws IllegalStateException If the consumer is closed, or if the connection is broken.
     * @throws JMSSecurityException  If the requester is not a READER on the destination.
     * @throws JMSException          If the request fails for any other reason.
     */
    public Message receive() throws JMSException {
        return receive(Message.DEFAULT_TIME_TO_LIVE); //0L
    }


    /**
     * API method. It is a Synch receive implementation which reads ExternalEntry from space, then
     * it checks if the ExternalEntry is still valid and did not expired. In case of a CLIENT_ACK
     * then we prepare a JMSAckDataEntry to be later written to space, otherwise (if ACK is not
     * necessary) if it is a Queue, we perform a space.clear() operation, which clears the entry
     * from space, but if it is a Topic we leave it as is.
     *
     * @param timeout This call blocks until a message arrives, the timeout expires, or this message
     *                consumer is closed. The jms client may select A <CODE>timeout</CODE> of zero,
     *                that means it never expires, and the call blocks indefinitely. It is the same
     *                as the Space timeout which its value to  Long.MAX_VALUE Regarding the space
     *                timeout, it means how long the client is willing to wait for a transactionally
     *                proper matching entry. A timeout of <code>IJSpace.NO_WAIT</code> means to wait
     *                no time at all; this is equivalent to a wait of zero (in space only, not in
     *                jms).
     * @throws IllegalStateException If the consumer is closed, or if the connection is broken.
     * @throws JMSSecurityException  If the requester is not a READER on the destination.
     * @throws JMSException          If the request fails for any other reason.
     */

    public Message receive(long timeout) throws JMSException {
        if (m_closed) {
            // FIX for CTS tests: not throwing IllegalStateException
            return null;
        }

        // make sure the session is not in asynchronous mode
        if (m_session.m_msgListeners.getValue() != 0) {
            throw new IllegalStateException("Forbidden call to receive() on asynchronous session.");
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug(toString2() + "GSMessageConsumerImpl.receive()");
        }

        // Message.DEFAULT_TIME_TO_LIVE = 0L
        if (timeout == Message.DEFAULT_TIME_TO_LIVE) {
            timeout = Lease.FOREVER;
        }

        long startTime = 0;
        GSMessageImpl message = null;

        while (!m_closed && timeout > 10) {
            try {
                startTime = SystemTime.timeMillis();

                // wait in case the session is stopped
                synchronized (m_session.stopMonitor) {
                    if (m_session.m_stopped) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(toString2() +
                                    "receive(): Waiting on a stopped session.");
                        }

                        // wait for session.start()
                        try {
                            m_session.stopMonitor.wait(timeout);
                        } catch (InterruptedException e) {
                            if (_logger.isErrorEnabled()) {
                                _logger.error(toString2() +
                                        "receive(): Interrupted while waiting on a stopped session: " + e);
                            }
                        }

                        if (_logger.isDebugEnabled()) {
                            _logger.debug(toString2() +
                                    "receive(): Stopped waiting on a stopped session - restarting iteration.");
                        }

                        // timeout is updated in finally block!!
                        continue;
                    }

                    if (_logger.isTraceEnabled()) {
                        _logger.trace(toString2() + "onProcess=true");
                    }
                    m_session.onProcess = true;
                }

                //////////////////////
                // get the message
                //////////////////////
                if (m_isQueue) {
                    if (_logger.isTraceEnabled()) {
                        _logger.trace(toString2() +
                                "Retrieving a message from queue.");
                    }
                    message = receiveFromQueueNoBlock(timeout);
                } else {
                    if (_logger.isTraceEnabled()) {
                        _logger.trace(toString2() +
                                "Retrieving a message from topic.");
                    }
                    message = receiveFromTopic(timeout);
                }

                // if the message is not here it is possible that
                // 1. the consumer is closed
                // 2. the session is stopped
                // 3. timed out
                if (message == null) {
                    // timeout is updated in finally block!!
                    continue;
                }

                if (_logger.isDebugEnabled()) {
                    _logger.debug(toString2() +
                            "receive(): Got message: " + message.JMSMessageID);
                }

                // check message validity
                if (checkMessageForConsumer(message)) {
                    prepareMessageForConsumer(message);

                    // if AUTO/DUP ack is sent right away.
                    if (m_session.isAutoAck()) {
                        // We acknowledge here only in case of AUTO or DUP_OK.
                        // An exception can be thrown only for Queue consumer.
                        // In case of failover we need to rollback the transaction
                        // and create a new one. To make this transparent to the
                        // client we don't throw an exception, but continuing
                        // the loop to try again.
                        m_session.acknowledge();

                        // we renew the transaction for queue
                        if (m_isQueue && !m_session.isAutoAck()) {
                            m_session.renewTransaction();
                        }
                    } else {
                        m_session.addUnackedMessage(message, m_consumerID);
                    }

                    m_session.m_numOfConsumedMsg++;

                    break;
                } else {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(toString2() +
                                "Message not valid for consumer: " + message.JMSMessageID);
                    }
                    // TODO: send to dead letter queue...
                }
            } // try
            catch (ReceiveFromQueueException e) {
                // cause by the following exceptions:
                //		TransactionException
                // 		RemoteException
                // 		UnusableEntryException
                // 		InternalSpaceException
                // 		InterruptedException

                // Failover handling!
                // something happened while we tried to take from space.
                // Note: In QUEUE consumption we use local transactions.
                String text = "Internal error while fetching a message from a Queue. ";
                if (_logger.isErrorEnabled()) {
                    _logger.error(toString2() + text + e.orig);
                }

                //m_session.getConn().onException(new JMSException("Space Lost"));

                //if (m_session.m_acknowledgeMode == Session.AUTO_ACKNOWLEDGE ||
                //	m_session.m_acknowledgeMode == Session.DUPS_OK_ACKNOWLEDGE)
                if (m_session.isAutoAck()) {
                    // In this case, each take is in it's own transaction.
                    // Therefore if the transaction is lost, we just create
                    // a new transaction and try again (new iteration).
                    // We don't throw JMSException to the client - to make
                    // it transaparent to the user.
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(
                                toString2() + "receive(): Session's ack mode is AUTO_ACKNOWLEDGE" +
                                        " or DUPS_OK_ACKNOWLEDGE.\nRenewing transaction.");
                    }
//					try
//					{
//						m_session.renewTransaction();
//					}
//					catch (TransactionCreateException e1)
//					{
//						if( _logger.isErrorEnabled())
//						{
//							_logger.error(
//									toString2()+"receive(): Failed to renew transaction."+e1.orig);
//						}
//						// TODO: how do we handle failure to create a new
//						// local transaction? Maybe throwing a JMSException?
//					}
                } else if (m_session.m_acknowledgeMode == Session.SESSION_TRANSACTED) {
                    // In this case we have to rollback the transaction and start a
                    // new transaction. This will dispose the produced messages as well.
                    // We throw a TransactionRolledBackException (which extends JMSException)
                    // to notify the user that the transaction is canceled.
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(
                                toString2() + "receive(): Session's ack mode is SESSION_TRANSACTED.\n" +
                                        "Rolling back transaction " + m_session.getTransaction());
                    }
                    try {
                        m_session.rollback();
                    } catch (JMSException e1) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    toString2() + "receive(): Failed to roll back transaction");
                        }
                    }

//					String exText = text+"The session's ack mode is SESSION_TRANSACTED. " +
//							"The transaction was rolled back.";
//					m_session.getConn().onException(new SpaceLostException(exText, e.orig));

                    TransactionRolledBackException re = new TransactionRolledBackException(
                            text + "Transaction rolled back.");
                    re.setLinkedException(e.orig);
                    throw re;
                } else if (m_session.m_acknowledgeMode == Session.CLIENT_ACKNOWLEDGE) {
                    // We implement CLIENT_ACK with transactions. When the user acks
                    // for the consumed messages we commit the local transaction.
                    // On space failure the transaction is canceled and therefore it
                    // is impossible to perform the ack.
                    // In this case we have to recover the unacked messages. With QUEUE
                    // this is done by aborting the local transaction. We need to notify
                    // the client about it, so we throw TransactionRolledBackException.
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(
                                toString2() + "receive(): Session's ack mode is CLIENT_ACKNOWLEDGE.\n" +
                                        "Recovering messages of transaction " + m_session.getTransaction());
                    }
                    try {
                        m_session.recoverMessages();
                    } catch (RollbackFailedException e1) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    toString2() + "receive(): Failed to recover messages of transaction " +
                                            m_session.getTransaction() + e1.orig);
                        }
                    }
                    try {
                        m_session.renewTransaction();
                    } catch (TransactionCreateException e1) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    toString2() + "receive(): Failed to renew transaction", e1.orig);
                        }
                    }

//					String exText = text+"The session's ack mode is CLIENT_ACKNOWLEDGE. " +
//							"The transaction was rolled back.";
//					m_session.getConn().onException(new SpaceLostException(exText, e.orig));

                    TransactionRolledBackException re = new TransactionRolledBackException(
                            text + "Transaction rolled back.");
                    re.setLinkedException(e.orig);
                    throw re;
                }
            } catch (CommitFailedException e) {
                // cause by the following exceptions:
                //		UnknownTransactionException
                // 		RemoteException
                // 		CannotCommitException

                // Failover handling!
                // Only with QUEUE and only in auto/dup_ok we try to commit.
                // To make it transparent to the client, we don't throw an exception
                // but try to receive a message again in a new transaction.
                String text = "Internal error during auto commit. Message="
                        + message.JMSMessageID + ", Txn=" + m_session.getTransaction();
                if (_logger.isErrorEnabled()) {
                    _logger.error(toString2() + text + e.orig);
                }

                try {
                    m_session.renewTransaction();
                } catch (TransactionCreateException e1) {
                    if (_logger.isErrorEnabled()) {
                        _logger.error(
                                toString2() + "receive(): Failed to renew transaction: " + e1.orig);
                    }
                    // TODO: how do we handle failure to create a new
                    // local transaction? Maybe throwing a JMSException?
                }
            } catch (TransactionCreateException e) {
                // Only in QUEUE, happens if we fail to renew the transaction
                // after a successfull acknowledge.
                if (_logger.isErrorEnabled()) {
                    _logger.error(
                            toString2() + "receive(): Failed to renew transaction: " + e.orig);
                }
                break;
                // TODO: how do we handle failure to create a new
                // local transaction? Kill the thread?
            } finally {
                long now = SystemTime.timeMillis();
                timeout = timeout - (now - startTime);
                synchronized (m_session.stopMonitor) {
                    // notify that the processing is finished.
                    // onProcess=false means that we weren't
                    // in the middle of processing a message.
                    if (m_session.onProcess) {
                        if (_logger.isTraceEnabled()) {
                            _logger.trace(toString2() + "onProcess=false");
                        }
                        m_session.onProcess = false;
                        m_session.stopMonitor.notifyAll();
                    }
                }
            }
        } // end of while

        return (Message) message;
    }


    /**
     * API method.
     *
     * @throws IllegalStateException If the consumer is closed, or if the connection is broken.
     * @throws JMSSecurityException  If the requester is not a READER on the destination.
     * @throws JMSException          If the request fails for any other reason.
     */
    public Message receiveNoWait()
            throws JMSException {
        return receive(RECEIVE_NO_WAIT_TIMEOUT);
    }


    /**
     * Determines if the consumer is closed
     *
     * @return <code>true</code> if the consumer is closed
     */
    public boolean isClosed() {
        return m_closed;
    }


    /**
     * Cancels the onMessage notify delegator, using its Lease.cancel() if it is an Asyc operation
     * Also it removes the current consumer from the consumers list held by the session.
     *
     * @see MessageConsumer#close()
     */

    public synchronized void close() throws JMSException {
        // ignore if the consumer is already closed
        if (m_closed) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "GSMessageConsumerImpl.close(): Consumer already closed.");
            }
            return;
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug(toString2() + "GSMessageConsumerImpl.close(): Closing consumer.");
        }


        m_session.removeConsumer(this);


        // only topics use notifications - unregister
        if (!m_isQueue) {
            unregisterToNotifications();
        }


        // in case we are onProcess we need to block.
        synchronized (m_session.stopMonitor) {
            // mark the consumer as closed
            m_closed = true;

            // wait for the process to finish
            while (m_session.onProcess) {
                try {
                    this.notifyStop();
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(toString2() + "GSMessageConsumerImpl.close(): Waiting for process to finish.");
                    }
                    m_session.stopMonitor.wait();
                } catch (InterruptedException e) {
                    if (_logger.isErrorEnabled()) {
                        _logger.error(toString2() + "GSMessageConsumerImpl.close(): InterruptedException while waiting for process to finish.");
                    }
                }
            }

            if (_logger.isDebugEnabled()) {
                _logger.debug(toString2() + "GSMessageConsumerImpl.close(): No message on process.");
            }

            // if the session is stopped:
            // Async: the notify thread may be waiting. release it.
            // Sync: The receive() thread may be waiting. release it.
            if (m_session.m_stopped) {
                m_session.stopMonitor.notifyAll();
            }
        }

        // At this point, no consumer is in the middle of onMessage or receive.

        // this also interrupts and kills the polling thread
        if (m_messageListener != null) {
            innerSetMessageListener(null);
        }

        // topic - release the notification thread
        // not the receive thread because we are no longer in onProcess.
        if (!m_isQueue) {
            synchronized (synchTopicNotifyLock) {
                synchTopicNotifyLock.notify();
            }
        }
    }


    public String toString() {
        return "MessageConsumer | Consumer ID: " + m_consumerID + " | Session ID: " + m_session.getSessionID();
    }

    private String toString2() {
        return "MessageConsumer[" + m_consumerID + "], Listener=" + m_messageListener + ": ";
    }


    /**
     * Returns the consumer ID.
     *
     * @return The consumer ID
     */
    public String getConsumerID() {
        return m_consumerID;
    }


    /**
     * Returns the m_session that created this consumer. It might be a GSTopicSessionImpl or
     * GSQueueSessionImpl
     *
     * @return the m_session that created this consumer
     */
    protected GSSessionImpl getSession() {
        return m_session;
    }

    /**
     * Retrieves a message from the topic. Recovered messages are returned first.
     *
     * @param timeout The timeout
     * @return The next message for this consumer.
     */
    private GSMessageImpl receiveFromTopic(long timeout)// throws InterruptedException
    //throws ReceiveFromTopicException
    {
        if (_logger.isDebugEnabled()) {
            _logger.debug("receiveFromTopic(): Receiving from Topic");
        }

        // try to get a message from the recovered messages list
        GSMessageImpl message = m_session.getNextRecoveredMessage();
        if (message != null) {
            if (_logger.isTraceEnabled()) {
                _logger.trace(toString2() +
                        "receiveFromTopic(): Recovered message found: " + message.JMSMessageID);
            }

            // recovered messages are marked as redelivered
            message.JMSRedelivered = Boolean.TRUE;
            return message;
        }

        long now;
        long startTime = SystemTime.timeMillis();

        synchronized (synchTopicNotifyLock) {
            while (currentMessage == null && timeout > 0) {
                try {
                    synchTopicNotifyLock.wait(timeout);
                } catch (InterruptedException e) {
                    if (_logger.isErrorEnabled()) {
                        _logger.error(toString2() +
                                "receiveFromTopic(): InterruptedException while waiting " +
                                "for a message from a topic: " + e);
                    }
                }
                now = SystemTime.timeMillis();
                timeout = timeout - (now - startTime);
                startTime = now;
            }

            if (currentMessage != null) {
                // if the session is stopped, we might have added
                // the wakeup object to wake up the waiting thread.
                // in this case we return null.
                message = currentMessage != topicWakeupObject ? currentMessage : null;
                currentMessage = null;
                synchTopicNotifyLock.notify();
                return message;
            }

            // else - timeout

            return null;
        }
    }


    /**
     * Called when we stop the consumer to wake up a control thread that might be waiting for a
     * message.
     */
    void notifyStop() {
        // only for topics
        if (!m_isQueue) {
            // this code can't run when a notification is waiting.
            // in this case we do nothing.
            // notification can overwrite the wakeup object but we don't care.
            synchronized (synchTopicNotifyLock) {
                if (currentMessage == null) // notification is not waiting
                {
                    currentMessage = this.topicWakeupObject;
                    synchTopicNotifyLock.notify();
                }
            }
        }
    }


    /**
     * Retrieves a message from the queue.
     *
     * @param timeout The time out.
     * @return The next message for this consumer.
     */
    private GSMessageImpl receiveFromQueue(long timeout)
            throws ReceiveFromQueueException
    //throws RemoteException, UnusableEntryException,
    //	TransactionException, InterruptedException
    {
        // get message from space
        Transaction txn = m_session.getTransaction();
        if (_logger.isDebugEnabled()) {
            _logger.debug("receiveFromQueue(): Receiving from Queue, txn=" + txn);
        }
        try {
            return (GSMessageImpl) m_space.take(m_jmsMessageTemplate, txn, timeout);
        } catch (Exception e) {
            throw new ReceiveFromQueueException(e);
        }

    }


    /**
     * Receives a message from a queue, breaking the timeout into smaller time frames, to prevent
     * potential eternal blocking.
     *
     * @return The next message.
     */
    private GSMessageImpl receiveFromQueueNoBlock(long timeout)
            throws ReceiveFromQueueException
//		throws RemoteException, UnusableEntryException,
//					TransactionException, InterruptedException
    {
        GSMessageImpl message;
        long now;
        long timeToWait;
        long startTime = SystemTime.timeMillis();
        if (_logger.isTraceEnabled()) {
            _logger.trace(toString2() + "timeout=" + timeout);
        }
        while (!m_closed && !m_session.m_stopped && timeout > 10) {
            timeToWait = Math.min(timeout, RECEIVE_TIME_FRAME);
            if (_logger.isTraceEnabled()) {
                _logger.trace(toString2() + "Next timeToWait=" + timeToWait);
            }
            message = receiveFromQueue(timeToWait);
            if (message != null) {
                if (_logger.isTraceEnabled()) {
                    _logger.trace(toString2() + "Got message: " + message);
                }
                return message;
            }
            now = SystemTime.timeMillis();
            timeout = timeout - (now - startTime);
            startTime = now;
        }
        if (_logger.isTraceEnabled()) {
            _logger.trace(toString2() + "Exiting: m_closed=" +
                    m_closed + ", m_stopped=" + m_session.m_stopped + ", timeout=" + timeout);
        }
        return null;
    }


    /**
     * Reacts to message notification in asynchronous delivery.
     *
     * @author shaiw
     */
    class OnMessageEventListener implements RemoteEventListener {
        public OnMessageEventListener() {
        }

        public void notify(RemoteEvent theEvent)
                throws UnknownEventException, RemoteException {
            GSMessageImpl message;
            try {
                message = (GSMessageImpl) ((EntryArrivedRemoteEvent) theEvent).getObject();

                // the check is here to log the message
                if (!m_closed) {
                    messageArrived(message);
                } else {
                    if (_logger.isWarnEnabled()) {
                        _logger.warn(toString2() + "OnMessageEventListener.notify():" +
                                " Called on a closed consumer." + message);
                    }
                }
            } catch (UnusableEntryException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(toString2() + "OnMessageEventListener.notify():" +
                            " UnusableEntryException while extracting ExternalEntry: " + e);
                }
                //TODO: what to do with the message?
            }
        }


        /**
         * Make the next arrived message available to the topic consumer. The thread waits until the
         * message is taken.
         *
         * @param message The next message.
         */
        private void messageArrived(GSMessageImpl message) {
            if (_logger.isTraceEnabled()) {
                _logger.trace(toString2() +
                        "messageArrived(): " + message.JMSMessageID);
            }
            synchronized (synchTopicNotifyLock) {
                // in case there was an exception in previous notify thread
                // there maight still be a waiting message here. we need to
                // wait for the consumer to take it or we may lose it.
                while (currentMessage != null) {
                    if (_logger.isTraceEnabled()) {
                        _logger.trace(toString2() +
                                "messageArrived(): A message is already pending: " +
                                currentMessage.JMSMessageID);
                    }
                    try {
                        synchTopicNotifyLock.wait();
                    } catch (InterruptedException e) {
                        if (m_session.isLocalConsumer(m_consumerID)) {
                            if (_logger.isErrorEnabled()) {
                                _logger.error(toString2() +
                                        "messageArrived(): The notification thread was interrupted. Notification message: " +
                                        message.JMSMessageID);
                            }
                        } else {
                            if (_logger.isInfoEnabled()) {
                                _logger.info(toString2() +
                                        "messageArrived(): The notification thread is exiting. Notification message: " +
                                        message.JMSMessageID);
                            }
                        }
                    }
                }
                currentMessage = message;
                synchTopicNotifyLock.notify();
                while (!m_closed && currentMessage != null) {
                    try {
                        synchTopicNotifyLock.wait();
                    } catch (InterruptedException e) {
                        if (m_session.isLocalConsumer(m_consumerID)) {
                            if (_logger.isErrorEnabled()) {
                                _logger.error(toString2() +
                                        "messageArrived(): The notification thread was interrupted while waiting to be released: " +
                                        currentMessage.JMSMessageID);
                            }
                        } else {
                            if (_logger.isInfoEnabled()) {
                                _logger.info(toString2() +
                                        "messageArrived(): The notification thread is exiting. The consumer is closed. Pending message: " +
                                        currentMessage.JMSMessageID);
                            }
                        }
                    }
                }
            }
        }

    } // end of class OnMessageEventListener


    /**
     * Return the durable subscription name
     *
     * @return the durable subscription name, or <code>null</code> if this is a non-durable
     * subscriber
     */
    public String getName() {
        return this.m_durableSubscriptionName;
    }

    /**
     * Determines if the subscriber is durable
     *
     * @return <code>true</code> if the subscriber is durable
     */
    public boolean isDurableSubscriber() {
        return !StringsUtils.isEmpty(m_durableSubscriptionName);
    }


    /**
     * @see QueueReceiver#getQueue()
     */
    public Queue getQueue() throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed consumer.");
        }

        if (m_dest == null) {
            return null;
        }

        if (m_dest instanceof Queue) {
            return (Queue) m_dest;
        }

        throw new JMSException("The destination type of this producer is not a queue.");
    }


    /**
     * @see TopicSubscriber#getNoLocal()
     */
    public boolean getNoLocal() throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed consumer.");
        }

        return m_noLocal;
    }


    /**
     * @see TopicSubscriber#getTopic()
     */
    public Topic getTopic() throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed consumer.");
        }

        if (m_dest == null) {
            return null;
        }

        if (m_dest instanceof Topic) {
            return (Topic) m_dest;
        }

        throw new JMSException("The destination type of this producer is not a topic.");
    }

    public Destination getDestination() throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed consumer.");
        }

        return m_dest;
    }


    /**
     * Used in asynchronous queue consumption.
     *
     * @author shaiw
     */
    private class AsyncPoller extends GSThread {
        boolean shutdown = false;

        public AsyncPoller(String consumerID) {
            super(consumerID + "_JMSAsyncPoller");
        }

        public void setShutDown() {
            shutdown = true;
        }

        @Override
        public void run() {
            GSMessageImpl message = null;
            while (!shutdown && !m_closed && m_messageListener != null) {
                try {
                    // wait in case the session is stopped
                    synchronized (m_session.stopMonitor) {
                        if (m_session.m_stopped) {
                            if (_logger.isDebugEnabled()) {
                                _logger.debug(getName() +
                                        ": Waiting on a stopped session.");
                            }

                            // wait for session.start()
                            try {
                                m_session.stopMonitor.wait();
                            } catch (InterruptedException e) {
                                if (_logger.isErrorEnabled()) {
                                    _logger.error(getName() +
                                            ": Interrupted while waiting on a stopped session: " + e);
                                }
                            }

                            if (_logger.isDebugEnabled()) {
                                _logger.debug(getName() +
                                        ": Stopped waiting on a stopped session - restarting iteration.");
                            }

                            continue;
                        }

                        if (_logger.isTraceEnabled()) {
                            _logger.trace(getName() + ": onProcess=true");
                        }
                        m_session.onProcess = true;
                    }

                    // at this point onProcess=true
                    // get the message
                    if (m_isQueue) {
                        message = receiveFromQueue(RECEIVE_TIME_FRAME);
                    } else {
                        message = receiveFromTopic(Lease.FOREVER);
                    }

                    if (message == null) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(getName() +
                                    ": No message was received.");
                        }
                        continue;
                    }

                    if (_logger.isDebugEnabled()) {
                        _logger.debug(getName() +
                                ": Received message: " + message.JMSMessageID);
                    }


                    // check if the message is valid
                    if (!checkMessageForConsumer(message)) {
                        continue;
                    }

                    // may throw JMSException
                    prepareMessageForConsumer(message);

                    // add the message to the unacked messages list
                    // so it will be there in case of recovery.
                    m_session.addUnackedMessage(message, m_consumerID);

                    // deliver the message to the listener
                    m_session.m_numOfConsumedMsg++;
                    m_messageListener.onMessage((Message) message);

                    // if this is auto/dup_ok mode than ack the message
                    //if (m_session.m_acknowledgeMode == Session.AUTO_ACKNOWLEDGE ||
                    //	m_session.m_acknowledgeMode == Session.DUPS_OK_ACKNOWLEDGE)
                    if (m_session.isAutoAck()) {
                        m_session.acknowledge();
                        // we passed the ack with no problem

                        // we renew the transaction for queue
                        if (m_isQueue && !m_session.isAutoAck()) {
                            m_session.renewTransaction();
                        }
                    }
                } catch (JMSException e) {
                    // this exception is thrown only when processing
                    // a message before onMessage
                    if (_logger.isErrorEnabled()) {
                        _logger.error(getName() +
                                ": JMSException while handling message: " + message, e);
                    }
                } catch (ReceiveFromQueueException e) {
                    // cause by the following exceptions:
                    //		TransactionException
                    // 		RemoteException
                    // 		UnusableEntryException
                    // 		InternalSpaceException
                    // 		InterruptedException

                    // Failover handling!
                    // something happened while we tried to take from space.
                    // Note: In QUEUE consumption we use local transactions.
                    String text = "Internal error while fetching a message from a Queue -" +
                            " The space might be lost.\n";
                    if (_logger.isErrorEnabled()) {
                        _logger.error(getName() + ": " + text, e.orig);
                    }

                    if (m_session.isAutoAck()) {
                        // In this case we don't use a transaction.
                        // So we just continue to the next receive.
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(
                                    getName() + ": The session's acknowledge mode is" +
                                            " AUTO_ACKNOWLEDGE or DUPS_OK_ACKNOWLEDGE.");
                        }
                    } else if (m_session.m_acknowledgeMode == Session.SESSION_TRANSACTED) {
                        // In this case we have to rollback the transaction and start a
                        // new transaction. This will dispose the produced messages as well.
                        // To notify the client that the transaction is aborted we pass a
                        // a SpaceLostException to the Connection's ExceptionListener.
                        if (_logger.isInfoEnabled()) {
                            _logger.info(
                                    getName() + ": The session's acknowledge mode is" +
                                            " SESSION_TRANSACTED.\n" +
                                            "Rolling back transaction " + m_session.getTransaction());
                        }
                        try {
                            m_session.rollback();
                        } catch (JMSException e1) {
                            if (_logger.isDebugEnabled()) {
                                _logger.debug(
                                        getName() + ": Failed to roll back transaction: " +
                                                m_session.getTransaction(), e1.getLinkedException());
                            }
                        }

                        String exText = text + "The session's" +
                                " acknowledge mode is SESSION_TRANSACTED. " +
                                "The transaction was rolled back.";
                        m_session.getConn().onException(new SpaceLostException(exText, e.orig, m_session));
                    } else if (m_session.m_acknowledgeMode == Session.CLIENT_ACKNOWLEDGE) {
                        // Caused by one of the following:
                        // 1. RemoteException
                        // 2. UnusableEntryException
                        // 3. InternalSpaceException
                        // 4. InterruptedException

                        // Failover handling!
                        // in this case we have to roll back the local transaction.
                        // in other words, we have to recover the messages.
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(
                                    getName() + ": The session's acknowledge mode is CLIENT_ACKNOWLEDGE.\n" +
                                            "Recovering messages of transaction " + m_session.getTransaction());
                        }
                        try {
                            m_session.recoverMessages();
                        } catch (RollbackFailedException e1) {
                            if (_logger.isDebugEnabled()) {
                                _logger.debug(
                                        getName() + ": Failed to recover messages of transaction " +
                                                m_session.getTransaction(), e1.orig);
                            }
                        }
                        try {
                            m_session.renewTransaction();
                        } catch (TransactionCreateException e1) {
                            if (_logger.isErrorEnabled()) {
                                _logger.error(
                                        getName() + ": Failed to renew transaction", e1.orig);
                            }
                            // TODO: how do we handle failure to create a new
                            // local transaction? Kill the thread?
                        }

                        String exText = text + "The session's" +
                                " acknowledge mode is CLIENT_ACKNOWLEDGE. " +
                                "Unacknowledged messages are recovered.";
                        m_session.getConn().onException(new SpaceLostException(exText, e.orig, m_session));
                    }
                } catch (CommitFailedException e) {
                    // Caused by one of the following:
                    // 1. UnknownTransactionException
                    // 2. CannotCommitException
                    // 3. RemoteException

                    // Failover handling!
                    // Only in QUEUE and only in auto/dup_ok we try to commit
                    String text = ": Internal error during commit. Message="
                            + message.JMSMessageID + ", Txn=" + m_session.getTransaction();
                    if (_logger.isErrorEnabled()) {
                        _logger.error(getName() + text + e.orig);
                    }

                    try {
                        m_session.renewTransaction();
                    } catch (TransactionCreateException e1) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    getName() + ": Failed to renew transaction: " + e1.orig);
                        }
                        // TODO: how do we handle failure to create a new
                        // local transaction? Kill the thread?
                    }
                } catch (TransactionCreateException e) {
                    // Only in QUEUE, happens if we fail to renew the transaction
                    // after a successfull iteration.
                    if (_logger.isErrorEnabled()) {
                        _logger.error(
                                getName() + ": Failed to renew transaction: " + e.orig);
                    }
                    // TODO: how do we handle failure to create a new
                    // local transaction? Kill the thread?
                } catch (RuntimeException e) {
                    // Runtime exceptions not caught by the client!!
                    // The behavior of handling exceptions during MessageListener.onMessage()
                    // is described in sections 4.4.12 and 4.5.2 in the spec.
                    if (_logger.isErrorEnabled()) {
                        _logger.error(getName() +
                                ": RuntimeException not caught by client during asynchronous delivery: " + e + message);
                    }

                    //if (m_session.m_acknowledgeMode == Session.AUTO_ACKNOWLEDGE ||
                    //	m_session.m_acknowledgeMode == Session.DUPS_OK_ACKNOWLEDGE)
                    if (m_session.isAutoAck()) {
                        try {
                            m_session.recoverMessages();
                        } catch (RollbackFailedException e1) {
                            if (_logger.isErrorEnabled()) {
                                _logger.error(
                                        getName() + ": Failed to roll back transaction: " +
                                                m_session.getTransaction(), e1.orig);
                            }
                        } finally {
                            if (m_isQueue) {
                                try {
                                    m_session.renewTransaction();
                                } catch (TransactionCreateException e1) {
                                    if (_logger.isErrorEnabled()) {
                                        _logger.error(getName() +
                                                ": Failed to renew transaction: " + e1.orig);
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // catch any other exception
                    if (_logger.isErrorEnabled()) {
                        _logger.error(getName() +
                                ": Exception during asynchronous message delivery: " + e);
                    }
                } finally {
                    synchronized (m_session.stopMonitor) {
                        if (m_session.onProcess) {
                            if (_logger.isTraceEnabled()) {
                                _logger.trace(getName() + ": onProcess=false");
                            }
                            // allow the session.stop() method resume
                            m_session.onProcess = false;
                            m_session.stopMonitor.notifyAll();
                        }
                    }
                }
            } // end of while

            // shutting down
            if (_logger.isDebugEnabled()) {
                _logger.debug(getName() + ": shutting down!");
            }
        }
    }

}//end of class
