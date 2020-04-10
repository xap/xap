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
 * @author Gershon Diner Title: The GigaSpaces Platform Copyright: Copyright (c)
 * GigaSpaces Team 2004 Company: GigaSpaces Technologies Ltd.
 * 
 * @version 4.0
 */
package com.j_spaces.jms;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.jms.utils.IMessageConverter;

import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

/**
 * GigaSpaces implementation of the <code>javax.jms.MessageProducer</code> interface.
 *
 * A client uses a <CODE>MessageProducer</CODE> object to send messages to a destination. A
 * <CODE>MessageProducer</CODE> object is created by passing a <CODE>Destination</CODE> object to a
 * message-producer creation method supplied by a m_session.
 *
 * <P> <CODE>MessageProducer</CODE> is the parent interface for all message producers.
 *
 * <P> A client also has the option of creating a message producer without supplying a destination.
 * In this case, a destination must be provided with every send operation. A typical use for this
 * kind of message producer is to send replies to requests using the request's
 * <CODE>JMSReplyTo</CODE> destination.
 *
 * <P> A client can specify a default delivery mode, priority, and time to live for messages sent by
 * a message producer. It can also specify the delivery mode, priority, and time to live for an
 * individual message.
 *
 * <P> A client can specify a time-to-live value in milliseconds for each message it sends. This
 * value defines a message expiration time that is the sum of the message's time-to-live and the GMT
 * when it is sent (for transacted sends, this is the time the client sends the message, not the
 * time the transaction is committed).
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 * @see TopicPublisher
 * @see QueueSender
 * @see javax.jms.Session#createProducer
 */
public class GSMessageProducerImpl implements MessageProducer, QueueSender, TopicPublisher {

    /**
     * Default delivery mode. The value is set for all the messages that will be sent, in the
     * Session level.
     */
    private int m_deliveryMode = DeliveryMode.NON_PERSISTENT;

    /**
     * Default priority. The value is set for all the messages that will be sent, in the Session
     * level.
     */
    private int m_priority = Message.DEFAULT_PRIORITY;

    /**
     * Default time to live. The value is set for all the messages that will be sent, in the Session
     * level. The default is infinite value in jms message is Message.DEFAULT_TIME_TO_LIVE=0 while
     * then it is calculated according to the space ttl Lease.Forever which is Long.max_value
     */
    private long m_timeToLive = Message.DEFAULT_TIME_TO_LIVE; //Lease.FOREVER;

    /**
     * The session the producer belongs to
     */
    protected GSSessionImpl m_session = null;

    /**
     * The destination the producer belongs to
     */
    protected Destination m_dest = null;


    /* TODO do we still need it?? <code> true </code> if the producer is closed. */
    protected volatile boolean m_closed = false;

    /**
     * <code>true</code> if the client requests not to use the message identifiers; however it is
     * not taken into account, as our jms message and the external entry needs message identifiers
     * for managing acknowledgments.
     */
    private volatile boolean m_messageIDDisabled = false;

    /**
     * <code>true</code> if the time stamp is disabled.
     */
    private volatile boolean m_timestampDisabled = false;

    private String m_producerID;
    private String m_connectionKey;

    private String m_msgIDPrefix;

    private IMessageConverter messageConverter;


    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);

    /**
     * @param sess   - The Session the producer belongs to
     * @param m_dest - The destination (Queue or Topic) that the producer sends messages to
     */
    public GSMessageProducerImpl(GSSessionImpl sess,
                                 Destination m_dest,
                                 IMessageConverter messageConverter)
            throws JMSException {
        if (sess == null) {
            throw new IllegalArgumentException(
                    "GSMessageProducerImpl  Argument 'session' is null");
        }
        this.m_session = sess;
        this.m_dest = m_dest;
        this.messageConverter = messageConverter;
        m_msgIDPrefix = "ID:" + m_session.getSessionID() + "_m";
        m_connectionKey = m_session.getConn().getCnxKey();
    }


    /**
     * @see MessageProducer#setDisableMessageID(boolean)
     */
    public void setDisableMessageID(boolean value) throws JMSException {
        //		if (m_closed)
        //	 		throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        this.m_messageIDDisabled = value;
    }

    /**
     * @see MessageProducer#getDisableMessageID()
     */
    public boolean getDisableMessageID() throws JMSException {
        //		if (m_closed)
        //	  		throw new IllegalStateException("Forbidden call on a closed
        // producer.");

        return m_messageIDDisabled;
    }

    /**
     * @see MessageProducer#setDisableMessageTimestamp(boolean)
     */
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        //		if (m_closed)
        //	  		throw new IllegalStateException("Forbidden call on a closed
        // producer.");

        this.m_timestampDisabled = value;
    }

    public void setProducerID(String m_producerID) {
        this.m_producerID = m_producerID;
    }

    /**
     * @return m_timestampDisabled
     * @see MessageProducer#getDisableMessageTimestamp()
     */
    public boolean getDisableMessageTimestamp() throws JMSException {
        //		if (m_closed)
        //	  		throw new IllegalStateException("Forbidden call on a closed
        // producer.");

        return m_timestampDisabled;
    }

    /**
     * Sets the messages DeliveryMode for all the messages which will be sent via this producer in
     * the m_session level. available DeliveryModes are: DeliveryMode.PERSISTENT - the default and
     * DeliveryMode.NON_PERSISTENT
     *
     * @see MessageProducer#setDeliveryMode(int)
     */
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        //		if (m_closed)
        //			  throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        this.m_deliveryMode = deliveryMode;
    }

    /**
     * @see MessageProducer#getDeliveryMode()
     */
    public int getDeliveryMode() throws JMSException {
        //		if (m_closed)
        //			  throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        return m_deliveryMode;
    }

    /**
     * Sets the priority for all the messages which be sent via this producer in the m_session
     * level.
     *
     * @see MessageProducer#setPriority(int)
     */
    public void setPriority(int priority) throws JMSException {
        //		if (m_closed)
        //			throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        if (priority < 0 || priority > 9)
            throw new JMSException(priority + " is an invalid priority value");
        this.m_priority = priority;
    }

    /**
     * @see MessageProducer#getPriority()
     */
    public int getPriority() throws JMSException {
        //		if (m_closed)
        //			  throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        return this.m_priority;
    }

    /**
     * Sets the default length of time in milliseconds from its dispatch time that a produced
     * message should be retained by the message system.
     *
     * <P> Time to live is set to zero by default.
     *
     * @param timeToLive the message time to live in milliseconds; zero is unlimited, while then it
     *                   is calculated according to the space ttl Lease.Forever which is
     *                   Long.max_value
     * @throws JMSException if the JMS provider fails to set the time to live due to some internal
     *                      error.
     * @see MessageProducer#getTimeToLive
     * @see Message#DEFAULT_TIME_TO_LIVE
     */
    public void setTimeToLive(long timeToLive) throws JMSException {
        //		if (m_closed)
        //			throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        if (timeToLive < 0)
            throw new JMSException(timeToLive + " is an invalid TimeToLive value");
        this.m_timeToLive = timeToLive;
    }

    /**
     * @see MessageProducer#getTimeToLive()
     */
    public long getTimeToLive() throws JMSException {
        //		if (m_closed)
        //			  throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        return this.m_timeToLive;
    }

    /**
     * @see MessageProducer#getDestination()
     */
    public Destination getDestination() throws JMSException {
        //		if (m_closed)
        //			  throw new IllegalStateException("Forbidden call on a closed
        // producer.");
        return this.m_dest;
    }


    /**
     * Cancels the AckDataEntry notify delegator, using its Lease.cancel() Also it removes the
     * current producer from the producers list held by the m_session.
     *
     * @see MessageProducer#close()
     */
    public synchronized void close() throws JMSException {
        if (m_closed) {
            return;
        }
        if (_logger.isDebugEnabled()) {
            _logger.debug("GSMessageProducerImpl.close(): " + toString());
        }
        m_session.removeProducer(this);
        m_session = null;
        m_closed = true;
    }


    /**
     * @see MessageProducer#send(Message)
     */
    public void send(Message message) throws JMSException {
        if (m_dest == null) {
            throw new UnsupportedOperationException("No destination is " +
                    "specified for this producer: " + m_producerID);
        }
        this.send(m_dest, message, getDeliveryMode(), getPriority(),
                getTimeToLive());
    }


    /**
     * @see MessageProducer#send(Message, int, int, long)
     */
    public void send(Message message, int deliveryMode, int priority,
                     long timeToLive) throws JMSException {
        if (m_dest == null) {
            throw new UnsupportedOperationException("No destination is " +
                    "specified for this producer: " + m_producerID);
        }
        this.send(m_dest, message, deliveryMode, priority, timeToLive);
    }


    /**
     * @see MessageProducer#send(Destination, Message)
     */
    public void send(Destination destination, Message message)
            throws JMSException {
        this.send(destination, message, getDeliveryMode(), getPriority(),
                getTimeToLive());
    }


    /**
     * Sends a message with given delivery parameters for an unidentified message producer.
     *
     * @throws UnsupportedOperationException When the producer did not properly identify itself.
     * @throws JMSSecurityException          If the user if not a WRITER on the specified
     *                                       destination.
     * @throws IllegalStateException         If the producer is m_closed, or if the connection is
     *                                       broken.
     * @throws JMSException                  If the request fails for any other reason.
     * @see MessageProducer#send(Destination, Message, int, int, long)
     */
    public void send(Destination _destination, Message _message,
                     int _deliveryMode, int _priority, long _timeToLive)
            throws JMSException {
        // validating destination
        if (_destination == null ||
                !(_destination instanceof Queue ||
                        _destination instanceof Topic)) {
            throw new InvalidDestinationException("Can't send message to"
                    + " an unidentified destination: " + _destination);
        }

        // validating delivery mode
        if (_deliveryMode != DeliveryMode.NON_PERSISTENT &&
                _deliveryMode != DeliveryMode.PERSISTENT)
            throw new JMSException("Delivery Mode of " + _deliveryMode + " is not valid"
                    + " (should be " + DeliveryMode.NON_PERSISTENT +
                    " integer for a Non-Persistent delivery, or " + DeliveryMode.PERSISTENT +
                    " integer for a Persistent delivery).");

        // validating priority
        if (0 > _priority || _priority > 9) {
            throw new JMSException("Message Priority of " + _priority + " is not valid"
                    + " (should be an integer between 0 and 9).");
        }

        GSMessageImpl message = (GSMessageImpl) _message;
        try {
            prepareMessageToSend(message,
                    _deliveryMode,
                    _priority,
                    _timeToLive,
                    _destination);

            m_session.handleSendMessage(message);
        } catch (RemoteException re) {
            if (_logger.isErrorEnabled()) {
                _logger.error("Exception inside GSMessageProducerImpl.send(: "
                        + re.toString(), re);
            }
            JMSException e = new JMSException("RemoteException : "
                    + re.toString());
            e.setLinkedException(re);
            throw e;
        } catch (TransactionException te) {
            if (_logger.isErrorEnabled()) {
                _logger.error("Exception inside GSMessageProducerImpl.send(: "
                        + te.toString(), te);
            }
            JMSException e = new JMSException("TransactionException : "
                    + te.toString());
            e.setLinkedException(te);
            throw e;
        } catch (EntryAlreadyInSpaceException eaine) {
            if (_logger.isErrorEnabled()) {
                _logger.error("GSMessageProducerImpl.send() EntryAlreadyInSpaceException: "
                        + message.DestinationName + "  |  destination:  "
                        + _destination + "  |  uid:  "
                        + message.__getEntryInfo().m_UID);
            }
            JMSException e = new JMSException("EntryAlreadyInSpaceException : "
                    + eaine.toString());
            e.setLinkedException(eaine);
            throw e;
        } catch (Exception e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Exception inside GSMessageProducerImpl.send(): ", e);
            }
            if (!(e instanceof InterruptedException)) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(e.toString(), e);
                }
                JMSException jmse = new JMSException("Exception: " + e.toString());
                jmse.setLinkedException(e);
                throw jmse;
            }
        }
    }


    /**
     * Prepares the message for sending.
     *
     * @param message      The message
     * @param deliveryMode The delivery mode
     * @param priority     The priority
     * @param timeToLive   The time to live
     * @param destination  The destination
     */
    private void prepareMessageToSend(GSMessageImpl message,
                                      int deliveryMode,
                                      int priority,
                                      long timeToLive,
                                      Destination destination)
            throws JMSException {
        if (message instanceof GSBytesMessageImpl) {
            try {
                ((GSBytesMessageImpl) message).seal();
            } catch (IOException e) {
                JMSException jmse = new JMSException(
                        "IOException while clearing message body: " + e.toString());
                jmse.setLinkedException(e);
                throw jmse;
            }
        }
        message.setJMSDeliveryMode(deliveryMode);
        message.setJMSPriority(priority);
        message.setJMSDestination(destination);
        message.setJMSRedelivered(false);

        String msgID = m_msgIDPrefix + m_session.nextMessageNum();
        message.setJMSMessageID(msgID);

        message.__setEntryInfo(null);

        Long now = SystemTime.timeMillis();

        if (timeToLive == Message.DEFAULT_TIME_TO_LIVE) {
            message.setJMSExpiration(Message.DEFAULT_TIME_TO_LIVE);
        } else {
            message.setJMSExpiration(now.longValue() + timeToLive);
        }

        // fix GS-2560
        message.setTTL(timeToLive);

        if (!m_timestampDisabled) {
            message.setJMSTimestamp(now.longValue());
        }

        //message.setJMSGSProducerID(m_producerID);

        //message.setJMSXUserID(m_clientID);
        message.setBodyReadOnly(false);
        message.setPropertiesReadOnly(false);
        message.setStringProperty(GSMessageImpl.JMS_GSCONNECTION_KEY_NAME, m_connectionKey);

        // if there is no MessageConverter set in the message and there is
        // one set in the producer, set it in the message.
        if (messageConverter != null &&
                !message.Properties.containsKey(GSMessageImpl.JMS_GSCONVERTER)) {
            message.Properties.put(GSMessageImpl.JMS_GSCONVERTER, messageConverter);
        }

        // done by the user:
        //message.setJMSCorrelationID();
        //message.setJMSReplyTo();

        // done by constructors:
        //message.setJMSType();


    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MessageProducer | Producer ID: ");
        sb.append(m_producerID);
        sb.append("  |  Session ID: ");
        if (m_session == null) {
            sb.append(" NULL session");
        } else {
            sb.append(m_session.getSessionID());
        }
        return sb.toString();
    }


    /**
     * @return m_producerID
     */
    public String getProducerID() {
        return m_producerID;
    }


    /**
     * Returns the m_session that created this producer. It might be a GSTopicSessionImpl or
     * GSQueueSessionImpl
     *
     * @return the m_session that created this producer
     */
    protected GSSessionImpl getSession() {
        return m_session;
    }


    /**
     * @see QueueSender#getQueue()
     */
    public Queue getQueue() throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed producer.");
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
     * @see QueueSender#send(Queue, Message)
     */
    public void send(Queue queue, Message message) throws JMSException {
        send((Destination) queue, message);
    }


    /**
     * @see QueueSender#send(Queue, Message, int, int, long)
     */
    public void send(Queue queue,
                     Message message,
                     int deliveryMode,
                     int priority,
                     long timeToLive)
            throws JMSException {
        send((Destination) queue,
                message,
                deliveryMode,
                priority,
                timeToLive);
    }


    /**
     * @see TopicPublisher#getTopic()
     */
    public Topic getTopic() throws JMSException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed producer.");
        }

        if (m_dest == null) {
            return null;
        }

        if (m_dest instanceof Topic) {
            return (Topic) m_dest;
        }

        throw new JMSException("The destination type of this producer is not a topic.");
    }


    /**
     * @see TopicPublisher#send(Message)
     */
    public void publish(Message message) throws JMSException {
        send(message);
    }


    /**
     * @see TopicPublisher#send(Destination, Message)
     */
    public void publish(Topic topic, Message message) throws JMSException {
        send(topic, message);
    }


    /**
     * @see TopicPublisher#send(Message, int, int, long)
     */
    public void publish(Message message,
                        int deliveryMode,
                        int priority,
                        long timeToLive)
            throws JMSException {
        send(message, deliveryMode, priority, timeToLive);
    }


    /**
     * @see TopicPublisher#send(Destination, Message, int, int, long)
     */
    public void publish(Topic topic,
                        Message message,
                        int deliveryMode,
                        int priority,
                        long timeToLive)
            throws JMSException {
        send(topic, message, deliveryMode, priority, timeToLive);
    }

}//end of class
