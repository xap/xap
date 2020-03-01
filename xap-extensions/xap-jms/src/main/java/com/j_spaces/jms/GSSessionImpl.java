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

import com.gigaspaces.internal.io.CompressedMarshObjectConvertor;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.jms.utils.CyclicCounter;
import com.j_spaces.jms.utils.IMessageConverter;
import com.j_spaces.jms.utils.StringsUtils;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.pool.ResourcePool;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.jms.TransactionRolledBackException;
import javax.jms.XASession;

/**
 * GigaSpaces implementation of the <code>javax.jms.Session</code> interface. <p> Limitations:<br> -
 * Sessions support only a single <code>MessageConsumer</code>.<br> - Durable subscribers are not
 * supported. - StreamMessage is not supported. </p>
 *
 * @author Gershon Diner
 * @author Shai Wolf
 * @version 6.0
 * @see Session
 */
public class GSSessionImpl
        implements Session, TopicSession, QueueSession, Runnable {
    String m_sessionID;

    IJSpace m_space = null;

    /**
     * <code>true</code> if the session is closed.
     */
    protected volatile boolean m_closed = false;

    /**
     * If true, indicates that the session is in the process of being closed
     */
    protected volatile boolean m_closing = false;

    /**
     * This flag determines whether message delivery is enabled or disabled. Message delivery if
     * disabled if the enclosing connection is stopped. If true then the session was started, else
     * the session is stopped.
     */
    protected volatile boolean m_stopped = true;

    /**
     * Maintains a list of m_consumers for the session.
     */
    protected Map<String, GSMessageConsumerImpl> m_consumers;

    /**
     * Maintains a list of m_producers for the session.
     */
    protected Vector<GSMessageProducerImpl> m_producers;

    /**
     * Maintains a list of m_browsers for the session.
     */
    protected Vector<GSQueueBrowserImpl> m_browsers;

    /**
     * The connection that the session belongs to.
     */
    private GSConnectionImpl m_conn;

	/*
     * This set maintains the list of active durable subscriber names for the
     * session. Durable subscriber names must be unique within the session
     */
    //private HashSet<String> m_durableNames = new HashSet<String>();


    /**
     * Indicates whether the consumer or the client will acknowledge any messages it receives.
     * Ignored if the session is transacted. Legal values are <code>Session.AUTO_ACKNOWLEDGE</code>,
     * <code>Session.CLIENT_ACKNOWLEDGE</code> and <code>Session.DUPS_OK_ACKNOWLEDGE</code>.
     */
    final int m_acknowledgeMode;

    protected Transaction _tx = null;

    //	TODO take it out to config file
    public final static long txLeaseTime = 36000 * 1000;

    //TODO take it out to config file - currently 5 seconds
    //public final static long ackLeaseTime = 5000;

    /**
     * Counter of message listeners.
     */
    volatile CyclicCounter m_msgListeners = new CyclicCounter();

    /**
     * Messages counter.
     */
    volatile private CyclicCounter m_messagesC = new CyclicCounter();

    /**
     * Message m_producers counter.
     */
    volatile private CyclicCounter m_producersC = new CyclicCounter();

    /**
     * Message m_consumers counter.
     */
    volatile private CyclicCounter m_consumersC = new CyclicCounter();

    /**
     * The number of messages consumed by this session
     */
    volatile int m_numOfConsumedMsg = 0;

    /**
     * The number of messages sent by this session
     */
    volatile int m_numOfProducedMsg = 0;

    static private boolean useLocalTransactions;

    static private String txnType;


    /**
     * This object is used to control the control thread. When the session is stopped, the control
     * thread "waits" on this object. When the session is started, the start thread wakes the
     * control thread.
     */
    Object stopMonitor = new Object();

    /**
     * True if there is a consumer that currently processing a message. It needs to be false in
     * order for the session to be stopped.
     */
    boolean onProcess = false;


	/*
     * Table holding the <code>Hashtable</code> (_sendings)
	 * <p>
	 * <b>Key: </b><code>Transaction</code><br>
	 * <b>Object: </b> <code>Hashtable</code>
	 *
    protected Hashtable<Transaction, Hashtable<String, ProducerMessages>>   _sendingsTable;
	 */

    /**
     * Holds the messages produced and sent by this session during a transaction.
     */
    protected LinkedList<GSMessageImpl> sentMessages;


    /**
     * Holds the messages consumed consumers during a transaction and were not acked.
     */
    protected LinkedList<MessageQueueElement> unackedMessages;

    /**
     * Holds the messaged that have been recovered
     */
    private LinkedList<MessageQueueElement> recoverMessages;


    /**
     * Table holding the identifiers of the jms messages delivered per destination or subscription,
     * and NOT acknowledged YET. <p> <b>Key: </b> destination or subscription name <br> <b>Object:
     * </b> <code> MessageAcks </code> instance
     */
    private Hashtable<String, MessageAcks> _deliveries;


    protected boolean m_isQueue = true;

	/*
     * The number of time a message will be redelivered to the consumer when
	 * the listener throws RuntimeException.
	 * See: spce 4.5.2
	 * TODO: make this configurable
	 */
    //protected final int REDELIVERY_LIMIT = 5;


    private static java.util.Random random = new java.util.Random();


    //logger
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_JMS);


    //Reserved identifiers (e.g. for Message selector usage)
    static final HashSet<String> reservedSelectorIdentifiers = new HashSet<String>();

    static {
        reservedSelectorIdentifiers.add("NULL");
        reservedSelectorIdentifiers.add("TRUE");
        reservedSelectorIdentifiers.add("FALSE");
        reservedSelectorIdentifiers.add("NOT");
        reservedSelectorIdentifiers.add("AND");
        reservedSelectorIdentifiers.add("OR");
        reservedSelectorIdentifiers.add("BETWEEN");
        reservedSelectorIdentifiers.add("LIKE");
        reservedSelectorIdentifiers.add("IN");
        reservedSelectorIdentifiers.add("IS");
        reservedSelectorIdentifiers.add("ESCAPE");

        useLocalTransactions = !Boolean.getBoolean(SystemProperties.JMS_USE_MAHALO_PROP);
        txnType = useLocalTransactions ? "Local" : "Mahalo";
    }

    static {
        reservedSelectorIdentifiers.add("JMSReplyTo");
        reservedSelectorIdentifiers.add("JMSDestination");
    }

    /**
     * Recognized provider property names that may be set by clients, and their expected types
     */
    static final Object[][] JMSX_CLIENT_NAMES =
            {
                    {GSMessageImpl.JMSX_GROUPID, String.class},
                    {GSMessageImpl.JMSX_GROUPSEQ, Integer.class}};

    private String m_providerName;

    private ResourcePool<CompressedMarshObjectConvertor> _compressedConvertorPool = null;
    private int m_compressionMinSize;

    /**
     * Creates a session.
     *
     * @param conn            The connection the session belongs to.
     * @param isTransacted    <code>true</code> for a transacted session.
     * @param acknowledgeMode 1 (auto), 2 (client) or 3 (dups ok).
     * @throws JMSException In case of an invalid acknowledge mode.
     */
    //public GSSessionImpl(GSConnectionImpl conn, boolean isQueue, boolean isTransacted,
    public GSSessionImpl(GSConnectionImpl conn,
                         boolean isTransacted,
                         int acknowledgeMode)
            throws JMSException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "GSSessionImpl.GSSessionImpl()");
        }
        if (!isTransacted
                && acknowledgeMode != Session.AUTO_ACKNOWLEDGE
                && acknowledgeMode != Session.CLIENT_ACKNOWLEDGE
                && acknowledgeMode != Session.DUPS_OK_ACKNOWLEDGE)
            throw new JMSException(
                    "Can't create a non transacted session with an"
                            + " invalid acknowledge mode.");

        this.m_space = conn.getSpace();
        this.m_sessionID = conn.nextSessionId();
        this.m_conn = conn;
        this.m_providerName = conn.getMetaData().getJMSProviderName();
        //this.m_isQueue = isQueue;

        // We use blocking linked list as the implentation of the messages queue.
        // It means that is the queue is empty the poller/taker will block
        // until a message is inserted to the queue.
        //messagesQueue = new LinkedBlockingQueue<MessageQueueElement>(MESSAGES_QUEUE_CAPACITY);
        //unackedMessages = new LinkedList<GSMessageImpl>();
        _deliveries = new Hashtable<String, MessageAcks>();
        m_consumers = new HashMap<String, GSMessageConsumerImpl>();
        m_producers = new Vector<GSMessageProducerImpl>();
        m_browsers = new Vector<GSQueueBrowserImpl>();

        recoverMessages = new LinkedList<MessageQueueElement>();
        unackedMessages = new LinkedList<MessageQueueElement>();

        if (!isTransacted) {
            m_acknowledgeMode = acknowledgeMode;
            if (m_acknowledgeMode == Session.CLIENT_ACKNOWLEDGE) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.log(Level.FINE, "GSSessionImpl.GSSessionImpl(): " +
                            "The session's acknowledge mode is CLIENT_ACKNOWLEDGE." +
                            " The session will use " + txnType + " transactions: " + m_sessionID);
                }
                try {
                    renewTransaction();
                } catch (TransactionCreateException e) {
                    JMSException jmse = new JMSException(e.msg);
                    jmse.setLinkedException(e.orig);
                    throw jmse;
                }
            } else {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.log(Level.FINE, "GSSessionImpl.GSSessionImpl(): The session is not transacted: " + m_sessionID);
                }
            }
        } else {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.log(Level.FINE, "GSSessionImpl.GSSessionImpl(): Session is transacted: " + m_sessionID);
            }
            m_acknowledgeMode = Session.SESSION_TRANSACTED;
            sentMessages = new LinkedList<GSMessageImpl>();

            if (_logger.isLoggable(Level.FINE)) {
                _logger.log(Level.FINE, "GSSessionImpl.GSSessionImpl(): " + "The session is transacted " +
                        " and it will use " + txnType + " transactions: " + m_sessionID);
            }
            try {
                renewTransaction();
            } catch (TransactionCreateException e) {
                JMSException jmse = new JMSException(e.msg);
                jmse.setLinkedException(e.orig);
                throw jmse;
            }
        }

        this.m_compressionMinSize = m_conn.getCompressionMinSize();
        _compressedConvertorPool = new ResourcePool<CompressedMarshObjectConvertor>(CompressedMarshObjectConvertor.getFactory(), 0, 100);

        conn.addSession(this);//TODO according to j2EE 1.4 every m_conn
        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.GSSessionImpl() session: "
                    + this.getClass().getName()
                    + " |	" + toString()
                    + " |	space: " + m_space);
        }
        // should have only one session
    }


    public String getJMSProviderName() {
        return m_providerName;
    }

    /**
     * Creates a <CODE>GSBytesMessageImpl</CODE> object. A <CODE>BytesMessage </CODE> object is used
     * to send a message containing a stream of uninterpreted bytes.
     *
     * @throws JMSException          if the JMS provider fails to create this message due to some
     *                               internal error.
     * @throws IllegalStateException If the session is closed.
     * @see Session#createBytesMessage()
     */
    public BytesMessage createBytesMessage() throws JMSException {
        return createBytesMessage(null);
    }

    /**
     * Creates a <CODE>GSBytesMessageImpl</CODE> object. A <CODE>BytesMessage </CODE> object is used
     * to send a message containing a stream of uninterpreted bytes. This method is used in
     * MessageConsumer.recieve() when the JMSMessageID is known already.
     *
     * @throws JMSException          if the JMS provider fails to create this message due to some
     *                               internal error.
     * @throws IllegalStateException If the session is closed.
     * @see Session#createBytesMessage()
     */
    public BytesMessage createBytesMessage(byte[] bytesArrayBody)
            throws JMSException {
        ensureOpen();
        GSBytesMessageImpl bytesMsg = new GSBytesMessageImpl(this,
                bytesArrayBody);
        return bytesMsg;
    }

    /**
     * Creates a <CODE>GSMapMessageImpl</CODE> object. A <CODE> GSMapMessageImpl</CODE> object is
     * used to send a self-defining set of name-value pairs, where names are <CODE>String</CODE>
     * objects and values are primitive values in the Java programming language.
     *
     * @throws JMSException          if the JMS provider fails to create this message due to some
     *                               internal error.
     * @throws IllegalStateException If the session is closed.
     * @see Session#createMapMessage()
     */
    public MapMessage createMapMessage() throws JMSException {
        return createMapMessage(new HashMap<String, Object>());
    }

    /*
     * Not an API method.
     * Creates a <CODE>GSMapMessageImpl</CODE> object. A <CODE>
     * GSMapMessageImpl</CODE> object is used to send a self-defining set of
     * name-value pairs, where names are <CODE>String</CODE> objects and
     * values are primitive values in the Java programming language.
     *
     * @param jmsHashMapBody
     * @exception JMSException
     *                if the JMS provider fails to create this message due to
     *                some internal error.
     * @exception IllegalStateException
     *                If the session is closed.
     * @see javax.jms.Session#createMapMessage()
     */
    MapMessage createMapMessage(HashMap<String, Object> map)
            throws JMSException {
        ensureOpen();
        GSMapMessageImpl mapMsg = new GSMapMessageImpl(this, map);
        return mapMsg;
    }

    /**
     * @throws IllegalStateException If the session is closed.
     * @see Session#createMessage()
     */
    public Message createMessage() throws JMSException {
        ensureOpen();
        return new GSSimpleMessageImpl(this);
    }

    /**
     * Creates a <CODE>GSObjectMessageImpl</CODE> object. A <CODE> GSObjectMessageImpl</CODE> object
     * is used to send a Serializable java object.
     *
     * @throws IllegalStateException If the session is closed.
     * @see Session#createObjectMessage()
     */
    public ObjectMessage createObjectMessage() throws JMSException {
        ensureOpen();
        GSObjectMessageImpl objMsg = new GSObjectMessageImpl(this);
        return objMsg;
    }

    /**
     * Creates a <CODE>GSObjectMessageImpl</CODE> object. A <CODE> GSObjectMessageImpl</CODE> object
     * is used to send a Serializable java object.
     *
     * @throws IllegalStateException If the session is closed.
     * @see Session#createObjectMessage(Serializable)
     */
    public ObjectMessage createObjectMessage(Serializable object)
            throws JMSException {
        ensureOpen();
        GSObjectMessageImpl objMsg = new GSObjectMessageImpl(this, object);
        return objMsg;
    }

    /**
     * Creates a <CODE>StreamMessage</CODE> object. A <CODE>StreamMessage</CODE> object is used to
     * send a self-defining stream of primitive values in the Java programming language.
     *
     * @throws IllegalStateException If the session is closed.
     * @see Session#createStreamMessage()
     */
    public StreamMessage createStreamMessage() throws JMSException {
        ensureOpen();
        GSStreamMessageImpl streamMsg = new GSStreamMessageImpl(this);
        return streamMsg;
    }

    /**
     * Creates an initialized <CODE>TextMessage</CODE> object. A <CODE> TextMessage</CODE> object is
     * used to send a message containing a <CODE> String</CODE>.
     *
     * @throws JMSException          if the JMS provider fails to create this message due to some
     *                               internal error.
     * @throws IllegalStateException If the session is closed.
     * @see Session#createTextMessage()
     */
    public TextMessage createTextMessage() throws JMSException {
        return createTextMessage(null);
    }

    /**
     * Creates an initialized <CODE>TextMessage</CODE> object. A <CODE> TextMessage</CODE> object is
     * used to send a message containing a <CODE> String</CODE>.
     *
     * @param text the string used to initialize this message
     * @throws JMSException          if the JMS provider fails to create this message due to some
     *                               internal error.
     * @throws IllegalStateException If the session is closed.
     * @see Session#createTextMessage(String)
     */
    public TextMessage createTextMessage(String text) throws JMSException {
        ensureOpen();
        GSTextMessageImpl textMsg = new GSTextMessageImpl(this, text);
        return textMsg;
    }

    /**
     * @return true if the session is transacted.
     * @see Session#getTransacted()
     */
    public boolean getTransacted() throws JMSException {
        ensureOpen();
        return m_acknowledgeMode == Session.SESSION_TRANSACTED;
    }

    /**
     * @return m_tx Returns the session m_tx in case when transcated == true
     */
    protected Transaction getTransaction() {
        return this._tx;
    }

    /**
     * JMS 1.1 API
     *
     * @return m_acknowledgeMode
     * @see Session#getAcknowledgeMode()
     */
    public int getAcknowledgeMode() throws IllegalStateException {
        //ensureOpen();
        return this.m_acknowledgeMode;
    }


    /**
     * Sends a JMS message. Called by the MessageProducer. If the session is transacted the message
     * is added to the sent messages list.
     *
     * @param message The message to send
     */
    synchronized void handleSendMessage(GSMessageImpl message)
            throws RemoteException, TransactionException, JMSException {
        if (m_acknowledgeMode != Session.SESSION_TRANSACTED) {
            doSend(message, null);
            // TODO PATCH!!
            // When we use embedded space the message is not serialized
            // through the network, but being copied shallow. We need to
            // clone the properties in case the client do changes in it
            // after we send it.
            if (m_space.isEmbedded()) {
                message.setProperties(message.cloneProperties());
            }
        } else {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("GSSessionImpl.sendMessage():"
                        + " Adding message to sent messages list: " + message.JMSMessageID);
            }

            // save a duplicate of the original message in case the
            // client wants to use the same instance to send more messages.
            GSMessageImpl duplicate = message.duplicate();
            sentMessages.add(duplicate);
        }
    }


    /*
     * Called from within the commit(). Actually sends a message to a given
     * destination. It is done by performing a space.write() operation with the
     * external entry that is extracted from the jms message.
     *
     * @param message
     * @param tx
     * @exception MessageFormatException If the message to send is invalid.
     * @exception InvalidDestinationException If the specified destination is invalid.
     * @exception IllegalStateException If the connection is broken.
     * @exception JMSException If the request fails for any other reason.
     */
    synchronized boolean doSend(GSMessageImpl message, Transaction tx)
            throws RemoteException,
            TransactionException,
            JMSException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.finest("GSSessionImpl.doSend(): Sending message: Txn=" + tx + message);
        } else if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.doSend(): Sending message: Txn=" + tx + ", ID=" + message.JMSMessageID);
        }

        // decide the lease time of the object
        long ttl = message.getTTL();
        if (ttl != Message.DEFAULT_TIME_TO_LIVE) {
            long now = SystemTime.timeMillis();
            ttl = ttl - (now - message.getJMSTimestamp());
            // if the message has expired and was
            // not sent yet we discard it.
            if (ttl <= 0) {
                if (_logger.isLoggable(Level.WARNING)) {
                    _logger.warning("GSSessionImpl.doSend(): Message expired and won't be sent: " + message.getJMSMessageID());
                }
                return false;
            }
        } else {
            ttl = Lease.FOREVER;
        }

        // convert the message to anything if needed
        // remove the MessageConverter here because we don't want to write it to the space.
        // In case of embedded space, it will not be copied, which is good.
        Object messageConverterProp = message.Properties.remove(GSMessageImpl.JMS_GSCONVERTER);
        if (messageConverterProp != null && messageConverterProp instanceof IMessageConverter) {
            IMessageConverter messageConverter = (IMessageConverter) messageConverterProp;
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("GSSessionImpl.doSend(): Converting JMS message " +
                        message.JMSMessageID + " with MessageConverter: " +
                        messageConverter.getClass().getName());
            }
            Object objToWrite = messageConverter.toObject((Message) message);
            if (objToWrite == null) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("GSSessionImpl.doSend(): MessageConverter returned null for message " +
                            message.JMSMessageID + ". Nothing is sent to the space.");
                }
                return false;
            }
            if (objToWrite instanceof Entry) {
                return writeEntry((Entry) objToWrite, tx, ttl);
            } else if (objToWrite instanceof Entry[]) {
                try {
                    return writeEntries((Entry[]) objToWrite, tx, ttl);
                } catch (IOException e) {
                    JMSException jmse = new JMSException("Failed to write entries");
                    jmse.setLinkedException(e);
                    throw jmse;
                }
            } else {
                return writeObject(objToWrite, tx, ttl);
            }
        }

        return writeEntry(message, tx, ttl);

    }


    private boolean writeEntry(Entry entry, Transaction tx, long ttl)
            throws RemoteException, TransactionException {
        // if it is a text message we may need to compress
        //if (message instanceof TextMessage && message.Body != null)
        if (entry instanceof GSTextMessageImpl) {
            GSTextMessageImpl txtMsg = (GSTextMessageImpl) entry;
            if (txtMsg.Body != null) {
                int lengthInBytes = txtMsg.Body.toString().length() * 2;
                if (lengthInBytes > m_compressionMinSize) {
                    if (_logger.isLoggable(Level.FINE)) {
                        _logger.fine("GSSessionImpl.doSend(): Compressing message body. bytes=" + lengthInBytes);
                    }
                    doSendCompressMessage(txtMsg, tx, ttl);
                    return true;
                }
            }
        }

        m_space.write(entry, tx, ttl);

        // increment the sent messages counter
        m_numOfProducedMsg++;

        return true;
    }

    private boolean writeEntries(Entry[] entries, Transaction tx, long ttl)
            throws RemoteException, TransactionException, IOException {
        GSTextMessageImpl txtMsg = null;
        Entry entry;
        MarshObject compressedObject;
        HashMap<GSTextMessageImpl, Object> contents = new HashMap<GSTextMessageImpl, Object>();
        if (entries.length == 0) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("GSSessionImpl.writeEntries(): MessageConverter returned a zero" +
                        " length Entry array. Nothing is sent to the space.");
            }
            return false;
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.writeEntries(): Writing " +
                    entries.length + " entries to the space. tx=" + tx + ", ttl=" + ttl);
        }

        try {
            // if it is a text message we may need to compress
            //if (message instanceof TextMessage && message.Body != null)
            for (int i = 0; i < entries.length; i++) {
                entry = entries[i];
                if (entry instanceof GSTextMessageImpl) {
                    txtMsg = (GSTextMessageImpl) entry;
                    if (txtMsg.Body != null) {
                        int lengthInBytes = txtMsg.Body.toString().length() * 2;
                        if (lengthInBytes > m_compressionMinSize) {
                            if (_logger.isLoggable(Level.FINE)) {
                                _logger.fine("GSSessionImpl.writeEntries(): Compressing message body. bytes=" + lengthInBytes);
                            }

                            // replace the message body with the compressed object
                            compressedObject = compressObject(txtMsg.Body);
                            contents.put(txtMsg, txtMsg.Body);
                            txtMsg.Body = compressedObject;
                        }
                    }
                }
            }

            m_space.writeMultiple(entries, tx, ttl);
            // increment the sent messages counter
            m_numOfProducedMsg += entries.length;
        } catch (IOException e) {
            if (_logger.isLoggable(Level.SEVERE) && txtMsg != null) {
                _logger.severe("GSSessionImpl.writeEntries(): IOException while compressing message body: " +
                        txtMsg.Body);
            }
        } finally {
            // restore messages body
            if (contents.size() > 0) {
                //GSTextMessageImpl txtMsg;
                Iterator<GSTextMessageImpl> iter = contents.keySet().iterator();
                {
                    txtMsg = iter.next();
                    txtMsg.Body = contents.get(txtMsg);
                }
            }
        }

        return true;
    }

    private boolean writeObject(Object obj, Transaction tx, long ttl)
            throws RemoteException, TransactionException {
        if (obj instanceof Object[]) {
            Object[] objects = (Object[]) obj;
            if (objects.length > 0) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("GSSessionImpl.writeObject(): Writing " +
                            objects.length + " objects to the space. tx=" + tx + ", ttl=" + ttl);
                }
                m_space.writeMultiple(objects, tx, ttl);
                // increment the sent messages counter
                m_numOfProducedMsg += objects.length;
            } else {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("GSSessionImpl.writeObject(): MessageConverter returned a zero" +
                            " length array. Nothing is sent to the space.");
                }
                return false;
            }
        } else {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("GSSessionImpl.writeObject(): Writing object: " +
                        obj + ", tx=" + tx + ", ttl=" + ttl);
            }
            m_space.write(obj, tx, ttl);
            // increment the sent messages counter
            m_numOfProducedMsg++;
        }

        return true;
    }


    /**
     * Compresses a message and then sends it.
     *
     * @param message the message
     * @param tx      local transaction
     * @param ttl     time to live
     */
    private void doSendCompressMessage(GSTextMessageImpl message, Transaction tx, long ttl)
            throws RemoteException, TransactionException {
        // store the message body
        Object messageBody = message.Body;
        try {
            MarshObject compressedObject = compressObject(message.Body);

            // replace the message body with the compressed object
            message.Body = compressedObject;
        } catch (IOException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.severe("GSSessionImpl.doSend(): IOException while compressing message body. Sending uncompressed: " +
                        message.Body);
            }
        }
        try {
            m_space.write(message, tx, ttl);
            m_numOfProducedMsg++;
        } finally {
            // restore the message body
            if (message.Body instanceof MarshObject) {
                message.Body = messageBody;
            }
        }
    }


    /**
     * Compress the desired object using GigaSpaces compression facility
     */
    private MarshObject compressObject(Object objToCompress) throws IOException {
        CompressedMarshObjectConvertor cv = null;
        try {
            cv = _compressedConvertorPool.getResource();
            return new MarshObject(cv.toBinary(objToCompress));
        } finally {
            if (cv != null) {
                _compressedConvertorPool.freeResource(cv);
            }
        }
    }


    /**
     * Decompress the desired object using GigaSpaces compression facility
     *
     * @return obj decompressed object
     */
    Object decompressObject(MarshObject objToDecompress)
            throws IOException, ClassNotFoundException {
        CompressedMarshObjectConvertor cv = null;
        Object obj = null;
        try {
            cv = _compressedConvertorPool.getResource();
            obj = cv.fromBinary(objToDecompress.getBytes());
        } finally {
            if (cv != null)
                _compressedConvertorPool.freeResource(cv);
        }
        return obj;
    }


    /*
     * Method called by message m_consumers when receiving a message for
     * preparing the session to later acknowledge or deny (TBD) it.
     * @param destName Name of the destination or of the proxy subscription the message
     * comes from. To this destination name we ack back for successful or
     * not-successful message delivery.
     * @param producerKey - The Message Producer id
     * @param consumerKey - The Message Consumer id
     * @param ackedExternalEntryUID - The acked ExternalEntry UID @param ackedMessageID -
     * The acked jms message id
     * @param queueMode <code> true </code> if the message consumed comes from a queue.
     */
    void prepareAck(String destName, String producerKey, String consumerKey,
			/*String ackedExternalEntryUID, */String ackedMessageID,
                    boolean queueMode) {
        //			if(m_isDebug)
        //			{
        //				GSJMSAdmin.say("GSSession.prepareAck() | Destination name: " +
        // destName
        //															+ " | ackedExternalEntryUID: " + ackedExternalEntryUID
        //															+ " | ackedMessageID: " + ackedMessageID
        //															+ " | producerKey: " + producerKey
        //															+ " | consumerKey: " + consumerKey, Log.D_DEBUG);
        //			}
        //getting the acks vector of Ack objects according to the destName
        MessageAcks acks = _deliveries.get(destName);
        if (acks == null) {
            acks = new MessageAcks(queueMode, destName);
            _deliveries.put(destName, acks);
        }
        acks.addAck(producerKey, consumerKey, ackedMessageID, destName);
        //acks.setTargetName(destName);
    }


    /**
     * Note that the acknowledge method of Message acknowledges all messages received on that
     * messages session.
     *
     * Message.acknowledge() method: Clarify that the method applies to all consumed messages of the
     * session. Rationale for this change: A possible misinterpretation of the existing Java API
     * documentation for Message.acknowledge assumed that only messages received prior to ?this?
     * message should be acknowledged. The updated Java API documentation statement emphasizes that
     * message acknowledgment is really a session-level activity and that this message is only being
     * used to identify the session in order to acknowledge all messages consumed by the session.
     * The acknowledge method was placed in the message object only to enable easy access to
     * acknowledgment capability within a message listener?s onMessage method. This change aligns
     * the specification and Java API documentation to define Message.acknowledge in the same
     * manner. Method acknowledging the received messages.
     *
     * We iterate over all the Destination names (the m_deliveries hash) and then on each
     * MessageAcks vec we perform the actual write of JMSAckDataEntry to the space, using the info
     * inside each Ack obj. The Ack obj, is an inner class wrapper that holds all the required
     * information for a successful acknowledge.
     *
     * @throws IllegalStateException If the connection is broken.
     */
    void acknowledge() throws CommitFailedException {
        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSsessionImpl.acknowledge(): " + m_sessionID);
        }

        unackedMessages.clear();

        // currently tx.commit() is the ack - we use it only for queue
        if (this.m_isQueue && !isAutoAck()) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.log(Level.FINE, "Acknowledging messages. Txn=" + _tx);
            }
            commitLocalTransaction();
        }
    }


    /**
     * Commits all messages done in this transaction and releases any locks currently held.
     *
     * @throws IllegalStateException If the session is m_closed or it is NOT transacted
     * @see Session#commit()
     */
    public void commit() throws JMSException {
        ensureOpen();
        ensureTX();

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.commit(): Committing JMS transaction: " + _tx);
        }

        try {
            // send all produced messages
            sendMessages(this.sentMessages, _tx);

            // ack by committing the local transacrtion
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("GSSessionImpl.commit(): Committing local transaction: " + _tx);
            }
            //_tx.commit();
            commitLocalTransaction();

            // in case we don't get here, the rollback will create a new transaction
            renewTransaction();
        } catch (JMSException mfE) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE,
                        "JMSException during GSSessionImpl.commit(): ", mfE);

                _logger.log(Level.SEVERE,
                        "Rolling back transaction: " + _tx);
            }
            try {
                rollback();
            } catch (JMSException e) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE,
                            "Failed to roll back transaction: " + _tx, e);
                }
            }

            TransactionRolledBackException tE = new TransactionRolledBackException(
                    "JMSException during GSSessionImpl.commit()");
            tE.setLinkedException(mfE);
            throw tE;
        } catch (TransactionException te) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE,
                        "TransactionException during GSSessionImpl.commit(): ", te);

                _logger.log(Level.SEVERE,
                        "Rolling back transaction: " + _tx);
            }
            try {
                rollback();
            } catch (JMSException e) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE,
                            "Failed to roll back transaction: " + _tx, e);
                }
            }

            TransactionRolledBackException e = new TransactionRolledBackException(
                    "TransactionException during GSSessionImpl.commit()");
            e.setLinkedException(te);
            throw e;
        } catch (RemoteException re) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE,
                        "RemoteException during GSSessionImpl.commit(): ", re);

                _logger.log(Level.SEVERE,
                        "Rolling back transaction: " + _tx);
            }
            try {
                rollback();
            } catch (JMSException e) {
            }

            TransactionRolledBackException e = new TransactionRolledBackException(
                    "RemoteException during GSSessionImpl.commit()");
            e.setLinkedException(re);
            throw e;
        } catch (CommitFailedException cfe) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE,
                        "Internal error during GSSessionImpl.commit(): ", cfe.orig);

                _logger.log(Level.SEVERE,
                        "Rolling back transaction: " + _tx);
            }
            try {
                rollback();
            } catch (JMSException e) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE,
                            "Failed to roll back transaction: " + _tx, e);
                }
            }

            TransactionRolledBackException e = new TransactionRolledBackException(
                    "Failed to commit transaction:" + _tx + "\nThe transaction was rolled back.");
            e.setLinkedException(cfe.orig);
            throw e;
        } catch (TransactionCreateException tce) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE,
                        "Failed to renew transaction after commit: ", tce.orig);

                _logger.log(Level.SEVERE,
                        "Rolling back transaction: " + _tx);
            }

            JMSException e = new JMSException(
                    "Failed to renew transaction after commit");
            e.setLinkedException(tce.orig);
            throw e;
        } finally {
            sentMessages.clear();
            unackedMessages.clear();
        }
    }


    /**
     * Send all messages to the space.
     *
     * @return true if no message was sent
     */
    boolean sendMessages(LinkedList<GSMessageImpl> messages, Transaction tx)
            throws RemoteException, TransactionException, JMSException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.finest("GSSession.sendProducedMessages()");
        }

        boolean messagesSent = false;

        int size = messages.size();
        GSMessageImpl[] copy = new GSMessageImpl[size];
        Iterator<GSMessageImpl> iter = messages.iterator();
        for (int j = 0; j < size; j++) {
            copy[j] = iter.next();
        }

        for (int i = 0; i < copy.length; i++) {
            messagesSent = messagesSent | doSend(copy[i], tx);
        }

        return messagesSent;
    }


    public void rollback() throws JMSException {
        ensureOpen();
        ensureTX();

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.rollback(): Rolling back transaction: " + _tx);
        }

        // delete produced messages
        sentMessages.clear();

        // recover the messages
        try {
            recoverMessages();
        } catch (RollbackFailedException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.fine("GSSessionImpl.rollback(): Failed to roll back transaction: " + _tx);
            }
            JMSException je = new JMSException("Failed to roll back transaction: " + _tx);
            je.setLinkedException(e.orig);
            throw je;
        } finally {
            try {
                renewTransaction();
            } catch (TransactionCreateException e) {
                JMSException jmse = new JMSException(e.msg);
                jmse.setLinkedException(e.orig);
                throw jmse;
            }
        }
    }


    /**
     * Called after a transaction is finished to start a new transaction.
     */
    void renewTransaction() throws TransactionCreateException {
        if (m_conn == null || m_closing) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("GSSessionImpl.renewTransaction(): The Session is closed. Not renewing transaction.");
            }
            _tx = null;
            return;
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.renewTransaction(): Renewing transaction.");
        }

        try {
            _tx = m_conn.getTransaction(useLocalTransactions, GSSessionImpl.txLeaseTime);
        } catch (TransactionCreateException re) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, "GSSessionImpl.renewTransaction(): Failed to renew transaction: ", re.orig);
            }
            _tx = null;
            throw re;
        }
        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.renewTransaction(): New transaction: " + _tx);
        }
    }


    /**
     * @return true if not prepared yet
     */
    boolean cancel(Transaction tx) throws IllegalStateException {
//		ensureOpen();
//
//		if( _logger.isLoggable(Level.FINE))
//		{
//			_logger.fine( "GSSessionImpl.cancel(): " + toString() );
//		}
//
//		deny();
//
//		if (!sentMessages.isEmpty()) {
//			sentMessages.clear();
//			return true;
//		}
//
//
//
//		return false;
        return false;
    }


    boolean isAutoAck() {
        return (m_acknowledgeMode == Session.AUTO_ACKNOWLEDGE ||
                m_acknowledgeMode == Session.DUPS_OK_ACKNOWLEDGE);
    }

    /**
     * @see Session#recover()
     */
    public void recover() throws JMSException {
        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "GSSessionImpl.recover()");
        }

        //checked in getTransacted()
        //ensureOpen();

        // calling recover on a transacted session is forbidden
        // according to the specification.
        if (getTransacted()) {
            throw new IllegalStateException(
                    "Forbidden call for recover on a transacted session.");
        }

        // stop message delivery
        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "GSSessionImpl.recover(): Stopping session");
        }
        stop();
        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "GSSessionImpl.recover(): Session stopped");
        }

        // start deliver from the first unacked message
        try {
            recoverMessages();
        } catch (RollbackFailedException e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.fine("GSSessionImpl.recover(): Failed to roll back transaction: " + _tx);
            }
            JMSException je = new JMSException("Failed to roll back transaction: " + _tx);
            je.setLinkedException(e.orig);
            throw je;
        } finally {
            if (m_isQueue && !isAutoAck()) {
                try {
                    renewTransaction();
                } catch (TransactionCreateException e) {
                    JMSException jmse = new JMSException(e.msg);
                    jmse.setLinkedException(e.orig);
                    throw jmse;
                } finally {
                    // resume message delivery
                    if (_logger.isLoggable(Level.FINE)) {
                        _logger.log(Level.FINE, "GSSessionImpl.recover(): Restarting session");
                    }
                    start();
                }
            } else {
                // resume message delivery
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.log(Level.FINE, "GSSessionImpl.recover(): Restarting session");
                }
                start();
            }
        }
    }


    /**
     * Performs session recovery. With topics, we redeliver the unacked messages that arrived to the
     * session (LOCAL REDELIVERY). With queues, we return the messages to the space so they can be
     * redelivered from there.
     */
    void recoverMessages() throws RollbackFailedException//JMSException
    {
        if (m_isQueue) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.log(Level.FINE, "GSSessionImpl.recoverMessages(): Recovering messages of Queue");
            }

            unackedMessages.clear();

            // return the messages to the space
            if (!isAutoAck()) {
                rollbackLocalTransaction();
            }
        } else {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.log(Level.FINE, "GSSessionImpl.recoverMessages(): Recovering messages of Topic");
            }

            unackedMessages.addAll(recoverMessages);
            LinkedList<MessageQueueElement> temp = unackedMessages;
            recoverMessages.clear();
            unackedMessages = recoverMessages;
            recoverMessages = temp;
        }
    }


    /**
     * Tells whether the session is recovering.
     *
     * @return true is the if the session is during message recovery.
     */
    boolean isRecovering() {
        return recoverMessages.size() > 0;
    }


    /**
     * Returns the next recovered message.
     *
     * @return The next recovered message. Null if no recovered message exists.
     */
    GSMessageImpl getNextRecoveredMessage() {
        if (recoverMessages.size() == 0) {
            return null;
        }
        return recoverMessages.remove(0).getMessage();
    }


    //final private ReentrantLock denyLock = new ReentrantLock();
	/*
	 * Method denying the received messages. TODO what should we do in this
	 * case.
	 *
	 * Used in rollback() and close()
	 */
//	void deny()
//	{
//		denyLock.lock();
//		try
//		{
//			//GSJMSAdmin.say("GSSessionImpl.deny(): " + toString(), Log.D_DEBUG);
//			//SessDenyRequest deny;
//			//each target is a Destination name
//			Enumeration<String> allDestinations = _deliveries.keys();
//			while (allDestinations.hasMoreElements())
//			{
//				try
//				{
//					//destination name
//					String target = allDestinations.nextElement();
//					MessageAcks messageAcks = _deliveries.remove(target);
//					//true, if the messages to acknowledge are on a queue. -
//					// TBD support
//					boolean isQueue = messageAcks.getQueueMode();
//					Vector acksVec = messageAcks.getAcksVec();
//					int denysNum = acksVec.size();
//					JMSAckDataEntry[] jmsAckDataEntries = new JMSAckDataEntry[denysNum];
//					for (int i = 0; i < denysNum; i++)
//					{
//						MessageAcks.Ack ack = (MessageAcks.Ack)acksVec.get(i);
//						//							GSJMSAdmin.say("GSSessionImpl.deny() - should report
//								// on denying the received/ACKED messages: "
//						//							+ " | ", Log.D_DEBUG);
//						//TODO use the JMSAckDataEntry and notify somebody
//						// about the problematic issue
//
//						//the actual writting of JMSAckDataEntry to the space.
//						if (ack != null)
//						{
//							jmsAckDataEntries[i] = prepareDenydJMSAckDataEntry(
//									false,//was not acked SUCESSFULY
//									ack.getProducerKey(), ack.getConsumerKey(),
//									/*ack.getAckedExternalEntryUID(),*/ ack.getAckedMessageID(), target);
//							MessagePool.releaseEntry( jmsAckDataEntries[i] );
//						}
//					}
//					if (jmsAckDataEntries != null)
//						writeMultiJMSAckDataEntry(jmsAckDataEntries);
//				}
//				catch (EntryAlreadyInSpaceException entryInSpaceException)
//				{
//					if( _logger.isLoggable(Level.FINE))
//					{
//						_logger.log(Level.FINE, "EntryAlreadyInSpaceException inside GSSession.deny(): "
//								+ entryInSpaceException.toString() );
//					}
//					//										   JMSException e = new
//					// JMSException("EntryAlreadyInSpaceException : "+
//					// entryInSpaceException.toString());
//					//										   e.setLinkedException( entryInSpaceException);
//					//										   throw e;
//				}
//				catch (TransactionException te)
//				{
//					if( _logger.isLoggable(Level.FINE))
//					{
//						_logger.log(Level.FINE, "TransactionException inside GSSession.deny(): ", te );
//					}
//					//				   JMSException e = new JMSException("TransactionException :
//					// "+ te.toString());
//					//				   e.setLinkedException( te);
//					//				   throw e;
//				}
//				catch (RemoteException re)
//				{
//					if( _logger.isLoggable(Level.FINE))
//					{
//						_logger.log(Level.FINE, "RemoteException inside GSSession.deny(): ", re );
//					}
//					//				   JMSException e = new JMSException("RemoteException : "
//					// +re.toString());
//					//				   e.setLinkedException( re);
//					//				   throw e;
//				}
//				catch (JMSException e)
//				{
//					if( _logger.isLoggable(Level.FINE))
//					{
//						_logger.log(Level.FINE, "JMSException inside GSSession.deny(): ", e );
//					}
//				}//end of try/catch
//			}//end of while
//		}
//		finally
//		{
//			denyLock.unlock();
//		}
//	}

    //final private ReentrantLock rollbackLock = new ReentrantLock();


//	public boolean cancel( Transaction tx) throws IllegalStateException{
//
//		if( _logger.isLoggable(Level.FINE))
//		{
//			_logger.fine( "GSSessionImpl.rollback() ... " + toString() );
//		}
//
//		ensureOpen();
//		//GSJMSAdmin.say("GSSession.rollback() ... BEFORE tx.abort(): tx: "
//		// + tx , Log.D_DEBUG);
//
//
//		Hashtable<String, ProducerMessages> sendings = _sendingsTable.get( tx);
//		if( sendings == null)
//			return false;
//
//		//GSJMSAdmin.say("GSSession.rollback() ... AFTER tx.abort(): tx: "
//		// + tx + " | isTXUsed: " + isTXUsed, Log.D_DEBUG);
//		/**
//		 * TODO we should redeliver (setRedelivered) messages which where
//		 * rollbacked while consuming them. - TBD
//		 */
//
//		//Denying the received messages:
//		//GSJMSAdmin.say("GSSession.rollback() ... BEFORE deny() ",
//		// Log.D_DEBUG);
//		deny();
//		// Deleting the produced messages:
//		sendings.clear();
//		//GSJMSAdmin.say("GSSession.rollback() ... AFTER deny() ",
//		// Log.D_DEBUG);
//		return true;
//	}


    void addUnackedMessage(GSMessageImpl message, String consumerID)
            throws JMSException {
        // we need to keep a duplicarte in case the message is altered by the
        // client and then recovered. We want the original data.
        if (!this.m_isQueue) {
            message = message.duplicate();
        }
        unackedMessages.add(new MessageQueueElement(consumerID, message));
    }


    /**
     * Commits the local transaction.
     *
     * @throws JMSException in case thee was an error during the commit.
     */
    private void commitLocalTransaction() throws CommitFailedException//JMSException
    {
        if (_tx == null) {
            return;
        }

        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "Committing local transaction: " + _tx);
        }

        try {
            _tx.commit();
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE,
                        "Failed to commit local transaction: " + _tx, e);
            }
            throw new CommitFailedException(e);
        }
//		catch (UnknownTransactionException e)
//		{
//			// The transaction is not known to the transaction manager.
//			// Therefore rolling back won't do us good.
//			String text = "Internal error during local transaction commit: "+_tx;
//			if( _logger.isLoggable(Level.SEVERE))
//			{
//				_logger.log(Level.SEVERE, text, e );
//			}
//			JMSException jmse = new JMSException(text);
//			jmse.setLinkedException(e);
//			throw jmse;
//		}
//		catch (CannotCommitException e)
//		{
//			String text = "Internal error during local transaction commit: "+_tx;
//			if( _logger.isLoggable(Level.SEVERE))
//			{
//				_logger.log(Level.SEVERE, text, e );
//			}
//			JMSException jmse = new JMSException(text);
//			jmse.setLinkedException(e);
//			throw jmse;
//		}
//		catch (RemoteException e)
//		{
//			String text = "Internal error during local transaction commit: "+_tx;
//			if( _logger.isLoggable(Level.SEVERE))
//			{
//				_logger.log(Level.SEVERE, text, e );
//			}
//			JMSException jmse = new JMSException(text);
//			jmse.setLinkedException(e);
//			throw jmse;
//		}
    }


    /**
     * Rolls back the local transaction.
     *
     * @throws JMSException in case thee was an error during the rollback.
     */
    void rollbackLocalTransaction() throws RollbackFailedException//JMSException
    {
        if (_tx == null) {
            return;
        }

        try {
            if (_logger.isLoggable(Level.FINEST)) {
                _logger.log(Level.FINEST, "Aborting local transaction: " + _tx);
            }
            _tx.abort();
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE,
                        "Internal error during local transaction abort: "
                                + _tx, e);
            }
            throw new RollbackFailedException(e);
        }
//		catch (UnknownTransactionException e)
//		{
//			// The transaction is not known to the transaction manager.
//			// Therefore rolling back won't do us good.
//			String text = "Internal error during local transaction abort: "+_tx;
//			if( _logger.isLoggable(Level.SEVERE))
//			{
//				_logger.log(Level.SEVERE, text, e );
//			}
//			JMSException jmse = new JMSException(text);
//			jmse.setLinkedException(e);
//			throw jmse;
//		}
//		catch (CannotAbortException e)
//		{
//			String text = "Internal error during local transaction abort: "+_tx;
//			if( _logger.isLoggable(Level.SEVERE))
//			{
//				_logger.log(Level.SEVERE, text, e );
//			}
//			JMSException jmse = new JMSException(text);
//			jmse.setLinkedException(e);
//			throw jmse;
//		}
//		catch (RemoteException e)
//		{
//			String text = "Internal error during local transaction abort: "+_tx;
//			if( _logger.isLoggable(Level.SEVERE))
//			{
//				_logger.log(Level.SEVERE, text, e );
//			}
//			JMSException jmse = new JMSException(text);
//			jmse.setLinkedException(e);
//			throw jmse;
//		}
    }


    /*
     * Stops the asynchronous m_deliveries processing in the session. <p> This
     * method must be carefully used. When the session is stopped, the
     * connection might very well going on pushing m_deliveries in the session's
     * queue. If the session is never re-started, these m_deliveries will never
     * be poped out, and this may lead to a situation of consumed but never
     * acknowledged messages. <p> This fatal situation never occurs as the
     * <code> stop() </code> method is either called by the <code> recover()
     * </code> method, which then calls the <code> start() </code> method, or by
     * the <code> Session.close() </code> and <code> Connection.stop() </code>
     * methods, which first empty the session's m_deliveries and forbid any
     * further push.
     */
    void stop() throws JMSException {
        ensureOpen();

        // Ignoring the call if the session is already stopped
        if (m_stopped) {
            return;
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.stop(): Stopping the session: " + toString());
        }

        synchronized (stopMonitor) {
            m_stopped = true;

            // the stop method should not return until the control thread
            // is stopped. the control thread notifies about it.
            if (onProcess) {
                if (_logger.isLoggable(Level.FINEST)) {
                    _logger.finest("Waiting for the consumers to stop: " + toString());
                }
                try {
                    Iterator<GSMessageConsumerImpl> consumers = m_consumers.values().iterator();
                    while (consumers.hasNext()) {
                        consumers.next().notifyStop();
                    }
                    stopMonitor.wait();
                } catch (InterruptedException e) {
                    if (_logger.isLoggable(Level.SEVERE)) {
                        _logger.severe("GSSessionImpl.stop(): InterruptedException during wait: " + e);
                    }
                }
            }
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.stop(): The session is stopped: " + toString());
        }
    }

    /*
     *
     * @throws JMSException
     */
    void start() throws JMSException {
        ensureOpen();

        // Ignoring the call if the session is already started
        if (!m_stopped) {
            return;
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.start(): Starting the session: " + toString());
        }

        //synchronized (sessionStopMonitor) {
        this.m_stopped = false;

        // wake up the consumer's control thread - sync or async - the same
        synchronized (stopMonitor) {
            stopMonitor.notifyAll();
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.start()  session was started: " + toString());
        }
    }

    final private ReentrantLock closeLock = new ReentrantLock();

    /**
     * @see Session#close()
     */
    public void close() throws JMSException {
        closeLock.lock();
        try {
            //Ignoring the call if the session is already m_closed:
            if (!m_closed) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("GSSession.close(): Closing the session: " + toString());
                }

                m_closing = true;

                // must stop first before we close
                stop();

                if (!(this instanceof XASession) && getTransacted()) {
                    // roll back a transacted session
                    rollback();
                } else {
                    // abort the local transaction to
                    // return unacked messaged to the space for redelivery.
                    try {
                        rollbackLocalTransaction();
                    } catch (RollbackFailedException e) {
                        if (_logger.isLoggable(Level.SEVERE)) {
                            _logger.severe("Failed to rollback transaction during session.close(): " + e.orig);
                        }
                        // continue the close procedure.
                    }
                }

                // Closing the session's resources:
                if (_logger.isLoggable(Level.FINEST)) {
                    _logger.finest("Closing the session's resources: " + toString());
                }

                // removing consumers is a little more tricky because we use an iterator.
                // we can't remove an element from the collection while we iterate.
                // so we iterate over another data structure.
                Iterator<GSMessageConsumerImpl> i = m_consumers.values().iterator();
                while (i.hasNext()) {
                    GSMessageConsumerImpl consumer = i.next();
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("Closing consumer: " + consumer.toString());
                    }
                    consumer.close();
                }
                while (!m_browsers.isEmpty()) {
                    GSQueueBrowserImpl browser = m_browsers.get(0);
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("Closing queue browser: " + browser.toString());
                    }
                    browser.close();
                }
                while (!m_producers.isEmpty()) {
                    GSMessageProducerImpl producer = m_producers.get(0);
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("Closing producer: " + producer.toString());
                    }
                    producer.close();
                }


                m_conn.removeSession(this);
                //m_conn = null;
                //update the session state
                m_closed = true;
                m_closing = false;
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("GSSession.close() session was closed: " + toString());
                }
            }
        } finally {
            closeLock.unlock();
        }
    }

    /**
     * Verifies that the session isn't closed
     *
     * @throws IllegalStateException if the session is closed
     */
    void ensureOpen() throws IllegalStateException {
        if (m_closed) {
            throw new IllegalStateException("Forbidden call on a closed session.");
        }
    }

    /**
     * Verifies that the session is under TX
     *
     * @throws IllegalStateException if the session is not under tx
     */
    protected void ensureTX() throws IllegalStateException {
        if (m_acknowledgeMode != Session.SESSION_TRANSACTED) {
            throw new IllegalStateException(
                    "Forbidden call on a non transacted session.");
        }
    }


    /*
     * Sets the session's distinguished message listener (optional).
     *
     * <P> When the distinguished message listener is set, no other form of
     * message receipt in the session can be used; however, all forms of sending
     * messages are still supported.
     *
     * <P> This is an expert facility not used by regular JMS clients.
     *
     * @param listener the message listener to associate with this session
     *
     * @exception JMSException if the JMS provider fails to set the message
     * listener due to an internal error.
     *
     * @see javax.jms.Session#getMessageListener
     * @see javax.jms.ServerSessionPool
     * @see javax.jms.ServerSession
     *
     * NOT USED IN OUR IMPLEMENTATION. WE USE THE MESSAGE LISTENERS IN THE
     * MESSAGE CONSUMER LEVEL.
     *
     */
    public MessageListener getMessageListener() throws JMSException {
        ensureOpen();
        throw new JMSException(
                "Forbidden call to GSSessionImpl.getMessageListener(). You should Call its subclass");
    }

    /*
     * NOT USED IN OUR IMPLEMENTATION. WE USE THE MESSAGE LISTENERS IN THE
     * MESSAGE CONSUMER LEVEL.
     */
    public void setMessageListener(MessageListener listener)
            throws JMSException {
        ensureOpen();
        throw new JMSException(
                "Forbidden call to GSSessionImpl.setMessageListener(). You should Call its subclass");
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Runnable#run()
     */
    public void run() {
        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("GSSessionImpl.run() method is not implemented. ");
        }
    }

    /**
     * @see Session#createProducer(Destination)
     */
    public MessageProducer createProducer(Destination destination)
            throws JMSException {
        ensureOpen();
        if (destination == null ||
                destination instanceof Queue ||
                destination instanceof Topic) {
            return createGenericProducer(destination);
        } else {
            throw new InvalidDestinationException(
                    "The destination type is not recognized: " + destination);
        }
    }


    private MessageProducer createGenericProducer(Destination dest)
            throws JMSException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "Creating publisher for destination: " + dest);
        }
        GSMessageProducerImpl producer;
        synchronized (this) {
            String destName = "";
            if (dest != null) {
                destName = dest.toString();
            }

            if (getConn() != null) {
                String clientID = getConn().getClientID();
                getConn().updateClientIDInternally(
                        clientID + ":" + destName);
            }
            IMessageConverter messageConverter = this.m_conn.connFacParent.getMessageConverter();
            producer = new GSMessageProducerImpl(this, dest, messageConverter);
            producer.setProducerID(nextProducerId());
            addProducer(producer);
        }
        return producer;
    }


    /**
     * @see Session#createConsumer(Destination)
     */
    public MessageConsumer createConsumer(Destination destination)
            throws JMSException {
        return createConsumer(destination, null, false);
    }


    /**
     * @see Session#createConsumer(Destination, String)
     */
    public MessageConsumer createConsumer(Destination destination,
                                          String messageSelector) throws JMSException {
        return createConsumer(destination, messageSelector, false);
    }

    /**
     * @see Session#createConsumer(Destination, String, boolean)
     */
    public MessageConsumer createConsumer(Destination destination,
                                          String messageSelector,
                                          boolean noLocal)
            throws JMSException {
        ensureOpen();
        if (destination == null) {
            throw new InvalidDestinationException("Invalid destination: null");
        }

        if (!(destination instanceof Queue ||
                destination instanceof Topic)) {
            throw new InvalidDestinationException(
                    "The destination type is not recognized: " + destination);
        }

        if (m_consumers.size() > 0) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.severe("GSSession.addConsumer(): Sessions support only one consumer: " + toString());
            }
            throw new JMSException("Sessions support only one consumer.");
        }

        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "Creating MessageConsumer: dest=" + destination
                    + ", selector=" + messageSelector + ", noLocal=" + noLocal);
        }

        GSMessageConsumerImpl consumer;
        synchronized (this) {
            String queueName = "";
            if (destination != null) {
                queueName = destination.toString();
            }

            // setting the connection clientID to the Destination name
            // together with the space name
            if (getConn() != null) {
                String clientID = getConn().getClientID();
                if (clientID.indexOf(":") != -1)
                    getConn().updateClientIDInternally(
                            clientID.substring(0, clientID.indexOf(":"))
                                    + ":" + queueName);
                else
                    getConn().updateClientIDInternally(
                            clientID + ":" + queueName);
            }
            consumer = new GSMessageConsumerImpl(this,
                    destination,
                    nextConsumerId(),
                    null,
                    messageSelector,
                    noLocal);
            addConsumer(consumer);
        }
        m_isQueue = (destination instanceof Queue);
        if (m_isQueue && !this.isAutoAck() && _tx == null) {
            try {
                renewTransaction();
            } catch (TransactionCreateException e) {
                JMSException jmse = new JMSException(e.msg);
                jmse.setLinkedException(e.orig);
                throw jmse;
            }
        }
        return consumer;
    }


    /**
     * @return String m_sessionID
     */
    public String getSessionID() {
        return m_sessionID;
    }

    /**
     * @param string
     */
    public void setSessionID(String string) {
        m_sessionID = string;
    }

    // private final ReentrantLock removeExtEntryLock = new ReentrantLock();

    /**
     * Removing the ExternalMessage, using space.clear(), from the space.
     *
     * @param extEntryToClear
     * @throws JMSMessage
     */
//	public void removeExternalEntryFromSpace(ExternalEntry extEntryToClear)
//		throws JMSException
//	{
//		boolean isTXUsed = false;
//		//removeExtEntryLock.lock();
//		try
//		{
//			//ExternalEntry extEntry = MessagePool.getExternalEntry();
//			//extEntry.m_UID = removedExternalEntryUID;
//			//extEntry.m_FieldsValues[GSMessageImpl.JMS_MESSAGE_ID_NUM] = removedJMSMessageID;
//			//clear the acked ExternalEntry from the space
//			Transaction curTX = getSessionTransaction();
//
//			m_space.clear(extEntryToClear, curTX);
//			if (curTX != null)
//			{
//				isTXUsed = true;
//				try
//				{
//					//						GSJMSAdmin.say("removeExternalEntryFromSpace ... BEFORE
//					// tx.commit() tx: " + curTX.toString() + " Thread.sleep()
//					// ", Log.D_DEBUG);
//					Thread.sleep(200);
//				}
//				catch (InterruptedException e)
//				{
//					// TODO Auto-generated catch block
//					if( _logger.isLoggable(Level.SEVERE))
//					{
//						_logger.log(Level.SEVERE, e.toString(), e );
//					}
//				}
//				curTX.commit();
//				if( _logger.isLoggable(Level.FINE))
//				{
//					_logger.fine("GSSessionImpl.removeExternalEntryFromSpace() After clear() + After tx.commit()	TX:  "
//							+ ((ServerTransaction) curTX).id
//							+ " | m_sessionID:  " + m_sessionID );
//				}
//			}
//
//			//				if(m_isDebug)
//			//				{
//			//					GSJMSAdmin.say("GSSessionImpl.removeExternalEntryFromSpace()
//			// AFTER space.clear() of the ExternalEntry with UID: "
//			//												+ removedExternalEntryUID
//			//												+ " | from Destination: " + destinationName, Log.D_DEBUG);
//			//				}
//		}
//		catch (EntryAlreadyInSpaceException entryInSpaceException)
//		{
//			//				JMSException e =
//			//						new JMSException("EntryAlreadyInSpaceException in
//			// GSSessionImpl.removeExternalEntryFromSpace()" +
//			// entryInSpaceException.toString());
//			//				e.setLinkedException( entryInSpaceException);
//			//				throw e;
//			if( _logger.isLoggable(Level.FINE))
//			{
//				_logger.log(Level.FINE, "GSSessionImpl.removeExternalEntryFromSpace() EntryAlreadyInSpaceException:  "
//						+ entryInSpaceException.toString(), entryInSpaceException );
//			}
//		}
//		catch (TransactionException te)
//		{
//			JMSException e = new JMSException("TransactionException in GSSessionImpl.removeExternalEntryFromSpace()"
//					+ te.toString());
//			e.setLinkedException(te);
//			throw e;
//		}
//		catch (UnusableEntryException uue)
//		{
//			if (uue instanceof EntryNotInSpaceException)
//			{
//				if( _logger.isLoggable(Level.FINE))
//				{
//					_logger.log(Level.FINE, "GSSessionImpl.removeExternalEntryFromSpace(): The Entry "
//							+ ((EntryNotInSpaceException) uue).getUID()
//							+ " No Longer In Space " + uue.getCause(), uue );
//				}
//			}
//			else
//			{
//				final JMSException ex = new JMSException(
//						"UnusableEntryException : ");
//				ex.setLinkedException(uue);
//				throw ex;
//			}
//		}
//		catch (RemoteException re)
//		{
//			//				 JMSException e =
//			//				 		new JMSException("RemoteException in
//			// GSSessionImpl.removeExternalEntryFromSpace()"+re.toString());
//			//				 e.setLinkedException( re);
//			//				 throw e;
//			if( _logger.isLoggable(Level.FINE))
//			{
//				_logger.log(Level.FINE, "GSSessionImpl.removeExternalEntryFromSpace() RemoteException:  "
//						+ re.toString(), re );
//			}
//		}
//		finally
//		{
////			re-new a new m_tx after the abort() if we are not in process of closing
//			if (m_conn != null && isTXUsed && !m_closing)
//				_tx = m_conn.getTransaction(GSSessionImpl.txLeaseTime);
//			//removeExtEntryLock.unlock();
//
//		}
//	}//removeExternalEntryFromSpace

    /**
     * Removing/clear() the JMSAckDataEntry from the space usually called after successful Ack to
     * the producer or while cancel().
     */
    void removeJMSAckDataEntryFromSpace(JMSAckDataEntry ackDataEntry)
            throws JMSException {
        try {
            //CLEAR the JMSAckDataEntry from the space
            m_space.clear(ackDataEntry, null /* getSessionTransaction() */);
            //				GSJMSAdmin.say("GSSessionImpl.removeJMSAckDataEntryFromSpace()
            // AFTER space.clear() of the JMSAckDataEntry with UID: "
            //												+ ackDataEntry.__getEntryInfo().m_UID +
            //											   " | for ackedMessageID: " + ackDataEntry.m_ackedMessageID +
            //											   " | produced from: " + ackDataEntry.m_producerKey +
            //											   " | consumed by: " + ackDataEntry.m_consumerKey, Log.D_DEBUG);
        } catch (EntryAlreadyInSpaceException entryInSpaceException) {
            JMSException e = new JMSException(
                    "EntryAlreadyInSpaceException in GSSessionImpl.removeJMSAckDataEntryFromSpace()");
            e.setLinkedException(entryInSpaceException);
            throw e;
        } catch (TransactionException te) {
            JMSException e = new JMSException(
                    "TransactionException in GSSessionImpl.removeJMSAckDataEntryFromSpace()");
            e.setLinkedException(te);
            throw e;
        } catch (UnusableEntryException uue) {
            if (uue instanceof EntryNotInSpaceException) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.log(Level.FINE, "GSSessionImpl.removeJMSAckDataEntryFromSpace(): The Entry "
                            + ((EntryNotInSpaceException) uue).getUID()
                            + " No Longer In Space " + uue.getCause(), uue);
                }
            } else {
                final JMSException ex = new JMSException(
                        "UnusableEntryException : ");
                ex.setLinkedException(uue);
                throw ex;
            }
        } catch (RemoteException re) {
            JMSException e = new JMSException(
                    "RemoteException in GSSessionImpl.removeJMSAckDataEntryFromSpace()"
                            + re.toString());
            e.setLinkedException(re);
            throw e;
        }
    }

	/*
	 * Writing an array of JMSAckDataEntry entries, which holds an Ack , into
	 * the space so the Message producer, which requested it and is listening
	 * for it, with a NotifyDelegator, will be notified too.
	 *
	 * Note that this method is used for 2 method calls: for successful ack- via
	 * acknowledge()- and for deny - an un-successful ack- via deny().TODO
	 *
	 * @param jmsAckDataEntries @throws JMSException @throws RemoteException
	 * @throws TransactionException
	 */
//	private void writeMultiJMSAckDataEntry(JMSAckDataEntry[] jmsAckDataEntries)
//		throws JMSException, RemoteException, TransactionException
//	{
//		//		   //Write the array of jmsAckDataEntries to the space with write() -
//		// NOT writeMultiple()
//		//		   for (int i = 0; i < jmsAckDataEntries.length; i++)
//		//		   {
//		//				/*Lease l = */ m_space.write(jmsAckDataEntries[i], null ,Lease.FOREVER);
//		//				/*LeaseProxy lp =(LeaseProxy)l;*/
//		//				GSJMSAdmin.say("Wrote jmsAckDataEntry [ " + i + " ] with the uid: " +
//		// lp.getEntryInfo().m_UID );
//		//		   }
//
//		//Write the array of jmsAckDataEntries to the space with
//		// writeMultiple()
//		/*Lease[] l = */ m_space.writeMultiple(jmsAckDataEntries, null, ackLeaseTime);//5 seconds lease
//		//		   GSJMSAdmin.say("AFTER writeMultiple() jmsAckDataEntries array: " +
//		// jmsAckDataEntries, Log.D_DEBUG);
//	}

	/*
	 * Preparing a JMSAckDataEntry that holds an Ack, so it will be used as
	 * array of JMSAckDataEntry which will be later be used in
	 * writeJMSAckDataEntry().
	 * @param isAcknowledged - acked or naked
	 * @param ackMode
	 * @param producerKey
	 * @param consumerKey
	 * @param ackedExternalEntryUID
	 * @return ackDataEntry
	 * @throws JMSException @throws
	 * RemoteException
	 * @throws TransactionException
	 */
//	private JMSAckDataEntry prepareJMSAckDataEntry(
//			boolean isAcknowledged, String producerKey, String consumerKey,
//			/*String ackedExternalEntryUID,*/ String ackedMessageID,
//			String destinationName)// throws JMSException, RemoteException,
//	//TransactionException
//	{
//		//Setting Entry UID
//		JMSAckDataEntry ackDataEntry = MessagePool.getAckDataEntry();
//		try {
//			ackDataEntry.m_ackMode = getAcknowledgeMode();
//		} catch (IllegalStateException e) {}
//		ackDataEntry.m_wasAck = Boolean.valueOf( isAcknowledged);
//		ackDataEntry.m_producerKey = producerKey;
//		ackDataEntry.m_consumerKey = consumerKey;
//		/*ackDataEntry.m_ackedExternalEntryUID = ackedExternalEntryUID;*/
//		ackDataEntry.m_ackedMessageID = ackedMessageID;
//		ackDataEntry.m_destinationNameToBeAcked = destinationName;
//
//		ackEntryInfo.m_UID = ClientUIDHandler.createUIDFromName(
//				//holds returned JMS Message id that was sent and acked..
//				ackedMessageID + "_ACK", JMSAckDataEntry.class.getName());
//		//		  GSJMSAdmin.say("GSSessionImpl.prepareJMSAckDataEntry() for write()
//		// UID: " +
//		//		  				uid1+ " for destination: " + destinationName + "\n\t\t | " +
//		// ackDataEntry.toString() + "\n", Log.D_DEBUG);
//
//		ackDataEntry.__setEntryInfo( ackEntryInfo );
//		return ackDataEntry;
//	}

	/*
	 * Preparing a JMSAckDataEntry that holds an Ack, so it will be used as
	 * array of JMSAckDataEntry which will be later be used in
	 * writeJMSAckDataEntry().
	 *
	 * These acked messages represent messages which had been received by the
	 * consumer but were DENIED and were not consumed properly. There are 2
	 * reasons for denying a received message: 1. If there was a TX.rollback()
	 * 2. If there was a call to close() while TX
	 *
	 * We write later the array of denied messages to the space and each one one
	 * them has a suffix of "_DEN" string.
	 * @param isAcknowledged - acked or naked
	 * @param ackMode
	 * @param producerKey
	 * @param consumerKey
	 * @param ackedExternalEntryUID
	 * @param destinationName
	 * @return ackDataEntry
	 * @throws JMSException @throws RemoteException @throws TransactionException
	 */
//	private JMSAckDataEntry prepareDenydJMSAckDataEntry(
//			boolean isAcknowledged, String producerKey, String consumerKey,
//			/*String ackedExternalEntryUID,*/ String ackedMessageID,
//			String destinationName) throws IllegalStateException
//			{
//		//Setting Entry UID
//		JMSAckDataEntry ackDataEntry = MessagePool.getAckDataEntry();
//		ackDataEntry.m_ackMode = getAcknowledgeMode();
//		ackDataEntry.m_wasAck = Boolean.valueOf( isAcknowledged);
//		ackDataEntry.m_producerKey = producerKey;
//		ackDataEntry.m_consumerKey = consumerKey;
//		/*ackDataEntry.m_ackedExternalEntryUID = ackedExternalEntryUID;*/
//		ackDataEntry.m_ackedMessageID = ackedMessageID;
//		ackDataEntry.m_destinationNameToBeAcked = destinationName;
//
//		denyEntryInfo.m_UID = ClientUIDHandler.createUIDFromName(
//				//holds returned JMS Message id that was sent and acked..
//				ackedMessageID + "_DEN", JMSAckDataEntry.class.getName());
//		//			GSJMSAdmin.say("GSSessionImpl.prepareDenydJMSAckDataEntry() for
//		// write() UID: " +
//		//						   uid1+ "\n\t\t | " + ackDataEntry.toString() + "\n", Log.D_DEBUG);
//
//		ackDataEntry.__setEntryInfo( denyEntryInfo );
//		return ackDataEntry;
//			}

    /**
     * returns true if the prodKey is a Producer key which is part of the current session's
     * Producer's vector, otherwise it is considered as a Producer which belongs to other session
     * and returns false;
     */
    boolean isLocalProducer(String prodKey) {
        if (m_producers != null) {
            int prodKeyHashCode = prodKey.hashCode();
            for (int i = 0; i < m_producers.size(); i++) {
                GSMessageProducerImpl messageProducer = m_producers.get(i);
                if (messageProducer != null) {
                    return messageProducer.getProducerID().hashCode() == prodKeyHashCode;
                }
            }
        }
        return false;
    }

    /*
     * returns true if the consKey is a Consumer key which is part of the
     * current session's consumer's vector, otherwise it is considered as a
     * consumer which belongs to other session and returns false; @param consKey
     * @return
     */
    boolean isLocalConsumer(String consKey) {
        return m_consumers.containsKey(consKey);
//		int consKeyHashCode = consKey.hashCode();
//		Iterator<String> i = m_consumers.keySet().iterator();
//		while (i.hasNext()) {
//			String consumerId = i.next();
//			if (consumerId.hashCode() == consKeyHashCode) {
//				return true;
//			}
//		}
//		return false;
    }

    //	/*
    //	 * Sets an existing Consumer ID, the same one that
    //	 * was used by the DurableSubscriber which was re-launched with the
    // persistent client details.
    //	 * This method is called AFTER the setConsumersC() was called from
    // GSTopicSessionImpl
    //	 * and updated the m_consumers counter.
    //	 * This method returns the consumer identifier that was used for the
    // durable subscriber.
    //	 * */
    //	 synchronized String getDurableConsumerId()
    //	 {
    //		return m_conn.getCnxKey() + "_cons_" + m_consumersC;
    //	 }

    //final private ReentrantLock nextIDsLock = new ReentrantLock();

    /**
     * Creates a new message consumer ID.
     *
     * @return a new message consumer ID.
     */
    String nextConsumerId() {
        return m_sessionID + "_cons_" + m_consumersC.increment();
    }


    /**
     * Creates a new message producer ID.
     *
     * @return a new message producer ID.
     */
    String nextProducerId() {
        return m_sessionID + "_prod_" + m_producersC.increment();
    }

    /*
     * Returns a new message identifier.
     */
    int nextMessageNum() {
        return m_messagesC.increment();
    }

    protected int getRandomInt() {
        return random.nextInt(Integer.MAX_VALUE);
    }

    /*
     * @return m_consumersC
     */
    public int getConsumersC() {
        return m_consumersC.getValue();
    }

    /*
     * @return m_messagesC
     */
    public int getMessagesC() {
        return m_messagesC.getValue();
    }

    /*
     * @return m_producersC
     */
    public int getProducersC() {
        return m_producersC.getValue();
    }

    /*
     * @param i
     */
    public void setConsumersC(int i) {
        m_consumersC.setValue(i);
    }

    /*
     * @param i
     */
    public void setMessagesC(int i) {
        m_messagesC.setValue(i);
    }

    /*
     * @param i
     */
    public void setProducersC(int i) {
        m_producersC.setValue(i);
    }

    public String toString() {
        return "GSSessionImpl || session ID: " + m_sessionID;
    }

    /**
     * @return m_numOfConsumedMsg
     */
    public int getNumOfConsumedMsg() {
        return m_numOfConsumedMsg;
    }

    /**
     * @return m_numOfProducedMsg
     */
    public int getNumOfProducedMsg() {
        return m_numOfProducedMsg;
    }

    /*
     * Add a consumer to the list of m_consumers managed by this session
     *
     * @param consumer the consumer to add
     */
    protected void addConsumer(GSMessageConsumerImpl consumer) {
        m_consumers.put(consumer.getConsumerID(), consumer);
    }

    /*
     * Remove the consumer from the list of managed m_consumers by this session.
     * @param consumer the consumer to remove
     */
    protected void removeConsumer(GSMessageConsumerImpl consumer) {
        m_consumers.remove(consumer.getConsumerID());
    }

    /*
     * Add a producer to the list of m_producers managed by this session
     *
     * @param producer the producer to add
     */
    protected void addProducer(GSMessageProducerImpl producer) {
        m_producers.addElement(producer);
    }

    /*
     * Remove the producer from the list of managed m_producers
     *
     * @param producer the producer to remove
     */
    protected void removeProducer(GSMessageProducerImpl producer) {
        m_producers.remove(producer);
    }

    /**
     * Check if the session is closed
     *
     * @return <code>true</code> if the session is m_closed
     */
    protected final boolean isClosed() {
        return m_closed;
    }

    /**
     * @return m_conn
     */
    protected GSConnectionImpl getConn() {
        return m_conn;
    }


    /**
     * Start new transaction on this session
     *
     * @param resume true if resume old transaction (Used by XA)
     */
    void startTransaction(Transaction transaction, boolean resume) {
//		if( resume){
//			_sendings = _sendingsTable.get( transaction);
//		}
//		else{
//			_sendings = new Hashtable<String, ProducerMessages>();
//			_sendingsTable.put( transaction, _sendings);
//		}
    }


    /**
     * Currently not supported.
     *
     * @see Session#unsubscribe(String)
     */
    public void unsubscribe(String subscriptionName)
            throws JMSException {
        ensureOpen();
        throw new JMSException("This version of JMS does not support durable subscribers.");
//    	ensureOpen();
//        synchronized(this)
//        {
//        	if( _logger.isLoggable(Level.FINE))
//			{
//				_logger.fine( "GSSessionImpl.unsubscribe(): unsubscribe from: "
//	                + subscriptionName + " || " + toString() );
//			}
//	        //getConn().ensureOpen();
//	        if (!StringsUtils.isEmpty(subscriptionName))
//	        {
//	            if (m_durableNames.contains(subscriptionName))
//	            {
//	                throw new JMSException(
//	                        "Cannot unsubscribe while an active "
//	                                + "TopicSubscriber still exists with the subscription name: "
//	                                + subscriptionName);
//	            }
//	            String clientID = getConn().getClientID();
//	            //resolving the topic name (out of clientID) which we used to
//	            // subscribe to with a durable subscriber.
//	            String subscribedTopicName = clientID.substring(clientID
//	                    .lastIndexOf(":") + 1);
//
//	            //removing subscriber from the JMSDurableSubService
//	            unsubscribeFromTopic(subscriptionName, subscribedTopicName,
//	                    clientID);
//
//	            //removing subscriber details from config file
//	            //updating the jms-config.xml with the removed durable subscriber
//	            // details
//	            Hashtable<String, String> durbleSubscriberDetails = new Hashtable<String, String>();
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_CLIENT_ID, clientID);
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_CONNECTION_ID,
//	                    getConn().getCnxKey());
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_SESSION_ID,
//	                    getSessionID());
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_TOPIC_NAME,
//	                    subscribedTopicName);
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_SUBSCRIPTION_NAME,
//	                    subscriptionName);
//	            getConn().getConnFacParent().getAdmin()
//	                    .unsubscribeDurableSubscriberDetailsFromXML(
//	                            durbleSubscriberDetails);
//	            m_durableNames.remove(subscriptionName);
//	        }
//	        else
//	        {
//	            throw new InvalidDestinationException(
//	                    "Cannot unsubscribe with a null subscription name.");
//	        }
//        }
    }


    /*
     * writing to space the JMSDurableSubDataEntry to indicate the
     * JMSDurableSubService about unsubscribe() call for the current durable
     * subscription. @param _subscriptionName @throws JMSException
     */
//    private void unsubscribeFromTopic(String _subscriptionName, String _subscribedTopicName,
//            String _clientID) throws JMSException
//    {
//
//    	ensureOpen();
//    	try
//        {
//            JMSDurableSubDataEntry subscriptionTemplate = new JMSDurableSubDataEntry();
//            subscriptionTemplate.m_durableSubscriptionName = _subscriptionName;
//            subscriptionTemplate.m_topicName = _subscribedTopicName;
//            //			subscriptionTemplate.m_isSubscribed = Boolean.valueOf(false);
//            //			subscriptionTemplate.m_isSubscriberOnline =
//            // Boolean.valueOf(false);
//            subscriptionTemplate.m_subscriberClientID = _clientID;
//
//            JMSDurableSubDataEntry removedSubscription = new JMSDurableSubDataEntry();
//            removedSubscription.m_durableSubscriptionName = _subscriptionName;
//            removedSubscription.m_topicName = _subscribedTopicName;
//            removedSubscription.m_isSubscribed = Boolean.valueOf(false);
//            removedSubscription.m_isSubscriberOnline = Boolean.valueOf(false);
//            removedSubscription.m_subscriberClientID = _clientID;
//            Object[] reObj = m_space.replace(subscriptionTemplate,
//                    removedSubscription, null, Lease.FOREVER);
//
//            //			for (int i = 0; i < reObj.length; i++)// remove the for - for
//            // debug only
//            //			{
//            //			   	 if ( reObj[i] instanceof Exception )
//            //				 {
//            //					Exception e = (Exception)reObj[i];
//            //					RemoteException re = new RemoteException("unsubscribeFromTopic()
//            // -- failed to replace(): ", e );
//            //					throw re;
//            //				 }
//            //				 else if ( reObj[i] == null )
//            //				 {
//            //				   String uid =
//            // ((JMSDurableSubDataEntry)reObj[i]).__getEntryInfo().m_UID;
//            //				   throw new RemoteException("Failed to replace() Entry UID " + uid
//            // + " - This entry is blocked by another user");
//            //				 }
//            //			}
//        }
//        catch (TransactionException te)
//        {
//        	if( _logger.isLoggable(Level.FINE))
//			{
//				_logger.log(Level.FINE, "TransactionException inside GSTopicSessionImpl.unsubscribeFromTopic(): "
//                            + te.toString(), te );
//			}
//            JMSException e = new JMSException("TransactionException : "
//                    + te.toString());
//            e.setLinkedException(te);
//            throw e;
//        }
//        catch (RemoteException re)
//        {
//        	if( _logger.isLoggable(Level.FINE))
//			{
//				_logger.log(Level.FINE, "RemoteException inside GSTopicSessionImpl.unsubscribeFromTopic(): "
//                            + re.toString(), re );
//			}
//            JMSException e = new JMSException("RemoteException : "
//                    + re.toString());
//            e.setLinkedException(re);
//            throw e;
//        }
//        catch (UnusableEntryException uue)
//        {
//            if (uue instanceof EntryNotInSpaceException)
//            {
//            	if( _logger.isLoggable(Level.FINE))
//				{
//					_logger.log(Level.FINE, "GSTopicSessionImpl.unsubscribeFromTopic(): The Entry "
//                                + ((EntryNotInSpaceException) uue).getUID()
//                                + " No Longer In Space " + uue.getCause(), uue);
//
//				}
//            }
//            else
//            {
//                final JMSException ex = new JMSException(
//                        "UnusableEntryException : ");
//                ex.setLinkedException(uue);
//                throw ex;
//            }
//        }
//    }


    /**
     * Currently not supported.
     *
     * @see Session#createDurableSubscriber(Topic, String, String, boolean)
     */
    public TopicSubscriber createDurableSubscriber(Topic topic,
                                                   String name,
                                                   String messageSelector,
                                                   boolean noLocal)
            throws JMSException {
        ensureOpen();
        throw new JMSException("This version of JMS does not support durable subscribers.");
//    	ensureOpen();
//    	if (topic == null)
//    	{
//    		throw new InvalidDestinationException("Invalid destination: null");
//    	}
//    	if (m_consumers.size() > 0)
//		{
//			if( _logger.isLoggable(Level.SEVERE))
//			{
//				_logger.severe( "GSSession.addConsumer(): Sessions support only one consumer: " + toString() );
//			}
//			throw new JMSException("Sessions support only one consumer.");
//		}
//    	if( _logger.isLoggable(Level.FINEST))
//		{
//			_logger.log(Level.FINEST, "GSTopicSessionImpl.createDurableSubscriber(Topic, String, String, boolean)");
//		}
//        GSTopicSubscriberImpl durableSubscriber;
//        synchronized(this)
//        {
//	        //GERSHON ADDED A FIX: 25042006 - Not to throw an exception in such case since its a legal senario in the spec.
//            //supporting anonymous publishers
//            //if (topic == null)
//	        //    throw new InvalidDestinationException("Topic cannot be null");
//	        if (topic instanceof TemporaryTopic)
//	            throw new InvalidDestinationException(
//	                    "Attempt to create a durable subscription for a temporary topic");
//	        if (name == null || name.trim().length() == 0)
//	            throw new JMSException("Null or empty subscription");
//
//	        //ensure that no durable subscriber has already been created
//	        // with the same name
//	        if (m_durableNames.contains(name))
//	        {
//	            throw new JMSException(
//	                    "A durable subscriber already exists with the name: "
//	                            + name);
//	        }//TODO
//	        //check to see if the topic is a temporary topic. You cannot
//	        // create a durable subscriber for a temporary topic
//	        //		if (((GSTopicImpl) topic).isTemporaryDestination()) {
//	        //		 throw new InvalidDestinationException(
//	        //			 "Cannot create a durable subscriber for a temporary topic");
//	        //		}
//
//
//            String topicName = "";
//            if(topic != null)
//               topicName = topic.getTopicName();
//
//	        GSJMSAdmin _admin = getConn().connFacParent.getAdmin();
//	        Hashtable existingDurDetailsHash = _admin
//	                .getDurableSubscriberDetailsFromXML(topicName, name);
//
//	        //setting the connection clientID to the Destination name together with
//	        // the space name
//	        String clientID = null;
//	        String consumerID = null;
//	        String sessionID = null;
//	        if (existingDurDetailsHash == null)
//	        {
//	            clientID = getConn().getClientID();
//	            if (clientID.indexOf(":") != -1)
//	                getConn().updateClientIDInternally(
//	                        clientID.substring(0, clientID.indexOf(":")) + ":"
//	                                + topicName);
//	            else
//	                getConn().updateClientIDInternally(
//	                        clientID + ":" + topicName);
//
//	            consumerID = nextConsumerId();
//	            //updating the jms-config.xml with the newly durable subscriber
//	            // details
//	            Hashtable<String, String> durbleSubscriberDetails = new Hashtable<String, String>();
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_CLIENT_ID, getConn()
//	                    .getClientID());
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_CONNECTION_ID,
//	                    getConn().getCnxKey());
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_SESSION_ID,
//	                    getSessionID());
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_TOPIC_NAME, topicName);
//	            durbleSubscriberDetails.put(GSJMSAdmin.DUR_SUB_SUBSCRIPTION_NAME,
//	                    name);
//	            if( _logger.isLoggable(Level.FINE))
//				{
//					_logger.fine( "GSTopicSessionImpl.createDurableSubscriber(): "
//					              + "updating jms-config.xml about newly subscribed DurableSubscriber details." );
//				}
//	            _admin
//	                    .addNewDurableSubscriberDetailsIntoXML(durbleSubscriberDetails);
//	        }
//	        else
//	        {
//	            clientID = existingDurDetailsHash.get(GSJMSAdmin.DUR_SUB_CLIENT_ID)
//	                    .toString();
//	            sessionID = existingDurDetailsHash.get(
//	                    GSJMSAdmin.DUR_SUB_SESSION_ID).toString();
//	            String connectionID = existingDurDetailsHash.get(
//	                    GSJMSAdmin.DUR_SUB_CONNECTION_ID).toString();
//
//	            //overwrite the existing getConn()/sess id's
//	            getConn().setCnxKey(connectionID);
//	            getConn().updateClientIDInternally(clientID);
//	            setSessionID(sessionID);
//	            String consumerStr = sessionID;
//	            consumerID = JSpaceUtilities.replaceInString(consumerStr, "_sess_",
//	                    "_cons_", false);
//	            nextConsumerId();//we still increment the m_consumers counter after
//	            // using the durable client details
//	            if( _logger.isLoggable(Level.FINE))
//				{
//					_logger.fine( "GSTopicSessionImpl.createDurableSubscriber(): "
//					              + "loading existing subscribed DurableSubscriber details from jms-config.xml file." );
//				}
//	        }
//	        if( _logger.isLoggable(Level.FINE))
//			{
//				_logger.fine( "GSTopicSessionImpl.createDurableSubscriber(): || sessionID:"
//	                        + getSessionID() + " || consumerID: " + consumerID
//	                        + " || clientID: " + clientID );
//			}
//
//	        //adding the new subscription name into the the HashSet which manages
//	        // all the subscriptions for this session
//	        //and assures that no duplicated subscription names will exist.
//	        m_durableNames.add(name);
//           if( !(topic instanceof GSTopicImpl) )
//           {
//        	   if( _logger.isLoggable(Level.FINE))
//        	   {
//        		   _logger.fine( "GSTopicSessionImpl.createTopicSession(): "
//        		                 + "Invalid JMS Topic of type: " + topic.getClass().getName() );
//        	   }
//           }
//	        durableSubscriber = new GSTopicSubscriberImpl(this, (GSTopicImpl) topic, consumerID, name, messageSelector,
//	                noLocal, getParserManager());
//	        addConsumer(durableSubscriber);
//        }
//        return durableSubscriber;
    }


    /**
     * Currently not supported.
     *
     * @see Session#createDurableSubscriber(Topic, String)
     */
    public TopicSubscriber createDurableSubscriber(Topic topic, String name)
            throws JMSException {
        ensureOpen();
        throw new JMSException("This version of JMS does not support durable subscribers.");
//    	if( _logger.isLoggable(Level.FINEST))
//		{
//			_logger.log(Level.FINEST, "GSTopicSessionImpl.createDurableSubscriber(Topic, String)");
//		}
//        return this.createDurableSubscriber(topic, name, null, false);
    }


//    /**
//     * Un-register a subscriber.
//     *
//     * @param subscriber
//     *            the subscriber to deregister
//     * @throws JMSException
//     *             if the subscriber cannot be deregistered from the server
//     */
//    protected void removeSubscriber(GSTopicSubscriberImpl subscriber)
//    	throws JMSException
//    {
//    	if( _logger.isLoggable(Level.FINEST))
//		{
//    		_logger.log(Level.FINEST, "GSTopicSessionImpl.removeSubscriber(GSTopicSubscriberImpl)");
//		}
//        if (!isClosed())
//        {
//            removeConsumer(subscriber);
//            if (subscriber.isDurableSubscriber())
//            {
//                m_durableNames.remove(subscriber.getName());
//            }
//        }
//    }


    /**
     * Creates a <CODE>TemporaryTopic</CODE> object. Its lifetime will be that of the
     * <CODE>TopicConnection</CODE> unless it is deleted earlier.
     *
     * We use the same space proxy we obtained in the session and we write another External Entry,
     * which will have a class name of 'TempQueue_ < >.
     *
     * The only message m_consumers that can consume from a temporary destination are those created
     * by the same connection that created the destination. Any message producer can send to the
     * temporary destination. If you close the connection that a temporary destination belongs to,
     * the destination is closed and its contents lost. You can use temporary destinations to
     * implement a simple request/reply mechanism. If you create a temporary destination and specify
     * it as the value of the JMSReplyTo message header field when you send a message, the consumer
     * of the message can use the value of the JMSReplyTo field as the destination to which it sends
     * a reply and can also reference the original request by setting the JMSCorrelationID header
     * field of the reply message to the value of the JMSMessageID header field of the request.
     *
     * @see Session#createTemporaryQueue()
     */
    public synchronized TemporaryTopic createTemporaryTopic() throws JMSException {
        ensureOpen();
        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "Creating temporary Topic");
        }
        return new GSTemporaryTopicImpl(m_sessionID + ":" + getRandomInt(), m_sessionID);
    }


    /**
     * @see Session#createTopic(String)
     */
    public Topic createTopic(String topicName) throws JMSException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "GSTopicSessionImpl.createTopic(String)");
        }
        GSTopicImpl topic;
        synchronized (this) {
            ensureOpen();
            if (!StringsUtils.isEmpty(topicName)) {
                topic = new GSTopicImpl(topicName);//TODO register in jndi
            } else {
                throw new JMSException("Invalid or null topic name specified");
            }
        }
        return topic;
    }


    /**
     * @see Session#createQueue(String)
     */
    public Queue createQueue(String queueName)
            throws JMSException {
        GSQueueImpl queue;
        synchronized (this) {
            ensureOpen();
            if (!StringsUtils.isEmpty(queueName)) {
                queue = new GSQueueImpl(queueName);//TODO register in jndi
            } else {
                throw new JMSException("Invalid or null queue name specified");
            }
        }
        return queue;
    }

    /**
     * @see Session#createBrowser(Queue)
     */
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        GSQueueBrowserImpl browser;
        synchronized (this) {
            ensureOpen();
            if (queue == null) {
                throw new InvalidDestinationException("Cannot browse a null queue.");
            }
            browser = new GSQueueBrowserImpl(this, queue, null);
            addBrowser(browser);
        }
        return browser;
    }

    /**
     * @see Session#createBrowser(Queue, String)
     */
    public QueueBrowser createBrowser(Queue queue, String messageSelector)
            throws JMSException {
        GSQueueBrowserImpl browser;
        synchronized (this) {
            ensureOpen();
            if (queue == null) {
                throw new InvalidDestinationException("Cannot browse a null queue.");
            }
            browser = new GSQueueBrowserImpl(this, queue, messageSelector);
            addBrowser(browser);
        }
        return browser;
    }


    /*
    * Add a browser to the list of m_browsers managed by this session
    *
    * @param browser the browser to add
    */
    protected void addBrowser(GSQueueBrowserImpl browser) {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "GSQueueSessionImpl.addBrowser(GSQueueBrowserImpl)");
        }
        if (m_browsers == null) {
            m_browsers = new Vector<GSQueueBrowserImpl>();
        }
        m_browsers.addElement(browser);
    }

    /*
     * Remove the browser from the list of managed m_browsers
     * by this session.
     * @param browser the browser to remove
     */
    protected void removeBrower(GSQueueBrowserImpl browser) {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.log(Level.FINEST, "GSQueueSessionImpl.removeBrower(GSQueueBrowserImpl)");
        }
        m_browsers.remove(browser);
        browser = null;
    }

    /**
     * We use the same space proxy we obtained in the session and we write another External Entry,
     * which will have a class name of 'TempQueue'.
     *
     * The only message consumers that can consume from a temporary destination are those created by
     * the same connection that created the destination. Any message producer can send to the
     * temporary destination. If you close the connection that a temporary destination belongs to,
     * the destination is closed and its contents lost. You can use temporary destinations to
     * implement a simple request/reply mechanism. If you create a temporary destination and specify
     * it as the value of the JMSReplyTo message header field when you send a message, the consumer
     * of the message can use the value of the JMSReplyTo field as the destination to which it sends
     * a reply and can also reference the original request by setting the JMSCorrelationID header
     * field of the reply message to the value of the JMSMessageID header field of the request.
     *
     * @see Session#createTemporaryQueue()
     */
    public synchronized TemporaryQueue createTemporaryQueue() throws JMSException {
        ensureOpen();
        if (_logger.isLoggable(Level.FINE)) {
            _logger.log(Level.FINE, "Creating temporary Queue");
        }
        return new GSTemporaryQueueImpl(m_sessionID + ":" + getRandomInt(), m_sessionID);
    }


    /**
     * @see TopicSession#createPublisher(Topic)
     */
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return (TopicPublisher) createProducer(topic);
    }


    /**
     * @see TopicSession#createSubscriber(Topic)
     */
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return (TopicSubscriber) createConsumer(topic);
    }


    /**
     * @see TopicSession#createSubscriber(Topic, String, boolean)
     */
    public TopicSubscriber createSubscriber(Topic topic,
                                            String messageSelector,
                                            boolean noLocal)
            throws JMSException {
        return (TopicSubscriber) createConsumer(topic, messageSelector, noLocal);
    }


    /**
     * @see QueueSession#createReceiver(Queue)
     */
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return createReceiver(queue, null);
    }


    /**
     * @see QueueSession#createReceiver(Queue, String)
     */
    public QueueReceiver createReceiver(Queue queue, String messageSelector)
            throws JMSException {
        return (QueueReceiver) createConsumer(queue, messageSelector);
    }


    /**
     * @see QueueSession#createSender(Queue)
     */
    public QueueSender createSender(Queue queue) throws JMSException {
        return (QueueSender) createProducer(queue);
    }

    /**
     * An entry in the message queue, that maps a message to it's proper consumer.
     */
    class MessageQueueElement {

        private String consumerId;
        private GSMessageImpl message;

        public MessageQueueElement(String consumerId, GSMessageImpl message) {
            this.consumerId = consumerId;
            this.message = message;
        }

        public String getConsumerId() {
            return consumerId;
        }

        public GSMessageImpl getMessage() {
            return message;
        }
    }

}//end of class
