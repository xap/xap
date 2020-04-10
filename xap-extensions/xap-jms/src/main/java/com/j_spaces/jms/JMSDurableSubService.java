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

import com.gigaspaces.events.DataEventSession;
import com.gigaspaces.events.DataEventSessionFactory;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.NotifyModifiers;
import com.j_spaces.core.client.SpaceFinder;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Vector;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;

/**
 * @author Gershon Diner
 * @version 4.0
 */
public class JMSDurableSubService extends GSThread {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_JMS);
    private static final String DELIM = "!#$%";

    private static final String[] FIELDS_NAMES = new String[]{
            //The Message Body
            GSMessageImpl.BODY_STR_NAME,
            //The Message Header
            GSMessageImpl.JMS_DESTINATION,
            GSMessageImpl.JMS_DELIVERY_MODE,
            GSMessageImpl.JMS_EXPIRATION,
            GSMessageImpl.JMS_PRIORITY,
            GSMessageImpl.JMS_MESSAGE_ID,
            GSMessageImpl.JMS_TIMESTAMP,
            GSMessageImpl.JMS_CORRELATION_ID,//it might me String or byte[]
            GSMessageImpl.JMS_REPLY_TO,
            GSMessageImpl.JMS_TYPE,
            GSMessageImpl.JMS_REDELIVERED,
            //The Message JMS_GS* Properties
            GSMessageImpl.JMS_GSPRODUCER_KEY_PROP_NAME,
            //GSMessageImpl.JMS_GSEXTERNAL_ENTRY_UID_PROP_NAME,
            GSMessageImpl.JMS_GSTTL_KEY_PROP_NAME,
            //Currently supported Message JMSX Properties
            //set by the client
            GSMessageImpl.JMSX_GROUPID,
            GSMessageImpl.JMSX_GROUPSEQ,
            //set in send()
            GSMessageImpl.JMSX_USERID,
            //GSMessageImpl.JMSX_APPID,
            //set in receive()
            //GSMessageImpl.JMSX_RCV_TIMESTEMP,
            //Message properties
            GSMessageImpl.PROPERTIES_STR_NAME};

    private static final String FIELDS_TYPES[] = new String[]{
            Object.class.getName(),                 //Body 0
            javax.jms.Destination.class.getName(),  //JMSDestination 1
            Integer.class.getName(),                //JMSDeliveryMode 2 	//FIXED vals
            Long.class.getName(),                   //JMSExpiration 3
            Integer.class.getName(),                //JMSPriority 4		//FIXED vals
            String.class.getName(),                 //JMSMessageID 5
            Long.class.getName(),                   //JMSTimestamp 6
            Object.class.getName(),                 //JMSCorrelationID 7
            javax.jms.Destination.class.getName(),  //JMSReplyTo 8
            String.class.getName(),                 //JMSType 9				//FIXED vals
            Boolean.class.getName(),                //JMSRedelivered 10		//FIXED vals
            String.class.getName(),                 //JMS_GSProducerKey 11
            Long.class.getName(),                   //JMS_GSTTLKey 12
            String.class.getName(),                 //JMSXGroupID 13
            Integer.class.getName(),                //JMSXGroupSeq 14
            String.class.getName(),                 //JMSXUserID 15			//FIXED vals
            java.util.HashMap.class.getName()       //Properties 16
    };

    private boolean m_ShouldShutdown = false;
    private final Object m_ShutdownMonitor;

    /**
     * Hashtable contains the following key/val pairs:
     *
     * key: ID (clientID + subscription name + subscribedTopicName ) val: JMSDurableSubDataEntry
     */
    private final Hashtable<String, JMSDurableSubDataEntry> m_subscriptionNamesHash;

    /**
     * Hashtable contains the following key/val pairs:
     *
     * key: subscribedTopicName val: Hashtable of JMSDurableSubDataEntry (the same
     * JMSDurableSubDataEntry reference as in m_subscriptionNamesHash) instances. Every topic in
     * this hash has a hashtable of all the subsription objects, sorted with their ID as keys.
     *
     * When the consumer closes a lease expiration event is fired, and as a result we assume that
     * this durable subsriber became offline, we add the right JMSDurableSubDataEntry subsription
     * object to the hashtable of subsribers,
     *
     * When an DurableSubsriber.unsubsribe() is called we remove the approperiate subsription object
     * from the vector.
     */
    private final Hashtable m_topicsHash;

    /**
     * Hashtable contains the following key/val pairs:
     *
     * key: subscribedTopicName val: NotifyDelegator returned leases Used while processing Lease
     * expiration notifications- in order to renew/cancel the lease.
     */
    private final Hashtable m_registrations;

    private DataEventSession _eventSession;
    private final WriteRemoteEventListener m_WriteRemoteEventListener;
    private final IJSpace m_space;
    private ExternalEntry _defaultTemplate;

    public JMSDurableSubService(IJSpace space) {
        super("JMSDurableSubscriptionService");
        setDaemon(true);
        this.m_space = space;
        m_subscriptionNamesHash = new Hashtable<String, JMSDurableSubDataEntry>();
        m_topicsHash = new Hashtable();
        m_registrations = new Hashtable();
        m_ShutdownMonitor = new Object();
        SubscriptionRemoteEventListener m_subscriptionRemoteEventListener = new SubscriptionRemoteEventListener();
        m_WriteRemoteEventListener = new WriteRemoteEventListener();

        //init of NotifyDelegator for listening to the JMSDurableSubDataEntry entry.
        try {
            _eventSession = DataEventSessionFactory.create(space, new EventSessionConfig().setFifo(true));

            //we listen for take/write/expiration events
            //to update the pending entries vector
            NotifyActionType notifyType = NotifyActionType.NOTIFY_WRITE
                    .or(NotifyActionType.NOTIFY_TAKE)
                    .or(NotifyActionType.NOTIFY_LEASE_EXPIRATION);
            _eventSession.addListener(new JMSDurableSubDataEntry(), m_subscriptionRemoteEventListener, notifyType);

            //init of NotifyDelegator for listening to
            // the JMSOfflineStateDurSubDataEntry entry, that is fired when the
            // durable subscriber goes offline
            //we listen for expiration events that fired when the durable subscriber goes offline
            _eventSession.addListener(new JMSOfflineStateDurSubDataEntry(), m_subscriptionRemoteEventListener,
                    NotifyActionType.NOTIFY_LEASE_EXPIRATION);
        } catch (RemoteException e) {
            if (_logger.isErrorEnabled())
                _logger.error(e.toString(), e);
        }
    }

    /*
     * Implements the <code> net.jini.core.event.RemoteEventListener </code>
     * interface, for the NotifyDelegator. This listener gets the
     * EntryArrivedRemoteEvent and resolves the JMSDurableSubDataEntry or the
     * expired JMSOfflineStateDurSubDataEntry - out of it. - It is called when a
     * durable
     *  
     */
    class SubscriptionRemoteEventListener implements RemoteEventListener {
        final private ReentrantLock notifyLock = new ReentrantLock();

        /**
         * @see net.jini.core.event.RemoteEventListener#notify(net.jini.core.event.RemoteEvent)
         */
        public void notify(RemoteEvent theEvent) throws UnknownEventException,
                RemoteException {
            notifyLock.lock();
            try {
                //since we are using NotifyDelegator, we can obtain the
                // MetaDataEntry extansion entry that
                // triggered the event
                Object entry = ((EntryArrivedRemoteEvent) theEvent).getObject();//getting the JMSDurableSubDataEntry
                //System.out.println("SubscriptionRemoteEventListener.notify()
                // -- entry is of type: " + entry.getClass().getName() );
                int notifyType = ((EntryArrivedRemoteEvent) theEvent).getNotifyType();

                if (entry instanceof JMSOfflineStateDurSubDataEntry) {
                    JMSOfflineStateDurSubDataEntry jmsOfflineStateDurSubDataEntry = (JMSOfflineStateDurSubDataEntry) entry;
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(" SubscriptionRemoteEventListener.notify()  "
                                + jmsOfflineStateDurSubDataEntry.toString());
                    }

                    Boolean isSubscribed = jmsOfflineStateDurSubDataEntry.m_isSubscribed;
                    Boolean isSubscriberOnline = jmsOfflineStateDurSubDataEntry.m_isSubscriberOnline;
                    String durableSubscriptionName = jmsOfflineStateDurSubDataEntry.m_durableSubscriptionName;
                    String subscriberClientID = jmsOfflineStateDurSubDataEntry.m_subscriberClientID;
                    String subscribedTopicName = jmsOfflineStateDurSubDataEntry.m_topicName;

                    if (NotifyModifiers.isLeaseExpiration(notifyType)) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(" SubscriptionRemoteEventListener.notify() -- Lease has expired -- the durable subscriber went down.  || --  "
                                    + jmsOfflineStateDurSubDataEntry.toString());
                        }
                        //TODO actually- here we get into the offline durable
                        // subscriber zone ....
                        addOfflineStateSubscription(durableSubscriptionName,
                                subscribedTopicName, subscriberClientID);
                    }
                }//entry instanceof JMSOfflineStateDurSubDataEntry

                else if (entry instanceof JMSDurableSubDataEntry) {
                    JMSDurableSubDataEntry jmsDurableSubDataEntry = (JMSDurableSubDataEntry) entry;
                    if (_logger.isDebugEnabled()) {
                        _logger.debug(" SubscriptionRemoteEventListener.notify() "
                                + " || --  " + jmsDurableSubDataEntry.toString());
                    }

                    Boolean isSubscribed = jmsDurableSubDataEntry.m_isSubscribed;
                    Boolean isSubscriberOnline = jmsDurableSubDataEntry.m_isSubscriberOnline;
                    String durableSubscriptionName = jmsDurableSubDataEntry.m_durableSubscriptionName;
                    String subscriberClientID = jmsDurableSubDataEntry.m_subscriberClientID;
                    String subscribedTopicName = jmsDurableSubDataEntry.m_topicName;

                    JMSDurableSubDataEntry subscriptionDataEntry = getDurSubEntryFromSubscriptionNamesHash(
                            durableSubscriptionName, subscribedTopicName,
                            subscriberClientID);

                    //first time the durable consumer turns on we add the
                    // subscription entry (if still subscribed)
                    if (subscriptionDataEntry == null
                            && isSubscriberOnline.booleanValue()
                            && isSubscribed.booleanValue()) {
                        //adding for the first time, a subscription entry
                        // object
                        addOnlineStateSubsciption(durableSubscriptionName,
                                subscribedTopicName, subscriberClientID,
                                jmsDurableSubDataEntry);
                    }

                    //if there is already a subscription entry we check if the
                    // isSubscriberOnline flag == false:
                    //if it is false, it means that the consumer is online
                    // again so we can stop consuming its messages. (if still
                    // subscribed)
                    else if (subscriptionDataEntry != null
                            && !isSubscriberOnline.booleanValue()
                            && isSubscribed.booleanValue()) {

                        removeOfflineStateSubscription(durableSubscriptionName,
                                subscribedTopicName, subscriberClientID);
                    }

                    //handle the case when the topicSession.unsubscribed() is
                    // called
                    else if (subscriptionDataEntry != null
                            && !isSubscribed.booleanValue()) {
                        unsubscribe(durableSubscriptionName,
                                subscribedTopicName, subscriberClientID);
                    }
                }//entry instanceof JMSDurableSubDataEntry
            } catch (UnusableEntryException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(e.toString(), e);
                }
            } catch (JMSException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(e.toString(), e);
                }
            } finally {
                notifyLock.unlock();
            }

        }//end of notify()

    }//end of SubscriptionRemoteEventListener class

    /**
     * -------------------- END OF SubscriptionRemoteEventListener class
     * ------------------------
     */

    /**
     * Implements the <code>net.jini.core.event.RemoteEventListener</code> interface, for the
     * NotifyDelegator. This listener gets the ExternalEntry which was written to space by the
     * producer, while the durable subscriber is offline. In this case the entries are cought by
     * this listener, processed to find the right targeted topic subscription and clientID and keeps
     * the entry UID for later read when the durable subscriber will get online again.
     */
    class WriteRemoteEventListener implements RemoteEventListener {
        private ReentrantLock notifyLock = new ReentrantLock();

        public void notify(RemoteEvent theEvent) throws UnknownEventException, RemoteException {
            //since we are using NotifyDelegator, we can obtain the
            // JMSDurableSubDataEntry entry that
            // triggered the event
            notifyLock.lock();
            try {
                final EntryArrivedRemoteEvent spaceEvent = (EntryArrivedRemoteEvent) theEvent;
                final String uid = spaceEvent.getEntryPacket().getUID();
                final String className = spaceEvent.getEntryPacket().getTypeName();
                if (_logger.isDebugEnabled())
                    _logger.debug("WriteRemoteEventListener.notify() -- className: " + className + " | uid: " + uid);
                //the topic name of the destination to where this extEntry
                // suppose to be redirected and later consumed from
                String topicName = className;
                JMSDurableSubDataEntry[] updatedSubEntriesArray = updateDurSubEntriesFromTopicsHash(
                        topicName, uid);
                if (null != updatedSubEntriesArray) {
                    long[] leases = new long[updatedSubEntriesArray.length];
                    Arrays.fill(leases, Constants.Engine.UPDATE_NO_LEASE);
                    m_space.updateMultiple(updatedSubEntriesArray, null, leases);
                }
            } catch (UnusableEntryException e) {
                if (_logger.isErrorEnabled())
                    _logger.error(e.toString(), e);
            } catch (JMSException e) {
                if (_logger.isErrorEnabled())
                    _logger.error(e.toString(), e);
            } catch (RemoteException e) {
                if (_logger.isErrorEnabled())
                    _logger.error(e.toString(), e);
            } catch (TransactionException e) {
                if (_logger.isErrorEnabled())
                    _logger.error(e.toString(), e);
            } finally {
                notifyLock.unlock();
            }
        }
    }

    final private ReentrantLock unsubscribeLock = new ReentrantLock();

    /*
     * Unsubscribes a durable subscription that has been created by a client.
     * Called when the durable subscriber's topicSession.unsubscribed() is
     * called. Removing the JMSDurableSubDataEntry subscription entry from the
     * m_subscriptionNamesHash and the m_topicsHash of the specified topic and
     * clientID
     * 
     * We remove the JMSDurableSubDataEntry from the m_topicsHash's
     * subscriptionEntriesVec, which holds all the JMSDurableSubDataEntry
     * subscription entry's for that topic, while the subscription name and
     * clientID are matched too. @param subscriptionName @param
     * unsubscribedTopicName @param clientIDToUnSubscribe @throws JMSException
     *  
     */
    private void unsubscribe(String subscriptionName,
                             String unsubscribedTopicName, String clientIDToUnSubscribe) {
        unsubscribeLock.lock();
        try {
            String id = clientIDToUnSubscribe + DELIM + subscriptionName
                    + DELIM + unsubscribedTopicName;
            JMSDurableSubDataEntry subscriptionEntryToRemove = m_subscriptionNamesHash
                    .remove(id);
            if (_logger.isDebugEnabled()) {
                _logger.debug(" unsubscribe() with ID: " + id);
            }
            Vector subscriptionEntriesVec = (Vector) m_topicsHash
                    .remove(unsubscribedTopicName);
            for (int i = 0; i < subscriptionEntriesVec.size(); i++) {
                JMSDurableSubDataEntry subscriptionEntry = (JMSDurableSubDataEntry) subscriptionEntriesVec
                        .get(i);
                //if we fetched the subscription entry with the same
                // subscription name and clientID
                //then we remove it from the subscription entries vector
                if (subscriptionEntry.m_durableSubscriptionName
                        .equalsIgnoreCase(subscriptionName)
                        && subscriptionEntry.m_subscriberClientID
                        .equalsIgnoreCase(clientIDToUnSubscribe)) {
                    subscriptionEntriesVec.remove(i);
                }
            }//end of for
            m_topicsHash.put(unsubscribedTopicName, subscriptionEntriesVec);

            //if there are no more subscription entry objects in the vector for
            // that topic,
            //then we can cancel the ND for the ExternalEntry of that topic.
            //if there are still objects in the vec, then it means that there
            // are still more
            //durable subscribers registered on that topic, so we leave the ND
            // listening.
            if (subscriptionEntriesVec.size() <= 0) {
                EventRegistration registration = (EventRegistration) m_registrations.get(unsubscribedTopicName);
                if (registration != null)
                    try {
                        _eventSession.removeListener(registration);
                    } catch (UnknownLeaseException e) {
                        if (_logger.isDebugEnabled())
                            _logger.debug("unsubscribe() cancel ND lease:  " + e.toString(), e);
                    } catch (RemoteException e) {
                        if (_logger.isDebugEnabled())
                            _logger.debug("unsubscribe() cancel ND lease:  " + e.toString(), e);
                    }
            }
        } finally {
            unsubscribeLock.unlock();
        }
    }

    private JMSDurableSubDataEntry getDurSubEntryFromSubscriptionNamesHash(
            String subscriptionName, String subscribedTopicName,
            String clientIDToSubscribe) throws JMSException {
        String id = clientIDToSubscribe + DELIM + subscriptionName + DELIM
                + subscribedTopicName;
        Object retObj = m_subscriptionNamesHash.get(id);
        if (retObj != null)
            return (JMSDurableSubDataEntry) retObj;
        return null;
    }

    private JMSDurableSubDataEntry[] updateDurSubEntriesFromTopicsHash(
            String subscribedTopicName, String offlineExtEntryUID)
            throws JMSException {
        Vector subscriptionEntriesVec = (Vector) m_topicsHash
                .get(subscribedTopicName);
        JMSDurableSubDataEntry[] jmsDurableSubDataEntryArray = new JMSDurableSubDataEntry[subscriptionEntriesVec
                .size()];
        for (int i = 0; i < subscriptionEntriesVec.size(); i++) {
            JMSDurableSubDataEntry subscriptionEntry = (JMSDurableSubDataEntry) subscriptionEntriesVec
                    .get(i);
            subscriptionEntry.m_offlineEntryUIDsVec
                    .addElement(offlineExtEntryUID);
            jmsDurableSubDataEntryArray[i] = subscriptionEntry;
        }
        return jmsDurableSubDataEntryArray;
    }

    /**
     * If there is already a subscription entry for that topic with matched clientID and
     * subscriptionName, we check if the isSubscriberOnline flag == false: if it is false, it means
     * that the consumer is online AGAIN so we can stop consuming its messages, via the
     * subscriptionEntriesVec at the m_topicsHash.
     */
    private void removeOfflineStateSubscription(String subscriptionName,
                                                String subscribedTopicName, String clientIDToSubscribe)
            throws JMSException {
        String id = clientIDToSubscribe + DELIM + subscriptionName + DELIM
                + subscribedTopicName;
        JMSDurableSubDataEntry subscriptionEntryToRemove = m_subscriptionNamesHash
                .get(id);
        if (_logger.isDebugEnabled()) {
            _logger.debug(" removeOfflineStateSubscription() with ID: " + id);
        }

        Vector subscriptionEntriesVec = (Vector) m_topicsHash
                .get(subscribedTopicName);
        for (int i = 0; i < subscriptionEntriesVec.size(); i++) {
            JMSDurableSubDataEntry subscriptionEntry = (JMSDurableSubDataEntry) subscriptionEntriesVec
                    .get(i);
            //if we fetched the subscription entry with the same subscription
            // name and clientID
            //then we remove it from the subscription entries vector
            if (subscriptionEntry.m_durableSubscriptionName
                    .equalsIgnoreCase(subscriptionName)
                    && subscriptionEntry.m_subscriberClientID
                    .equalsIgnoreCase(clientIDToSubscribe)) {
                subscriptionEntriesVec.remove(i);
            }
        }//end of for
    }

    /*
     * Called when the durable subscriber goes offline.
     * 
     * Adding the JMSDurableSubDataEntry ( the same instance which is used in
     * the m_subscriptionNamesHash) to the m_topicsHash, or more specificly, to
     * the subscriptionEntriesVec which holds all the JMSDurableSubDataEntry
     * (which meet the same clientID and same subscription name). All these
     * JMSDurableSubDataEntry in the subscriptionEntriesVec are for the same
     * topic and were, added into the subscriptionEntriesVec only when the
     * subscriber went offline, therefore when messages are sent to topic
     * MyTopic, then all the entry's cought by the
     * ExternalEntryWriteRemoteEventListener and added into the
     * JMSDurableSubDataEntry.m_offlineEntryUIDsVec for each one of the
     * JMSDurableSubDataEntry subscription entry elements in the
     * subscriptionEntriesVec.
     * 
     * @param subscriptionName @param subscribedTopicName @param
     * clientIDToSubscribe @throws JMSException
     */
    private void addOfflineStateSubscription(String subscriptionName,
                                             String subscribedTopicName, String clientIDToSubscribe)
            throws JMSException {
        String id = clientIDToSubscribe + DELIM + subscriptionName + DELIM
                + subscribedTopicName;
        JMSDurableSubDataEntry subscriptionEntry = m_subscriptionNamesHash.get(id);
        if (_logger.isDebugEnabled()) {
            _logger.debug("JMSDurableSubService.addOfflineStateSubscription() with ID: " + id);
        }
        Vector subscriptionEntriesVec = (Vector) m_topicsHash.remove(subscribedTopicName);
        subscriptionEntriesVec.addElement(subscriptionEntry);

        m_topicsHash.put(subscribedTopicName, subscriptionEntriesVec);
        //if it is the first time we create ND for this subscribedTopicName
        if (m_registrations.get(subscribedTopicName) == null) {
            //preparing the template for the JMSDurableSubDataEntry ND
            // according to the subscription and topic names
            Object offlineWrittenExtEntryTemplate = getDefaultTemplate(subscribedTopicName);
            try {
                EventRegistration registration = _eventSession.addListener(offlineWrittenExtEntryTemplate,
                        m_WriteRemoteEventListener, NotifyActionType.NOTIFY_WRITE);
                m_registrations.put(subscribedTopicName, registration);
            } catch (RemoteException e) {
                if (_logger.isErrorEnabled())
                    _logger.error(e.toString(), e);
            }
        }
    }

    /*
     * 
     * @param subscriptionName @param subscribedTopicName @param
     * clientIDToSubscribe @param subsriptionDataEntry @throws JMSException
     */
    private void addOnlineStateSubsciption(String subscriptionName,
                                           String subscribedTopicName, String clientIDToSubscribe,
                                           JMSDurableSubDataEntry subsriptionDataEntry) throws JMSException {
        String id = clientIDToSubscribe + DELIM + subscriptionName + DELIM
                + subscribedTopicName;
        if (_logger.isDebugEnabled()) {
            _logger.debug("JMSDurableSubService.addOnlineStateSubscription() with ID: " + id);
        }
        m_subscriptionNamesHash.put(id, subsriptionDataEntry);

        //adding to the topics hash the new subscribedTopicName key with a
        // blank
        //vector which will include all the JMSDurableSubDataEntry that
        //are subscribed to this topic and were offline while these entries
        // were sent.
        //If there is already a key with the subscribedTopicName, we add the
        // JMSDurableSubDataEntry
        //to the subscriptionEntriesVec.
        Object subscriptionEntriesVec = m_topicsHash.get(subscribedTopicName);
        if (subscriptionEntriesVec == null)
            m_topicsHash.put(subscribedTopicName, new Vector());
    }

    /*
     * Returns the defualt ExternalEntry which is used in space
     * read/clear/notify operations That entry holds the default 19 values/field
     * names /field types @param destName
     */
    private Object getDefaultTemplate(String destName) {
        if (_defaultTemplate == null || !_defaultTemplate.m_ClassName.equalsIgnoreCase(destName)) {
            _defaultTemplate = new ExternalEntry(destName, new Object[17], FIELDS_NAMES, FIELDS_TYPES);
            _defaultTemplate.setFifo(true);
        }
        return _defaultTemplate;
    }

    /*
     * @see java.lang.Runnable#run()
     */
    public void run() {
        //System.out.println("JMSDurableSubService.run() " );
        //		if(somthing != null)
        //		{
        while (!shouldShutdown()) {
            try {
                //Thread.sleep(
                // Long.parseLong(System.getProperty("com.gs.mdn.refresh_rate")));
                Thread.sleep(2000);
            } catch (InterruptedException ie) {
                if (_logger.isTraceEnabled()) {
                    _logger.trace(this.getName() + " interrupted.", ie);
                }

                //Restore the interrupted status
                interrupt();

                //fall through
                break;
            }

            //				if(!shouldShutdown())
            //					//
        }
        //		}
    }

    /*
     * Returns <code> true </code> if should shutdown, otherwise <code> false
     * </code> .
     */
    private boolean shouldShutdown() {
        synchronized (m_ShutdownMonitor) {
            return m_ShouldShutdown;
        }
    }

    public void shutdown() {
        if (_logger.isDebugEnabled()) {
            _logger.debug("inside shutdown() ");
        }
        synchronized (m_ShutdownMonitor) {
            // check if shutdown already executed
            if (m_ShouldShutdown)
                return;

            m_ShouldShutdown = true;
        }
        shutdown();
    }

    /**
     * Main for standalone service tests
     */
    public static void main(String[] args) {
        IJSpace space = null;
        try {
            space = (IJSpace) SpaceFinder.find(args[0]);
        } catch (FinderException e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.toString(), e);
            }
        }
        new JMSDurableSubService(space);
    }

}
