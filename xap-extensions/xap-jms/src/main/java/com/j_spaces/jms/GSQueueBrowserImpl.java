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
 * Created on 26/06/2004
 *
 * @author		Gershon Diner
 * Title:        	The GigaSpaces Platform
 * Copyright:   Copyright (c) GigaSpaces Team 2004
 * Company:    GigaSpaces Technologies Ltd.
 * @version 	 4.0
 */
package com.j_spaces.jms;

import com.gigaspaces.client.iterator.GSIteratorConfig;
import com.gigaspaces.client.iterator.IteratorScope;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.GSIterator;

import net.jini.core.entry.UnusableEntryException;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;

/**
 * GigaSpaces implementation of the <code>javax.jms.QueueBrowser</code> interface. A client uses a
 * <CODE>QueueBrowser</CODE> object to look at messages on a m_queue without removing them.
 *
 * <P>The <CODE>getEnumeration</CODE> method returns a <CODE>java.util.Enumeration</CODE> that is
 * used to scan the m_queue's messages. It may be an enumeration of the entire content of a m_queue,
 * or it may contain only the messages matching a message m_selector. (currently we support only a
 * custom m_selector hash based implementation) The getEnumeration() calls the space <CODE>{@link
 * GSIterator}</CODE> method which returns all the currently existing ExternalEntries from the
 * specified Queue/class name.
 *
 * @author Gershon Diner
 * @version 4.0 Copyright: Copyright (c) 2004 Company: GigaSpaces Technologies,Ltd.
 **/
public class GSQueueBrowserImpl implements QueueBrowser {

    /**
     * The session the browser belongs to.
     */
    private GSSessionImpl m_session;
    /**
     * The m_queue the browser browses.
     */
    private Queue m_queue;
    /**
     * The m_selector for filtering messages.
     */
    private String m_selector;

    /**
     * <code>true</code> if the browser is m_closed.
     */
    private boolean m_closed = false;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_JMS);

    /**
     * The iterator buffer size used for the QueueBrowser. Defaults to 1000 objects.
     */
    private static final int iteratorBufferSize = Integer.getInteger("com.gs.jms.iterator.buffersize", 1000);
    private GSIterator gsIterator;

    /**
     * Constructs a browser.
     *
     * @param sess     The session the browser belongs to.
     * @param queue    The queue the browser browses.
     * @param selector The selector for filtering messages.
     * @throws InvalidSelectorException If the m_selector syntax is invalid.
     * @throws IllegalStateException    If the connection is broken.
     * @throws JMSException             If the creation fails for any other reason.
     */
    GSQueueBrowserImpl(GSSessionImpl sess, Queue queue, String selector)
            throws JMSException {
        this.m_session = sess;
        this.m_queue = queue;
        this.m_selector = selector;
        if (m_queue == null)
            throw new InvalidDestinationException("GSQueueBrowserImpl Invalid queue: " + m_queue);

        GSMessageImpl template = new GSMessageImpl();
        template.setDestinationName(m_queue.getQueueName());

        ArrayList<GSMessageImpl> templates = new ArrayList<GSMessageImpl>(1);
        templates.add(template);
        GSIteratorConfig config = new GSIteratorConfig();
        config.setBufferSize(iteratorBufferSize);
        config.setIteratorScope(IteratorScope.CURRENT_AND_FUTURE);
        try {
            gsIterator = new GSIterator(m_session.getConn().getSpace(), templates, config);
        } catch (RemoteException re) {
            if (_logger.isErrorEnabled()) {
                _logger.error(
                        "Exception inside GSQueueBrowserImpl: "
                                + re.toString(), re);
            }
            JMSException e = new JMSException("RemoteException: " + re.toString());
            e.setLinkedException(re);
            throw e;
        } catch (UnusableEntryException uue) {
            if (uue instanceof EntryNotInSpaceException) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("The Entry " +
                            ((EntryNotInSpaceException) uue).getUID() +
                            " is no longer in Space " + uue.getCause(), uue);
                }
            } else {
                final JMSException ex = new JMSException("UnusableEntryException : ");
                ex.setLinkedException(uue);
                throw ex;
            }
        }
    }

    /**
     * Returns a string view of this browser.
     */
    public String toString() {
        return "QueueBrowser for the session:" + m_session.toString();
    }

    /**
     * API method.
     *
     * @throws IllegalStateException If the browser is m_closed.
     */
    public Queue getQueue() throws JMSException {
        if (m_closed)
            throw new IllegalStateException("Forbidden call on a closed browser.");

        return m_queue;
    }

    /**
     * API method.
     *
     * @throws IllegalStateException If the browser is m_closed.
     */
    public String getMessageSelector() throws JMSException {
        if (m_closed)
            throw new IllegalStateException("Forbidden call on a closed browser.");

        return this.m_selector;
    }


    /**
     * API method.
     *
     * @throws IllegalStateException If the browser is m_closed, or if the connection is broken.
     * @throws JMSSecurityException  If the client is not a READER on the m_queue.
     * @throws JMSException          If the request fails for any other reason.
     */
    public Enumeration getEnumeration() throws JMSException {
        if (m_closed)
            throw new IllegalStateException("Forbidden call on a closed browser.");
        // Return an enumeration:
        return new QueueEnumeration(gsIterator);
    }

    /**
     * API method.
     *
     * @throws JMSException Actually never thrown.
     */
    public void close() throws JMSException {
//	Ignoring the call if the browser is already m_closed:
        try {
            if (!m_closed) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("GSQueueBrowserImpl.close() closing browser: " + toString());
                }
                // unregister this browser from the session before closing, and
                // then call the base class method.
                m_session.removeBrower(this);
            }
            m_closed = true;
        } finally {
            //canceling the iterator if still exists
            if (null != gsIterator) {
                gsIterator.cancel();
                gsIterator = null;
            }
        }
    }


    /*
     * The <code>QueueEnumeration</code> class is used to enumerate the browses
     * sent by queues.
     */
    private class QueueEnumeration implements Enumeration {
        private GSIterator iterator;

        /*
         * Constructs a <code>QueueEnumeration</code> instance.
         *
         * @param messages  The GSIterator to enumerate with.
         */
        private QueueEnumeration(GSIterator _gsIterator) {
            this.iterator = _gsIterator;
        }

        /**
         * API method.
         */
        public boolean hasMoreElements() {
            if (iterator == null)
                return false;
            return iterator.hasNext();
        }

        /**
         * API method.
         */
        public Object nextElement() {
            if (iterator == null || !iterator.hasNext())
                throw new NoSuchElementException();

            //Now we iterate to the next ExternalEntry and then
            //call to the converter to return a GSMessageImpl
            Object o = iterator.next();
            GSMessageImpl msg = (GSMessageImpl) o;
            msg.setBodyReadOnly(true);
            msg.setPropertiesReadOnly(true);

            return o;
        }
    }//end of inner class QueueEnumeration
}//end of class GSQueueBrowserImpl
