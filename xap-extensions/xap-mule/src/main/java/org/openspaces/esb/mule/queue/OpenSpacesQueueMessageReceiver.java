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


package org.openspaces.esb.mule.queue;


import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.query.ISpaceQuery;
import com.j_spaces.core.exception.SpaceUnavailableException;

import org.mule.DefaultMuleMessage;
import org.mule.VoidMuleEvent;
import org.mule.api.MuleContext;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.construct.FlowConstruct;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.service.Service;
import org.mule.api.transport.Connector;
import org.mule.api.transport.PropertyScope;
import org.mule.transport.NullPayload;
import org.mule.transport.PollingReceiverWorker;
import org.mule.transport.TransactedPollingMessageReceiver;
import org.openspaces.core.SpaceClosedException;
import org.openspaces.core.SpaceInterruptedException;

import java.util.LinkedList;
import java.util.List;

/**
 * Receives (takes) a message from an internal queue. The queue is a virtualized queue represented
 * by the {@link org.openspaces.esb.mule.queue.OpenSpacesQueueObject} with its endpoint address set
 * (and not the message).
 *
 * @author kimchy
 */
public class OpenSpacesQueueMessageReceiver extends TransactedPollingMessageReceiver {
    private static final MuleEvent voidEvent = new VoidMuleEvent() {
        // This override is required for org.mule.transport.TransactedPollingMessageReceiver$1.process(TransactedPollingMessageReceiver.java:161) ~[mule-core-3.7.0.jar:3.7.0]
        @Override
        public MuleContext getMuleContext() {
            return null;
        }
    };

    private OpenSpacesQueueConnector connector;


    private ISpaceQuery<OpenSpacesQueueObject> template;

    public OpenSpacesQueueMessageReceiver(Connector connector, FlowConstruct flowConstruct, InboundEndpoint endpoint) throws CreateException {
        super(connector, flowConstruct, endpoint);
        init(connector, endpoint);
    }

    public OpenSpacesQueueMessageReceiver(Connector connector,
                                          Service service,
                                          final InboundEndpoint endpoint) throws CreateException {
        super(connector, service, endpoint);
        init(connector, endpoint);
    }

    private void init(Connector connector, final InboundEndpoint endpoint) {
        this.connector = (OpenSpacesQueueConnector) connector;
        this.setReceiveMessagesInTransaction(endpoint.getTransactionConfig().isTransacted());
        // use the defined timeout to set the frequency of the non-blocking polling
        this.setFrequency(this.connector.getTimeout() / 10L);
    }

    protected void doConnect() throws Exception {
        OpenSpacesQueueObject internalTemplate = connector.newQueueTemplate(endpoint.getEndpointURI().getAddress());
        template = connector.getGigaSpaceObj().prepareTemplate(internalTemplate);
    }

    protected void doDispose() {
        // template method
    }

    protected void doDisconnect() throws Exception {
        // template method
    }

    public Object onCall(MuleMessage message, boolean synchronous) throws MuleException {
        // Rewrite the message to treat it as a new message
        MuleMessage newMessage = new DefaultMuleMessage(message);
        return routeMessage(newMessage);
    }

    protected List getMessages() throws Exception {
        // The list of retrieved messages that will be returned
        List<MuleMessage> messages = new LinkedList<MuleMessage>();

        // try to get the first event off the queue
        try {
            /*
             * Determine how many messages to batch in this poll: we need to drain the queue quickly, but not by
             * slamming the workManager too hard. It is impossible to determine this more precisely without proper
             * load statistics/feedback or some kind of "event cost estimate". Therefore we just try to use half
             * of the receiver's workManager, since it is shared with receivers for other endpoints.
             */

            OpenSpacesQueueObject entry = connector.getGigaSpaceObj().take(template);

            if (entry != null) {
                appendMessage(messages, entry);
                // batch more messages if needed
                OpenSpacesQueueObject[] entries = connector.getGigaSpaceObj().takeMultiple(template, connector.getBatchSize());
                if (entries != null) {
                    for (OpenSpacesQueueObject entry1 : entries) {
                        appendMessage(messages, entry1);
                    }
                }
            }
        } catch (SpaceInterruptedException e) {
            // do nothing, we are being stopped
        } catch (SpaceClosedException e) {
            // do nothing, we are being stopped
        } catch (SpaceUnavailableException e) {
            // do nothing, we are being stopped
        }

        // let our workManager handle the batch of events
        return messages;
    }

    private void appendMessage(List<MuleMessage> messages, OpenSpacesQueueObject entry) throws Exception {
        MuleMessage inboundMessage = createMuleMessage(entry);
        // keep first dequeued event
        messages.add(inboundMessage);
    }

    @Override
    protected MuleEvent processMessage(Object msg) throws Exception {
        // getMessages() returns UMOEvents
        MuleMessage message = (MuleMessage) msg;

        // Rewrite the message to treat it as a new message
        MuleMessage newMessage = new DefaultMuleMessage(message, this.connector.getMuleContext());
        MuleEvent response = routeMessage(newMessage);

        //write response 
        //should send back only if remote synch is set or no outbound endpoints
        if (endpoint.getExchangePattern().hasResponse() && response != null) {

            MuleMessage responseMessage = response.getMessage();

            String correlationId = message.getCorrelationId();

            OpenSpacesQueueObject responseEntry = connector.newQueueEntry(getEndpointURI().getAddress() + OpenSpacesQueueMessageDispatcher.DEFAULT_RESPONSE_QUEUE);
            responseEntry.setCorrelationID(correlationId);

            DocumentProperties payloadMetaData = new DocumentProperties();
            for (String propertyName : responseMessage.getPropertyNames(PropertyScope.OUTBOUND)) {
                Object property = responseMessage.getProperty(propertyName, PropertyScope.OUTBOUND);
                payloadMetaData.put(propertyName, property);
            }
            responseEntry.setPayloadMetaData(payloadMetaData);

            Object payload = responseMessage.getPayload();
            if (payload instanceof NullPayload)
                payload = null;

            responseEntry.setPayload(payload);

            if (logger.isDebugEnabled()) {
                logger.debug(getEndpointURI() + " sending response to client  " + responseEntry);
            }

            Integer lease = responseMessage.getOutboundProperty(OpenSpacesQueueObject.RESPONSE_TIMEOUT_PROPERTY);
            if (lease != null) {
                connector.getGigaSpaceObj().write(responseEntry, (long) lease);
            } else {
                connector.getGigaSpaceObj().write(responseEntry);
            }
        }

        return response != null ? response : voidEvent;
    }

    /*
     * We create our own "polling" worker here since we need to evade the standard scheduler.
     */
    // @Override
    protected PollingReceiverWorker createWork() {
        return new ReceiverWorker(this);
    }

    /*
     * Even though the OpenSpaces Queue transport is "polling" for messages, the nonexistent cost of accessing the queue is
     * a good reason to not use the regular scheduling mechanism in order to both minimize latency and
     * maximize throughput.
     */
    protected static class ReceiverWorker extends PollingReceiverWorker {
        static final long serialVersionUID = 2838384288676282809L;
        public ReceiverWorker(OpenSpacesQueueMessageReceiver pollingMessageReceiver) {
            super(pollingMessageReceiver);
        }

        public void run() {
            /*
             * We simply run our own polling loop all the time as long as the receiver is started. The
             * blocking wait defined by VMConnector.getQueueTimeout() will prevent this worker's receiver
             * thread from busy-waiting.
             */
            if (this.getReceiver().isConnected()) {
                super.run();
            }
        }
    }

}
