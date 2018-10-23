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

import org.mule.api.MuleMessage;
import org.mule.api.ThreadSafeAccess;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.transport.AbstractMessageRequester;
import org.openspaces.core.SpaceClosedException;
import org.openspaces.core.SpaceInterruptedException;

/**
 * Requests (takes) a message from an internal queue. The queue is a virtualized queue represented
 * by the {@link org.openspaces.esb.mule.queue.OpenSpacesQueueObject} with its endpoint address set
 * (and not the message).
 *
 * @author kimchy
 */
public class OpenSpacesQueueMessageRequestor extends AbstractMessageRequester {

    private final OpenSpacesQueueConnector connector;

    private Object template;

    public OpenSpacesQueueMessageRequestor(InboundEndpoint endpoint) {
        super(endpoint);
        this.connector = (OpenSpacesQueueConnector) endpoint.getConnector();
    }

    /**
     * Make a specific request to the underlying transport
     *
     * @param timeout the maximum time the operation should block before returning. The call should
     *                return immediately if there is data available. If no data becomes available
     *                before the timeout elapses, null will be returned
     * @return the result of the request wrapped in a UMOMessage object. Null will be returned if no
     * data was available
     * @throws Exception if the call to the underlying protocol causes an exception
     */
    protected MuleMessage doRequest(long timeout) throws Exception {
        try {
            MuleMessage message = null;
            if (logger.isDebugEnabled()) {
                logger.debug("Waiting for a message on " + endpoint.getEndpointURI().getAddress());
            }
            try {
                long startTime = System.currentTimeMillis();
                long currentTime = System.currentTimeMillis();
                long interval = 100;
                do {

                    OpenSpacesQueueObject entry = (OpenSpacesQueueObject) connector.getGigaSpaceObj().take(template);
                    if (entry != null) {
                        return createMuleMessage(entry);
                    }
                    // entry was not found or taken by another thread
                    Thread.sleep(interval);
                    currentTime = System.currentTimeMillis();
                } while (timeout == -1 || currentTime - startTime < timeout);
            } catch (SpaceInterruptedException e) {
                // do nothing, we are being stopped
            } catch (SpaceClosedException e) {
                // do nothing, we are being stopped
            }
            if (message != null) {
                //The message will contain old thread information, we need to reset it
                if (message instanceof ThreadSafeAccess) {
                    ((ThreadSafeAccess) message).resetAccessControl();
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Message received: " + message);
                }
                return message;
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No event received after " + timeout + " ms");
                }
                return null;
            }
        } catch (Exception e) {
            throw e;
        }
    }

    protected void doDispose() {
        // template method
    }

    protected void doConnect() throws Exception {
        OpenSpacesQueueObject internalTemplate = connector.newQueueTemplate(endpoint.getEndpointURI().getAddress());
        template = connector.getGigaSpaceObj().prepareTemplate(internalTemplate);
    }

    protected void doDisconnect() throws Exception {
        // template method
    }

}