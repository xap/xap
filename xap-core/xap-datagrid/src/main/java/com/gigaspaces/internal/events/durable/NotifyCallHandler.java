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

package com.gigaspaces.internal.events.durable;

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.transport.ITemplatePacket;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class NotifyCallHandler implements BatchRemoteEventListener {

    private final Logger _logger;

    private final RemoteEventListener _listener;
    private final NotifyInfo _notifyInfo;
    private final ITemplatePacket _templatePacket;
    private final ReplicationNotificationClientEndpoint _endpoint;

    private final Object _lock = new Object();
    private boolean _closed;

    public NotifyCallHandler(RemoteEventListener listener,
                             NotifyInfo notifyInfo, ITemplatePacket templatePacket,
                             ReplicationNotificationClientEndpoint endpoint) {
        _logger = endpoint.getLogger();
        _listener = listener;
        _notifyInfo = notifyInfo;
        _templatePacket = templatePacket;
        _endpoint = endpoint;
    }

    public void notifyBatch(BatchRemoteEvent batchEvent) {
        try {
            ((BatchRemoteEventListener) _listener).notifyBatch(batchEvent);
        } catch (UnknownEventException e) {
            synchronized (_lock) {
                if (_closed)
                    return;

                if (_logger.isWarnEnabled())
                    _logger.warn(getUnknownEventExceptionMessage(), e);

                closeRegistration();

                _closed = true;
            }

        } catch (Exception e) {
            if (_logger.isWarnEnabled())
                _logger.warn(getExceptionMessage(), e);
        }
    }


    @Override
    public void notify(RemoteEvent theEvent) {
        try {
            _listener.notify(theEvent);
        } catch (UnknownEventException e) {
            synchronized (_lock) {
                if (_closed)
                    return;

                if (_logger.isWarnEnabled())
                    _logger.warn(getUnknownEventExceptionMessage(), e);

                closeRegistration();

                _closed = true;
            }

        } catch (Exception e) {
            if (_logger.isWarnEnabled())
                _logger.warn(getExceptionMessage(), e);
        }
    }

    private void closeRegistration() {
        new Thread(new Runnable() {
            public void run() {
                _endpoint.close();
            }
        }).start();
    }

    private String getUnknownEventExceptionMessage() {
        return "UnknownEventException occurred during call to client listener with template:" + _templatePacket.toString()
                + ", template UID: " + _notifyInfo.getTemplateUID()
                + ", performing unregistration";
    }

    private String getExceptionMessage() {
        return "Exception occurred during call to client listener with template:" + _templatePacket.toString()
                + ", template UID: " + _notifyInfo.getTemplateUID()
                + ", ignoring exception";
    }

}
