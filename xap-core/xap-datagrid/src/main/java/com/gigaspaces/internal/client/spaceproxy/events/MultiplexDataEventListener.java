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

package com.gigaspaces.internal.client.spaceproxy.events;

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.ManagedRemoteEventListener;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom listener which is used to group all notification registrations from a space proxy using
 * a single listener.
 *
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class MultiplexDataEventListener implements ManagedRemoteEventListener, BatchRemoteEventListener {
    private static final Logger logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACEPROXY_DATA_EVENTS_LISTENER);

    private final Map<String, RegistrationInfo> _registrations;


    public MultiplexDataEventListener() {
        this._registrations = new ConcurrentHashMap<String, RegistrationInfo>();
    }

    public void close() {
        int remains = _registrations.size();
        for (RegistrationInfo regInfo : _registrations.values()) {
            remains -= 1;
            try {
                regInfo.getEventRegistration().getLease().cancel();
            } catch (RemoteException ex) {
                logger.warn("Failed to close event registration " + regInfo.getEventRegistration() + " skipping more " + remains + " registrations");
                break;
            } catch (Exception ex) {
                logger.debug("Failed to close event registration ", ex);
            }
        }

        _registrations.clear();
    }

    public void add(String templateId, ManagedRemoteEventListener localListener) {
        _registrations.put(templateId, new RegistrationInfo(localListener));
    }

    @Override
    public void init(GSEventRegistration registration) {
        RegistrationInfo regInfo = _registrations.get(registration.getKey());
        regInfo.setEventRegistration(registration);
        regInfo.getListener().init(registration);
    }

    @Override
    public void shutdown(GSEventRegistration registration) {
        final RegistrationInfo regInfo = _registrations.remove(registration.getTemplateID());
        if (regInfo != null)
            regInfo.getListener().shutdown(registration);
    }

    @Override
    public void notify(RemoteEvent event) throws UnknownEventException, RemoteException {
        RegistrationInfo regInfo = _registrations.get(((EntryArrivedRemoteEvent) event).getKey());
        // regInfo is null if registration was already closed.
        if (regInfo != null)
            regInfo.getListener().notify(event);
    }

    @Override
    public void notifyBatch(BatchRemoteEvent batchEvent) throws UnknownEventException, RemoteException {
        RegistrationInfo regInfo = _registrations.get(batchEvent.getTemplateUID());
        // regInfo is null if registration was already closed.
        if (regInfo != null)
            ((BatchRemoteEventListener) regInfo.getListener()).notifyBatch(batchEvent);
    }

    private static class RegistrationInfo {
        private final ManagedRemoteEventListener listener;
        private GSEventRegistration eventRegistration;

        public RegistrationInfo(ManagedRemoteEventListener listener) {
            this.listener = listener;
        }

        public ManagedRemoteEventListener getListener() {
            return listener;
        }

        public GSEventRegistration getEventRegistration() {
            return eventRegistration;
        }

        public void setEventRegistration(GSEventRegistration eventRegistration) {
            this.eventRegistration = eventRegistration;
        }
    }
}
