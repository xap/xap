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

package com.gigaspaces.events;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.INotifyDelegatorFilter;

import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.lease.Lease;

import java.rmi.MarshalledObject;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * the super class of all DataEventSession implementations. <br> it supplies an automatic renew
 * service for the listeners.
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.0
 */
public abstract class AbstractDataEventSession implements DataEventSession {
    private static final long DEFAULT_LEASE = Lease.FOREVER;
    private static final NotifyActionType DEFAULT_NOTIFY_TYPE = NotifyActionType.NOTIFY_ALL;

    private final ISpaceProxy _space;
    private final IDirectSpaceProxy _notificationsSpace;
    private final EventSessionConfig _config;
    private final Logger _logger;

    protected AbstractDataEventSession(IJSpace space, EventSessionConfig config) {
        this._space = (ISpaceProxy) space;
        this._notificationsSpace = _space.getNotificationsDirectProxy();
        this._config = config;
        this._logger = _notificationsSpace.getDataEventsManager().getLogger();
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "DataEventSession created - " + config.toString());
    }

    @Override
    public IJSpace getSpace() {
        return _space;
    }

    protected IDirectSpaceProxy getNotificationsSpace() {
        return _notificationsSpace;
    }

    @Override
    public EventSessionConfig getSessionConfig() {
        return _config;
    }

    @Override
    public void close() throws RemoteException {
    }

    @Override
    public EventRegistration addListener(Object template, RemoteEventListener listener)
            throws RemoteException {
        return addListener(template, listener, DEFAULT_LEASE, DEFAULT_NOTIFY_TYPE);
    }

    @Override
    public EventRegistration addListener(Object template, RemoteEventListener listener, NotifyActionType actionTypes)
            throws RemoteException {
        return addListener(template, listener, DEFAULT_LEASE, actionTypes);
    }

    @Override
    public EventRegistration addListener(Object template, RemoteEventListener listener, long lease, NotifyActionType actionTypes)
            throws RemoteException {
        return addListener(template, listener, lease, null, null, actionTypes);
    }

    @Override
    public EventRegistration addListener(Object template, RemoteEventListener listener, NotifyActionType notifyType,
                                         MarshalledObject handback, INotifyDelegatorFilter filter)
            throws RemoteException {
        return addListener(template, listener, DEFAULT_LEASE, handback, filter, notifyType);
    }

    @Override
    public EventRegistration addListener(Object template, RemoteEventListener listener, long lease,
                                         MarshalledObject handback, INotifyDelegatorFilter filter, NotifyActionType notifyType)
            throws RemoteException {
        return addListener(template, lease, new NotifyInfo(listener, notifyType, _config, handback, filter, null));
    }

    public EventRegistration addListener(Object template, long lease, NotifyInfo notifyInfo)
            throws RemoteException {
        _space.applyNotifyInfoDefaults(notifyInfo);
        notifyInfo.validateModifiers();

        return addListenerInternal(template, lease, notifyInfo);
    }

    protected abstract EventRegistration addListenerInternal(Object template, long lease, NotifyInfo notifyInfo)
            throws RemoteException;

    public NotifyInfo createNotifyInfo(RemoteEventListener listener, NotifyActionType notifyType) {
        return new NotifyInfo(listener, notifyType, _config, null, null, null);
    }

    @Override
    public void removeListener(EventRegistration registration)
            throws RemoteException {
    }
}
