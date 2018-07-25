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

package com.gigaspaces.internal.cluster.node.impl.router;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.DisconnectionProxy;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IConnectionMonitor;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateSet;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ILRMIProxy;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.lrmi.nio.async.IExceptionHandler;
import com.gigaspaces.lrmi.nio.async.IFuture;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;
import com.gigaspaces.time.SystemTime;

import net.jini.core.lookup.ServiceID;
import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


@SuppressWarnings("rawtypes")
public abstract class AbstractProxyBasedReplicationMonitoredConnection<T, L> implements IExceptionHandler {

    public enum StateChangedEvent {
        CONNECTED_NEW, CONNECTED_OLD, DISCONNECTED
    }

    private volatile IReplicationConnectionProxy _connectionProxy;
    private volatile T _tag;
    private volatile String _endPointLookupName;
    private volatile Uuid _proxyId;
    private volatile ConnectionState _connectionState;
    private volatile Exception _lastDisconnectionReason;
    private volatile Long _timeOfDisconnection;
    private final Logger _specificLogger;
    private volatile boolean _connectionClosed;
    private final IConnectionMonitor<T, L> _monitor;
    private final L _url;
    private final AbstractConnectionProxyBasedReplicationRouter<T, L> _router;
    private final String _targetLookupName;
    private final Set<IConnectionStateListener> _stateListeners = new HashSet<IConnectionStateListener>();
    private final Set<IConnectivityCheckListener> _connectivityCheckListeners = new CopyOnUpdateSet<IConnectivityCheckListener>();
    private final Object _stateLock = new Object();
    private final Object _dispatchLock = new Object();
    private final Queue<StateChangedEvent> _pendingStateEvents = new ConcurrentLinkedQueue<StateChangedEvent>();
    private final AtomicInteger _reference = new AtomicInteger(0);
    private final IAsyncContextProvider _asyncContextProvider;

    public AbstractProxyBasedReplicationMonitoredConnection(
            AbstractConnectionProxyBasedReplicationRouter<T, L> router,
            String targetLookupName,
            IReplicationConnectionProxy connectionProxy, T tag,
            String endpointLookupName, IConnectionMonitor<T, L> monitor, L url,
            ConnectionState state, Exception disconnectionReason, Uuid proxyId,
            IAsyncContextProvider asyncContextProvider) {
        _router = router;
        _targetLookupName = targetLookupName;
        _connectionProxy = connectionProxy;
        _tag = tag;
        _endPointLookupName = endpointLookupName;
        _monitor = monitor;
        _url = url;
        _proxyId = proxyId;
        _connectionState = state;
        _timeOfDisconnection = _connectionState == ConnectionState.DISCONNECTED ? SystemTime.timeMillis() : null;
        _lastDisconnectionReason = disconnectionReason;
        _asyncContextProvider = asyncContextProvider;
        _specificLogger = Logger.getLogger(Constants.LOGGER_REPLICATION_ROUTER_COMMUNICATION
                + "." + ReplicationLogUtils.toShortLookupName(router.getMyLookupName()));
        _monitor.monitor(this);
    }

    public AbstractConnectionProxyBasedReplicationRouter<T, L> getRouter() {
        return _router;
    }

    private void close() {
        onClose();

        IReplicationConnectionProxy connectionProxy = getConnectionProxy();
        if (connectionProxy != null)
            try {
                connectionProxy.close();
                if (connectionProxy instanceof ILRMIProxy)
                    ((ILRMIProxy) connectionProxy).closeProxy();
            } catch (RemoteException e) {
                // Hide exception
            } finally {
                _connectionClosed = true;
            }
    }

    public boolean isConnectionClosed() {
        return _connectionClosed;
    }

    protected abstract void onClose();

    public <TR> TR dispatch(AbstractReplicationPacket<TR> packet)
            throws RemoteException {
        if (_router.isSetMyIdBeforeDispatch()) {
            packet.setSourceEndpointDetails(_router.getMyEndpointDetails());
        }
        try {
            final boolean logCommunication = _specificLogger.isLoggable(Level.FINEST);
            if (logCommunication)
                _specificLogger.finest("dispatching packet to " + ReplicationLogUtils.toShortLookupName(_endPointLookupName) + " - " + packet);
            TR result = _connectionProxy.dispatch(packet);
            if (logCommunication)
                _specificLogger.finest("dispatch result from " + ReplicationLogUtils.toShortLookupName(_endPointLookupName) + " for packet " + packet.toIdString() + " is - " + result);
            return result;
        } catch (RemoteException e) {
            _monitor.updateDisconnected(this, e);
            throw e;
        }
    }

    public <TR> AsyncFuture<TR> dispatchAsync(
            AbstractReplicationPacket<TR> packet) throws RemoteException {
        if (_router.isSetMyIdBeforeDispatch()) {
            packet.setSourceEndpointDetails(_router.getMyEndpointDetails());
        }
        try {
            IReplicationConnectionProxy connectionProxy = _connectionProxy;
            _asyncContextProvider.setExceptionHandler(this);
            if (_specificLogger.isLoggable(Level.FINEST))
                _specificLogger.finest("async dispatching packet to " + ReplicationLogUtils.toShortLookupName(_endPointLookupName) + " - " + packet);
            TR result = connectionProxy.dispatchAsync(packet);
            return _asyncContextProvider.getFutureContext(result, connectionProxy);
        } catch (RemoteException e) {
            _monitor.updateDisconnected(this, e);
            throw e;
        }
    }

    @Override
    public Throwable handleException(Throwable ex, IFuture future) {
        if (ex instanceof RemoteException)
            _monitor.updateDisconnected(this, (RemoteException) ex);
        return ex;
    }

    public void setConnected(IReplicationConnectionProxy proxy, T tag,
                             String endPointLookupName, Uuid proxyId) {
        boolean newTarget = _proxyId == null ? true : !_proxyId.equals(proxyId);
        synchronized (getStateLock()) {
            if (_connectionState == ConnectionState.CONNECTED)
                return;

            _timeOfDisconnection = null;

            _connectionProxy = proxy;
            _tag = tag;
            _endPointLookupName = endPointLookupName;
            _proxyId = proxyId;
            _connectionState = ConnectionState.CONNECTED;

            if (_specificLogger.isLoggable(Level.INFO))
                _specificLogger.info("Connection state updated to 'CONNECTED', Lookup name: "+endPointLookupName);

            addPendingEvent(newTarget ? StateChangedEvent.CONNECTED_NEW
                    : StateChangedEvent.CONNECTED_OLD);
        }
    }

    // Must be called under state lock
    private void addPendingEvent(StateChangedEvent stateChangedEvent) {
        _pendingStateEvents.add(stateChangedEvent);
        Thread trd = new Thread(new Runnable() {
            public void run() {
                synchronized (_dispatchLock) {
                    while (!_pendingStateEvents.isEmpty()) {
                        StateChangedEvent event = _pendingStateEvents.poll();
                        if (event == null)
                            break;
                        for (IConnectionStateListener listener : _stateListeners) {
                            switch (event) {
                                case CONNECTED_NEW:
                                    listener.onConnected(true);
                                    break;
                                case CONNECTED_OLD:
                                    listener.onConnected(false);
                                    break;
                                case DISCONNECTED:
                                    listener.onDisconnected();
                                    break;
                            }
                        }
                    }
                }
            }
        });
        trd.start();
    }

    public void setDisconnected(Exception reason) {
        synchronized (getStateLock()) {
            if (_connectionState == ConnectionState.DISCONNECTED)
                return;

            _timeOfDisconnection = SystemTime.timeMillis();

            _connectionProxy = new DisconnectionProxy();
            _connectionState = ConnectionState.DISCONNECTED;

            if (_specificLogger.isLoggable(Level.WARNING)) {
                _specificLogger.warning("Connection state updated to 'DISCONNECTED', Lookup name: " +_endPointLookupName+ ", Reason: " + reason.getStackTrace());
            }

            addPendingEvent(StateChangedEvent.DISCONNECTED);
        }
    }

    public ConnectionState getState() {
        return _connectionState;
    }

    public L getFinderURL() {
        return _url;
    }

    public IReplicationConnectionProxy getConnectionProxy() {
        return _connectionProxy;
    }

    public String getTargetLookupName() {
        return _targetLookupName;
    }

    public String getFinalEndpointLookupName() {
        return _endPointLookupName;
    }

    public void addStateListener(IConnectionStateListener listener) {
        synchronized (getStateLock()) {
            _stateListeners.add(listener);
        }
    }

    public void removeStateListener(IConnectionStateListener listener) {
        synchronized (getStateLock()) {
            _stateListeners.remove(listener);
        }
    }

    public void removeConnectivityCheckListener(IConnectivityCheckListener connectivityCheckListener) {
        _connectivityCheckListeners.remove(connectivityCheckListener);
    }

    public void addConnectivityCheckListener(IConnectivityCheckListener connectivityCheckListener) {
        _connectivityCheckListeners.add(connectivityCheckListener);
    }

    public void removeReference() {
        if (_reference.decrementAndGet() == 0)
            close();
    }

    public void addReference() {
        _reference.incrementAndGet();
    }

    public Exception getLastDisconnectionReason() {
        return _lastDisconnectionReason;
    }

    public void setLastDisconnectionReason(Exception exception) {
        _lastDisconnectionReason = exception;
    }

    public T getTag() {
        return _tag;
    }

    public ServiceID getServiceId() {
        if (_proxyId == null)
            return null;

        return new ServiceID(_proxyId.getMostSignificantBits(),
                _proxyId.getLeastSignificantBits());
    }

    public Uuid getProxyId() {
        return _proxyId;
    }

    public Object getStateLock() {
        return _stateLock;
    }

    public Object getConnectionUrl() {
        return _url;
    }

    public String dumpState() {
        return "Connection [" + _router.getMyLookupName() + "->"
                + getTargetLookupName() + "(id=" + getProxyId() + ")] state ["
                + _connectionState + "] timeOfDisconnection [" + _timeOfDisconnection + "]" + StringUtils.NEW_LINE
                + "\ttarget url: " + _url;
    }

    @Override
    public String toString() {
        return dumpState();
    }

    public long getGeneratedTraffic() {
        if (!(_connectionProxy instanceof ILRMIProxy))
            return 0;

        return ((ILRMIProxy) _connectionProxy).getGeneratedTraffic();
    }

    public long getReceivedTraffic() {
        if (!(_connectionProxy instanceof ILRMIProxy))
            return 0;

        return ((ILRMIProxy) _connectionProxy).getReceivedTraffic();
    }

    public Object getClosestEndpointAddress() {
        if (!(_connectionProxy instanceof ILRMIProxy))
            return "Not available";

        return ((ILRMIProxy) _connectionProxy).getConnectionUrl();
    }

    public PlatformLogicalVersion getClosestEndpointLogicalVersion() {
        IReplicationConnectionProxy connectionProxy = _connectionProxy;
        // Safety code, should not occur
        if (connectionProxy == null)
            return PlatformLogicalVersion.getLogicalVersion();
        return LRMIUtilities.getServicePlatformLogicalVersion(connectionProxy);
    }

    public ConnectionEndpointDetails getClosestEndpointDetails() {
        return LRMIUtilities.getConnectionEndpointDetails(_connectionProxy);
    }

    public void triggerSuccessfulConnectivityCheckEvent() {
        for (IConnectivityCheckListener listener : _connectivityCheckListeners) {
            try {
                listener.afterSuccessfulConnectivityCheck();
            } catch (Exception e) {
                // Hide exceptions for not interfering with connection monitor
            }
        }
    }

    public abstract boolean supportsConnectivityCheckEvents();

    public Long getTimeOfDisconnection() {
        return _timeOfDisconnection;
    }

}
