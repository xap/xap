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

package com.gigaspaces.cluster.activeelection;

import com.gigaspaces.cluster.activeelection.core.ActiveElectionException;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.j_spaces.core.cluster.ReplicationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Provides runtime primary/backup selection in cluster topologies.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 6.0
 */
public abstract class LeaderSelectorHandler implements LeaderSelector {
    protected Logger _logger;

    protected String _spaceMember;
    protected SpaceImpl _space;

    protected PrimarySpaceModeListeners _primarySpaceModeListeners;

    private final ThreadLocal<ExecutorService> _dispatchRemoteEventExecutor = new ThreadLocal<ExecutorService>();

    protected volatile SpaceMode _spaceMode = SpaceMode.NONE;
    protected volatile Throwable _lastError;

    public LeaderSelectorHandler() {
        _primarySpaceModeListeners = null;
        _space = null;
        _spaceMember = null;
        _logger = null;
    }

    public void initialize(LeaderSelectorHandlerConfig config) throws Exception {
        _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLUSTER_ACTIVE_ELECTION + "." + config.getSpace().getNodeName());
        if (config.getSpace().getClusterPolicy().m_FailOverPolicy == null)
            throw new IllegalArgumentException("Failed to initialize LeaderElectorHandler. FailoverPolicy is <null>");

        _space = config.getSpace();
        _spaceMember = config.getSpace().getServiceName();
        _primarySpaceModeListeners = config.getSpace().getPrimarySpaceModeListeners();

        ReplicationPolicy policy = config.getSpace().getClusterPolicy().getReplicationPolicy();
        if (policy != null) {
            policy.setPrimarySpaceSelector(this);
        }
    }

    /**
     * @return <code>true</code>if the Space is primary, otherwise the space in backup mode.
     */
    @Override
    public boolean isPrimary() {
        return _spaceMode == SpaceMode.PRIMARY;
    }

    /**
     * Notify the listeners about the event
     */
    protected synchronized void beforeSpaceModeChange(SpaceMode newMode) {
        if (_logger.isDebugEnabled())
            _logger.debug("Invoking beforeSpaceModeChange event, new mode " + newMode);

        Queue<ISpaceModeListener> remoteListeners = new LinkedList<ISpaceModeListener>();
        for (Iterator<ISpaceModeListener> iter = _primarySpaceModeListeners.iterator(); iter.hasNext(); ) {
            ISpaceModeListener listener = iter.next();
            try {
                // set space initializer , so the components can access the space when it is still backup
                SpaceInitializationIndicator.setInitializer();
                if (LRMIUtilities.isRemoteProxy(listener)) {
                    if (_logger.isTraceEnabled())
                        _logger.trace("located remote listener for beforeSpaceModeChange, adding to asynchronous dispatch queue [" + listener.toString() + "]");
                    remoteListeners.add(listener);
                    continue;
                }
                listener.beforeSpaceModeChange(newMode);
            } catch (DirectPersistencyRecoveryException dpe) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(dpe.getMessage());
                }
                // cancel notifying and turn off the space in case DirectPersistencyRecoveryException occurred, see DirectPersistencyRecoveryHelper#beforeSpaceModeChange
                throw dpe;
            } catch (Exception rex) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(
                            "Failed to invoke method ISpaceModeListener.beforeSpaceModeChange(...) implemented by listener ["
                                    + listener
                                    + "]. Action Taken: Unregistered listener",
                            rex);
                }

                _primarySpaceModeListeners.removeListener(listener); //remove failed listener
            } finally {
                SpaceInitializationIndicator.unsetInitializer();

            }
        }
        //We have pending remote listener
        if (remoteListeners.size() > 0)
            dispatchBeforeEventToRemoteListeners(remoteListeners, newMode);

        if (_logger.isTraceEnabled())
            _logger.trace("completed invoking synchronous beforeSpaceModeChange event with mode " + newMode);
    }


    /**
     * Notify the listeners about the event
     */
    protected synchronized void afterSpaceModeChange(SpaceMode newMode) {
        if (_logger.isDebugEnabled())
            _logger.debug("Invoking afterSpaceModeChange event, new mode is " + newMode);
        Queue<ISpaceModeListener> remoteListeners = new LinkedList<ISpaceModeListener>();
        for (Iterator<ISpaceModeListener> iter = _primarySpaceModeListeners.iterator(); iter.hasNext(); ) {
            ISpaceModeListener listener = iter.next();
            try {
                if (LRMIUtilities.isRemoteProxy(listener)) {
                    if (_logger.isTraceEnabled())
                        _logger.trace("located remote listener for afterSpaceModeChange, adding to asynchronous dispatch queue [" + listener.toString() + "]");
                    remoteListeners.add(listener);
                    continue;
                }
                listener.afterSpaceModeChange(newMode);
            } catch (Exception rex) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(
                            "Failed to invoke remote method ISpaceModeListener.afterSpaceModeChange(...) implemented by listener ["
                                    + listener
                                    + "]. Action Taken: Unregistered listener",
                            rex);
                }

                _primarySpaceModeListeners.removeListener(listener); //remove failed listener
            }
        }
        //We have pending remote listener
        if (remoteListeners.size() > 0)
            dispatchAfterEventToRemoteListeners(remoteListeners, newMode);

        if (_logger.isTraceEnabled())
            _logger.trace("completed invoking synchronous afterSpaceModeChange event with mode " + newMode);
    }


    protected void dispatchBeforeEventToRemoteListeners(
            Queue<ISpaceModeListener> remoteListeners, final SpaceMode newMode) {
        if (_logger.isTraceEnabled())
            _logger.trace("dispatching asynchronous beforeSpaceModeChange event, new mode is " + newMode + ", pending listeners count is " + remoteListeners.size());
        //Pool size is fixed to 1 in order to keep the order of before/after events
        ExecutorService executor = Executors.newFixedThreadPool(1);
        _dispatchRemoteEventExecutor.set(executor);
        while (!remoteListeners.isEmpty()) {
            final ISpaceModeListener remoteListener = remoteListeners.remove();
            final boolean isLast = remoteListeners.isEmpty();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        remoteListener.beforeSpaceModeChange(newMode);
                        if (isLast && _logger.isTraceEnabled())
                            _logger.trace("completed invoking asynchronous beforeSpaceModeChange event with mode " + newMode);
                    } catch (Exception e) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(
                                    "Failed to invoke remote method ISpaceModeListener.beforeSpaceModeChange(...) implemented by listener ["
                                            + remoteListener
                                            + "]. Action Taken: Unregistered listener",
                                    e);
                        }

                        _primarySpaceModeListeners.removeListener(remoteListener); //remove failed listener
                    }
                }
            });
        }
    }

    private void dispatchAfterEventToRemoteListeners(Queue<ISpaceModeListener> remoteListeners, final SpaceMode newMode) {
        if (_logger.isTraceEnabled())
            _logger.trace("dispatching asynchronous afterSpaceModeChange event, new mode is " + newMode + ", pending listeners count is " + remoteListeners.size());
        ExecutorService executor = _dispatchRemoteEventExecutor.get();
        while (!remoteListeners.isEmpty()) {
            final ISpaceModeListener remoteListener = remoteListeners.remove();
            final boolean isLast = remoteListeners.isEmpty();
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        remoteListener.afterSpaceModeChange(newMode);
                        if (isLast && _logger.isTraceEnabled())
                            _logger.trace("completed invoking asynchronous afterSpaceModeChange event with mode " + newMode);
                    } catch (Exception e) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(
                                    "Failed to invoke remote method ISpaceModeListener.AfterSpaceModeChange(...) implemented by listener ["
                                            + remoteListener
                                            + "]. Action Taken: Unregistered listener",
                                    e);
                        }

                        _primarySpaceModeListeners.removeListener(remoteListener); //remove failed listener
                    }
                }
            });
        }
        //This will shutdown the pool once all the currently submitted tasks are executed (Acording to API docs)
        executor.shutdown();
        _dispatchRemoteEventExecutor.remove();
    }


    /**
     * Change space mode to primary
     */
    protected void moveToPrimary() {
        // *** Before and after must be called in the same thread to keep to order of events for remote listeners
        /* notify the listeners that space becomes primary */
        beforeSpaceModeChange(SpaceMode.PRIMARY);

		/* mark space state as primary */
        setSpaceMode(SpaceMode.PRIMARY);

		/* notify the listeners that space became primary */
        afterSpaceModeChange(SpaceMode.PRIMARY);
    }


    /**
     * Change space mode to backup
     */
    protected void moveToBackup() {
        //*** Before and after must be called in the same thread to keep to order of events for remote listeners
        /* notify the listeners that space becomes backup */
        beforeSpaceModeChange(SpaceMode.BACKUP);
		
		/* mark space state as backup */
        setSpaceMode(SpaceMode.BACKUP);

		/* notify the listeners that space became backup */
        afterSpaceModeChange(SpaceMode.BACKUP);
    }

    /**
     * Change space state to UNHEALTHY, so it is redeployed
     */
    protected void moveToUnusable() {
        //stop the space , so it won't be accessed by clients and replication
        try {
            _logger.info("==> STARTED -> STOPPED state");
            _space.stopInternal();
            Executors.newSingleThreadExecutor().submit(() -> {
                _logger.info("==> STOPPED -> ABORTED state ");
                _space.shutdown(true, false);
                return null;
            });
        } catch (RemoteException e) {
            _logger.warn("Failed to move Space to STOPPED state", e);
        }
    }

    /**
     * Add listener for space availability and notify the listener with the current space mode
     */
    public synchronized void addListenerAndNotify(ISpaceModeListener listener) throws RemoteException {
        SpaceMode currentSpaceMode = _spaceMode;
        listener.beforeSpaceModeChange(currentSpaceMode);
        listener.afterSpaceModeChange(currentSpaceMode);

        _primarySpaceModeListeners.addListener(listener);
    }

    /**
     * Remove listener for space availability
     */
    public synchronized void removeListener(ISpaceModeListener listener) {
        _primarySpaceModeListeners.removeListener(listener);
    }

    public void setSpaceMode(SpaceMode spaceMode) {
        _spaceMode = spaceMode;
    }


    public SpaceMode getSpaceMode() {
        return _spaceMode;
    }

    /**
     * Checks if space mode equals given mode. If true -  registers the given listener to election
     * events. Otherwise returns false.
     */
    public synchronized boolean compareAndRegister(SpaceMode spaceMode, ISpaceModeListener listener) {
        if (_spaceMode != spaceMode)
            return false;

        _primarySpaceModeListeners.addListener(listener);

        return true;
    }

    @Override
    public Throwable getLastError() {
        return _lastError;
    }

    @Override
    public void setLastError(Throwable lastError) {
        this._lastError = lastError;
    }

    public void forceMoveToPrimary() throws RemoteException {
        setLastError(new ActiveElectionException("forceMoveToPrimary->moveToUnusable"));
        moveToUnusable();
    }

}
