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

import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.j_spaces.core.LeaseManager;

import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.LeaseMap;
import net.jini.core.lease.UnknownLeaseException;

import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class DurableNotificationLease
        implements Lease {
    private final Logger _logger;

    private final ReplicationNotificationClientEndpoint _clientEndpoint;
    private final IAsyncHandlerProvider _asyncProvider;


    private final Object _lock = new Object();

    // guarded by _lock
    private IAsyncHandler _currentTask;
    private boolean _cancelled;

    private volatile long _currentExpiration;
    private volatile long _currentLease;

    public DurableNotificationLease(
            ReplicationNotificationClientEndpoint replicationNotificationClientEndpoint,
            long lease,
            EventSessionConfig sessionConfig,
            IAsyncHandlerProvider asyncProvider) {
        _logger = replicationNotificationClientEndpoint.getLogger();
        _clientEndpoint = replicationNotificationClientEndpoint;
        _asyncProvider = asyncProvider;
        updateLeaseAndExpiration(getActualLease(lease, sessionConfig));
    }

    public Logger getLogger() {
        return _logger;
    }

    private long getActualLease(long lease, EventSessionConfig sessionConfig) {
        return sessionConfig.isAutoRenew() ? Math.max(lease, sessionConfig.getRenewExpiration())
                : lease;
    }

    public void startLeaseReaperIfNecessary() {
        if (_currentLease != Lease.FOREVER &&
                _currentLease != Lease.ANY &&
                _currentExpiration > 0) {
            restartLeaseExpirationReaper(_currentLease);
        }
    }

    private void restartLeaseExpirationReaper(long duration) {
        synchronized (_lock) {
            if (_currentTask != null) {
                _currentTask.stop(10, TimeUnit.MILLISECONDS);
            }

            if (_logger.isDebugEnabled())
                _logger.debug("Scheduling lease cancel thread, will be activated in " + duration + "ms");

            _currentTask = _asyncProvider.start(new AsyncCallable() {
                public CycleResult call() throws Exception {
                    new Thread(new Runnable() {
                        public void run() {
                            if (_logger.isDebugEnabled())
                                _logger.debug("Lease cancel thread was activated, canceling lease");

                            DurableNotificationLease.this.cancel();
                        }
                    }).start();
                    return CycleResult.TERMINATE;
                }
            }, duration, "DurableNotifyLeaseMonitor", true /* waitIdleDelayBeforeStart */);

            updateLeaseAndExpiration(duration);
        }
    }

    @Override
    public long getExpiration() {
        return _currentExpiration;
    }

    @Override
    public void cancel() {
        synchronized (_lock) {
            if (_cancelled)
                return;

            _cancelled = true;

            if (_currentTask != null)
                _currentTask.stop(10, TimeUnit.MILLISECONDS);
        }

        if (_logger.isDebugEnabled())
            _logger.debug("Lease cancel request, canceling lease on registration");

        _clientEndpoint.close();
    }

    @Override
    public void renew(long duration) throws LeaseDeniedException,
            UnknownLeaseException, RemoteException {
        synchronized (_lock) {
            if (_cancelled)
                throw new UnknownLeaseException("Lease is already cancelled");

            if (duration < 0)
                return;

            if (duration == Lease.FOREVER) {
                if (_currentTask != null) {
                    _currentTask.stop(10, TimeUnit.MILLISECONDS);
                    _currentTask = null;
                }
                updateLeaseAndExpiration(Lease.FOREVER);
                return;
            }

            restartLeaseExpirationReaper(duration);
        }
    }

    private void updateLeaseAndExpiration(long lease) {
        _currentLease = lease;
        _currentExpiration = LeaseManager.toAbsoluteTime(_currentLease);
    }

    @Override
    public void setSerialFormat(int format) {
    }

    @Override
    public int getSerialFormat() {
        return 0;
    }

    @Override
    public LeaseMap createLeaseMap(long duration) {
        return null;
    }

    @Override
    public boolean canBatch(Lease lease) {
        return false;
    }

}
