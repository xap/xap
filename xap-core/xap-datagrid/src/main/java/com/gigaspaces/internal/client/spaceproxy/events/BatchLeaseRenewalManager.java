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

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.events.lease.EventLeaseRenewalManager;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.lease.LeaseUpdateBatch;
import com.gigaspaces.internal.lease.LeaseUpdateDetails;
import com.gigaspaces.internal.lease.LeaseUtils;
import com.gigaspaces.internal.lease.SpaceLease;
import com.gigaspaces.internal.lease.SpaceNotifyLease;
import com.gigaspaces.internal.utils.PropertiesUtils;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.Constants;

import net.jini.core.event.EventRegistration;
import net.jini.core.lease.Lease;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class BatchLeaseRenewalManager implements EventLeaseRenewalManager {
    private final SpaceProxyImpl _spaceProxy;
    private final Logger _logger;
    private final long _renewDuration;
    private final long _renewInterval;
    private final Object _lock;
    private final Map<SpaceLease, LeaseListener> _leases;
    private final BatchLeaseRenewalTask _renewalTask;
    private LeaseUpdateBatch _batch;

    public BatchLeaseRenewalManager(SpaceProxyImpl spaceProxy, Logger logger) {
        _spaceProxy = spaceProxy;
        _logger = logger;
        Properties properties = spaceProxy.getProxySettings().getCustomProperties();
        _renewDuration = PropertiesUtils.getLong(properties, Constants.SpaceProxy.Events.REGISTRATION_DURATION,
                Constants.SpaceProxy.Events.REGISTRATION_DURATION_DEFAULT);
        _renewInterval = PropertiesUtils.getLong(properties, Constants.SpaceProxy.Events.REGISTRATION_RENEW_INTERVAL,
                Constants.SpaceProxy.Events.REGISTRATION_RENEW_INTERVAL_DEFAULT);
        _lock = new Object();
        _renewalTask = new BatchLeaseRenewalTask();
        _leases = new ConcurrentHashMap<SpaceLease, LeaseListener>();
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "BatchLeaseRenewalManager initialized (" +
                    "renewDuration=" + _renewDuration + ", " +
                    "renewInterval=" + _renewInterval + ")");
        if (_renewInterval > _renewDuration)
            throw new IllegalStateException("Renew interval (" + _renewInterval + ") cannot exceed renew duration ("
                    + _renewDuration + ")");
    }

    @Override
    public boolean supportsCustomLease() {
        return false;
    }

    @Override
    public long getRenewalDuration() {
        return _renewDuration;
    }

    @Override
    public void registerAutoRenew(EventRegistration eventRegistration, LeaseListener listener) {
        synchronized (_lock) {
            if (_logger.isLoggable(Level.FINEST))
                _logger.log(Level.FINEST, "lease has been registered for auto-renewal");
            _leases.put((SpaceNotifyLease) eventRegistration.getLease(), listener);
            _batch = null;
            if (_leases.size() == 1)
                _spaceProxy.getProxyRouter().getAsyncHandlerProvider().start(_renewalTask, _renewInterval,
                        "BatchLeaseRenewalTask", true);
        }
    }

    @Override
    public void unregisterAutoRenew(EventRegistration eventRegistration) {
        unregisterAutoRenew(eventRegistration.getLease());
    }

    public void unregisterAutoRenew(Lease lease) {
        synchronized (_lock) {
            _leases.remove(lease);
            _batch = null;
        }
    }

    @Override
    public void close() {
        synchronized (_lock) {
            _leases.clear();
            _batch = null;
        }
    }

    private LeaseUpdateBatch getBatch() {
        synchronized (_lock) {
            if (_batch == null && _leases.size() > 0) {
                final SpaceLease[] leases = new SpaceLease[_leases.size()];
                final LeaseUpdateDetails[] leasesUpdateDetails = new LeaseUpdateDetails[_leases.size()];
                int index = 0;
                for (SpaceLease lease : _leases.keySet()) {
                    leases[index] = lease;
                    leasesUpdateDetails[index] = new LeaseUpdateDetails(lease, _renewDuration);
                    index++;
                }
                _batch = new LeaseUpdateBatch(leases, leasesUpdateDetails, true);
                if (_logger.isLoggable(Level.FINEST))
                    _logger.log(Level.FINEST, "Cached lease batch was modified - now contains " + _batch.getSize() +
                            " leases.");
            }

            return _batch;
        }
    }

    private class BatchLeaseRenewalTask extends AsyncCallable {

        @Override
        public IAsyncHandlerProvider.CycleResult call() throws Exception {

            LeaseUpdateBatch batch = getBatch();
            if (batch == null) {
                if (_logger.isLoggable(Level.FINEST))
                    _logger.log(Level.FINEST, "No leases to renew.");
                return IAsyncHandlerProvider.CycleResult.TERMINATE;
            }
            final long newExpiration = LeaseUtils.safeAdd(SystemTime.timeMillis(), _renewDuration);
            AsyncFutureListener<Map<SpaceLease, Throwable>> listener = new AsyncFutureListener<Map<SpaceLease, Throwable>>() {
                @Override
                public void onResult(AsyncResult<Map<SpaceLease, Throwable>> result) {
                    final Map<SpaceLease, Throwable> errorsMap = result.getResult();
                    if (_logger.isLoggable(Level.FINEST))
                        _logger.log(Level.FINEST, "Async batch lease renewal completed - " + (errorsMap == null ? 0 :
                                errorsMap.size()) + " errors");
                    if (errorsMap != null) {
                        for (Map.Entry<SpaceLease, Throwable> entry : errorsMap.entrySet()) {
                            final SpaceLease lease = entry.getKey();
                            final LeaseListener leaseListener = _leases.get(lease);
                            // leaseListener is null if lease has been removed from a different thread:
                            if (leaseListener != null) {
                                final Throwable error = entry.getValue();
                                // If lease does not exist in server, stop renewing it:
                                if (error instanceof UnknownLeaseException)
                                    unregisterAutoRenew(lease);
                                leaseListener.notify(new LeaseRenewalEvent(this, lease, newExpiration, error));
                            }
                        }
                    }
                }
            };

            if (_logger.isLoggable(Level.FINEST))
                _logger.log(Level.FINEST, "Executing async batch renewal of " + batch.getSize() + " registrations...");
            LeaseUtils.updateBatchAsync(_spaceProxy, batch, listener);
            return IAsyncHandlerProvider.CycleResult.IDLE_CONTINUE;
        }
    }
}
