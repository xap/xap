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

import com.gigaspaces.events.lease.EventLeaseRenewalManager;
import com.gigaspaces.events.lease.JiniEventLeaseRenewalManager;
import com.gigaspaces.internal.client.spaceproxy.events.SpaceProxyDataEventsManager;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.j_spaces.core.IJSpace;

import net.jini.core.event.EventRegistration;
import net.jini.core.lease.Lease;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;

import java.rmi.RemoteException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class DefaultDataEventSession extends AbstractDataEventSession {
    private static final LeaseListener DEFAULT_LEASE_LISTENER = new DefaultLeaseListener();

    private final Logger logger;
    private final SpaceProxyDataEventsManager eventsManager;
    private final Set<EventRegistration> events;
    private final boolean isMultiplex;
    private final EventLeaseRenewalManager renewalManager;
    private final LeaseListener leaseListener;

    DefaultDataEventSession(IJSpace space, EventSessionConfig config) {
        super(space, config);
        this.eventsManager = getNotificationsSpace().getDataEventsManager();
        this.logger = eventsManager.getLogger();
        this.events = new ConcurrentHashSet<EventRegistration>();
        this.isMultiplex = true;
        this.renewalManager = initRenewalManager(config);
        this.leaseListener = config.getLeaseListener() != null ? config.getLeaseListener() : DEFAULT_LEASE_LISTENER;
    }

    private EventLeaseRenewalManager initRenewalManager(EventSessionConfig config) {
        if (!config.isAutoRenew())
            return null;
        if (!isMultiplex)
            return new JiniEventLeaseRenewalManager(config);

        if (EventSessionConfig.USE_OLD_LEASE_RENEWAL_MANAGER) {
            if (logger.isWarnEnabled())
                logger.warn("Multiplex auto-renew is disabled because "
                        + EventSessionConfig.USE_OLD_LEASE_RENEWAL_MANAGER_PROPERTY + " is set to true.");
            return new JiniEventLeaseRenewalManager(config);
        }
        if (config.getRenewDuration() != EventSessionConfig.DEFAULT_RENEW_DURATION) {
            if (logger.isWarnEnabled())
                logger.warn("Multiplex auto-renew is disabled because renewDuration has been modified.");
            return new JiniEventLeaseRenewalManager(config);
        }
        if (config.getRenewExpiration() != EventSessionConfig.DEFAULT_RENEW_EXPIRATION) {
            if (logger.isWarnEnabled())
                logger.warn("Multiplex auto-renew is disabled because renewExpiration has been modified" +
                        ".");
            return new JiniEventLeaseRenewalManager(config);
        }
        if (config.getRenewRTT() != EventSessionConfig.DEFAULT_RENEW_RTT) {
            if (logger.isWarnEnabled())
                logger.warn("Multiplex auto-renew is disabled because renewRTT has been modified.");
            return new JiniEventLeaseRenewalManager(config);
        }

        if (logger.isTraceEnabled())
            logger.trace("Multiplex auto-renew is enabled.");
        return eventsManager.getRenewalManager();
    }

    @Override
    protected EventRegistration addListenerInternal(Object template, long lease, NotifyInfo info)
            throws RemoteException {
        if (renewalManager != null && !renewalManager.supportsCustomLease()) {
            if (lease != Lease.FOREVER && lease != renewalManager.getRenewalDuration())
                logger.warn("Custom lease is ignored when using a multiplex data event session with auto-renew - " +
                        "the session's renewalDuration (" + renewalManager.getRenewalDuration() + ") is used instead."
                );
            lease = renewalManager.getRenewalDuration();
        }

        EventRegistration registration = eventsManager.addListener(template, lease, info, isMultiplex);
        this.events.add(registration);

        if (renewalManager != null)
            renewalManager.registerAutoRenew(registration, leaseListener);
        return registration;
    }

    @Override
    public void removeListener(EventRegistration registration)
            throws RemoteException {
        if (renewalManager != null)
            renewalManager.unregisterAutoRenew(registration);

        this.events.remove(registration);
        try {
            registration.getLease().cancel();
        } catch (UnknownLeaseException e) {
            // Already been canceled ?
        } catch (RemoteException e) {
            // invalid stub - lease has been canceled
        }
    }

    @Override
    public void close() throws RemoteException {
        for (EventRegistration registration : this.events)
            removeListener(registration);

        if (renewalManager != null && renewalManager != eventsManager.getRenewalManager()) // when isMultiplex renewalManager is owned by eventsManager.
            renewalManager.close();

        super.close();
    }

    private static class DefaultLeaseListener implements LeaseListener {
        private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLIENT);

        @Override
        public void notify(LeaseRenewalEvent e) {
            _logger.debug("Cannot renew lease", e.getException());
        }
    }
}
