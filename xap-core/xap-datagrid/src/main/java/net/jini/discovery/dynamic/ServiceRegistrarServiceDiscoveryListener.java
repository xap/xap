/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
package net.jini.discovery.dynamic;

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.gigaspaces.internal.utils.concurrent.SharedHandlerProviderCache;
import com.gigaspaces.time.SystemTime;

import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;

import org.jini.rio.boot.BootUtil;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dan Kilman
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ServiceRegistrarServiceDiscoveryListener implements ServiceDiscoveryListener {
    public static final String LOCATORS_CLEANUP_TASK_INTERVAL_PROP = "com.gs.jini_lus.locators.dynamic.cleanup_task_interval";
    public static final long LOCATORS_CLEANUP_TASK_INTERVAL_DEFAUL = 30 * 1000;

    public static final String LOCATORS_REMOVAL_THRESHOLD_PROP = "com.gs.jini_lus.locators.dynamic.locator_removal_threshold";
    public static final long LOCATORS_REMOVAL_THRESHOLD_DEFULT = 120 * 1000;

    public static final String GET_REGISTRAR_PROXY_TIMEOUT_PROP = "com.gs.jini_lus.locators.dynamic.registrar_proxy_timeout";
    public static final int GET_REGISTRAR_PROXY_TIMEOUT_DEFAULT = 1000;

    public static final String DYNAMIC_LOCATORS_REMOVE_SEEDS_ENABELD_PROP = "com.gs.jini_lus.locators.dynamic.remove_seeds_enabled";
    public static final boolean DYNAMIC_LOCATORS_REMOVE_SEEDS_ENABELD_DEFAULT = true;

    private final Logger _logger = LoggerFactory.getLogger("com.gs.locator.dynamic.listener");

    private final Object _lock = new Object();

    private final long _locatorRemovalThreshold;
    private final long _locatorRemovalTaskInterval;
    private final int _getRegistrarProxyTimeout;
    private final boolean _enabledSeedRemoval;

    private final Set<LookupLocatorsChangeListener> _listeners = new ConcurrentHashSet<LookupLocatorsChangeListener>();

    private final IAsyncHandlerProvider _asyncHanlderProvider;

    private final Map<ServiceID, Set<LookupLocator>> _locatorsPendingRemovalServiceIDs = new HashMap<ServiceID, Set<LookupLocator>>();
    private final Map<LookupLocator, Long> _locatorsPendingRemovalEventTime = new HashMap<LookupLocator, Long>();

    private final DiscoveryLocatorManagement _dlm;
    private final Map<ServiceID, Set<LookupLocator>> _locators;
    private final LookupLocator[] _initialSeedLocators;
    private final IAsyncHandler _asyncHandler;

    private final ServiceID _ourRegistrarServiceID;
    private ServiceRegistrar _ourRegistrar;
    private LookupLocator _ourRegistrarLocator;

    public ServiceRegistrarServiceDiscoveryListener(
            DiscoveryLocatorManagement dlm,
            Map<ServiceID, LookupLocator> initialLocatorsMap,
            ServiceRegistrar registrar,
            ServiceID registrarServiceID) throws RemoteException {
        _dlm = dlm;
        _ourRegistrar = registrar;
        _ourRegistrarServiceID = registrarServiceID;
        if (_ourRegistrar != null)
            _ourRegistrarLocator = _ourRegistrar.getLocator();
        else
            _ourRegistrarLocator = null;

        _locatorRemovalThreshold = Long.getLong(LOCATORS_REMOVAL_THRESHOLD_PROP,
                LOCATORS_REMOVAL_THRESHOLD_DEFULT);
        _locatorRemovalTaskInterval = Long.getLong(LOCATORS_CLEANUP_TASK_INTERVAL_PROP,
                LOCATORS_CLEANUP_TASK_INTERVAL_DEFAUL);
        _getRegistrarProxyTimeout = Integer.getInteger(GET_REGISTRAR_PROXY_TIMEOUT_PROP,
                GET_REGISTRAR_PROXY_TIMEOUT_DEFAULT);
        _enabledSeedRemoval = Boolean.parseBoolean(System.getProperty(DYNAMIC_LOCATORS_REMOVE_SEEDS_ENABELD_PROP,
                Boolean.toString(DYNAMIC_LOCATORS_REMOVE_SEEDS_ENABELD_DEFAULT)));

        _locators = new HashMap<ServiceID, Set<LookupLocator>>();

        for (Map.Entry<ServiceID, LookupLocator> entry : initialLocatorsMap.entrySet()) {
            Set<LookupLocator> locatorSet = new HashSet<LookupLocator>();
            locatorSet.add(entry.getValue());
            _locators.put(entry.getKey(), locatorSet);
        }

        // try and map the initial locators (seeds) to the above service ids
        _initialSeedLocators = _dlm.getLocators();
        for (LookupLocator locator : _initialSeedLocators) {
            if (initialLocatorsMap.values().contains(locator)) {
                // we already have to correct mapping
                continue;
            }

            try {
                if (_logger.isDebugEnabled())
                    _logger.debug("Trying to find find registrar with initial seed locator: " + locator);

                ServiceID serviceID = locator.getRegistrar(_getRegistrarProxyTimeout).getServiceID();
                Set<LookupLocator> set = _locators.get(serviceID);
                // lus might have just started and so was not passed as part of the initialLocatorsMap
                if (set == null)
                    set = new HashSet<LookupLocator>();

                set.add(locator);
                _locators.put(serviceID, set);
            } catch (IOException e) {
                // we'll move on
                // no point of logging a warning here, as it might be normal for
                // a seed lus to not be up yet
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Could not get proxy to registrar at " + locator, e);
                }
            } catch (ClassNotFoundException e) {
                // This should never happer
                throw new IllegalStateException(e);
            }
        }

        _asyncHanlderProvider = SharedHandlerProviderCache.getSharedThreadPoolProviderCache().getProvider();
        _asyncHandler = _asyncHanlderProvider.start(new RemovePendingRemovalLocatorsTaskAsyncCallable(),
                _locatorRemovalTaskInterval,
                "GS-Locators-Cleanup",
                true);
    }

    @Override
    public void serviceAdded(ServiceDiscoveryEvent event) {
        try {
            Object service = event.getPostEventServiceItem().getService();
            ServiceRegistrar registrar = (ServiceRegistrar) service;

            if (_logger.isDebugEnabled()) {
                _logger.debug("ServiceRegistrar service added with serviceID: "
                        + registrar.getServiceID()
                        + ", Current locators: "
                        + BootUtil.arrayToCommaDelimitedString(_dlm.getLocators()));
            }

            synchronized (_lock) {

                if (_locatorsPendingRemovalServiceIDs.containsKey(registrar.getServiceID())) {
                    Set<LookupLocator> locators = _locatorsPendingRemovalServiceIDs.remove(registrar.getServiceID());

                    for (LookupLocator locator : locators)
                        _locatorsPendingRemovalEventTime.remove(locator);

                    if (_logger.isDebugEnabled()) {
                        _logger.debug("locators " + locators + " removed from pending removal locators");
                    }
                    return;
                } else if (_locators.containsKey(registrar.getServiceID())) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("ServiceRegistrar with locators "
                                + _locators.get(registrar.getServiceID())
                                + " already registered");
                    }
                    return;
                }

                LookupLocator[] locators = new LookupLocator[]{registrar.getLocator()};

                if (registrar.getServiceID().equals(_ourRegistrarServiceID) &&
                        _ourRegistrar == null) {
                    if (_logger.isInfoEnabled()) {
                        _logger.info("Found previously missing registrar with service id: " + registrar.getServiceID() +
                                ", This lookup service will now update its state properly");
                    }

                    _ourRegistrar = registrar;
                    _ourRegistrarLocator = locators[0];
                }

                if (_ourRegistrar != null) {
                    if (_logger.isInfoEnabled()) {
                        _logger.info("Adding locator " + locators[0]
                                + " to ServiceRegistrar at "
                                + _ourRegistrarLocator);
                    }
                    Administrable administrable = (Administrable) _ourRegistrar;
                    JoinAdmin joinAdmin = (JoinAdmin) administrable.getAdmin();
                    joinAdmin.addLookupLocators(locators);
                } else {
                    if (_logger.isInfoEnabled()) {
                        _logger.info("Adding locator " + locators[0]
                                + " to DiscoveryLocatorManagement");
                    }
                    _dlm.addLocators(locators);
                    notifyListeners(_dlm.getLocators());
                }

                Set<LookupLocator> locatorSet = new HashSet<LookupLocator>();
                locatorSet.add(locators[0]);
                _locators.put(registrar.getServiceID(), locatorSet);

                if (_logger.isDebugEnabled()) {
                    _logger.debug("After addition, current locators are: "
                            + BootUtil.arrayToCommaDelimitedString(_dlm.getLocators()));
                }

            }
        } catch (RemoteException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed adding lookup locator", e);
        }
    }

    @Override
    public void serviceRemoved(ServiceDiscoveryEvent event) {
        Object service = event.getPreEventServiceItem().getService();
        ServiceRegistrar registrar = (ServiceRegistrar) service;

        if (_logger.isDebugEnabled()) {
            _logger.debug("ServiceRegistrar service removed with serviceID: "
                    + registrar.getServiceID()
                    + ", Current locators: "
                    + BootUtil.arrayToCommaDelimitedString(_dlm.getLocators()));
        }

        synchronized (_lock) {
            Set<LookupLocator> locators = _locators.get(registrar.getServiceID());
            if (locators != null) {
                if (!_enabledSeedRemoval && anySeedLocator(locators)) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Not removing locators of registrar with service id: " + registrar.getServiceID() +
                                ", as seed removal is disabled");
                    }
                } else if (!_locatorsPendingRemovalServiceIDs.containsKey(registrar.getServiceID())) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Adding locators " + locators + " to pending removal locators");
                    }

                    _locatorsPendingRemovalServiceIDs.put(registrar.getServiceID(), locators);
                    final long eventTime = SystemTime.timeMillis();
                    for (LookupLocator locator : locators)
                        _locatorsPendingRemovalEventTime.put(locator, eventTime);
                } else {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("locators: "
                                + locators + " already added to pending removal locators");
                    }
                }
            } else {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("ServiceRegistrar with serviceID "
                            + registrar.getServiceID() + " already removed");
                }
            }
        }
    }

    private boolean anySeedLocator(Set<LookupLocator> locators) {
        for (LookupLocator locator : _initialSeedLocators) {
            if (locators.contains(locator))
                return true;
        }
        return false;
    }

    @Override
    public void serviceChanged(ServiceDiscoveryEvent event) {
        // unused
    }

    public void terminate() {
        _asyncHandler.stop(1, TimeUnit.MILLISECONDS);
        _asyncHanlderProvider.close();
    }

    private class RemovePendingRemovalLocatorsTaskAsyncCallable extends AsyncCallable {

        private boolean _loggedPossibleDisconnetion = false;

        @Override
        public CycleResult call() throws Exception {
            synchronized (_lock) {
                List<LookupLocator> locatorsToRemove = new ArrayList<LookupLocator>();
                List<ServiceID> serviceIDs = new ArrayList<ServiceID>();
                for (Entry<ServiceID, Set<LookupLocator>> entry : _locatorsPendingRemovalServiceIDs.entrySet()) {
                    ServiceID serviceID = entry.getKey();
                    Set<LookupLocator> locatorSet = entry.getValue();

                    // pick any
                    long serviceRemovedTime = _locatorsPendingRemovalEventTime.get(locatorSet.iterator().next());

                    if (serviceRemovedTime + _locatorRemovalThreshold > SystemTime.timeMillis())
                        continue;

                    locatorsToRemove.addAll(locatorSet);
                    serviceIDs.add(serviceID);

                }

                if (locatorsToRemove.isEmpty())
                    return CycleResult.IDLE_CONTINUE;

                if (serviceIDs.size() >= _locators.size()) {
                    if (_logger.isWarnEnabled() && !_loggedPossibleDisconnetion) {
                        _logger.warn("Found that all registered locators were removed, this could mean that this " +
                                "machine has disconnected, so no locator removal will be made");

                        _loggedPossibleDisconnetion = true;
                    }


                    return CycleResult.IDLE_CONTINUE;
                }

                _loggedPossibleDisconnetion = false;

                try {
                    LookupLocator[] locators = locatorsToRemove.toArray(new LookupLocator[locatorsToRemove.size()]);

                    if (_ourRegistrar != null) {
                        if (_logger.isInfoEnabled()) {
                            _logger.info("Removing locators "
                                    + locatorsToRemove
                                    + " from ServiceRegistrar at "
                                    + _ourRegistrarLocator);
                        }
                        Administrable administrable = (Administrable) _ourRegistrar;
                        JoinAdmin joinAdmin = (JoinAdmin) administrable.getAdmin();
                        joinAdmin.removeLookupLocators(locators);
                    } else {
                        if (_logger.isInfoEnabled()) {
                            _logger.info("Removing locators " + locatorsToRemove
                                    + " from DiscoveryLocatorManagement");
                        }
                        _dlm.removeLocators(locators);
                        notifyListeners(_dlm.getLocators());
                    }

                    for (ServiceID serviceID : serviceIDs) {
                        _locators.remove(serviceID);
                        _locatorsPendingRemovalServiceIDs.remove(serviceID);
                    }

                    for (LookupLocator locator : locators)
                        _locatorsPendingRemovalEventTime.remove(locator);

                } catch (RemoteException e) {
                    // we shouldn't get a remote exception here because even
                    // though
                    // we have a JoinAdmin proxy, the calls to getAdmin and
                    // joinAdmin.removeLookupLocators are done from within
                    // the registrar jvm
                    if (_logger.isWarnEnabled()) {
                        _logger.warn("Failed removing locators", e);
                    }
                }

                return CycleResult.IDLE_CONTINUE;

            }
        }

    }

    private void notifyListeners(final LookupLocator[] locators) {
        if (_logger.isDebugEnabled()) {
            _logger.debug("Notifying registered listeners on locators changed event with locators: " +
                    StringUtils.arrayToCommaDelimitedString(locators));
        }

        _asyncHanlderProvider.start(new AsyncCallable() {
            @Override
            public CycleResult call() throws Exception {
                for (LookupLocatorsChangeListener listener : _listeners) {
                    try {
                        LocatorsChangedEvent event = new LocatorsChangedEvent(ServiceRegistrarServiceDiscoveryListener.this, locators);
                        listener.locatorsChanged(event);
                    } catch (Exception e) {
                        // ignore listener exceptions
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("Listener threw exception during notification", e);
                        }
                    }
                }

                return CycleResult.TERMINATE;
            }
        }, 1, "DynamicLocators-ChangeNotifier", false);
    }

    public void addLookupLocatorsChangeListener(
            LookupLocatorsChangeListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("listener cannot be null");

        _listeners.add(listener);
    }

    public void removeLookupLocatorsChangeListener(
            LookupLocatorsChangeListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("listener cannot be null");

        _listeners.remove(listener);
    }

}
