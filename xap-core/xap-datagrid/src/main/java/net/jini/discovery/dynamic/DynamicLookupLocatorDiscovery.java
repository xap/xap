/*******************************************************************************
 * Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

package net.jini.discovery.dynamic;

import com.j_spaces.core.jini.SharedDiscoveryManagement;
import com.j_spaces.core.jini.SharedDiscoveryManagement.SharedServiceDiscoveryManager;
import com.j_spaces.core.jini.SharedDiscoveryManagement.SharedServiceDiscoveryManagerResult;
import com.j_spaces.kernel.SystemProperties;

import net.jini.admin.Administrable;
import net.jini.admin.JoinAdmin;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.DiscoveryLocatorManagement;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.lookup.LookupCache;
import net.jini.lookup.entry.Comment;

import org.jini.rio.boot.BootUtil;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dan Kilman
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class DynamicLookupLocatorDiscovery implements DiscoveryListener {

    public static boolean dynamicLocatorsEnabled() {
        return Boolean.getBoolean(SystemProperties.ENABLE_DYNAMIC_LOCATORS);
    }

    public static final Comment DYNAMIC_LOCATORS_ENABLED_LOOKUP_ATTRIBUTE = new Comment("DynamicLocatorsEnabled");

    private static final ServiceTemplate LOOKUP_CACHE_TEMPLATE = new ServiceTemplate(null,
            new Class[]{ServiceRegistrar.class},
            null);

    private final Logger _logger = LoggerFactory.getLogger("com.gs.locator.dynamic.manager");

    private final Object _lock = new Object();
    private final Object _initialServiceRegistrarsLock = new Object();
    private final CountDownLatch _initializedIndicator = new CountDownLatch(1);
    private final List<ServiceRegistrar> _initialRegistrars = new ArrayList<ServiceRegistrar>();
    private CountDownLatch _initialRegistrarsDiscoveredLatch;

    private final LookupDiscoveryManager _ldm;

    private boolean _terminated = false;
    private SharedServiceDiscoveryManager _sdm;
    private LookupCache _lookupCache;
    private boolean _isSdmManaged;
    private ServiceRegistrarServiceDiscoveryListener _listener;

    private volatile Exception _initException;


    public DynamicLookupLocatorDiscovery(
            LookupDiscoveryManager ldm,
            boolean runningWithinRegistrar) {
        _ldm = ldm;

        // we only need to postpone initialization in case of use being 
        // registrar because we want to begin initialization only after
        // initial seed locators have been set
        if (!runningWithinRegistrar)
            init(null);
    }

    public void init(final ServiceID serviceID) {
        // Started in a different thread because we get a shared service discovery manager
        // and we might be part of a shared lookup discovery manager that is currently being initialized 
        // and is part of another service discovery manager which is also currently being initialized
        // and holds the same lock (SharedDiscoveryManagement.mutex) as we'll be holding.
        // we do this to ensure that we reuse the existing SDM instead of creating a new one,
        // so we spawn a thread and wait on that lock instead of getting a handle to a re-entrant lock (synchronized).
        // also, this is a long running task and we don't want to block the instantiation of the lookup discovery
        // manager.
        new Thread(new Runnable() {
            @Override
            public void run() {
                synchronized (_lock) {
                    try {
                        // someone could have called terminate by now
                        if (!_terminated) {
                            addDisoveryListenerAndWaitForInitialRegistrars();
                            
                            /*
                             * This test (isDynamicLocatorsEnabledAtRegistrar()) 
                             * is performed when no system wide property has been
                             * set to indicate dynamic locators are enabled (dynamicLocatorsEnabled()). We still check
                             * if any of the registrars denoted by the initial seed locators
                             * supports/use dynamic locators by checking their lookup attributes
                             * for the relevant attribute
                             */
                            if (!dynamicLocatorsEnabled() && !isDynamicLocatorsEnabledAtRegistrar()) {
                                if (_logger.isDebugEnabled())
                                    _logger.debug("Dynamic locators discovery is not enabled at discovered registrars");

                                _terminated = true;
                                return;
                            }

                            Map<ServiceID, LookupLocator> initialLocatorsMap = getInitialLookupLocatorsMap();
                            ServiceRegistrar registrar = getOurRegistrarInNeeded(serviceID, initialLocatorsMap);

                            getServiceDiscoveryManager(initialLocatorsMap);

                            getAndRegisterInLookupCache(initialLocatorsMap, registrar, serviceID);

                            if (_logger.isDebugEnabled())
                                _logger.debug("DynamicLookupLocatorDiscovery initialized");

                            _initializedIndicator.countDown();
                        }
                    } catch (Exception e) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    "Failed initializing DynamicLookupLocatorDiscovery. "
                                            + "Dynamic lookup locators will not work for this service",
                                    e);
                        }
                        _initException = e;
                        terminate();
                    } finally {
                        // this action has no effect if the listener never registered
                        // in the first place
                        _ldm.removeDiscoveryListener(DynamicLookupLocatorDiscovery.this);
                    }
                }
            }

        }).start();
    }

    private void addDisoveryListenerAndWaitForInitialRegistrars() {
        final int numberOfLocators = _ldm.getLocators().length;

        if (numberOfLocators == 0)
            return;

        long initDelay = Long.getLong(SystemProperties.DYNAMIC_LOCATORS_MAX_INIT_DELAY,
                SystemProperties.DYNAMIC_LOCATORS_MAX_INIT_DELAY_DEFAULT);


        // we set the counter here becuase the locators might have set after the 
        // lookup discovery manager has been instantiated
        _initialRegistrarsDiscoveredLatch = new CountDownLatch(numberOfLocators);

        if (_logger.isDebugEnabled()) {
            _logger.debug("Waiting for registrars at " +
                    BootUtil.arrayToCommaDelimitedString(_ldm.getLocators()) +
                    " to be discovered, for at most " + initDelay + "ms");
        }


        _ldm.addDiscoveryListener(DynamicLookupLocatorDiscovery.this);

        try {
            _initialRegistrarsDiscoveredLatch.await(initDelay, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // This thread cannot be interrupted (see init())
        }
    }

    private boolean isDynamicLocatorsEnabledAtRegistrar() {
        // we check if any of the discovered registrars support dynamic locators
        List<ServiceRegistrar> registrars = getRegistrars();

        for (ServiceRegistrar registrar : registrars) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Checking if registrar with service id: " + registrar.getServiceID() +
                        " supports dynamic locators");
            }

            try {
                Administrable administrable = (Administrable) registrar;
                JoinAdmin joinAdmin = (JoinAdmin) administrable.getAdmin();
                for (Entry entry : joinAdmin.getLookupAttributes()) {
                    if (DYNAMIC_LOCATORS_ENABLED_LOOKUP_ATTRIBUTE.equals(entry)) {
                        if (_logger.isDebugEnabled())
                            _logger.debug("Registrar with service id: " + registrar.getServiceID() + " supports dynamic locators");

                        return true;
                    }
                }

                if (_logger.isDebugEnabled())
                    _logger.debug("Registrar with service id: " + registrar.getServiceID() + " does not support dynamic locators");
            } catch (RemoteException e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Failed getting lookup attributes from registrar: " + registrar.getServiceID(), e);
                }
            }

        }
        return false;
    }

    private List<ServiceRegistrar> getRegistrars() {
        synchronized (_initialServiceRegistrarsLock) {
            return new ArrayList<ServiceRegistrar>(_initialRegistrars);
        }
    }

    private void getServiceDiscoveryManager(Map<ServiceID, LookupLocator> initialLocatorsMap) {
        if (_logger.isDebugEnabled()) {
            _logger.debug("Getting ServiceDiscoveryManager with initial service ids: " + initialLocatorsMap.keySet() +
                    ", locators: " + BootUtil.arrayToCommaDelimitedString(_ldm.getLocators()) +
                    ", groups: " + BootUtil.arrayToCommaDelimitedString(_ldm.getGroups()));
        }

        try {
            SharedServiceDiscoveryManagerResult result =
                    SharedDiscoveryManagement.getServiceDiscoveryManagerResult(_ldm,
                            _ldm.getGroups(),
                            _ldm.getLocators(),
                            null);
            _sdm = result.sdm;
            _isSdmManaged = result.isManaged;

            if (_logger.isTraceEnabled())
                _logger.trace("Number of shared SDM after creation: " + _sdm.getRefCounter() +
                        ", Is sdm managed: " + _isSdmManaged);

        } catch (IOException e) {
            throw new DynamicLookupLocatorDiscoveryException("Failed getting service discovery manager", e);
        }
    }

    private LookupCache getAndRegisterInLookupCache(
            Map<ServiceID, LookupLocator> initialLocatorsMap,
            ServiceRegistrar registrar,
            ServiceID serviceID) {
        if (_logger.isDebugEnabled())
            _logger.debug("Creating lookup cache for ServiceRegistrar services and adding listener");

        try {
            _lookupCache = _sdm.createLookupCache(LOOKUP_CACHE_TEMPLATE, null, null);
            _listener = new ServiceRegistrarServiceDiscoveryListener(_ldm,
                    initialLocatorsMap,
                    registrar,
                    serviceID);
            _lookupCache.addListener(_listener);

            return _lookupCache;
        } catch (Exception e) {
            throw new DynamicLookupLocatorDiscoveryException("Failed creating lookup cache", e);
        }
    }

    private ServiceRegistrar getOurRegistrarInNeeded(final ServiceID serviceID,
                                                     Map<ServiceID, LookupLocator> initialLocatorsMap) {
        if (serviceID == null)
            return null;

        ServiceRegistrar registrar = null;

        List<ServiceRegistrar> registrars = getRegistrars();

        for (ServiceRegistrar reg : registrars) {
            if (reg.getServiceID().equals(serviceID)) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Found registrar with service id: " + serviceID);

                registrar = reg;
                break;
            }
        }

        if (registrar == null) {
            if (_logger.isWarnEnabled())
                _logger.warn("Could not find registrar with service id: " + serviceID +
                        ", this lookup service will not update its state properly");
        }

        return registrar;
    }

    private Map<ServiceID, LookupLocator> getInitialLookupLocatorsMap()

    {
        List<ServiceRegistrar> registrars = getRegistrars();
        Map<ServiceID, LookupLocator> initialLocatorsMap = new HashMap<ServiceID, LookupLocator>();
        for (ServiceRegistrar registrar : registrars) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Trying to get locator from registrar with service id: " + registrar.getServiceID());
            }

            try {
                initialLocatorsMap.put(registrar.getServiceID(), registrar.getLocator());
            } catch (RemoteException e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn(
                            "Failed getting initial locator for registrar with service id: "
                                    + registrar.getServiceID(), e);
                }
            }
        }

        return initialLocatorsMap;
    }

    public void terminate() {
        synchronized (_lock) {
            if (_terminated)
                return;

            // if called before initialization started
            if (_sdm != null) {
                if (_listener != null)
                    _listener.terminate();

                if (_lookupCache != null)
                    _lookupCache.terminate();

                _sdm.terminate();

                // TODO DYNAMIC : remove 
                int sdmRefCounter = _sdm.getRefCounter();

                _sdm = null;

                if (_logger.isTraceEnabled())
                    _logger.trace("Number of shared SDM after termination: " + sdmRefCounter);

            }

            _terminated = true;
        }

        if (_logger.isDebugEnabled())
            _logger.debug("DynamicLookupLocatorDiscovery terminated");
    }

    public boolean awaitInitialization(long timeout, TimeUnit unit) throws InterruptedException {
        return _initializedIndicator.await(timeout, unit);
    }

    public boolean isInitialized() {
        try {
            return awaitInitialization(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    /**
     * @return If an exception occured during initialization, then it will be retured, otherwise,
     * null will be returned
     */
    public Exception getInitializationException() {
        return _initException;
    }


    @Override
    public void discovered(DiscoveryEvent e) {
        synchronized (_initialServiceRegistrarsLock) {
            for (ServiceRegistrar registrar : e.getRegistrars()) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Discovery event for registrar with service id: " + registrar.getServiceID() + " (discovered via initial seed locators)");

                _initialRegistrars.add(registrar);
                _initialRegistrarsDiscoveredLatch.countDown();
            }
        }
    }

    @Override
    public void discarded(DiscoveryEvent e) {
        for (ServiceRegistrar registrar : e.getRegistrars()) {
            if (_logger.isDebugEnabled())
                _logger.debug("Discarded event for registrar with service id: " + registrar.getServiceID() + " (discovered via initial seed locators)");
        }
    }

    /**
     * Adds a listener to be notified when current locators used by the {@link
     * DiscoveryLocatorManagement} are changed by this component
     */
    public void addLookupLocatorsChangeListener(LookupLocatorsChangeListener listener) {
        _listener.addLookupLocatorsChangeListener(listener);
    }

    public void removeLookupLocatorsChangeListener(LookupLocatorsChangeListener listener) {
        _listener.removeLookupLocatorsChangeListener(listener);
    }

}
