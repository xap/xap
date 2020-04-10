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

package com.gigaspaces.cluster.activeelection.core;

import com.gigaspaces.internal.naming.INamingService;
import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;
import com.j_spaces.core.IJSpace;
import com.j_spaces.kernel.SystemProperties;

import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides control for split-brain behavior. If more then active service was found, the
 * controller elects new active and notify the {@link IActiveElectionListener} listener.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 6.0
 **/
@com.gigaspaces.api.InternalApi
public class SplitBrainController {
    final private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLUSTER_ACTIVE_ELECTION);

    private static final int EXTRA_BACKUP_RESOLUTION_RETRIES = Integer.getInteger(SystemProperties.EXTRA_BACKUP_SPACE_RESOLUTION_RETRIES,
            SystemProperties.EXTRA_BACKUP_SPACE_RESOLUTION_RETRIES_DEFAULT);

    private ScheduledExecutorService threadPool = Executors.newSingleThreadScheduledExecutor(new GSThreadFactory(THREAD_EXECUTOR_NAME, true));

    /**
     * this inner class provides identification of discovered ACTIVE services
     */
    final private class ActiveDiscovery
            implements ServiceDiscoveryListener {

        @Override
        synchronized public void serviceAdded(ServiceDiscoveryEvent event) {
            /* late event, controller already terminated */
            if (isTerminated()) {
                return;
            }

            takeActionOnExtraBackup(event);
            takeActionOnSplitBrain(event);
        }

        private void takeActionOnSplitBrain(ServiceDiscoveryEvent event) {

            // check  the number of primary spaces - if less than 2 - it is not a split brain -ignore it
            final List<ServiceItem> splitActives = getActiveServices();
            if (splitActives.size() < 2) {
                return;
            }

            try {
                threadPool.schedule(new Runnable() {
                    @Override
                    public void run() {
                        // check if since this task was submitted the split brain was already resolved
                        if (isTerminated()) {
                            return;
                        }

                        try {
                            //double check if split brain still exists or it has been resolved by the previous task
                            final List<ServiceItem> splitActives = getActiveServices();

                            // check  the number of primary spaces - if less than 2 - it is not a split brain -ignore it
                            if (splitActives.size() < 2)
                                return;

                            _electManager.onSplitBrain(splitActives);
                        } catch (Exception ex) {
                            if (_logger.isErrorEnabled())
                                _logger.error(toString() + " Failed to resolve split-brain.", ex);
                        }
                    }
                }, 10, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ex) {
                //task submission failed due to shutdown
            }
        }

        private void takeActionOnExtraBackup(final ServiceDiscoveryEvent event) {

            if (_electManager.getState().equals(ActiveElectionState.State.ACTIVE)) {
                return;
            }

            try {
                threadPool.scheduleWithFixedDelay(new Runnable() {
                    private int retries = EXTRA_BACKUP_RESOLUTION_RETRIES; //number of scheduled retries allowed; 0 disables any action

                    @Override
                    public void run() {

                        if (isTerminated()) {
                            throw new CancellationException(); //cancel this task
                        }

                        final ServiceItem activeServiceItem = event.getPostEventServiceItem();

                        if ((retries--) == 0) {
                            if (_logger.isWarnEnabled()) {
                                _logger.warn("Exhausted " + EXTRA_BACKUP_RESOLUTION_RETRIES + " retry attempts to resolve " +
                                        "discovery of extra-backup Space using: [" + activeServiceItem + "]");
                            }
                            throw new CancellationException(); //cancel this task
                        }

                        _electManager.onActiveDiscoveryCheckExtraBackup(activeServiceItem);

                    }
                }, 5, 10, TimeUnit.SECONDS);
            } catch (RejectedExecutionException e) {
                //ignore
            }
        }

        @Override
        public void serviceChanged(ServiceDiscoveryEvent event) { // unused for now
        }

        @Override
        public void serviceRemoved(ServiceDiscoveryEvent event) { // unused for now
        }

        private List<ServiceItem> getActiveServices() {
            final ServiceItem[] activeSrv = _electManager.getNamingService().lookup(_template, Integer.MAX_VALUE, null);
            final List<ServiceItem> splitActives = ActiveElectionManager.trimServices(activeSrv);

            removeUnavailableServices(splitActives);
            return splitActives;
        }
    }// ActiveDiscovery

    final static private String THREAD_EXECUTOR_NAME = "GS-SplitBrainExecutor";
    final private ActiveElectionManager _electManager;
    final private LookupCache _namingCache;
    private boolean _isTerminated;
    private final ServiceTemplate _template;

    /**
     * constructor
     */
    SplitBrainController(ServiceTemplate template, ActiveElectionManager electManager)
            throws RemoteException {
        _electManager = electManager;
        _template = template;
        INamingService namingSrv = electManager.getNamingService();
        _namingCache = namingSrv.notify(template, null, new ActiveDiscovery());

        if (_logger.isDebugEnabled())
            _logger.debug(toString() + " started.");
    }

    /**
     * @return <code>true</code> if controller was terminated.
     */
    synchronized private boolean isTerminated() {
        return _isTerminated;
    }

    /**
     * terminate the split brain controller
     */
    synchronized public void terminate() {
      /* already terminated */
        if (isTerminated())
            return;

        _namingCache.terminate();
        _isTerminated = true;

        if (threadPool != null)
            threadPool.shutdown();

        if (_logger.isDebugEnabled())
            _logger.debug(toString() + " terminated.");
    }

    /**
     * @return <code>true</code> if the <code>thread</code> is a SplitBrainExecutor
     */
    final static boolean isSplitBrainExecutor(Thread thread) {
        return thread.getName().equals(THREAD_EXECUTOR_NAME);
    }

    @Override
    public String toString() {
        return "Split-Brain Controller";
    }


    private void removeUnavailableServices(
            final List<ServiceItem> services) {
        // check service accessibility
        for (Iterator<ServiceItem> iterator = services.iterator(); iterator.hasNext(); ) {
            ServiceItem serviceItem = iterator.next();

            IJSpace primarySpace = (IJSpace) serviceItem.getService();
            try {
                primarySpace.ping();
            } catch (RemoteException e) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Primary space [] is not available, ignoring it in election process.");
                iterator.remove();
            }
        }
    }
}
