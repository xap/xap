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

import com.gigaspaces.admin.quiesce.QuiesceState;
import com.gigaspaces.admin.quiesce.QuiesceStateChangedEvent;
import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.cluster.ClusterFailureDetector;
import com.gigaspaces.cluster.activeelection.ICandidateEntry;
import com.gigaspaces.cluster.activeelection.LusBasedSelectorHandler;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionState.State;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.naming.INamingService;
import com.gigaspaces.internal.server.space.quiesce.QuiesceHandler;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyAttributeStoreException;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin;
import com.j_spaces.core.admin.StatisticsAdmin;
import com.j_spaces.core.filters.ReplicationStatistics;
import com.j_spaces.core.filters.ReplicationStatistics.OutgoingChannel;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.service.Service;
import com.j_spaces.lookup.entry.HostName;
import com.sun.jini.lookup.entry.LookupAttributes;

import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.id.ReferentUuid;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * ActiveElectionManager based on distributed naming service {@link com.gigaspaces.internal.naming.INamingService}
 * and provides a distributed active election algorithm to select only <b>one active service</b>
 * between all candidates. <p> NOTE: The manage service <b>must</b> provide consistent
 * <code>equals()</code> method implementation or implement {@link net.jini.id.ReferentUuid}
 * interface.
 *
 * Example:
 * <pre>
 *   <code>
 *    public class ServiceIDOrderFilter
 *      implements IActiveElectionDecisionFilter
 *    {
 *  	 ServiceID _srvID;
 *
 *     public boolean isAcceptable(ActiveElectionState.State advanceState, List<ServiceItem>
 * candidateSrv )
 *     {
 *       for( ServiceItem srvItem : candidateSrv )
 *       {
 *        // return true if _srvID is the greater from all candidateSrv.serviceID;
 *       }
 *     }
 *    }
 *
 *    public class ElectionListener
 *    		implements IActiveElectionListener
 *    {
 *      ActiveElectionManager _electManager;
 *
 *      public void onActive( ActiveElectionEvent event )
 *      {
 *        System.out.println("OnActive -> Elected Active: " + event.getActiveServiceItem() );
 *
 *        if ( _electManager.getState() == ActiveElectionState.State.ACTIVE )
 *          System.out.println("The manage service is an ACTIVE");
 *        else
 *          System.out.println("The manage service is an NOT an ACTIVE");
 *
 *       }
 *
 *       public void onSplitBrain( ActiveElectionEvent event )
 *       {
 *         System.out.println("OnSplit -> Elected Active: " + event.getActiveServiceItem() ); *
 *       }
 *     }
 *
 *    public static void main( String[] args )
 *    {
 *     IJSpace space;
 *     ServiceTemplate srvTemplate;
 *     INamingService lookupSrv    = new LookupNamingService();
 *     ElectionListener listener   = new ElectionListener();
 *     ServiceIDOrderFilter filter = new ServiceIDOrderFilter();
 *
 *     ActiveElectionManager electManager = new ActiveElectionManager( space, srvTemplate,
 * lookupSrv, listener, filter );
 *     electManager.elect();
 *    }
 *   </code>
 * </pre>
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see com.gigaspaces.cluster.activeelection.core.IActiveElectionDecisionFilter
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class ActiveElectionManager {
    private final Logger _logger;
    private final Logger _xbLogger;

    final private ActiveElectionConfig _config;

    private final IActiveElectionListener _listener;
    private final IActiveElectionDecisionFilter _decisionFilter;
    private final SplitBrainRecoveryHolder splitBrainRecoveryHolder;

    private ActiveFailureDetector _activeFailureDetector;
    private SplitBrainController _splitBrainController;

    final private INamingService _namingService;
    final private ElectionEntry _electTemplate;
    final private AtomicBoolean recoveryIndicator;

    private State _currentState = State.NONE;

    private boolean _isTerminated;

    private final ClusterFailureDetector _clusterFailureDetector;

    /**
     * the extension of service-template. For internal usage only
     */
    final private static class ElectionEntry
            extends ServiceTemplate
            implements Cloneable {
        private static final long serialVersionUID = 1L;

        ActiveElectionState _actState;

        /**
         * not <code>null</code> if the manage service implements {@link net.jini.id.ReferentUuid}
         * interface
         */
        ServiceID _serviceID;

        /**
         * A service object
         */
        transient Object service;

        private ElectionEntry(Object service, ServiceTemplate srvTemplate) {
            /* constructor to make compiler happy :) */
            super(null, null, null);

            this.service = service;

            if (service instanceof ReferentUuid) {
                long mostSignBits = ((ReferentUuid) service).getReferentUuid().getMostSignificantBits();
                long leastSignBits = ((ReferentUuid) service).getReferentUuid().getLeastSignificantBits();
                _serviceID = new ServiceID(mostSignBits, leastSignBits);
            }

            /* keep the reference so that we can change the desired state on demand */
            _actState = new ActiveElectionState();

            /* Attribute set templates to match, or <tt>null</tt>. */
            this.attributeSetTemplates = LookupAttributes.add(srvTemplate.attributeSetTemplates, new Entry[]{_actState});

            /* Service types to match */
            this.serviceTypes = srvTemplate.serviceTypes;
        }// constructor

        /**
         * clone the ElectionEntry so that don't override on direct reference of
         * ActiveElectionState
         */
        @Override
        public ElectionEntry clone() {
            try {
                ElectionEntry cloneEntry = (ElectionEntry) super.clone();
                cloneEntry._actState = new ActiveElectionState();
                cloneEntry.attributeSetTemplates = attributeSetTemplates.clone();

                for (int i = 0; i < cloneEntry.attributeSetTemplates.length; i++) {
                    if (cloneEntry.attributeSetTemplates[i] instanceof ActiveElectionState) {
                        cloneEntry.attributeSetTemplates[i] = cloneEntry._actState;
                        break;
                    }
                }// for

                return cloneEntry;
            } catch (CloneNotSupportedException e) {
                // this shouldn't happen, since we are Cloneable
                throw new InternalError();
            }
        }

        private Object getService() {
            return service;
        }

        /**
         * set the desired state for this service template
         */
        private void setState(State state) {
            _actState.setState(state);
        }

        /**
         * @return the serviceID of manage service, null <code>null</code> if service implements
         * {@link net.jini.id.ReferentUuid} interface
         **/
        private ServiceID getServiceID() {
            return _serviceID;
        }
    }


    /**
     * Constructs an instance of this class that will add {@link com.gigaspaces.cluster.activeelection.core.ActiveElectionState}
     * to the set of service attributes see: {@link com.gigaspaces.internal.naming.INamingService
     * #addNamingAttributes(Object, Entry[])}.
     *
     * @param service                the service candidate which a participate in active
     *                               election.<br> NOTE: the service <b>must</b> provide consistent
     *                               <code>equals()</code> method implementation or implement {@link
     *                               ReferentUuid} interface.
     * @param participantSrvTemplate the template to match participant services with match
     *                               attributes set.
     * @param namingSrv              the naming service provider to lookup services, change states
     *                               and etc...
     * @param activeElectionListener the listener implementation to receive notification about new
     *                               active candidate.
     * @param decisionFilter         the decision filter is call to get acceptance to advance new
     *                               state.
     * @throws com.gigaspaces.cluster.activeelection.core.ActiveElectionException Failed to initialize
     *                                                                            ActiveElectionManager,
     *                                                                            naming service
     *                                                                            communication
     *                                                                            problem.
     **/
    public ActiveElectionManager(String nodeName,
                                 Object service,
                                 ServiceTemplate participantSrvTemplate,
                                 INamingService namingSrv,
                                 IActiveElectionListener activeElectionListener,
                                 IActiveElectionDecisionFilter decisionFilter,
                                 ActiveElectionConfig config,
                                 ClusterFailureDetector clusterFailureDetector,
                                 SplitBrainRecoveryHolder splitBrainRecoveryHolder,
                                 AtomicBoolean recoveryIndicator)
            throws ActiveElectionException {
        _logger = Logger.getLogger(Constants.LOGGER_CLUSTER_ACTIVE_ELECTION + "." + nodeName);
        _xbLogger = Logger.getLogger(Constants.LOGGER_CLUSTER_ACTIVE_ELECTION_XBACKUP + "." + nodeName);
        if (service == null)
            throw new NullPointerException("service can not be null.");

        if (participantSrvTemplate.attributeSetTemplates == null &&
                participantSrvTemplate.serviceTypes == null)
            throw new IllegalArgumentException("ServiceTemplate can not be initialized with attributeSetTemplates=null and serviceTypes=null");

        if (!(service instanceof ReferentUuid)) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.warning(service + " is not implements [" + ReferentUuid.class.getName() + "] interface. " +
                        "			  In this case the service must provide the consistent equals() implementation.");
        }

        _electTemplate = new ElectionEntry(service, participantSrvTemplate);
        _namingService = namingSrv;
        _listener = activeElectionListener;
        _decisionFilter = decisionFilter;
        _config = config;
        _clusterFailureDetector = clusterFailureDetector;
        this.splitBrainRecoveryHolder = splitBrainRecoveryHolder;
        this.recoveryIndicator = recoveryIndicator;

        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config("Initialized with: " +
                    "\n\t\t ServiceTemplate: " + participantSrvTemplate +
                    "\n\t\t NamingService: " + namingSrv.getName() +
                    "\n\t\t ActiveElectionListener: " + activeElectionListener +
                    "\n\t\t DecisionFilter: " + decisionFilter.getClass().getName() +
                    "\n\t\t " + _config);
        }

        /* add ActiveElectionState on naming service */
        initElectionState();
    }

    /**
     * @return true if backup Space is still in recovery or false if recovery completed.
     */
    protected boolean isSpaceInRecovery() {
        return recoveryIndicator.get();
    }


    /**
     * Add {@link com.gigaspaces.cluster.activeelection.core.ActiveElectionState} attribute on
     * naming service with PENDING state
     */
    private void initElectionState()
            throws ActiveElectionException {
        try {
            int retryCount = _config.getRetryConnection(); // retry count with count-down
            for (int i = retryCount; i >= 0 && !isTerminate(); --retryCount) {
                // check if ActiveElectionState.State already part of the attributes
                registerPendingState();

                // check whether PENDING state available on naming service
                if (confirmPendingStateRegistration(retryCount)) {
                    break;
                } else if (retryCount == 0) {
                    throw new ActiveElectionException("Failed to initialize " + toString() + ". Check that [" + _namingService.getName() + "] is available.");
                }
            }

            setCurrentState(State.PENDING);

            if (_logger.isLoggable(Level.FINE))
                _logger.fine("Added initial [" + State.PENDING + "] state on [" + _namingService.getName() + "]");

        } catch (RemoteException ex) {
            String exMsg = "ActiveElectionManager failed to add [" + ActiveElectionState.class.getName() + "] attribute on [" + _namingService.getName() + "] naming service.";
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, exMsg, ex);

            throw new ActiveElectionException(exMsg, ex);
        }
    }

    private void registerPendingState() throws RemoteException {
        Entry[] srvAttr = _namingService.getLookupAttributes(_electTemplate.getService());

        boolean isFound = false;
        for (Entry e : srvAttr) {
            if (e instanceof ActiveElectionState) {
                isFound = true;
                break;
            }
        }// for

        if (isFound) {
            _logger.fine("register [PENDING] state as existing service attributes");
            changeState(null /* any state */, State.PENDING, true /* force */);
        } else {
            _logger.fine("register [PENDING] state as new service attributes");
            _namingService.addNamingAttributes(_electTemplate.getService(), new Entry[]{new ActiveElectionState(State.PENDING)});
        }
    }


    /**
     * Confirmation PENDING state registration on naming service.
     *
     * @return <code>true</code> if registration confirmed, otherwise <code>false</code>.
     * @throws com.gigaspaces.cluster.activeelection.core.ActiveElectionException failed to confirm,
     *                                                                            the process might
     *                                                                            be interrupted.
     **/
    protected boolean confirmPendingStateRegistration(int retryCount) throws ActiveElectionException {
        /* check whether PENDING state was registered on naming service, wait a bit if naming service was not updated yet */
        //noinspection SpellCheckingInspection
        ServiceTemplate srvTmpl = new ServiceTemplate(_electTemplate._serviceID, new Class[]{Service.class}, new Entry[]{new ActiveElectionState(State.PENDING)});
        ServiceItem[] srvMatch = _namingService.lookup(srvTmpl, 1, null /* filter */);
        if (srvMatch == null || srvMatch.length == 0) {
            if (_logger.isLoggable(Level.INFO)) {
                _logger.log((retryCount <= 10 ? Level.WARNING : Level.INFO),
                        "Waiting [" + _config.getYieldTime() + " ms] for [" + State.PENDING + "] state registration on " + _namingService.getName()
                                + " {remaining retries=" + retryCount + ", registrars=" + _namingService.getNumberOfRegistrars() + "}");
            }

            try {
                Thread.sleep(_config.getYieldTime());
            } catch (InterruptedException ex) {
                // restore the interrupted status and return
                Thread.currentThread().interrupt();

                throw new ActiveElectionException("ActiveElectionManager process was interrupted.");
            }
            /* failed to confirm server registration */
            return false;
        } else {

            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("[" + State.PENDING + "] state registration successfully confirmed by " + _namingService.getName());
            }
            return true; // registration confirmed
        }
    }


    /**
     * Lookup the services with desired state.
     *
     * @param state      The state to find.
     * @param maxMatches maximum number of matched services.
     * @return The matched services or <code>null</code> if no service was found.
     **/
    protected List<ServiceItem> lookup(final State state, int maxMatches) throws InterruptedException {
        /* sleep a bit to give the opportunity for discovery
         * TODO change _namingService.lookup to accept wait duration before returning with no result */
        sleepYieldTime();

        _electTemplate.setState(state);
        ServiceItem[] foundSrv = _namingService.lookup(_electTemplate, maxMatches, null /*filter*/);

        if (foundSrv == null) {
            if (_logger.isLoggable(Level.FINEST)) {
                _logger.finest("Lookup service not found while querying for state: " + _electTemplate._actState.getState());
            }
            return null;
        }

        /*
         * The same service may be registered on multiple naming services. 
         * Removes the duplicates ServiceItems by ServiceID.
         */
        List<ServiceItem> matchedSrv = trimServices(foundSrv);

        if (_logger.isLoggable(Level.FINEST)) {
            int duplicates = foundSrv.length - matchedSrv.size();
            _logger.finest("Found: [" + matchedSrv.size() + "] matches for " +
                    " serviceTemplate: [" + _electTemplate + "]; matched services: " + matchedSrv +
                    (duplicates > 0 ? " duplicates: [" + duplicates + "]" : ""));
        }

        return matchedSrv;
    }


    /**
     * Change the service state on naming service.
     *
     * @param oldState oldState the old service state.
     * @param newState newState the new service state.
     * @param force    <code>true</code> if the newState <b>must</b> be changed without throwing
     *                 RemoteException. This operation will continue with retrying until the change
     *                 state operation will be succeed or election manager was terminated.
     * @throws java.rmi.RemoteException Failed to change state on naming-service, the reason might
     *                                  be a network problem.
     **/
    protected boolean changeState(State oldState, State newState, boolean force)
            throws RemoteException {
        Object service = _electTemplate.getService();

        ActiveElectionState oldJoinState = new ActiveElectionState(oldState);
        ActiveElectionState newJoinState = new ActiveElectionState(newState);

        while (!isTerminate()) {
            try {
                _namingService.modifyNamingAttributes(service, new Entry[]{oldJoinState}, new Entry[]{newJoinState});

                setCurrentState(newState);

                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("Changed state from [" + (oldState == null ? "any" : oldState) + "] to [" + newState + "]");
                }

                return true;
            } catch (RemoteException ex) {
                String msg = "ChangeState failed for service: " + service.getClass();

                if (force)
                    msg = msg + ".ForceChange enabled - Retry again...";

                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, msg, ex);

                if (force) {
                    try {
                        Thread.sleep(_config.getYieldTime());
                        continue; // retry again
                    } catch (InterruptedException e) {
                        return false;
                    }
                }

                throw new RemoteException(msg, ex);
            }// catch
        }// while

        /* satisfy the compiler */
        return false;
    }


    /**
     * Trim the list of ServiceItems by removing the duplicates ServiceItems from the list. The
     * duplicate items are identified by ServiceItem.serviceID.
     *
     * @param srvSet The array of services to trim.
     * @return the trim list of services.
     **/
    static List<ServiceItem> trimServices(ServiceItem[] srvSet) {
        TreeSet<ServiceItem> treeSet = new TreeSet<ServiceItem>(new Comparator<ServiceItem>() {
            @Override
            public int compare(ServiceItem o1, ServiceItem o2) {
                ServiceID srvID_1 = o1.serviceID;
                ServiceID srvID_2 = o2.serviceID;

                // The ordering is intentionally set up so that the UUIDs
                // can simply be numerically compared as two numbers
                return (srvID_1.getMostSignificantBits() < srvID_2.getMostSignificantBits() ? -1 :
                        (srvID_1.getMostSignificantBits() > srvID_2.getMostSignificantBits() ? 1 :
                                (srvID_1.getLeastSignificantBits() < srvID_2.getLeastSignificantBits() ? -1 :
                                        (srvID_1.getLeastSignificantBits() > srvID_2.getLeastSignificantBits() ? 1 : 0))));
            }
        });


        if (srvSet != null) {
            Collections.addAll(treeSet, srvSet);
        }

        List<ServiceItem> trimSet = new ArrayList<ServiceItem>();
        trimSet.addAll(treeSet);

        return trimSet;
    }

    /**
     * Poll the desired service state on naming service while available, when the
     * <code>pollingState</code> is not available make an attempt to find an ACTIVE state, if ACTIVE
     * was found notify the {@link com.gigaspaces.cluster.activeelection.core.IActiveElectionListener}
     * and return the active service.
     *
     * @param pollingState the polling service state.
     * @return The ACTIVE service or <code>null</code> if ACTIVE was not found <b>and</b> polling
     * state is not available on naming service..
     **/
    protected ServiceItem pollStateUntilAvailable(State pollingState)
            throws InterruptedException {
        while (!isTerminate()) {
            List<ServiceItem> matchedSrv = lookup(pollingState, 1 /* at least one */);
            if (matchedSrv == null || matchedSrv.isEmpty()) {
                break; //lookup not available or no results for state
            }

            if (_logger.isLoggable(Level.FINE))
                _logger.fine("Found better election candidate - continue polling [" + pollingState + "]...");

            /* polling state is still available, check if one from the candidate already become an ACTIVE */
            ServiceItem active = findActive();
            if (active != null)
                return active;
        }

        /* polling state is not available */
        return null;
    }


    /**
     * Request to advance to the new state. This method collects all candidates with currentState
     * and only if {@link com.gigaspaces.cluster.activeelection.core.IActiveElectionDecisionFilter#isAcceptable}
     * return <code>true</code> this manage service can advance to the <code>acquireState</code>
     * state.
     *
     * @param currentState the current state.
     * @param acquireState the requesting state to acquire.
     * @return <code>true</code> the manage service can advance the state from currentState to
     * acquireState, <code>false</code> another candidate should advance first, or current state not
     * in lookup.
     * @throws java.rmi.RemoteException Failed to communicate with Naming Service.
     * @throws InterruptedException     current thread was interrupted.
     **/
    protected boolean isAdvanceToStateAllowed(State currentState, State acquireState)
            throws RemoteException, InterruptedException {
        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("Request to advance from [" + currentState + "] to [" + acquireState + "]");
        }

        List<ServiceItem> matchSrv = lookup(currentState, Integer.MAX_VALUE /* all service with matched state */);

        /* lookup returns null in the case there are no lookup services registered */
        if (matchSrv == null) {
            throw new RemoteException(_namingService.getName() + " is not available");
        }

        /* unexpected behavior, failed to find currentState (can be due to lease expiration of service) */
        if (matchSrv.isEmpty()) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.warning("Cannot advance from [" + currentState + "] to [" + acquireState + "] - Current state is not available on " + _namingService.getName()
                        + "; force change state to [" + State.PENDING + "]");
            }
            changeState(null /* any state */, State.PENDING, true /* force */);
            sleepYieldTime();
            return false;
        }

        if (!_decisionFilter.isAcceptable(acquireState, matchSrv)) {
            if (_logger.isLoggable(Level.WARNING)) {
                _logger.warning("Cannot advance from [" + currentState + "] to [" + acquireState + "] - found better candidate"
                        + "; force change state to [" + State.PENDING + "]");
            }
            changeState(null /* any state */, State.PENDING, true /* force */);
            sleepYieldTime();
            return false;
        }

        //allowed to advance from current state to acquire state
        return true;
    }

    /**
     * change current state to acquire state in lookup service
     *
     * @return false if active was found or a remote exception was caught during change state. true
     * if no active was found and current state changed to acquire state
     */
    private boolean doChangeState(State currentState, State acquireState)
            throws RemoteException, InterruptedException {

        //before changing state, verify no other candidate has already reached ACTIVE state
        ServiceItem activeCandidate = findActive();
        if (activeCandidate != null) {
            if (_logger.isLoggable(Level.FINE)) {
                _logger.fine("Advance from [" + currentState + "] to [" + acquireState + "] was rejected - found [" + activeCandidate.service + "] in [ACTIVE] state");
            }
            return false;
        }

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("Advance from [" + currentState + "] to [" + acquireState + "] was accepted");
        }

        /* Offers itself as candidate for leadership with acquire-state */
        return changeState(currentState, acquireState, false /* force */);
    }

    private void sleepYieldTime() throws InterruptedException {
        long yieldTime = _config.getYieldTime();
        Thread.sleep(yieldTime);
    }

    /**
     * Find an ACTIVE service with the election template. If an active was found this service: <br>
     * 1. Register to the {@link com.gigaspaces.cluster.activeelection.core.ActiveFailureDetector},
     * so that to get notification on active failure.<br> 2. Notify the {@link
     * com.gigaspaces.cluster.activeelection.core.IActiveElectionListener} about new active
     * service.
     *
     * If split-brain has been identified( two or more active services were found), lookup again the
     * active while the split-bran will be resolved.
     *
     * @return the active service or <code>null</code> active not found.
     * @throws InterruptedException throws when operation was interrupted.
     **/
    protected ServiceItem findActive() throws InterruptedException {
        /* do while if in-case of split-brain */
        while (!isTerminate()) {
            List<ServiceItem> matchedSrv = lookup(State.ACTIVE, Integer.MAX_VALUE);
            if (matchedSrv == null || matchedSrv.isEmpty()) {
                return null; //lookup not available or no result for state ACTIVE
            }

            /* discovered more then one ACTIVE service, network split-brain */
            if (matchedSrv.size() > 1) {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("Identified network split-brain. Discovered [" +
                            matchedSrv.size() + "] " +
                            State.ACTIVE + " services. Waiting for split-brain resolution...");
                }

                continue;
            }// if matchedSrv

            ServiceItem activeService = matchedSrv.get(0);

            try {
                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("Found [" + activeService.service + "] in ACTIVE state");
                }

                /* register the active service to failure detector to get a callback on active failure  */
                ActiveFailureDetector detector = _activeFailureDetector;
                if (detector == null || detector.isTerminate()) {
                    _activeFailureDetector = new ActiveFailureDetector(this, activeService);
                }

                if (_logger.isLoggable(Level.FINE)) {
                    _logger.fine("Registered active failure callback for [" + activeService.service + "]");
                }
            } catch (RemoteException e) {
                /*
                 * the active space was found, but we failed to monitor it,
                 * handle as no active found.
                 */
                return null;
            }

            /* notify the listener about new ACTIVE */
            notifyListenerOnActive(activeService);

            return activeService;
        }// while


        /* active not found */
        return null;
    }


    /**
     * Elect the active service, this method will be blocked while the active service will be
     * elected.
     *
     * @throws InterruptedException                                               thrown when operation
     *                                                                            was interrupted.
     * @throws com.gigaspaces.cluster.activeelection.core.ActiveElectionException failed to elect
     *                                                                            active service,
     *                                                                            might be {@link
     *                                                                            com.gigaspaces.internal.naming.INamingService}
     *                                                                            communication.
     **/
    public void elect()
            throws InterruptedException, ActiveElectionException {
        /* indication for finally block */
        boolean isException = false;

        /* allows to elect only with PENDING state */
        if (getState() != State.PENDING)
            return;

        try {
            //noinspection LoopStatementThatDoesntLoop
            while (!isTerminate()) {
                try {
                    /**
                     * Lookup ACTIVE state or wait until ACTIVE if any other candidate is already in PREPARE
                     */
                    if (findActive() != null) {
                        return;
                    } else if (pollStateUntilAvailable(State.PREPARE) != null) {
                        return;
                    }

                    /*
                     * Request to acquire ActiveElection state from PENDING --> PREPARE
                     * - return if Active service was found or
                     * - continue if could not advance to acquire state
                     * - otherwise changed state
                     */
                    if (findActive() != null) {
                        return;
                    } else if (!isAdvanceToStateAllowed(State.PENDING, State.PREPARE)) {
                        continue;
                    } else if (!doChangeState(State.PENDING, State.PREPARE)) {
                        return;
                    }

                    /*
                     * Request to acquire ActiveElection state from PREPARE --> ACTIVE
                     * - return if Active service was found
                     * - continue if could not advance to acquire state
                     * - otherwise changed state
                     */
                    if (findActive() != null) {
                        return;
                    } else if (!isAdvanceToStateAllowed(State.PREPARE, State.ACTIVE)) {
                        continue;
                    } else if (!doChangeState(State.PREPARE, State.ACTIVE)) {
                        return;
                    }

                    /* become to be an ACTIVE */
                    notifyListenerOnActive(new ServiceItem(_electTemplate.getServiceID(), _electTemplate.getService(), null /*entry[]*/));
                    return;

                }
                // AttributeStore failed to set or get state
                catch (DirectPersistencyRecoveryException ex) {
                    try {
                        if (_logger.isLoggable(Level.WARNING)) {
                            if (ex instanceof DirectPersistencyAttributeStoreException) {
                                _logger.log(Level.WARNING, "Failed to set or get last primary state using AttributeStore, will try to reelect...", ex);
                            } else {
                                _logger.log(Level.WARNING, "Failed to elect as primary", ex);
                            }
                        }

                        isException = true;
                        changeState(getState(), State.PENDING, true);
                    } catch (RemoteException e) {
                        isException = true;
                        String exMsg = "Failed to communicate with [" + _namingService.getName() + "] naming service while changing state from " + getState() + " to PENDING. ";
                        throw new ActiveElectionException(exMsg, ex);
                    }
                } catch (RemoteException ex) {
                    isException = true;

                    String exMsg = "Failed to communicate with [" + _namingService.getName() + "] naming service. ";

                    throw new ActiveElectionException(exMsg, ex);
                }
            }// while
        } finally {
            /* exception happen during election manager initialization, continue throwing exception */
            if (!isException) {
                try {
                    /* if we got PREPARE state, but failed to acquired ACTIVE, move to initial PENDING */
                    if (getState() == State.PREPARE) {
                        changeState(State.PREPARE, State.PENDING, true /* force */);
                    }
                } catch (RemoteException ex) {
                    /* can't happen with force=true, but if so let's throw */
                    //noinspection ThrowFromFinallyBlock
                    throw new ActiveElectionException("Force change state failed.", ex);
                }

                /* start split brain controller to identify collision of ACTIVE services */
                try {
                    startSplitBrainController();
                } catch (Exception ex) {
                    //noinspection ThrowFromFinallyBlock
                    throw new ActiveElectionException("Failed to start SplitBrainController.", ex);
                }
            }// if
        }// finally
    }// elect()

    /**
     * Forcefully make this space to be primary
     */
    public void forceMoveToPrimary() throws RemoteException {
        if (getState() == State.ACTIVE)
            return;

        changeState(State.PENDING,
                State.ACTIVE,
                true /* force */);

        notifyListenerOnActive(new ServiceItem(_electTemplate.getServiceID(),
                _electTemplate.getService(),
                null /* entry[] */));
    }

    /**
     * notify the {@link com.gigaspaces.cluster.activeelection.core.IActiveElectionListener} about
     * elected ACTIVE service or an ACTIVE on split-brain occurrence.
     **/
    protected void notifyListenerOnActive(ServiceItem activeService) {
        /* a bit tricky, but can identify whether this call comes from SplitBrainController thread */
        if (SplitBrainController.isSplitBrainExecutor(Thread.currentThread())) {
            _listener.onSplitBrain(new ActiveElectionEvent(activeService));
        } else {
            _listener.onActive(new ActiveElectionEvent(activeService));
        }
    }


    /**
     * The callback method from {@link com.gigaspaces.cluster.activeelection.core.SplitBrainController}
     * on 2 or more identified ACTIVE services. This method should resolve the split-brain and only
     * 1 ACTIVE service should stay in the network.
     *
     * @throws java.rmi.RemoteException                                           Failed to communicate
     *                                                                            with naming service.
     * @throws com.gigaspaces.cluster.activeelection.core.ActiveElectionException Election failed
     * @throws InterruptedException                                               on split brain
     *                                                                            resolution was
     *                                                                            interrupted.
     **/
    protected void onSplitBrain(List<ServiceItem> splitActives) throws RemoteException, InterruptedException, ActiveElectionException {
        if (_activeFailureDetector != null)
            _activeFailureDetector.terminate();

        logSplitBrainDetection(splitActives);

        SplitBrainRecoveryPolicy splitBrainRecoveryPolicy = splitBrainRecoveryHolder.getSplitBrainRecoveryPolicy();
        _logger.info("Split-brain recovery policy is: " + splitBrainRecoveryPolicy.toString());

        if (SplitBrainRecoveryPolicy.DISCARD_LEAST_CONSISTENT.equals(splitBrainRecoveryPolicy)) {
            discardLeastConsistentOnSplitBrainDetection(splitActives);
        } else if (SplitBrainRecoveryPolicy.SUSPEND_PARTITION_PRIMARIES.equals(splitBrainRecoveryPolicy)) {
            suspendPartitionPrimariesOnSplitBrainDetection();
        }

    }// onSplitBrain

    //@since 10.2.0
    private void suspendPartitionPrimariesOnSplitBrainDetection() {
        QuiesceHandler quiesceHandler = splitBrainRecoveryHolder.getQuiesceHandler();
        if (!quiesceHandler.isOn()) {
            String description = "Space instance [" + _electTemplate.service + "] is in Quiesce state until split-brain is resolved";
            QuiesceToken quiesceToken = quiesceHandler.createSpaceNameToken();
            quiesceHandler.setQuiesceMode(new QuiesceStateChangedEvent(QuiesceState.QUIESCED,
                    quiesceToken, description));
            if (quiesceHandler.isOn()) {
                _logger.info(description + " - Quiesce token [" + quiesceToken + "]");
            } else if (!quiesceHandler.isSupported()) {
                _logger.warning("Split-brain will need to be resolved by terminating extra primary instances manually");
            }
        } else {
            _logger.info("Space instance [" + _electTemplate.service + "] is already in Quiesce state; awaiting resolution of split-brain");
        }
    }

    private void discardLeastConsistentOnSplitBrainDetection(List<ServiceItem> splitActives) throws InterruptedException, ActiveElectionException {
        _logger.info("Space instance [" + _electTemplate.service + "] is trying to resolve the split brain");

            /* true if the managed service is an ACTIVE */
        if (getState() == State.ACTIVE) {
            List<SplitBrainServiceEntry> splitBrainServiceEntries = getSplitBrainServices(splitActives);
            Collections.sort(splitBrainServiceEntries);

            SplitBrainServiceEntry electedPrimary = splitBrainServiceEntries.get(0);
            //log the order of
            logSplitBrainResolution(splitBrainServiceEntries, electedPrimary);

            if (electedPrimary.getService().serviceID.equals(_electTemplate._serviceID)) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.fine("Split-brain resolved. Staying Primary");

                _listener.onSplitBrainActive(new ActiveElectionEvent(electedPrimary.getService()));
            } else {
                /* not acceptable --> "shoot in the head!" found better ACTIVE */
                if (_logger.isLoggable(Level.FINE)) {
                    Object service = electedPrimary.getService().getService();
                    _logger.fine("Split-brain resolved. Space [" + service + "] remains Primary. ");
                }

                // change the space state to unhealthy
                _listener.onSplitBrainBackup(new ActiveElectionEvent(electedPrimary.getService()));
            }
        } else {
            /* the manage service in PENDING state, on split-brain find new ACTIVE */
            _logger.fine(" Split-brain -> looking for a new Primary.");
            elect();
        }
    }


    private void logSplitBrainResolution(
            List<SplitBrainServiceEntry> splitBrainServiceEntries,
            SplitBrainServiceEntry electedPrimary) {
        if (_logger.isLoggable(Level.INFO)) {
            String resolutionReason = getResolutionReason(splitBrainServiceEntries);
            Object service = electedPrimary.getService().getService();
            StringBuilder sb = new StringBuilder();
            sb.append("Split brain was resolved - ").append(service).append(" was chosen because - ").append(resolutionReason).append(".\n");
            sb.append(" Split-brain resolution priority list [");
            sb.append(StringUtils.NEW_LINE);

            for (int i = 0; i < splitBrainServiceEntries.size(); i++) {
                SplitBrainServiceEntry serviceEntry = splitBrainServiceEntries.get(i);
                sb.append(i + 1).append(". ");
                sb.append(serviceEntry.toString());
                sb.append(StringUtils.NEW_LINE);
            }
            sb.append("]");
            _logger.info(sb.toString());
        }
    }

    private void logSplitBrainDetection(List<ServiceItem> splitActives) {
        if (_logger.isLoggable(Level.WARNING)) {
            StringBuilder logBuf = new StringBuilder();
            //noinspection StringConcatenationInsideStringBufferAppend
            logBuf.append("Split-Brain detected by space instance [" + _electTemplate.service + "]. There is more than one primary space. Primary spaces are:\n");

            for (int i = 0; i < splitActives.size(); i++) {
                ServiceItem activeServiceItem = splitActives.get(i);
                String activeHost = HostName.getHostNameFrom(activeServiceItem.attributeSets);
                Object service = activeServiceItem.getService();
                logBuf.append(" ").append(i + 1).append(". [")
                        .append(service)
                        .append("] space");
                if (activeHost != null)
                    logBuf.append(" on [")
                            .append(activeHost)
                            .append("] machine.");

                logBuf.append("\n");
            }

            _logger.warning(logBuf.toString());
        }
    }

    /**
     * A backup Space verifies that it is a replication target of an active primary Space. If the
     * primary Space is already connected to a backup by the same name, but a different service ID
     * then this backup should move to stopped state and be declared 'unhealthy'.
     *
     * @param activeServiceItem the primary service received from the LUS
     */
    public void onActiveDiscoveryCheckExtraBackup(ServiceItem activeServiceItem) {
        //this backup target details
        final String targetMemberName = String.valueOf(_electTemplate.service);
        final ServiceID targetServiceID = _electTemplate.getServiceID();

        //verify instance has its mode set to backup
        if (!getSpaceMode().equals(SpaceMode.BACKUP)) {
            if (_xbLogger.isLoggable(Level.FINE)) {
                _xbLogger.log(Level.FINE, "Space instance [" + _electTemplate.service + "] current space mode ["
                        + getSpaceMode() + "] - validation requires [" + SpaceMode.BACKUP + "]");
            }
            throw new CancellationException(); //cancel this task
        }

        //verify instance is not elected (election happens before SpaceMode change)
        if (getState().equals(State.ACTIVE)) {
            if (_xbLogger.isLoggable(Level.FINE)) {
                _xbLogger.log(Level.FINE, "Space instance [" + _electTemplate.service + "] current election state ["
                        + getState() + "] - validation requires [" + State.ACTIVE + "]");
            }
            throw new CancellationException(); //cancel this task
        }

        /**
         * Extra-Backup exit point #1
         * --------------------------
         * In the case of Split-brain where each primary is also connected to a backup, the target member names gets switched.
         * on location A: [P1 connected to B1.1] and on location B: [P1.1 connected to B1]; now B1.1 receives added event of P1.1
         * and of course B1.1 (by name) will never be in it's replication targets. Same goes for B1 receiving an event of P1.
         */
        if (targetMemberName.equals(String.valueOf(activeServiceItem.getService()))) {
            _logger.warning("backup Space instance [" + _electTemplate.service
                    + "] will be removed since it has the same name identifier as the primary Space instance ["
                    + activeServiceItem.service + "] on host [" + HostName.getHostNameFrom(activeServiceItem.attributeSets) + "]");

            _listener.onExtraBackup(new ActiveElectionEvent(activeServiceItem));
            throw new CancellationException(); //done, no need to re-schedule
        }


        //get all replication targets from discovered active member
        HashMap<String, OutgoingChannel> allOutgoingReplicationChannels = new HashMap<String, OutgoingChannel>();
        ServiceReplicationStatus serviceReplicationStatus = getServiceReplicationStatus(activeServiceItem, allOutgoingReplicationChannels);
        if (ServiceReplicationStatus.UNKNOWN.equals(serviceReplicationStatus)
                || ServiceReplicationStatus.UNREACHABLE_TARGET.equals(serviceReplicationStatus)) {
            if (_xbLogger.isLoggable(Level.FINE)) {
                _xbLogger.log(Level.FINE, "primary Space instance [" + activeServiceItem.service
                        + "] on host [" + HostName.getHostNameFrom(activeServiceItem.attributeSets) + "] is unreachable");
            }
            throw new CancellationException(); //cancel this task
        }

        //not in replication target, retry until replication established
        if (!serviceReplicationStatus.containsReplicationTarget(targetMemberName)) {
            if (_xbLogger.isLoggable(Level.FINEST)) {
                _xbLogger.log(Level.FINEST, "backup Space instance [" + _electTemplate.service
                        + "] is not a replication target of primary Space instance ["
                        + activeServiceItem.service + "] on host [" + HostName.getHostNameFrom(activeServiceItem.attributeSets) + "]");
            }
            return; //re-schedule until replication is established
        }

        //verify replication pre-conditions
        OutgoingChannel outgoingChannel = allOutgoingReplicationChannels.get(targetMemberName);
        if (!outgoingChannel.getReplicationMode().equals(ReplicationMode.BACKUP_SPACE)) {
            if (_xbLogger.isLoggable(Level.FINE)) {
                _xbLogger.log(Level.FINE, "backup Space instance [" + _electTemplate.service + "] current replication mode ["
                        + outgoingChannel.getReplicationMode() + "] - validation requires [ " + ReplicationMode.BACKUP_SPACE + "]");
            }
            throw new CancellationException(); //cancel this task
        }

        if (!outgoingChannel.getChannelState().equals(ReplicationStatistics.ChannelState.ACTIVE)) {
            if (_xbLogger.isLoggable(Level.FINEST)) {
                _xbLogger.log(Level.FINEST, "backup Space instance [" + _electTemplate.service + "] current replication channel state ["
                        + outgoingChannel.getChannelState() + "] - validation requires [" + ReplicationStatistics.ChannelState.ACTIVE + "]");
            }
            return; //re-schedule until active (after handshake)
        }

        //this backup target comparison details
        final String targetServiceUuid = String.valueOf(targetServiceID);
        final String outgoingChannelTargetUuid = String.valueOf(outgoingChannel.getTargetUuid());

        if (_xbLogger.isLoggable(Level.FINER)) {
            _xbLogger.log(Level.FINER, "backup Space instance [" + _electTemplate.service + "] discovered a primary Space instance: ["
                    + activeServiceItem.service + "] on host [" + HostName.getHostNameFrom(activeServiceItem.attributeSets)
                    + "] with an outgoing replication channel - outgoingChannelTargetUuid=[" + outgoingChannelTargetUuid
                    + "] compared to targetServiceUuid=[" + targetServiceUuid
                    + (_xbLogger.isLoggable(Level.FINEST) ? "] outgoingChannel=[" + outgoingChannel + "]" : "]"));
        }

        /**
         * Extra-Backup exit point #2
         * --------------------------
         * primary replication target has a different service UUID by the same name. This means that primary is connected to a backup with the same
         * name but a different service ID.
         */
        if (!(String.valueOf(targetServiceID).equals(String.valueOf(outgoingChannel.getTargetUuid())))) {
            _logger.warning("backup Space instance [" + _electTemplate.service + "] has been detected as an extra backup Space and will be removed");
            _listener.onExtraBackup(new ActiveElectionEvent(activeServiceItem));
        } else {
            if (_xbLogger.isLoggable(Level.FINE)) {
                _xbLogger.log(Level.FINE, "backup Space instance [" + _electTemplate.service + "] is a replication target of ["
                        + activeServiceItem.service + "] on host [" + HostName.getHostNameFrom(activeServiceItem.attributeSets) + "]");
            }
        }

        //done, no need to re-schedule
        throw new CancellationException(); //cancel this task
    }

    private String getResolutionReason(
            List<SplitBrainServiceEntry> splitBrainServiceEntries) {
        if (splitBrainServiceEntries.isEmpty())
            return "Unknown";

        if (splitBrainServiceEntries.size() == 1)
            return "Only one primary space detected";

        SplitBrainServiceEntry chosenService = splitBrainServiceEntries.get(0);
        SplitBrainServiceEntry notChosenService = splitBrainServiceEntries.get(1);

        if (chosenService.getReplicationStatus().getPriority() < notChosenService.getReplicationStatus().getPriority())
            return "The space instance has the most consistent replication status";

        return "All spaces matched the split brain resolution criteria. Chosen arbitrary";
    }


    /**
     * Create split brain entries with all the details necessary to select the right primary
     */
    private List<SplitBrainServiceEntry> getSplitBrainServices(
            List<ServiceItem> activeServices) {
        List<SplitBrainServiceEntry> entries = new LinkedList<SplitBrainServiceEntry>();
        final Map<String, OutgoingChannel> allOutgoingReplicationChannels = new HashMap<String, OutgoingChannel>();
        for (ServiceItem activeService : activeServices) {
            ServiceReplicationStatus replicationStatus = getServiceReplicationStatus(activeService, allOutgoingReplicationChannels);
            ICandidateEntry electionPriorityEntry = getElectionFilter().getCandidateEntry(activeService);

            SplitBrainServiceEntry entry = new SplitBrainServiceEntry(activeService, replicationStatus, electionPriorityEntry);
            entries.add(entry);
        }
        // Reduce priority if there are missing channels
        for (SplitBrainServiceEntry entry : entries) {
            ServiceReplicationStatus replicationStatus = entry.getReplicationStatus();
            String serviceName = entry.getService().getService().toString();
            for (Map.Entry<String, OutgoingChannel> channelEntry : allOutgoingReplicationChannels.entrySet()) {
                if (!serviceName.equals(channelEntry.getKey())
                        && !replicationStatus.containsReplicationTarget(channelEntry.getKey()))
                    reduceReplicationStatusPriority(replicationStatus, channelEntry.getValue());
            }
        }
        return entries;
    }

    /**
     * @return can return {@link ServiceReplicationStatus#UNKNOWN} or {@link
     * ServiceReplicationStatus#UNREACHABLE_TARGET}
     */
    private ServiceReplicationStatus getServiceReplicationStatus(ServiceItem serviceItem, Map<String, OutgoingChannel> allOutgoingReplicationChannels) {
        Object service = serviceItem.getService();
        if (!(service instanceof ISpaceProxy))
            return ServiceReplicationStatus.UNKNOWN;

        ServiceReplicationStatus serviceReplicationStatus = calculateServiceReplicationStatus((ISpaceProxy) service, serviceItem, allOutgoingReplicationChannels);
        if (_logger.isLoggable(Level.FINER))
            _logger.finer("calculated replication status for " + serviceItem + " is " + serviceReplicationStatus);
        return serviceReplicationStatus;
    }

    private ServiceReplicationStatus calculateServiceReplicationStatus(ISpaceProxy serviceProxy, ServiceItem serviceItem, Map<String, OutgoingChannel> allOutgoingReplicationChannels) {
        boolean hasDisconnected;
        int iteration = 0;
        long INCOMPLETE_ITERATION_SLEEP_TIME = 1000;
        long MAX_TRIES = (_config.getResolutionTimeout() / INCOMPLETE_ITERATION_SLEEP_TIME) + 1;
        ServiceReplicationStatus serviceReplicationStatus;

        do {
            iteration++;
            hasDisconnected = false;
            serviceReplicationStatus = new ServiceReplicationStatus();
            try {
                final IInternalRemoteJSpaceAdmin admin = (IInternalRemoteJSpaceAdmin) (serviceProxy).getAdmin();
                final ReplicationStatistics replicationStatistics = ((StatisticsAdmin) admin).getHolder().getReplicationStatistics();
                serviceReplicationStatus.setProcessId(admin.getJVMDetails().getPid());
                if (_logger.isLoggable(Level.FINER))
                    _logger.finer("calculating election priority for " + serviceItem + " try " + iteration + " out of " + MAX_TRIES);

                if (replicationStatistics == null || replicationStatistics.getOutgoingReplication() == null || replicationStatistics.getOutgoingReplication().getChannels() == null) {
                    if (_logger.isLoggable(Level.WARNING))
                        _logger.warning("Failed to retrieve replicationStatistics for " + serviceItem);

                    return ServiceReplicationStatus.UNKNOWN;
                }
                for (OutgoingChannel outgoingChannel : replicationStatistics.getOutgoingReplication().getChannels()) {
                    allOutgoingReplicationChannels.put(outgoingChannel.getTargetMemberName(), outgoingChannel);
                    serviceReplicationStatus.addReplicationTargetName(outgoingChannel.getTargetMemberName());

                    if (outgoingChannel.isInconsistent()) {
                        reduceReplicationStatusPriority(serviceReplicationStatus, outgoingChannel);
                    }
                    //TODO currently connected state is just stating if the underlying connection is connected
                    //the channel can be in active due to incomplete handshake
                    else //noinspection deprecation
                        if (outgoingChannel.getState() != com.gigaspaces.cluster.replication.IReplicationChannel.State.CONNECTED) {
                            String description = "Service replication with target [" + outgoingChannel.getTargetMemberName() + "] is inactive";
                            if (_logger.isLoggable(Level.FINER))
                                _logger.finer(description + ", reducing priority by " + ServiceReplicationStatus.INACTIVE_FACTOR);
                            //A disconnected channel
                            serviceReplicationStatus.reducePriority(ServiceReplicationStatus.INACTIVE_FACTOR, description);
                            hasDisconnected = true;
                        }
                }

                if (hasDisconnected && iteration < MAX_TRIES) {
                    try {
                        if (_logger.isLoggable(Level.FINER))
                            _logger.finer("there are disconnected replication targets, sleeping for " + INCOMPLETE_ITERATION_SLEEP_TIME + " and retrying");
                        Thread.sleep(INCOMPLETE_ITERATION_SLEEP_TIME);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return ServiceReplicationStatus.UNKNOWN;
                    }
                }
            } catch (RemoteException e) {
                if (iteration < MAX_TRIES) {
                    try {
                        if (_logger.isLoggable(Level.FINER))
                            _logger.finer("target is unreachable, sleeping for " + INCOMPLETE_ITERATION_SLEEP_TIME + " and retrying");
                        Thread.sleep(INCOMPLETE_ITERATION_SLEEP_TIME);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return ServiceReplicationStatus.UNKNOWN;
                    }
                } else {
                    if (_logger.isLoggable(Level.FINER))
                        _logger.log(Level.FINER, "Got exception while calculating replication priority", e);

                    return ServiceReplicationStatus.UNREACHABLE_TARGET;
                }
            }
        } while (hasDisconnected && iteration < MAX_TRIES);
        return serviceReplicationStatus;
    }

    private void reduceReplicationStatusPriority(ServiceReplicationStatus serviceReplicationStatus, OutgoingChannel outgoingChannel) {
        //We give lowest priority to inconsistent mirror
        if (outgoingChannel.getReplicationMode() == ReplicationMode.MIRROR || outgoingChannel.getReplicationMode() == ReplicationMode.GATEWAY) {
            String targetType = outgoingChannel.getReplicationMode() == ReplicationMode.MIRROR ? "mirror" : "gateway";
            String description = "Service is inconsistent with a " + targetType + " target [" + outgoingChannel.getTargetMemberName() + "] (gives highest inconsistency state)";
            if (_logger.isLoggable(Level.FINER)) {
                _logger.finer(description + ", reducing priority by " + ServiceReplicationStatus.INCONSISTENT_WITH_MIRROR_FACTOR);
            }
            serviceReplicationStatus.reducePriority(ServiceReplicationStatus.INCONSISTENT_WITH_MIRROR_FACTOR, description);
        } else {
            String description = "Service is inconsistent with target [" + outgoingChannel.getTargetMemberName() + "]";
            if (_logger.isLoggable(Level.FINER)) {
                _logger.finer(description + ", reducing priority by " + ServiceReplicationStatus.INCONSISTENT_WITH_SPACE_FACTOR);
            }
            //Other in consistent target reduce priority by this rate
            serviceReplicationStatus.reducePriority(ServiceReplicationStatus.INCONSISTENT_WITH_SPACE_FACTOR, description);
        }
    }


    /**
     * The callback method from {@link com.gigaspaces.cluster.activeelection.core.ActiveFailureDetector}
     * on active failure.
     *
     * @throws com.gigaspaces.cluster.activeelection.core.ActiveElectionException Failed to elect
     *                                                                            new Active.
     * @throws InterruptedException                                               operation was
     *                                                                            interrupted.
     **/
    protected void onActiveFailure() throws InterruptedException, ActiveElectionException {

        /* elect new active */
        elect();
    }


    /**
     * Start the split-brain controller which controls the collision of 2 or more ACTIVE services.
     **/
    protected void startSplitBrainController() throws RemoteException {
        /* make sure the older one is closed */
        if (_splitBrainController != null)
            return;

        ElectionEntry splitBrainTemplate = _electTemplate.clone();

        /* control the collision only on ACTIVE services */
        splitBrainTemplate.setState(State.ACTIVE);

        _splitBrainController = new SplitBrainController(splitBrainTemplate, this);
    }


    /**
     * Terminate the ActiveElectionManager.
     **/
    public synchronized void terminate() {
        if (_isTerminated)
            return;

        if (_logger.isLoggable(Level.FINE))
            _logger.fine("Terminating...");

        try {
            changeState(null /* any state */, State.NONE, false /* force change */);
        } catch (RemoteException ex) {
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("Failed to reset current state on " + getNamingService().getName());
        }

        if (_splitBrainController != null)
            _splitBrainController.terminate();

        if (_activeFailureDetector != null)
            _activeFailureDetector.terminate();

        _isTerminated = true;

        _logger.fine("Terminated");
    }// terminate

    /**
     * @return the current state.
     **/
    synchronized public State getState() {
        return _currentState;
    }

    /**
     * @return active election configuration.
     */
    public ActiveElectionConfig getConfig() {
        return _config;
    }

    /**
     * @return the instance of naming service this ActiveElectionManager working with.
     */
    INamingService getNamingService() {
        return _namingService;
    }

    /**
     * @return the election decision filter
     */
    IActiveElectionDecisionFilter getElectionFilter() {
        return _decisionFilter;
    }

    /**
     * Set current state.
     *
     * @param currentState the current state.
     **/
    synchronized private void setCurrentState(State currentState) {
        _currentState = currentState;
    }

    /**
     * @return <code>true</code> if the manager should terminate, otherwise <code>false</code>.
     **/
    protected synchronized boolean isTerminate() {
        return _isTerminated;
    }

    @Override
    public String toString() {
        return "Space instance [" + _electTemplate.service + "] ";
    }


    /**
     * Re-elect the primary space. The elector state is reset and election is performed
     */
    public void reelect() throws InterruptedException, ActiveElectionException, RemoteException {
        changeState(getState(), State.PENDING, true);

        elect();
    }


    public ClusterFailureDetector getClusterFailureDetector() {
        return _clusterFailureDetector;
    }


    SpaceMode getSpaceMode() {
        return ((LusBasedSelectorHandler) _listener).getSpaceMode();
    }
}