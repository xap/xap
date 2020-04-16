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

import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.j_spaces.core.cluster.startup.FaultDetectionListener;

import net.jini.core.lookup.ServiceItem;

import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a failure detection on active service. On active failure elect the new active. On elect
 * the ActiveFailureDetector terminates all functionality.<br>
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see com.gigaspaces.cluster.activeelection.core.ActiveElectionManager
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class ActiveFailureDetector extends GSThread implements FaultDetectionListener {
    final private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLUSTER_ACTIVE_ELECTION);

    final private CountDownLatch _onInitialize;
    final private ActiveElectionManager _electionManager;

    /**
     * <code>true</code> if the detector should be terminated
     */
    private boolean _isTerminated;

    ActiveFailureDetector(ActiveElectionManager electionManager, ServiceItem monitorService)
            throws RemoteException {
        super("ActiveFailureDetector");
        setDaemon(true);

        _electionManager = electionManager;

        _onInitialize = new CountDownLatch(1);

        start();

        try { // wait while this thread will be ready in run()
            _onInitialize.await();
        } catch (InterruptedException ie) {
            if (_logger.isTraceEnabled()) {
                _logger.trace(Thread.currentThread().getName() + " interrupted", ie);
            }

            //Restore the interrupted status
            Thread.currentThread().interrupt();

            throw new RemoteException(Thread.currentThread().getName() + " interrupted", ie);
        }

        Object service = monitorService.getService();
        try {
            _electionManager.getClusterFailureDetector().register(this, monitorService.serviceID, service);

        } catch (Exception e) {
            terminate();

            String msg = "Failed to register service: "
                    + service.getClass() + " with fault detection manager";

            if (_logger.isDebugEnabled())
                _logger.debug(msg, e);

            throw new RemoteException(msg, e);
        }
    }// <init>


    /**
     * @see com.j_spaces.core.cluster.startup.FaultDetectionListener#serviceFailure(Object, Object)
     */
    @Override
    synchronized public void serviceFailure(Object service, Object serviceID) {
        if (_logger.isDebugEnabled())
            _logger.debug(toString() + " active election failure detector identified failure: [" + service
                    + "] serviceID: [" + serviceID + "]. Started to elect new ACTIVE service");

        notify();
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                if (isTerminate())
                    return;

				/* init finished, release constructor */
                _onInitialize.countDown();
                wait();
            }// sync

            terminate();

            if (_electionManager.isSpaceInRecovery()) {
                _logger.info("Detected primary Space failure during recovery");
                return; //don't elect, let recovery process complete and decide if election is needed
            }

            boolean wasErrorMessageLogged = false;
            while (true) {
                try {
                    _electionManager.onActiveFailure();
                    break;
                } catch (ActiveElectionException e) {
                    if (wasErrorMessageLogged)
                        continue;

                    if (_logger.isWarnEnabled()) {
                        _logger.warn(toString() + " could not participate in active election. " +
                                "Election process will continue once the Lookup Service is reconnected", e);
                    }
                    wasErrorMessageLogged = true;
                }
                // space needs to go down, it can't be elected as primary, see DirectPersistencyRecoveryHelper#beforeSpaceModeChange
                catch (DirectPersistencyRecoveryException ex) {
                    if (_logger.isErrorEnabled()) {
                        _logger.error(toString() + " Direct persistency recovery failure", ex);
                    }
                    break;
                }
            }
        } catch (InterruptedException ex) {
            //Restore the interrupted status
            Thread.currentThread().interrupt();
            //fall through
        } finally {
            terminate();
        }
    }// run()


    /**
     * @return if failure detector should be terminated
     */
    public synchronized boolean isTerminate() {
        return _isTerminated;
    }

    /**
     * Terminate the ActiveFailureDetector.
     **/
    synchronized public void terminate() {
        /* is already terminated */
        if (_isTerminated)
            return;

        _isTerminated = true;

		/* only if outsider thread */
        if (Thread.currentThread() != this)
            interrupt();

        if (_logger.isDebugEnabled())
            _logger.debug(toString() + " active failure detector has terminated");
    }

    @Override
    public String toString() {
        return _electionManager.toString();
    }
}
