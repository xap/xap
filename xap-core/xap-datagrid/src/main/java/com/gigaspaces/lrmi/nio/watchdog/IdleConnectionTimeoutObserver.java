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

package com.gigaspaces.lrmi.nio.watchdog;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ConnectionResource;
import com.gigaspaces.lrmi.nio.watchdog.Watchdog.ClientWatchedObject;
import com.gigaspaces.lrmi.nio.watchdog.Watchdog.WatchedObject;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * IdleConnectionTimeoutObserver monitors idle connections.
 *
 * @author anna
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class IdleConnectionTimeoutObserver
        implements TimeoutObserver {
    final static private Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_WATCHDOG);

    // Number of retries to reach the server before closing connection
    final private int _retries;

    /**
     *
     * @param retries
     */
    public IdleConnectionTimeoutObserver(int retries) {
        _retries = retries;
    }

    /**
     * {@inheritDoc}
     */
    public void timeoutOccured(Collection<WatchedObject> bucket) throws Exception {

        for (WatchedObject watchedObject : bucket) {
            ClientWatchedObject clientWatched = (ClientWatchedObject) watchedObject;

            // Get control over CPeer
            ConnectionResource cPeer = clientWatched.getClient();

            // First check if this connection is used by another thread
            if (!cPeer.acquire()) //if was unable to acquire, then is use
            {
                // If CPeer is in use - connection is not idle - abort
                clientWatched._retries = 0;
                clientWatched.startWatch();
                return;
            }

            //else - we have acquired the resource and later release it.
            try {
                // Check if this CPeer has reached its allowed number of retries
                // or heartbeat message failed
                // otherwise - timeout is ignored, retries number is updated
                if (clientWatched._retries < _retries && cPeer.sendKeepAlive()) {
                    clientWatched._retries++;

                    // Restart client watch
                    clientWatched.startWatch();

                } else {
                    if (_logger.isTraceEnabled()) {
                        _logger.trace("Closing idle connection to " + cPeer.getConnectionURL());
                    }

                    // Connection is idle - close it
                    clientWatched.stopWatch();

                    cPeer.disconnect();
                }
            } finally {
                cPeer.setAcquired(false);
            }
        }
    }
}
