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

package org.openspaces.pu.container.jee.jetty.holder;

import com.gigaspaces.internal.utils.SharedInstance;
import com.gigaspaces.internal.utils.Singletons;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;

/**
 * A shared jetty holder that keeps upon first construction will store a static jetty instance and
 * will reused it from then on. Upon the "last" call to stop, will actually stop the jetty
 * instance.
 *
 * @author kimchy
 */
public class SharedJettyHolder extends JettyHolder {

    private static final Log logger = LogFactory.getLog(SharedJettyHolder.class);
    private static final String SHARED_JETTY_KEY = "jetty.server";

    private final SharedInstance<Server> server;

    public SharedJettyHolder(Server localServer) {
        SharedInstance<Server> newServer = new SharedInstance<Server>(localServer);
        this.server = (SharedInstance<Server>) Singletons.putIfAbsent(SHARED_JETTY_KEY, newServer);
        if (server == newServer) {
            server.value().setStopAtShutdown(false);
            if (logger.isInfoEnabled()) {
                logger.info("Using new jetty server [" + server.value() + "]");
            }
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("Using existing jetty server [" + server.value() + "]");
            }
        }
    }

    public void start() throws Exception {
        if (server.increment() == 1) {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting jetty server [" + server + "]");
            }
            super.start();
        }
    }

    public void stop() throws Exception {
        if (server.decrement() == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Stopping jetty server [" + server + "]");
            }
            super.stop();
            Singletons.remove(SHARED_JETTY_KEY);
        }
    }

    public Server getServer() {
        return server.value();
    }

    public boolean isSingleInstance() {
        return true;
    }
}
