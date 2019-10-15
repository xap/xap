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

package com.j_spaces.start;

import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.logger.Constants;
import com.sun.jini.start.LifeCycle;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.logging.Level;
import java.util.logging.Logger;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class RMIRegistryServiceImpl {
    private static final String COMPONENT = "com.gigaspaces.start.rmi";
    private int registryPort;

    //logger
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_ADMIN);


    /**
     * Constructs a <code>RMIRegistryServiceImpl</code> instance.  This constructor is defined such
     * that it can be called from the {@link com.sun.jini.start.NonActivatableServiceDescriptor#create
     * NonActivatableServiceDescriptor.create} method which allows RMIRegistry to be started from
     * the {@link com.sun.jini.start.ServiceStarter}
     */
    public RMIRegistryServiceImpl(String[] configArgs, LifeCycle lifeCycle)
            throws IOException, ConfigurationException, RemoteException {
        final Configuration config = ConfigurationProvider.getInstance(
                configArgs, getClass().getClassLoader());

        registryPort = ((Integer) config.getEntry(COMPONENT, "registryPort", int.class,
                new Integer(10098))).intValue();

        LocateRegistry.createRegistry(registryPort);
        if (_logger.isLoggable(Level.INFO)) {
            _logger.info("RMIRegistry started [port " + registryPort + "]");
        }
    }
}
