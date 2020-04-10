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

/*
 * @(#)JSpaceProxyWrapper.java 1.0   01/25/2001  17:15PM
 */

package com.j_spaces.core.client;

import com.j_spaces.core.IJSpaceContainer;
import com.j_spaces.kernel.JSpaceUtilities;

import java.io.Serializable;
import java.rmi.Remote;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class JSpaceContainerProxyWrapper implements Remote, Serializable {
    private static final long serialVersionUID = 1L;

    private Object proxyObject;
    private String proxyName;
    private String proxyPresentName;
    private boolean isLoginProcessed;

    final private static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACEBROWSER);

    public JSpaceContainerProxyWrapper() {
    }

    /**
     * Constructor of JSpaceProxyWrapper
     */
    public JSpaceContainerProxyWrapper(IJSpaceContainer spacecContainerProxy, String containerName, String hostName) {
        this.proxyObject = spacecContainerProxy;

        if (_logger.isTraceEnabled()) {
            _logger.trace("%%% Debug Stack trace:" +
                    JSpaceUtilities.getStackTrace(new Exception("--DEBUG--")));
        }

        proxyName = containerName;
        proxyPresentName = JSpaceUtilities.createContainerPresentName(hostName, containerName);
    }

    public Object proxy() {
        return proxyObject;
    }


    @Override
    public String toString() {
        return proxyPresentName;
    }

    public String getProxyName() {
        return proxyName;
    }

    /**
     * Proxies for servers with the same uuid have the same hash code.
     */
    @Override
    public int hashCode() {
        return proxyObject.hashCode();
    }

    /**
     * Proxies for servers with the same Uuid are considered equal.
     */
    @Override
    public boolean equals(Object o) {
        return toString().equals(o.toString());
    }
}
