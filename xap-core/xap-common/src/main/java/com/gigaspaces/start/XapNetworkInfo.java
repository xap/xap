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
package com.gigaspaces.start;

import org.jini.rio.boot.BootUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class XapNetworkInfo {
    private final String localHostName;
    private final String localHostCanonicalName;
    private final String hostId;
    private final InetAddress host;

    public XapNetworkInfo() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            this.localHostName = localHost.getHostName();
            this.localHostCanonicalName = localHost.getCanonicalHostName();
            this.hostId = BootUtil.getHostAddress();
            this.host = InetAddress.getByName(hostId);
        } catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to get network information", e);
        }
    }

    public String getLocalHostName() {
        return localHostName;
    }

    public String getLocalHostCanonicalName() {
        return localHostCanonicalName;
    }

    public String getHostId() {
        return hostId;
    }

    public InetAddress getHost() {
        return host;
    }
}
